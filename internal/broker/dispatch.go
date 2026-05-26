package broker

import (
	"github.com/octarhq/octar/internal/protocol"
	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/storage"
)

func (b *Broker) startDispatchWorkers() {
	b.dispatchOnce.Do(func() {
		for i := 0; i < len(b.dispatchChs); i++ {
			b.dispatchWG.Add(1)
			go func(ch chan *queue.Message) {
				defer b.dispatchWG.Done()
				defer func() {
					if r := recover(); r != nil {
						b.logger.Error("dispatch worker panicked", "panic", r)
					}
				}()
				for {
					select {
					case msg := <-ch:
						b.dispatch(msg)
					case <-b.dispatchStop:
						return
					}
				}
			}(b.dispatchChs[i])
		}
	})
}

func (b *Broker) enqueueDispatch(msg *queue.Message) bool {
	idx := queue.HashKey(msg.GroupKey) % uint32(len(b.dispatchChs))
	ch := b.dispatchChs[idx]

	select {
	case ch <- msg:
		return true
	case <-b.dispatchStop:
		return false
	default:
		return false
	}
}

func (b *Broker) dispatch(msg *queue.Message) bool {
	conn := b.registry.next(msg.Namespace, msg.QueueName, msg.GroupKey)
	if conn == nil {
		b.logger.Debug("dispatch blocked: no subscriber",
			"msgID", msg.ID,
			"queue", msg.QueueName,
			"group", msg.GroupKey,
		)
		q := b.Scheduler.GetQueue(msg.Namespace, msg.QueueName)
		if q != nil {
			q.ReturnToPending(msg.GroupKey, msg.ID)
		}
		return false
	}

	if !b.quota.TryAcquire() {
		b.logger.Warn("dispatch global quota exceeded, returning message to pending",
			"namespace", msg.Namespace,
			"queue", msg.QueueName,
			"group", msg.GroupKey,
			"msgID", msg.ID,
		)
		_ = conn.WriteBackpressure(protocol.BackpressureFrame{
			Reason:     "broker global inflight quota exceeded",
			RetryAfter: 1000,
		})
		q := b.Scheduler.GetQueue(msg.Namespace, msg.QueueName)
		if q != nil {
			q.ReturnToPending(msg.GroupKey, msg.ID)
		}
		return false
	}

	if !conn.AcquireCredit() {
		b.quota.Release()
		b.logger.Debug("dispatch blocked: connection credit exhausted",
			"msgID", msg.ID,
			"queue", msg.QueueName,
			"group", msg.GroupKey,
		)
		_ = conn.WriteBackpressure(protocol.BackpressureFrame{
			Reason:     "per-connection inflight limit reached",
			RetryAfter: 500,
		})
		q := b.Scheduler.GetQueue(msg.Namespace, msg.QueueName)
		if q != nil {
			q.ReturnToPending(msg.GroupKey, msg.ID)
		}
		return false
	}

	// Snapshot Attempts before WriteMessage — once the frame is on the wire
	// the subscriber may NACK and group.fail() will mutate msg.Attempts
	// concurrently, causing a data race on the log line below.
	attempts := msg.Attempts + 1

	err := conn.WriteMessage(protocol.MessageFrame{
		MsgID:    msg.ID,
		Queue:    msg.QueueName,
		Group:    msg.GroupKey,
		Payload:  msg.Payload,
		Attempts: int32(attempts),
	})
	if err != nil {
		b.logger.Debug("failed to write message, returning to pending",
			"namespace", msg.Namespace,
			"queue", msg.QueueName,
			"group", msg.GroupKey,
			"msgID", msg.ID,
		)
		conn.RecordWriteFail()
		conn.ReleaseCredit()
		b.quota.Release()
		q := b.Scheduler.GetQueue(msg.Namespace, msg.QueueName)
		if q != nil {
			q.ReturnToPending(msg.GroupKey, msg.ID)
		}
		return false
	}

	conn.RecordDispatch(msg.ID)

	if err := b.WAL.Append(storage.Event{
		Type:      storage.EventLease,
		Namespace: msg.Namespace,
		Queue:     msg.QueueName,
		Group:     msg.GroupKey,
		MsgID:     msg.ID,
	}); err != nil {
		b.logger.Error("failed to append LEASE to WAL",
			"msgID", msg.ID,
			"queue", msg.QueueName,
			"group", msg.GroupKey,
			"err", err,
		)
	} else {
		b.logger.Debug("message dispatched",
			"msgID", msg.ID,
			"queue", msg.QueueName,
			"group", msg.GroupKey,
			"attempts", attempts,
		)
	}
	return true
}

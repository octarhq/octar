package broker

import (
	"github.com/octarhq/octar/internal/protocol"
	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/server"
	"github.com/octarhq/octar/internal/storage"
	"github.com/octarhq/octar/internal/xtime"
)

func (b *Broker) handleConnection(conn *server.Connection) {
	defer func() {
		leaked := int64(conn.Inflight())
		for i := int64(0); i < leaked; i++ {
			b.quota.Release()
		}
		b.registry.remove(conn)
	}()

	for {
		ft, frame, err := conn.ReadFrame()
		if err != nil {
			return
		}

		switch ft {
		case protocol.FramePublish:
			b.onPublish(conn, frame.(protocol.PublishFrame))
		case protocol.FrameSubscribe:
			b.onSubscribe(conn, frame.(protocol.SubscribeFrame))
		case protocol.FrameACK:
			b.onACK(conn, frame.(protocol.ACKFrame))
		case protocol.FrameNACK:
			b.onNACK(conn, frame.(protocol.NACKFrame))
		case protocol.FrameHeartbeat:
			_ = conn.WriteHeartbeat()
		}
	}
}

func (b *Broker) onPublish(conn *server.Connection, f protocol.PublishFrame) {
	b.logger.Debug("publish received",
		"user", conn.Session.Username,
		"namespace", conn.Session.Namespace,
		"queue", f.Queue,
		"group", f.Group,
		"payload_bytes", len(f.Payload),
	)

	q := b.Scheduler.GetQueue(conn.Session.Namespace, f.Queue)
	if q == nil {
		b.logger.Warn("publish rejected: queue not found",
			"namespace", conn.Session.Namespace,
			"queue", f.Queue,
		)
		_ = conn.WriteError(protocol.ErrorFrame{Code: 404, Message: "queue not found: " + f.Queue})
		return
	}

	if !b.IncGlobalMsgs() {
		b.logger.Warn("publish rejected: global message limit reached",
			"limit", b.Config.Server.GlobalMaxMsgs)
		_ = conn.WriteBackpressure(protocol.BackpressureFrame{
			Reason:     "global message limit reached",
			RetryAfter: 5,
		})
		return
	}
	willDec := true
	defer func() {
		if willDec {
			b.DecGlobalMsgs()
		}
	}()

	msgID := queue.GenerateID()

	if err := b.WAL.AppendSync(storage.Event{
		Type:      storage.EventPublish,
		Namespace: conn.Session.Namespace,
		Queue:     f.Queue,
		Group:     f.Group,
		MsgID:     msgID,
		Payload:   f.Payload,
	}); err != nil {
		b.logger.Error("failed to append PUBLISH to WAL", "msgID", msgID, "err", err)
		_ = conn.WriteError(protocol.ErrorFrame{Code: 500, Message: "internal durability error"})
		return
	}

	msg, err := q.PublishWithID(f.Group, msgID, f.Payload)
	if err != nil {
		b.logger.Error("failed to enqueue message after WAL write",
			"msgID", msgID, "queue", f.Queue, "group", f.Group, "err", err)
		_ = conn.WriteError(protocol.ErrorFrame{Code: 500, Message: err.Error()})
		return
	}

	willDec = false
	_ = conn.WritePublishOK(protocol.PublishOKFrame{MsgID: msg.ID})
	b.logger.Debug("publish acked to producer", "msgID", msg.ID, "queue", f.Queue, "group", f.Group)

	if next := q.TryDispatchOne(f.Group, xtime.Now()); next != nil {
		if !b.enqueueDispatch(next) {
			b.logger.Debug("immediate dispatch enqueue failed",
				"msgID", next.ID, "queue", next.QueueName, "group", next.GroupKey)
			q.ReturnToPending(f.Group, next.ID)
		}
	}
	if b.registry.has(conn.Session.Namespace, f.Queue, f.Group) {
		b.Scheduler.Activate(q, f.Group)
	}
}

func (b *Broker) onSubscribe(conn *server.Connection, f protocol.SubscribeFrame) {
	b.registry.add(conn.Session.Namespace, f.Queue, f.Group, conn)
	b.logger.Debug("consumer subscribed",
		"user", conn.Session.Username,
		"namespace", conn.Session.Namespace,
		"queue", f.Queue,
		"group", f.Group,
	)

	q := b.Scheduler.GetQueue(conn.Session.Namespace, f.Queue)
	if q == nil {
		return
	}

	if isGlobPattern(f.Group) {
		// Wildcard subscriber: activate every existing group in the queue so that
		// any messages already pending are dispatched immediately, rather than
		// sitting idle until the next publish arrives.
		//
		// PageGroupStats caps at 1 000 per call; loop until exhausted.
		cursor := ""
		const pageSize = 1000
		for {
			stats, next := q.PageGroupStats(cursor, pageSize)
			for _, gs := range stats {
				b.Scheduler.Activate(q, gs.Key)
			}
			if next == "" {
				break
			}
			cursor = next
		}
	} else {
		b.Scheduler.Activate(q, f.Group)
	}
}

func (b *Broker) onACK(conn *server.Connection, f protocol.ACKFrame) {
	conn.ReleaseCredit()
	b.quota.Release()
	conn.RecordACK(f.MsgID)
	b.logger.Debug("ack received", "msgID", f.MsgID, "queue", f.Queue, "group", f.Group)

	q := b.Scheduler.GetQueue(conn.Session.Namespace, f.Queue)
	if q == nil {
		b.logger.Warn("frame rejected: queue not found", "namespace", conn.Session.Namespace, "queue", f.Queue, "msgID", f.MsgID)
		return
	}

	if err := b.WAL.Append(storage.Event{
		Type:      storage.EventACK,
		Namespace: conn.Session.Namespace,
		Queue:     f.Queue,
		Group:     f.Group,
		MsgID:     f.MsgID,
	}); err != nil {
		b.logger.Error("failed to append ACK to WAL", "msgID", f.MsgID, "err", err)
		return
	}

	b.DecGlobalMsgs()

	if next := q.CompleteAndNext(f.Group, f.MsgID, xtime.Now()); next != nil {
		b.logger.Debug("dispatching next message after ack", "msgID", next.ID, "queue", next.QueueName, "group", next.GroupKey)
		b.enqueueDispatch(next)
	}
	b.Scheduler.Activate(q, f.Group)
}

func (b *Broker) onNACK(conn *server.Connection, f protocol.NACKFrame) {
	conn.ReleaseCredit()
	b.quota.Release()
	conn.RecordNACK(f.MsgID)
	b.logger.Debug("nack received", "msgID", f.MsgID, "queue", f.Queue, "group", f.Group, "reason", f.Reason)

	q := b.Scheduler.GetQueue(conn.Session.Namespace, f.Queue)
	if q == nil {
		b.logger.Warn("frame rejected: queue not found", "namespace", conn.Session.Namespace, "queue", f.Queue, "msgID", f.MsgID)
		return
	}

	if err := b.WAL.Append(storage.Event{
		Type:      storage.EventNACK,
		Namespace: conn.Session.Namespace,
		Queue:     f.Queue,
		Group:     f.Group,
		MsgID:     f.MsgID,
	}); err != nil {
		b.logger.Error("failed to append NACK to WAL", "msgID", f.MsgID, "err", err)
		return
	}

	dlqName, dlqMsg, next := q.FailAndNext(f.Group, f.MsgID, f.Reason, xtime.Now())
	if dlqMsg != nil {
		b.DecGlobalMsgs()
	}
	if next != nil {
		b.logger.Debug("dispatching retry after nack", "msgID", next.ID, "queue", next.QueueName, "group", next.GroupKey)
		b.enqueueDispatch(next)
	}
	b.Scheduler.Activate(q, f.Group)

	if dlqMsg != nil && dlqName != "" {
		dlq := b.Scheduler.GetQueue(conn.Session.Namespace, dlqName)
		if dlq != nil {
			if _, err := dlq.Publish(dlqMsg.GroupKey, dlqMsg.Payload); err == nil &&
				b.registry.has(conn.Session.Namespace, dlqName, dlqMsg.GroupKey) {
				b.Scheduler.Activate(dlq, dlqMsg.GroupKey)
			}
		}
	}
}

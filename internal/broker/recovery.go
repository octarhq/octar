package broker

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/octarhq/octar/internal/queue"
	stg "github.com/octarhq/octar/internal/storage"
	"github.com/octarhq/octar/internal/storage/recovery"
	"github.com/octarhq/octar/internal/storage/snapshot"
)

func (b *Broker) recoverQueues() error {
	if err := b.restoreFromDB(); err != nil {
		b.logger.Warn("db restore failed, continuing with WAL only", "error", err)
	}
	return b.replayWAL()
}

func (b *Broker) restoreFromDB() error {
	namespaces, err := b.DB.ListNamespaces()
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		queues, err := b.DB.ListQueues(ns.ID)
		if err != nil {
			b.logger.Warn("failed to list queues", "namespace", ns.Name, "error", err)
			continue
		}

		for _, dbQueue := range queues {
			q := queue.NewQueue(dbQueue.Name, ns.Name)

			groups, err := b.DB.ListGroups(dbQueue.ID)
			if err != nil {
				b.logger.Warn("failed to list groups", "queue", dbQueue.Name, "error", err)
			} else {
				for _, g := range groups {
					var cfg queue.GroupConfig
					if err := json.Unmarshal([]byte(g.Config), &cfg); err != nil {
						b.logger.Warn("corrupt group config, skipping",
							"queue", dbQueue.Name, "group", g.Key, "error", err)
						continue
					}
					cfg.Key = g.Key
					q.SetGroupConfig(cfg)
				}
			}

			b.registerQueueWithWAL(q)
			b.logger.Info("restored queue from db",
				"namespace", ns.Name,
				"queue", dbQueue.Name,
				"groups", len(groups),
			)
		}
	}
	return nil
}

func (b *Broker) replayWAL() error {
	if len(b.recoveryQueues) == 0 {
		return nil
	}

	replayer := recovery.NewReplay()

	for _, info := range b.recoveryQueues {
		q := b.Scheduler.GetQueue(info.Namespace, info.Queue)
		if q == nil {
			q = queue.NewQueue(info.Queue, info.Namespace)
			b.registerQueueWithWAL(q)
		}

		b.logger.Info("replaying WAL",
			"namespace", info.Namespace,
			"queue", info.Queue,
			"snapshot_seq", info.SnapshotSeq,
			"segment", info.SnapshotSegID,
		)

		if err := replayer.ReplayFromSnapshot(info, &queueRecoveryHandler{
			logger:    b.logger,
			queue:     q,
			namespace: info.Namespace,
			queueName: info.Queue,
		}); err != nil {
			b.logger.Warn("WAL replay failed", "queue", info.Queue, "error", err)
		}
	}
	return nil
}

func (b *Broker) registerQueueWithWAL(q *queue.Queue) {
	b.Scheduler.RegisterQueue(q)
	b.WAL.RegisterQueueState(q.Namespace, q.Name, func() interface{} {
		return q.ExportState()
	})
}

type queueRecoveryHandler struct {
	logger    *slog.Logger
	queue     *queue.Queue
	namespace string
	queueName string
}

func (h *queueRecoveryHandler) OnSnapshot(snap *snapshot.Snapshot) {
	h.logger.Info("loading snapshot", "groups", len(snap.Groups))

	now := time.Now()
	state := queue.QueueState{Groups: make(map[string]queue.GroupState)}
	for _, gs := range snap.Groups {
		groupState := queue.GroupState{
			Key:         gs.Key,
			Parallelism: int(gs.Parallelism),
			Quantum:     int(gs.Quantum),
		}
		for _, ms := range gs.Messages {
			msg := queue.NewMessageForRecovery(
				h.queueName,
				h.namespace,
				gs.Key,
				ms.Payload,
				time.Unix(0, ms.CreatedAt),
				ms.ID,
			)
			msg.State = queue.MessageState(ms.State)
			msg.Attempts = int(ms.Attempts)
			msg.ScheduledAt = time.Unix(0, ms.ScheduledAt)
			msg.LastError = ms.LastError

			switch msg.State {
			case queue.StateProcessing:
				if groupState.ProcessingMsgs == nil {
					groupState.ProcessingMsgs = make(map[string]*queue.Message)
				}
				groupState.ProcessingMsgs[msg.ID] = msg

			case queue.StatePending:
				if msg.ScheduledAt.After(now) {
					groupState.DelayedMsgs = append(groupState.DelayedMsgs, msg)
				} else {
					if groupState.ReadyMsgs == nil {
						groupState.ReadyMsgs = make([]*queue.Message, 0, len(gs.Messages))
					}
					groupState.ReadyMsgs = append(groupState.ReadyMsgs, msg)
				}
			}
		}
		if groupState.ReadyMsgs == nil {
			groupState.ReadyMsgs = make([]*queue.Message, 0)
		}
		state.Groups[gs.Key] = groupState
	}

	h.queue.ImportState(state)
	h.logger.Info("snapshot loaded",
		"groups", len(state.Groups),
	)
}

func (h *queueRecoveryHandler) OnPublish(event stg.Event) {
	h.queue.PublishWithID(event.Group, event.MsgID, event.Payload) //nolint:errcheck
}

func (h *queueRecoveryHandler) OnLease(event stg.Event) {
	h.queue.ReplayLease(event.Group, event.MsgID, time.Now())
}

func (h *queueRecoveryHandler) OnACK(event stg.Event) {
	if err := h.queue.Complete(event.Group, event.MsgID); err != nil {
		h.queue.RemoveMessage(event.Group, event.MsgID)
	}
}

func (h *queueRecoveryHandler) OnNACK(event stg.Event) {
	if _, _, err := h.queue.Fail(event.Group, event.MsgID, "replayed NACK"); err != nil {
		h.queue.RemoveMessage(event.Group, event.MsgID)
	}
}

func (h *queueRecoveryHandler) OnExpire(event stg.Event) {
	_ = h.queue.Complete(event.Group, event.MsgID)
}

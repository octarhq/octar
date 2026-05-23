package chaos

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/scheduler"
	stg "github.com/octarhq/octar/internal/storage"
	"github.com/octarhq/octar/internal/storage/recovery"
	"github.com/octarhq/octar/internal/storage/snapshot"
)

// Harness manages a broker lifecycle for chaos tests.
type Harness struct {
	t       *testing.T
	DataDir string
	WalDir  string
	cfg     *config.Config

	Wal       *stg.WAL
	Scheduler *scheduler.Scheduler
	Queues    map[string]*queue.Queue // key = "ns/name"
}

func New(t *testing.T) *Harness {
	t.Helper()

	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	cfg := &config.Config{
		Server: config.ServerConfig{
			GlobalMaxMsgs: 100000,
		},
		Storage: config.StorageConfig{
			DataDir: dataDir,
			WAL: config.WALConfig{
				FlushInterval:    50 * time.Millisecond,
				FlushMaxMessages: 100,
				SegmentMaxBytes:  64 << 20,
				Durable:          true,
			},
		},
	}

	wal, err := stg.NewWAL(walDir, stg.WALConfig{
		FlushInterval:    50 * time.Millisecond,
		FlushMaxMessages: 100,
		SegmentMaxBytes:  64 << 20,
		Durable:          true,
	})
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	sched := scheduler.NewScheduler()
	sched.Run(func(msg *queue.Message) bool {
		return true // no-op dispatch for chaos tests
	})

	return &Harness{
		t:         t,
		DataDir:   dataDir,
		WalDir:    walDir,
		Wal:       wal,
		Scheduler: sched,
		Queues:    make(map[string]*queue.Queue),
		cfg:       cfg,
	}
}

func (h *Harness) RegisterQueue(namespace, name string) *queue.Queue {
	q := queue.NewQueue(name, namespace)
	h.Scheduler.RegisterQueue(q)
	h.Wal.RegisterQueueState(namespace, name, func() interface{} {
		return q.ExportState()
	})
	h.Queues[namespace+"/"+name] = q
	return q
}

func (h *Harness) GetQueue(namespace, name string) *queue.Queue {
	return h.Queues[namespace+"/"+name]
}

func (h *Harness) GetQueueScheduled(namespace, name string) *queue.Queue {
	return h.Scheduler.GetQueue(namespace, name)
}

func (h *Harness) Publish(namespace, queueName, group string, payload []byte) (string, error) {
	q := h.GetQueue(namespace, queueName)
	if q == nil {
		return "", fmt.Errorf("queue not registered: %s/%s", namespace, queueName)
	}

	msgID := queue.GenerateID()
	if err := h.Wal.AppendSync(stg.Event{
		Type:      stg.EventPublish,
		Namespace: namespace,
		Queue:     queueName,
		Group:     group,
		MsgID:     msgID,
		Payload:   payload,
	}); err != nil {
		return "", fmt.Errorf("wal append: %w", err)
	}

	_, err := q.PublishWithID(group, msgID, payload)
	if err != nil {
		return "", fmt.Errorf("queue publish: %w", err)
	}

	h.Scheduler.Activate(q, group)
	return msgID, nil
}

func (h *Harness) Complete(namespace, queueName, group, msgID string) error {
	q := h.GetQueue(namespace, queueName)
	if q == nil {
		return fmt.Errorf("queue not registered: %s/%s", namespace, queueName)
	}
	if err := q.Complete(group, msgID); err != nil {
		return err
	}
	if err := h.Wal.AppendSync(stg.Event{
		Type:      stg.EventACK,
		Namespace: namespace,
		Queue:     queueName,
		Group:     group,
		MsgID:     msgID,
	}); err != nil {
		return fmt.Errorf("wal append ack: %w", err)
	}
	return nil
}

func (h *Harness) Crash() {
	h.Wal.Close()
	h.Scheduler.Stop()
	h.Queues = make(map[string]*queue.Queue)
}

func (h *Harness) Restart() {
	wal, err := stg.NewWAL(h.WalDir, stg.WALConfig{
		FlushInterval:    50 * time.Millisecond,
		FlushMaxMessages: 100,
		SegmentMaxBytes:  64 << 20,
		Durable:          true,
	})
	if err != nil {
		h.t.Fatalf("Restart NewWAL: %v", err)
	}
	h.Wal = wal

	sched := scheduler.NewScheduler()
	sched.Run(func(msg *queue.Message) bool { return true })
	h.Scheduler = sched

	infos, err := recovery.NewBootstrap(h.WalDir).DiscoverQueues()
	if err != nil {
		h.t.Logf("DiscoverQueues: %v", err)
	}

	replayer := recovery.NewReplay()
	for _, info := range infos {
		q := queue.NewQueue(info.Queue, info.Namespace)
		h.Scheduler.RegisterQueue(q)
		h.Wal.RegisterQueueState(info.Namespace, info.Queue, func() interface{} {
			return q.ExportState()
		})
		if err := replayer.ReplayFromSnapshot(info, &recoveryHandler{q: q}); err != nil {
			h.t.Logf("ReplayFromSnapshot for %s/%s: %v", info.Namespace, info.Queue, err)
		}
		h.Queues[info.Namespace+"/"+info.Queue] = q
		h.t.Logf("restored queue %s/%s: %d groups", info.Namespace, info.Queue, q.GroupCount())
	}
}

func (h *Harness) Close() {
	h.Wal.Close()
	h.Scheduler.Stop()
}

type recoveryHandler struct {
	q *queue.Queue
}

func (h *recoveryHandler) OnSnapshot(snap *snapshot.Snapshot) {
	state := queue.QueueState{Groups: make(map[string]queue.GroupState)}
	now := time.Now()
	for _, gs := range snap.Groups {
		gs2 := queue.GroupState{Key: gs.Key, Parallelism: int(gs.Parallelism), Quantum: int(gs.Quantum)}
		for _, ms := range gs.Messages {
			msg := queue.NewMessageForRecovery(h.q.Name, h.q.Namespace, gs.Key, ms.Payload, time.Unix(0, ms.CreatedAt), ms.ID)
			msg.State = queue.MessageState(ms.State)
			msg.Attempts = int(ms.Attempts)
			msg.ScheduledAt = time.Unix(0, ms.ScheduledAt)
			msg.LastError = ms.LastError
			switch msg.State {
			case queue.StateProcessing:
				if gs2.ProcessingMsgs == nil {
					gs2.ProcessingMsgs = make(map[string]*queue.Message)
				}
				gs2.ProcessingMsgs[msg.ID] = msg
			case queue.StatePending:
				if msg.ScheduledAt.After(now) {
					gs2.DelayedMsgs = append(gs2.DelayedMsgs, msg)
				} else {
					gs2.ReadyMsgs = append(gs2.ReadyMsgs, msg)
				}
			}
		}
		state.Groups[gs.Key] = gs2
	}
	h.q.ImportState(state)
}

func (h *recoveryHandler) OnPublish(event stg.Event) {
	_, _ = h.q.PublishWithID(event.Group, event.MsgID, event.Payload)
}

func (h *recoveryHandler) OnLease(event stg.Event) {
	h.q.ReplayLease(event.Group, event.MsgID, time.Now())
}

func (h *recoveryHandler) OnACK(event stg.Event) {
	if err := h.q.Complete(event.Group, event.MsgID); err != nil {
		h.q.RemoveMessage(event.Group, event.MsgID)
	}
}

func (h *recoveryHandler) OnNACK(event stg.Event) {
	if _, _, err := h.q.Fail(event.Group, event.MsgID, "replayed NACK"); err != nil {
		h.q.RemoveMessage(event.Group, event.MsgID)
	}
}

func (h *recoveryHandler) OnExpire(event stg.Event) {
	_ = h.q.Complete(event.Group, event.MsgID)
}

var _ = slog.Default()

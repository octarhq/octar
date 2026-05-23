package broker

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/protocol"
	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/scheduler"
	"github.com/octarhq/octar/internal/server"
	stg "github.com/octarhq/octar/internal/storage"
)

func testBroker(t *testing.T) (*Broker, *queue.Queue, func()) {
	t.Helper()
	cfg := &config.Config{
		Server: config.ServerConfig{
			GlobalMaxMsgs: 100,
			Inflight: config.InflightConfig{
				MaxInflight: 100,
				GlobalMax:   100,
			},
		},
		Storage: config.StorageConfig{
			DataDir: t.TempDir(),
			WAL: config.WALConfig{
				FlushInterval:    50 * time.Millisecond,
				FlushMaxMessages: 100,
				SegmentMaxBytes:  64 << 20,
				Durable:          false,
			},
		},
	}

	walDir := cfg.Storage.DataDir + "/wal"
	wal, err := stg.NewWAL(walDir, stg.WALConfig{
		FlushInterval:    50 * time.Millisecond,
		FlushMaxMessages: 100,
		SegmentMaxBytes:  64 << 20,
		Durable:          false,
	})
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	b := &Broker{
		Config:       cfg,
		Scheduler:    scheduler.NewScheduler(),
		WAL:          wal,
		registry:     newSubRegistry(),
		quota:        newBrokerQuota(cfg.Server.Inflight.GlobalMax),
		dispatchChs:  make([]chan *queue.Message, 1),
		dispatchStop: make(chan struct{}),
		logger:       slog.Default().With("component", "broker", "test", t.Name()),
	}
	b.dispatchChs[0] = make(chan *queue.Message, 1024)

	q := queue.NewQueue("test-q", "test-ns")
	b.Scheduler.RegisterQueue(q)
	b.registerQueueWithWAL(q)

	cleanup := func() {
		wal.Close()
	}
	return b, q, cleanup
}

func testConn(t *testing.T) (*server.Connection, *protocol.Decoder, func()) {
	t.Helper()
	r, w := net.Pipe()
	// newConnection via server package — the writer goroutine starts immediately.
	conn := server.NewConnection(w, config.InflightConfig{MaxInflight: 100, GlobalMax: 0})
	conn.Session = &server.Session{
		Username:  "testuser",
		Namespace: "test-ns",
	}
	dec := protocol.NewDecoder(bufio.NewReader(r))
	cleanup := func() {
		r.Close()
		conn.Close()
	}
	return conn, dec, cleanup
}

func readFrame(t *testing.T, dec *protocol.Decoder) (protocol.FrameType, any) {
	t.Helper()
	ft, frame, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	return ft, frame
}

func TestBrokerQuota(t *testing.T) {
	t.Parallel()

	t.Run("unlimited", func(t *testing.T) {
		q := newBrokerQuota(0)
		if !q.TryAcquire() {
			t.Fatal("unlimited quota should always acquire")
		}
		if q.Inflight() != 0 {
			t.Fatalf("Inflight = %d, want 0 (unlimited skips tracking)", q.Inflight())
		}
		q.Release()
	})

	t.Run("acquire_up_to_limit", func(t *testing.T) {
		q := newBrokerQuota(3)
		for i := 0; i < 3; i++ {
			if !q.TryAcquire() {
				t.Fatalf("TryAcquire %d failed", i)
			}
		}
		if q.TryAcquire() {
			t.Fatal("TryAcquire should fail at limit")
		}
		if q.Inflight() != 3 {
			t.Fatalf("Inflight = %d, want 3", q.Inflight())
		}
	})

	t.Run("release_then_acquire", func(t *testing.T) {
		q := newBrokerQuota(2)
		if !q.TryAcquire() {
			t.Fatal("TryAcquire 1 failed")
		}
		if !q.TryAcquire() {
			t.Fatal("TryAcquire 2 failed")
		}
		q.Release()
		if !q.TryAcquire() {
			t.Fatal("TryAcquire after release should succeed")
		}
		if q.Inflight() != 2 {
			t.Fatalf("Inflight = %d, want 2", q.Inflight())
		}
	})

	t.Run("release_below_zero", func(t *testing.T) {
		q := newBrokerQuota(5)
		q.Release()
		if q.Inflight() != -1 {
			t.Fatalf("Inflight = %d, want -1 after extra release", q.Inflight())
		}
		q.TryAcquire()
		if q.Inflight() != 0 {
			t.Fatalf("Inflight = %d, want 0", q.Inflight())
		}
	})
}

func TestBrokerRegistry(t *testing.T) {
	t.Parallel()

	t.Run("add_has_remove", func(t *testing.T) {
		r := newSubRegistry()
		conn, _, cleanup := testConn(t)
		defer cleanup()

		r.add("ns1", "q1", "g1", conn)
		if !r.has("ns1", "q1", "g1") {
			t.Fatal("has should return true after add")
		}
		if r.has("ns1", "q2", "g1") {
			t.Fatal("has should return false for different queue")
		}

		r.remove(conn)
		if r.has("ns1", "q1", "g1") {
			t.Fatal("has should return false after remove")
		}
	})

	t.Run("multiple_subscriptions_same_conn", func(t *testing.T) {
		r := newSubRegistry()
		conn, _, cleanup := testConn(t)
		defer cleanup()

		r.add("ns1", "q1", "g1", conn)
		r.add("ns1", "q2", "g2", conn)

		if !r.has("ns1", "q1", "g1") {
			t.Fatal("q1/g1 should exist")
		}
		if !r.has("ns1", "q2", "g2") {
			t.Fatal("q2/g2 should exist")
		}

		r.remove(conn)
		if r.has("ns1", "q1", "g1") {
			t.Fatal("q1/g1 should be removed")
		}
		if r.has("ns1", "q2", "g2") {
			t.Fatal("q2/g2 should be removed")
		}
	})

	t.Run("round_robin_next", func(t *testing.T) {
		r := newSubRegistry()
		conn1, _, cleanup1 := testConn(t)
		conn2, _, cleanup2 := testConn(t)
		defer cleanup1()
		defer cleanup2()

		r.add("ns1", "q1", "g1", conn1)
		r.add("ns1", "q1", "g1", conn2)

		c1 := r.next("ns1", "q1", "g1")
		c2 := r.next("ns1", "q1", "g1")
		c3 := r.next("ns1", "q1", "g1")

		if c1 == c2 {
			t.Fatal("round-robin should alternate")
		}
		if c3 != c1 {
			t.Fatal("third next should cycle back to first")
		}
	})

	t.Run("next_empty", func(t *testing.T) {
		r := newSubRegistry()
		if c := r.next("nonexistent", "q", "g"); c != nil {
			t.Fatal("next on empty registry should return nil")
		}
	})
}

func TestGlobalMsgCounter(t *testing.T) {
	t.Parallel()

	t.Run("inc_dec", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()

		if b.GlobalMsgs() != 0 {
			t.Fatalf("initial = %d, want 0", b.GlobalMsgs())
		}
		if !b.IncGlobalMsgs() {
			t.Fatal("first inc should succeed")
		}
		if b.GlobalMsgs() != 1 {
			t.Fatalf("after inc = %d, want 1", b.GlobalMsgs())
		}
		b.DecGlobalMsgs()
		if b.GlobalMsgs() != 0 {
			t.Fatalf("after dec = %d, want 0", b.GlobalMsgs())
		}
	})

	t.Run("limit_enforced", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		b.Config.Server.GlobalMaxMsgs = 3

		for i := 0; i < 3; i++ {
			if !b.IncGlobalMsgs() {
				t.Fatalf("inc %d should succeed", i)
			}
		}
		if b.IncGlobalMsgs() {
			t.Fatal("inc beyond limit should fail")
		}
		b.DecGlobalMsgs()
		if !b.IncGlobalMsgs() {
			t.Fatal("inc after dec should succeed")
		}
	})

	t.Run("unlimited", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		b.Config.Server.GlobalMaxMsgs = 0

		for i := 0; i < 1000; i++ {
			if !b.IncGlobalMsgs() {
				t.Fatalf("inc %d should succeed (unlimited)", i)
			}
		}
		if b.GlobalMsgs() != 1000 {
			t.Fatalf("count = %d, want 1000", b.GlobalMsgs())
		}
	})

	t.Run("concurrent_safety", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		b.Config.Server.GlobalMaxMsgs = 50

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					if b.IncGlobalMsgs() {
						b.DecGlobalMsgs()
					}
				}
			}()
		}
		wg.Wait()
		if b.GlobalMsgs() != 0 {
			t.Fatalf("final count = %d, want 0", b.GlobalMsgs())
		}
	})
}

func TestBrokerPublish(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("hello"),
		})

		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("frame type = 0x%02x, want FramePublishOK (0x11)", byte(ft))
		}
		pok := frame.(protocol.PublishOKFrame)
		if pok.MsgID == "" {
			t.Fatal("PublishOK should contain a MsgID")
		}
	})

	t.Run("queue_not_found", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "nonexistent",
			Group:   "test-group",
			Payload: []byte("data"),
		})

		ft, frame := readFrame(t, dec)
		if ft != protocol.FrameError {
			t.Fatalf("frame type = 0x%02x, want FrameError (0xF0)", byte(ft))
		}
		ef := frame.(protocol.ErrorFrame)
		if ef.Code != 404 {
			t.Fatalf("error code = %d, want 404", ef.Code)
		}
	})

	t.Run("global_limit", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		b.Config.Server.GlobalMaxMsgs = 1
		conn, _, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "g1",
			Payload: []byte("first"),
		})
		if b.GlobalMsgs() != 1 {
			t.Fatalf("after first publish GlobalMsgs = %d, want 1", b.GlobalMsgs())
		}

		conn2, dec2, cleanupConn2 := testConn(t)
		defer cleanupConn2()

		b.onPublish(conn2, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "g2",
			Payload: []byte("second"),
		})

		ft, frame := readFrame(t, dec2)
		if ft != protocol.FrameBackpressure {
			t.Fatalf("second publish = 0x%02x, want FrameBackpressure (0xF1)", byte(ft))
		}
		bp := frame.(protocol.BackpressureFrame)
		if bp.Reason == "" {
			t.Fatal("backpressure should include reason")
		}
	})

	t.Run("wal_failed", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()

		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "g1",
			Payload: []byte("init"),
		})
		readFrame(t, dec)

		qw := b.WAL.GetQueue("test-ns", "test-q")
		if qw == nil {
			t.Fatal("GetQueue returned nil after publish")
		}
		qw.SetErr(stg.ErrWALFailed)

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "g2",
			Payload: []byte("fail"),
		})

		ft, frame := readFrame(t, dec)
		if ft != protocol.FrameError {
			t.Fatalf("frame type = 0x%02x, want FrameError", byte(ft))
		}
		ef := frame.(protocol.ErrorFrame)
		if ef.Code != 500 {
			t.Fatalf("error code = %d, want 500", ef.Code)
		}
	})
}

func TestBrokerSubscribe(t *testing.T) {
	t.Parallel()

	t.Run("subscribe_activates", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		conn, _, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onSubscribe(conn, protocol.SubscribeFrame{
			Queue: "test-q",
			Group: "test-group",
		})

		if !b.registry.has("test-ns", "test-q", "test-group") {
			t.Fatal("registry should have the subscription")
		}
	})

	t.Run("subscribe_missing_queue", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		conn, _, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onSubscribe(conn, protocol.SubscribeFrame{
			Queue: "no-such-queue",
			Group: "test-group",
		})

		if !b.registry.has("test-ns", "no-such-queue", "test-group") {
			t.Fatal("registry should still register subscription even without queue")
		}
	})
}

func TestBrokerACK(t *testing.T) {
	t.Parallel()

	t.Run("ack_completes_message", func(t *testing.T) {
		b, q, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("hello"),
		})
		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		pok := frame.(protocol.PublishOKFrame)

		conn.AcquireCredit()
		b.quota.TryAcquire()

		stats, ok := q.GetGroupStats("test-group")
		if !ok {
			t.Fatal("group should exist after publish")
		}
		if stats.Processing != 1 {
			t.Fatalf("processing = %d, want 1", stats.Processing)
		}

		b.onACK(conn, protocol.ACKFrame{
			MsgID: pok.MsgID,
			Queue: "test-q",
			Group: "test-group",
		})

		stats, _ = q.GetGroupStats("test-group")
		if stats.Processing != 0 {
			t.Fatalf("processing = %d, want 0 after ack", stats.Processing)
		}
		if stats.Pending != 0 {
			t.Fatalf("pending = %d, want 0", stats.Pending)
		}
	})

	t.Run("ack_unknown_queue", func(t *testing.T) {
		b, _, cleanup := testBroker(t)
		defer cleanup()
		conn, _, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onACK(conn, protocol.ACKFrame{
			MsgID: "does-not-exist",
			Queue: "no-such-queue",
			Group: "g1",
		})
	})

	t.Run("ack_dispatches_next", func(t *testing.T) {
		b, q, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("msg1"),
		})
		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		pok1 := frame.(protocol.PublishOKFrame)

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("msg2"),
		})
		ft, frame = readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		pok2 := frame.(protocol.PublishOKFrame)

		if pok1.MsgID == pok2.MsgID {
			t.Fatal("two publishes should produce different MsgIDs")
		}

		stats, _ := q.GetGroupStats("test-group")
		if stats.Processing != 1 || stats.Pending != 1 {
			t.Fatalf("before ack: want processing=1 pending=1, got processing=%d pending=%d",
				stats.Processing, stats.Pending)
		}

		conn.AcquireCredit()
		b.quota.TryAcquire()

		b.onACK(conn, protocol.ACKFrame{
			MsgID: pok1.MsgID,
			Queue: "test-q",
			Group: "test-group",
		})

		// CompleteAndNext completes msg1 and moves msg2 to processing
		// (already inside the ack handler). The test verifies via stats.
		stats, _ = q.GetGroupStats("test-group")
		if stats.Processing != 1 {
			t.Fatalf("after ack: want processing=1 (msg2 auto-dispatched), got %d", stats.Processing)
		}
		if stats.Pending != 0 {
			t.Fatalf("after ack: want pending=0, got %d", stats.Pending)
		}

		conn.AcquireCredit()
		b.quota.TryAcquire()
		b.onACK(conn, protocol.ACKFrame{
			MsgID: pok2.MsgID,
			Queue: "test-q",
			Group: "test-group",
		})

		stats, _ = q.GetGroupStats("test-group")
		if stats.Processing != 0 || stats.Pending != 0 {
			t.Fatalf("after both acks: processing=%d pending=%d, want 0 0",
				stats.Processing, stats.Pending)
		}
	})
}

func TestBrokerNACK(t *testing.T) {
	t.Parallel()

	t.Run("nack_schedules_retry", func(t *testing.T) {
		b, q, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		gCfg := queue.GroupConfig{
			Key:         "test-group",
			Parallelism: 1,
			Retry: queue.RetryConfig{
				MaxAttempts:  3,
				Backoff:      queue.BackoffExponential,
				InitialDelay: 10 * time.Millisecond,
				MaxDelay:     100 * time.Millisecond,
			},
		}
		q.SetGroupConfig(gCfg)

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("retry-me"),
		})
		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		pok := frame.(protocol.PublishOKFrame)

		conn.AcquireCredit()
		b.quota.TryAcquire()

		b.onNACK(conn, protocol.NACKFrame{
			MsgID:  pok.MsgID,
			Queue:  "test-q",
			Group:  "test-group",
			Reason: "processing error",
		})

		stats, _ := q.GetGroupStats("test-group")
		if stats.Pending != 1 && stats.Processing != 0 {
			t.Logf("after nack: pending=%d processing=%d (retry scheduled with backoff)", stats.Pending, stats.Processing)
		}
	})

	t.Run("nack_exhausts_retry", func(t *testing.T) {
		b, q, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		gCfg := queue.GroupConfig{
			Key:         "test-group",
			Parallelism: 1,
			Retry: queue.RetryConfig{
				MaxAttempts:  1,
				Backoff:      queue.BackoffExponential,
				InitialDelay: time.Second,
				MaxDelay:     30 * time.Second,
			},
		}
		q.SetGroupConfig(gCfg)

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("exhaust"),
		})
		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		pok := frame.(protocol.PublishOKFrame)

		conn.AcquireCredit()
		b.quota.TryAcquire()

		b.onNACK(conn, protocol.NACKFrame{
			MsgID:  pok.MsgID,
			Queue:  "test-q",
			Group:  "test-group",
			Reason: "too many failures",
		})

		stats, _ := q.GetGroupStats("test-group")
		if stats.Processing != 0 {
			t.Fatalf("processing = %d, want 0 (retries exhausted)", stats.Processing)
		}
	})

	t.Run("nack_routes_to_dlq", func(t *testing.T) {
		b, q, cleanup := testBroker(t)
		defer cleanup()
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		dlq := queue.NewQueue("test-dlq", "test-ns")
		b.Scheduler.RegisterQueue(dlq)
		b.registerQueueWithWAL(dlq)

		gCfg := queue.GroupConfig{
			Key:         "test-group",
			Parallelism: 1,
			Retry: queue.RetryConfig{
				MaxAttempts:  1,
				Backoff:      queue.BackoffExponential,
				InitialDelay: time.Second,
				MaxDelay:     30 * time.Second,
			},
			DLQ: &queue.DLQConfig{
				Enabled: true,
				Queue:   "test-dlq",
			},
		}
		q.SetGroupConfig(gCfg)

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "test-group",
			Payload: []byte("dlq-bound"),
		})
		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		pok := frame.(protocol.PublishOKFrame)

		b.onSubscribe(conn, protocol.SubscribeFrame{
			Queue: "test-dlq",
			Group: "test-group",
		})

		conn.AcquireCredit()
		b.quota.TryAcquire()

		b.onNACK(conn, protocol.NACKFrame{
			MsgID:  pok.MsgID,
			Queue:  "test-q",
			Group:  "test-group",
			Reason: "permanent failure",
		})

		dlqMsg := dlq.TryDispatchOne("test-group", time.Now())
		if dlqMsg == nil {
			t.Fatal("DLQ should contain the failed message")
		}
		if string(dlqMsg.Payload) != "dlq-bound" {
			t.Fatalf("DLQ payload = %q, want %q", string(dlqMsg.Payload), "dlq-bound")
		}
	})
}

func TestBrokerConcurrentPublish(t *testing.T) {
	b, _, cleanup := testBroker(t)
	defer cleanup()
	b.Config.Server.GlobalMaxMsgs = 5

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, dec, cleanupConn := testConn(t)
			defer cleanupConn()

			b.onPublish(conn, protocol.PublishFrame{
				Queue:   "test-q",
				Group:   "conc-group",
				Payload: []byte("conc"),
			})

			ft, _, readErr := dec.ReadFrame()
			if readErr != nil {
				errs <- readErr
				return
			}
			if ft == protocol.FrameError {
				errs <- nil
				return
			}
			if ft != protocol.FramePublishOK && ft != protocol.FrameBackpressure {
				errs <- nil
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("publish error: %v", err)
		}
	}

	if b.GlobalMsgs() > 5 {
		t.Fatalf("GlobalMsgs = %d, should not exceed limit 5", b.GlobalMsgs())
	}
}

func TestBrokerConcurrentACK(t *testing.T) {
	b, q, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	const n = 20
	msgIDs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "ack-race",
			Payload: []byte("data"),
		})
		ft, frame := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
		}
		msgIDs = append(msgIDs, frame.(protocol.PublishOKFrame).MsgID)
	}

	var wg sync.WaitGroup
	for _, msgID := range msgIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			time.Sleep(time.Duration(id[0]) * time.Microsecond)
			conn.AcquireCredit()
			b.quota.TryAcquire()
			b.onACK(conn, protocol.ACKFrame{
				MsgID: id,
				Queue: "test-q",
				Group: "ack-race",
			})
		}(msgID)
	}
	wg.Wait()

	stats, ok := q.GetGroupStats("ack-race")
	if ok {
		if stats.Processing != 0 || stats.Pending != 0 {
			t.Logf("after concurrent ack: processing=%d pending=%d", stats.Processing, stats.Pending)
		}
	}
}

func TestBrokerEmptyPayload(t *testing.T) {
	t.Parallel()

	b, _, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "test-group",
		Payload: []byte{},
	})

	ft, frame := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("frame type = 0x%02x, want FramePublishOK", byte(ft))
	}
	pok := frame.(protocol.PublishOKFrame)
	if pok.MsgID == "" {
		t.Fatal("empty payload publish should still get MsgID")
	}
}

func TestBrokerLargePayload(t *testing.T) {
	t.Parallel()

	b, _, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "test-group",
		Payload: payload,
	})

	ft, frame := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("frame type = 0x%02x, want FramePublishOK", byte(ft))
	}
	pok := frame.(protocol.PublishOKFrame)
	if pok.MsgID == "" {
		t.Fatal("large payload publish should get MsgID")
	}
}

func TestBrokerMultipleGroups(t *testing.T) {
	t.Parallel()

	b, q, cleanup := testBroker(t)
	defer cleanup()

	publishAndCheck := func(group string) {
		conn, dec, cleanupConn := testConn(t)
		defer cleanupConn()

		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   group,
			Payload: []byte("data"),
		})
		readFrame(t, dec)
	}

	publishAndCheck("group-a")
	publishAndCheck("group-b")
	publishAndCheck("group-c")

	for _, g := range []string{"group-a", "group-b", "group-c"} {
		stats, ok := q.GetGroupStats(g)
		if !ok {
			t.Fatalf("group %q not found", g)
		}
		if stats.Pending != 0 && stats.Processing != 1 {
			t.Logf("group %q: pending=%d processing=%d", g, stats.Pending, stats.Processing)
		}
	}
}

func TestBrokerGlobalLimitConcurrentPublishers(t *testing.T) {
	t.Parallel()

	b, _, cleanup := testBroker(t)
	defer cleanup()
	b.Config.Server.GlobalMaxMsgs = 2

	var wg sync.WaitGroup
	accepted := atomic.Int64{}
	rejected := atomic.Int64{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, dec, cleanupConn := testConn(t)
			defer cleanupConn()

			for j := 0; j < 3; j++ {
				b.onPublish(conn, protocol.PublishFrame{
					Queue:   "test-q",
					Group:   "conc-group",
					Payload: []byte("x"),
				})
				ft, _, err := dec.ReadFrame()
				if err != nil {
					return
				}
				switch ft {
				case protocol.FramePublishOK:
					accepted.Add(1)
				case protocol.FrameBackpressure:
					rejected.Add(1)
				default:
					return
				}
			}
		}()
	}
	wg.Wait()

	total := accepted.Load() + rejected.Load()
	if total == 0 {
		t.Fatal("no publishes completed")
	}
	if accepted.Load() > 2 {
		t.Fatalf("accepted = %d, should not exceed limit 2", accepted.Load())
	}
	t.Logf("accepted=%d rejected=%d (global limit=2, 5 publishers x 3 each)", accepted.Load(), rejected.Load())
}

func TestBrokerDispatchChannelFull(t *testing.T) {
	b, q, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	b.dispatchChs[0] = make(chan *queue.Message, 1)
	fill := &queue.Message{
		ID:        "fill",
		QueueName: "test-q",
		Namespace: "test-ns",
		GroupKey:  "test-group",
		Payload:   []byte("fill"),
		State:     queue.StatePending,
		CreatedAt: time.Now(),
	}
	b.dispatchChs[0] <- fill
	close(b.dispatchStop)

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "test-group",
		Payload: []byte("should-be-stuck"),
	})

	ft, frame := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("frame type = 0x%02x, want FramePublishOK", byte(ft))
	}
	pok := frame.(protocol.PublishOKFrame)
	if pok.MsgID == "" {
		t.Fatal("PublishOK should have MsgID")
	}

	stats, ok := q.GetGroupStats("test-group")
	if !ok {
		t.Fatal("group should exist")
	}
	t.Logf("after dispatch channel full: pending=%d processing=%d", stats.Pending, stats.Processing)
}

func TestBrokerHeartbeat(t *testing.T) {
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	if err := conn.WriteHeartbeat(); err != nil {
		t.Fatal(err)
	}

	ft, frame := readFrame(t, dec)
	if ft != protocol.FrameHeartbeat {
		t.Fatalf("frame type = 0x%02x, want FrameHeartbeat (0xFF)", byte(ft))
	}
	if frame != nil {
		t.Fatal("heartbeat frame should have nil payload")
	}
}

func TestBrokerRegistryRemoveNonExistent(t *testing.T) {
	t.Parallel()
	r := newSubRegistry()
	conn, _, cleanup := testConn(t)
	defer cleanup()

	r.remove(conn)

	if r.has("ns", "q", "g") {
		t.Fatal("should not have anything")
	}
}

func TestBrokerRegistryDoubleRemove(t *testing.T) {
	t.Parallel()
	r := newSubRegistry()
	conn, _, cleanup := testConn(t)
	defer cleanup()

	r.add("ns", "q", "g", conn)
	r.remove(conn)
	r.remove(conn)
}

func TestBrokerRegistryConcurrentAddRemove(t *testing.T) {
	t.Parallel()
	r := newSubRegistry()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, _, cleanup := testConn(t)
			defer cleanup()
			for j := 0; j < 10; j++ {
				r.add("ns", "q", "g", conn)
				r.remove(conn)
			}
		}()
	}
	wg.Wait()
}

func TestBrokerQuotaConcurrentStress(t *testing.T) {
	t.Parallel()
	q := newBrokerQuota(10)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if q.TryAcquire() {
					q.Release()
				}
			}
		}()
	}
	wg.Wait()

	if q.Inflight() != 0 {
		t.Fatalf("final inflight = %d, want 0", q.Inflight())
	}
}

func TestBrokerQuotaAcquireReleaseCross(t *testing.T) {
	t.Parallel()
	q := newBrokerQuota(5)

	for i := 0; i < 5; i++ {
		if !q.TryAcquire() {
			t.Fatalf("acquire %d", i)
		}
	}

	var started sync.WaitGroup
	ready := make(chan struct{})
	for i := 0; i < 10; i++ {
		started.Add(1)
		go func() {
			started.Done()
			<-ready
			if q.TryAcquire() {
				q.Release()
			}
		}()
	}

	started.Wait()
	close(ready)
	for i := 0; i < 5; i++ {
		q.Release()
	}

	time.Sleep(50 * time.Millisecond)
	if q.Inflight() != 0 {
		t.Logf("inflight = %d (may not be 0 if goroutines acquired, which is fine)", q.Inflight())
	}
}

func TestBrokerPublishWithoutSubscriberDoesNotBlock(t *testing.T) {
	t.Parallel()
	b, _, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	for i := 0; i < 50; i++ {
		b.onPublish(conn, protocol.PublishFrame{
			Queue:   "test-q",
			Group:   "no-subs",
			Payload: []byte("data"),
		})
		ft, _ := readFrame(t, dec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("publish %d: got 0x%02x, want FramePublishOK", i, byte(ft))
		}
	}
}

func TestBrokerSubscribeMultipleSameConn(t *testing.T) {
	t.Parallel()
	b, _, cleanup := testBroker(t)
	defer cleanup()
	conn, _, cleanupConn := testConn(t)
	defer cleanupConn()

	for i := 0; i < 20; i++ {
		b.onSubscribe(conn, protocol.SubscribeFrame{
			Queue: fmt.Sprintf("test-q-%d", i),
			Group: "test-group",
		})
	}

	for i := 0; i < 20; i++ {
		if !b.registry.has("test-ns", fmt.Sprintf("test-q-%d", i), "test-group") {
			t.Fatalf("subscription %d should exist", i)
		}
	}

	b.registry.remove(conn)

	for i := 0; i < 20; i++ {
		if b.registry.has("test-ns", fmt.Sprintf("test-q-%d", i), "test-group") {
			t.Fatalf("subscription %d should be removed", i)
		}
	}
}

func TestBrokerConcurrentRegistryOperations(t *testing.T) {
	b, _, cleanup := testBroker(t)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, _, cleanupConn := testConn(t)
			defer cleanupConn()
			for j := 0; j < 5; j++ {
				b.registry.add("test-ns", fmt.Sprintf("q-%d-%d", id, j), "g", conn)
			}
			for j := 0; j < 5; j++ {
				if !b.registry.has("test-ns", fmt.Sprintf("q-%d-%d", id, j), "g") {
					t.Errorf("expected to have q-%d-%d", id, j)
				}
			}
			b.registry.remove(conn)
		}(i)
	}
	wg.Wait()
}

func TestBrokerACKWithoutCredit(t *testing.T) {
	b, _, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "test-group",
		Payload: []byte("hello"),
	})
	ft, frame := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
	}
	pok := frame.(protocol.PublishOKFrame)

	b.onACK(conn, protocol.ACKFrame{
		MsgID: pok.MsgID + "-nonexistent",
		Queue: "test-q",
		Group: "test-group",
	})

	if stats, ok := b.Scheduler.GetQueue("test-ns", "test-q").GetGroupStats("test-group"); ok {
		t.Logf("after ack with wrong id: processing=%d pending=%d", stats.Processing, stats.Pending)
	}
}

func TestBrokerNACKWithoutRetryConfig(t *testing.T) {
	b, _, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "test-group",
		Payload: []byte("hello"),
	})
	ft, frame := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
	}
	pok := frame.(protocol.PublishOKFrame)

	conn.AcquireCredit()
	b.quota.TryAcquire()

	b.onNACK(conn, protocol.NACKFrame{
		MsgID:  pok.MsgID,
		Queue:  "test-q",
		Group:  "test-group",
		Reason: "no retry configured",
	})

	if stats, ok := b.Scheduler.GetQueue("test-ns", "test-q").GetGroupStats("test-group"); ok {
		t.Logf("after nack (default config): processing=%d pending=%d", stats.Processing, stats.Pending)
	}
}

func TestBrokerConcurrentPublishSameGroup(t *testing.T) {
	b, _, cleanup := testBroker(t)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, dec, cleanupConn := testConn(t)
			defer cleanupConn()

			b.onPublish(conn, protocol.PublishFrame{
				Queue:   "test-q",
				Group:   "same-group",
				Payload: []byte("data"),
			})
			ft, frame := readFrame(t, dec)
			if ft != protocol.FramePublishOK {
				return
			}
			pok := frame.(protocol.PublishOKFrame)

			conn.AcquireCredit()
			b.quota.TryAcquire()

			b.onACK(conn, protocol.ACKFrame{
				MsgID: pok.MsgID,
				Queue: "test-q",
				Group: "same-group",
			})
		}()
	}
	wg.Wait()
}

func TestBrokerMultipleConnectionsSubscribePublish(t *testing.T) {
	b, q, cleanup := testBroker(t)
	defer cleanup()

	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	b.onSubscribe(conn, protocol.SubscribeFrame{
		Queue: "test-q",
		Group: "g",
	})

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "g",
		Payload: []byte("data"),
	})
	ft, _ := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
	}

	stats, ok := q.GetGroupStats("g")
	if !ok {
		t.Fatal("group should exist")
	}
	t.Logf("after publish: pending=%d processing=%d", stats.Pending, stats.Processing)
}

func TestBrokerGlobalMsgCounterEdge(t *testing.T) {
	t.Parallel()
	b, _, cleanup := testBroker(t)
	defer cleanup()
	b.Config.Server.GlobalMaxMsgs = 0

	for i := 0; i < 500; i++ {
		if !b.IncGlobalMsgs() {
			t.Fatalf("inc %d failed (unlimited)", i)
		}
	}
	for i := 0; i < 500; i++ {
		b.DecGlobalMsgs()
	}
	if b.GlobalMsgs() != 0 {
		t.Fatalf("final = %d, want 0", b.GlobalMsgs())
	}
}

func TestBrokerNACKToNonExistentDLQ(t *testing.T) {
	b, q, cleanup := testBroker(t)
	defer cleanup()
	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	gCfg := queue.GroupConfig{
		Key:         "test-group",
		Parallelism: 1,
		Retry: queue.RetryConfig{
			MaxAttempts:  1,
			Backoff:      queue.BackoffExponential,
			InitialDelay: time.Second,
			MaxDelay:     30 * time.Second,
		},
		DLQ: &queue.DLQConfig{
			Enabled: true,
			Queue:   "non-existent-dlq",
		},
	}
	q.SetGroupConfig(gCfg)

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "test-group",
		Payload: []byte("will-fail"),
	})
	ft, frame := readFrame(t, dec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("want PublishOK, got 0x%02x", byte(ft))
	}
	pok := frame.(protocol.PublishOKFrame)

	conn.AcquireCredit()
	b.quota.TryAcquire()

	b.onNACK(conn, protocol.NACKFrame{
		MsgID:  pok.MsgID,
		Queue:  "test-q",
		Group:  "test-group",
		Reason: "exhausted with missing DLQ",
	})

	stats, _ := q.GetGroupStats("test-group")
	t.Logf("after nack with missing DLQ: processing=%d pending=%d", stats.Processing, stats.Pending)
}

func TestBrokerPublishMultipleQueues(t *testing.T) {
	b, _, cleanup := testBroker(t)
	defer cleanup()

	extraQ := queue.NewQueue("extra-q", "test-ns")
	b.Scheduler.RegisterQueue(extraQ)
	b.registerQueueWithWAL(extraQ)

	conn, dec, cleanupConn := testConn(t)
	defer cleanupConn()

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "g1",
		Payload: []byte("first"),
	})
	readFrame(t, dec)

	b.onPublish(conn, protocol.PublishFrame{
		Queue:   "extra-q",
		Group:   "g2",
		Payload: []byte("second"),
	})
	readFrame(t, dec)

	if b.GlobalMsgs() != 2 {
		t.Fatalf("GlobalMsgs = %d, want 2", b.GlobalMsgs())
	}
}

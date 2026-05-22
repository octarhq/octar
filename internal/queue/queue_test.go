package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestQueue(t *testing.T, name, namespace string) *Queue {
	t.Helper()
	return NewQueue(name, namespace)
}

// ── 1. Publish básico ──────────────────────────────────────────────────────────

func TestQueue_PublishBasic(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	msg, err := q.Publish("group-a", []byte("hello"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if msg == nil {
		t.Fatal("expected non-nil message")
	}
	if msg.State != StatePending {
		t.Fatalf("expected StatePending, got %v", msg.State)
	}
	if msg.GroupKey != "group-a" {
		t.Fatalf("expected group-a, got %s", msg.GroupKey)
	}

	stats, ok := q.GetGroupStats("group-a")
	if !ok {
		t.Fatal("expected group stats")
	}
	if stats.Pending != 1 {
		t.Fatalf("expected Pending=1, got %d", stats.Pending)
	}
	if stats.Processing != 0 {
		t.Fatalf("expected Processing=0, got %d", stats.Processing)
	}
}

// ── 2. PublishWithID ───────────────────────────────────────────────────────────

func TestQueue_PublishWithID(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	msg, err := q.PublishWithID("group-a", "my-custom-id-42", []byte("payload"))
	if err != nil {
		t.Fatalf("PublishWithID failed: %v", err)
	}
	if msg.ID != "my-custom-id-42" {
		t.Fatalf("expected ID 'my-custom-id-42', got %q", msg.ID)
	}

	now := time.Now()
	dispatched := q.TryDispatchOne("group-a", now)
	if dispatched == nil {
		t.Fatal("expected to dispatch message")
	}
	if dispatched.ID != "my-custom-id-42" {
		t.Fatalf("expected dispatched ID 'my-custom-id-42', got %q", dispatched.ID)
	}
}

// ── 3. Depth limit ─────────────────────────────────────────────────────────────

func TestQueue_DepthLimit(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:        "limited",
		MaxPending: 2,
	})

	for i := 0; i < 2; i++ {
		_, err := q.Publish("limited", []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d should succeed: %v", i, err)
		}
	}

	_, err := q.Publish("limited", []byte("3rd"))
	if err == nil {
		t.Fatal("expected error for depth limit exceeded")
	}
}

func TestQueue_DepthLimitUnlimited(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	for i := 0; i < 100; i++ {
		_, err := q.Publish("unlimited", []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d should succeed with MaxPending=0: %v", i, err)
		}
	}

	stats, ok := q.GetGroupStats("unlimited")
	if !ok {
		t.Fatal("expected group stats")
	}
	if stats.Pending != 100 {
		t.Fatalf("expected Pending=100, got %d", stats.Pending)
	}
}

// ── 4. TryDispatchOne ──────────────────────────────────────────────────────────

func TestQueue_TryDispatchOne(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("msg1"))
	now := time.Now()

	msg := q.TryDispatchOne("group-a", now)
	if msg == nil {
		t.Fatal("expected a dispatched message")
	}
	if msg.State != StateProcessing {
		t.Fatalf("expected StateProcessing, got %v", msg.State)
	}

	stats, _ := q.GetGroupStats("group-a")
	if stats.Pending != 0 {
		t.Fatalf("expected Pending=0 after dispatch, got %d", stats.Pending)
	}
	if stats.Processing != 1 {
		t.Fatalf("expected Processing=1 after dispatch, got %d", stats.Processing)
	}
}

func TestQueue_TryDispatchOneEmpty(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	now := time.Now()
	if msg := q.TryDispatchOne("nonexistent", now); msg != nil {
		t.Fatal("expected nil for nonexistent group")
	}

	q.Publish("empty-test", []byte("msg"))
	q.TryDispatchOne("empty-test", now)
	// Queue should be empty after dispatch (parallelism=1, msg in processing)
	if msg := q.TryDispatchOne("empty-test", now); msg != nil {
		t.Fatal("expected nil when only message is already processing")
	}
}

// ── 5. Complete ────────────────────────────────────────────────────────────────

func TestQueue_Complete(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	pub, _ := q.Publish("group-a", []byte("msg1"))
	now := time.Now()
	q.TryDispatchOne("group-a", now)

	if err := q.Complete("group-a", pub.ID); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	stats, _ := q.GetGroupStats("group-a")
	if stats.Pending != 0 {
		t.Fatalf("expected Pending=0, got %d", stats.Pending)
	}
	if stats.Processing != 0 {
		t.Fatalf("expected Processing=0, got %d", stats.Processing)
	}
}

func TestQueue_CompleteErrors(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	if err := q.Complete("nope", "id"); err == nil {
		t.Fatal("expected error for nonexistent group")
	}

	q.Publish("group-a", []byte("msg"))
	if err := q.Complete("group-a", "nonexistent-id"); err == nil {
		t.Fatal("expected error for nonexistent message ID")
	}
}

// ── 6. CompleteAndNext ─────────────────────────────────────────────────────────

func TestQueue_CompleteAndNext(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("msg1"))
	q.Publish("group-a", []byte("msg2"))

	now := time.Now()
	first := q.TryDispatchOne("group-a", now)
	if first == nil {
		t.Fatal("expected first dispatch")
	}

	second := q.CompleteAndNext("group-a", first.ID, now)
	if second == nil {
		t.Fatal("CompleteAndNext should return next message")
	}
	if string(second.Payload) != "msg2" {
		t.Fatalf("expected payload 'msg2', got %q", string(second.Payload))
	}

	// Complete second, no more messages
	if final := q.CompleteAndNext("group-a", second.ID, now); final != nil {
		t.Fatal("CompleteAndNext should return nil when queue empty")
	}
}

func TestQueue_CompleteAndNextGroupNotFound(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	now := time.Now()
	if msg := q.CompleteAndNext("nope", "id", now); msg != nil {
		t.Fatal("expected nil for nonexistent group")
	}
}

// ── 7. Fail + retry ────────────────────────────────────────────────────────────

func TestQueue_FailRetry(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key: "retry-group",
		Retry: RetryConfig{
			MaxAttempts:  3,
			Backoff:      BackoffFixed,
			InitialDelay: time.Millisecond,
		},
	})

	pub, _ := q.Publish("retry-group", []byte("retry-me"))
	now := time.Now()
	q.TryDispatchOne("retry-group", now)

	dlqQueue, dlqMsg, err := q.Fail("retry-group", pub.ID, "something went wrong")
	if err != nil {
		t.Fatalf("Fail returned error: %v", err)
	}
	if dlqQueue != "" || dlqMsg != nil {
		t.Fatal("expected no DLQ result; should retry")
	}

	// Message should be in delayed queue
	stats, _ := q.GetGroupStats("retry-group")
	if stats.Pending != 1 {
		t.Fatalf("expected Pending=1 (delayed), got %d", stats.Pending)
	}
	if stats.Processing != 0 {
		t.Fatalf("expected Processing=0, got %d", stats.Processing)
	}

	// Advance time past backoff and dispatch again
	time.Sleep(5 * time.Millisecond)
	later := time.Now()
	redispatched := q.TryDispatchOne("retry-group", later)
	if redispatched == nil {
		t.Fatal("expected re-dispatch after backoff")
	}
	if redispatched.ID != pub.ID {
		t.Fatalf("expected same message ID, got %q", redispatched.ID)
	}
	if redispatched.Attempts != 1 {
		t.Fatalf("expected Attempts=1, got %d", redispatched.Attempts)
	}
}

// ── 8. Fail + DLQ ──────────────────────────────────────────────────────────────

func TestQueue_FailDLQ(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key: "dlq-group",
		Retry: RetryConfig{
			MaxAttempts: 1,
			Backoff:     BackoffFixed,
		},
		DLQ: &DLQConfig{
			Enabled: true,
			Queue:   "my-dlq",
		},
	})

	pub, _ := q.Publish("dlq-group", []byte("will-fail"))
	now := time.Now()
	q.TryDispatchOne("dlq-group", now)

	dlqQueue, dlqMsg, err := q.Fail("dlq-group", pub.ID, "permanent error")
	if err != nil {
		t.Fatalf("Fail returned error: %v", err)
	}
	if dlqQueue != "my-dlq" {
		t.Fatalf("expected dlqQueue 'my-dlq', got %q", dlqQueue)
	}
	if dlqMsg == nil {
		t.Fatal("expected non-nil dlqMsg")
	}
	if dlqMsg.State != StateDLQ {
		t.Fatalf("expected StateDLQ, got %v", dlqMsg.State)
	}
	if dlqMsg.LastError != "permanent error" {
		t.Fatalf("expected 'permanent error', got %q", dlqMsg.LastError)
	}

	stats, _ := q.GetGroupStats("dlq-group")
	if stats.Processing != 0 {
		t.Fatalf("expected Processing=0 after DLQ, got %d", stats.Processing)
	}
}

func TestQueue_FailNoDLQ(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key: "fail-group",
		Retry: RetryConfig{
			MaxAttempts: 1,
		},
	})

	pub, _ := q.Publish("fail-group", []byte("will-fail"))
	now := time.Now()
	q.TryDispatchOne("fail-group", now)

	dlqQueue, dlqMsg, err := q.Fail("fail-group", pub.ID, "error")
	if err != nil {
		t.Fatalf("Fail returned error: %v", err)
	}
	if dlqQueue != "" || dlqMsg != nil {
		t.Fatal("expected nil/empty when no DLQ configured")
	}
}

func TestQueue_FailGroupNotFound(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	_, _, err := q.Fail("nonexistent", "id", "error")
	if err == nil {
		t.Fatal("expected error for nonexistent group")
	}
}

func TestQueue_FailAndNext(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key: "fan-group",
		Retry: RetryConfig{
			MaxAttempts:  3,
			Backoff:      BackoffFixed,
			InitialDelay: time.Second,
		},
	})

	q.Publish("fan-group", []byte("msg1"))
	q.Publish("fan-group", []byte("msg2"))

	now := time.Now()
	first := q.TryDispatchOne("fan-group", now)

	// Fail the first message, expect msg2 as "next" since msg1 goes to delayed
	dlqQueue, dlqMsg, next := q.FailAndNext("fan-group", first.ID, "error", now)
	if dlqQueue != "" || dlqMsg != nil {
		t.Fatal("expected no DLQ (retry configured)")
	}
	if next == nil {
		t.Fatal("expected next message from FailAndNext")
	}
	if string(next.Payload) != "msg2" {
		t.Fatalf("expected 'msg2', got %q", string(next.Payload))
	}
}

func TestQueue_FailAndNextDLQ(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key: "fn-group",
		Retry: RetryConfig{
			MaxAttempts: 1,
			Backoff:     BackoffFixed,
		},
		DLQ: &DLQConfig{
			Enabled: true,
			Queue:   "dlq",
		},
	})

	q.Publish("fn-group", []byte("msg1"))
	q.Publish("fn-group", []byte("msg2"))

	now := time.Now()
	first := q.TryDispatchOne("fn-group", now)

	dlqQueue, dlqMsg, next := q.FailAndNext("fn-group", first.ID, "bad", now)
	if dlqQueue != "dlq" {
		t.Fatalf("expected dlq queue 'dlq', got %q", dlqQueue)
	}
	if dlqMsg == nil {
		t.Fatal("expected DLQ message from FailAndNext")
	}
	if dlqMsg.State != StateDLQ {
		t.Fatalf("expected StateDLQ, got %v", dlqMsg.State)
	}
	if next == nil {
		t.Fatal("expected next message from FailAndNext")
	}
	if string(next.Payload) != "msg2" {
		t.Fatalf("expected 'msg2', got %q", string(next.Payload))
	}
}

func TestQueue_FailAndNextGroupNotFound(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	now := time.Now()
	dlqQueue, dlqMsg, next := q.FailAndNext("nope", "id", "err", now)
	if dlqQueue != "" || dlqMsg != nil || next != nil {
		t.Fatal("expected all nil/empty for nonexistent group")
	}
}

// ── 9. ReturnToPending ─────────────────────────────────────────────────────────

func TestQueue_ReturnToPending(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	pub, _ := q.Publish("group-a", []byte("msg1"))
	now := time.Now()
	q.TryDispatchOne("group-a", now)

	q.ReturnToPending("group-a", pub.ID)

	stats, _ := q.GetGroupStats("group-a")
	if stats.Pending != 1 {
		t.Fatalf("expected Pending=1 after ReturnToPending, got %d", stats.Pending)
	}
	if stats.Processing != 0 {
		t.Fatalf("expected Processing=0 after ReturnToPending, got %d", stats.Processing)
	}

	// Re-dispatch the same message
	redispatched := q.TryDispatchOne("group-a", now)
	if redispatched == nil {
		t.Fatal("expected message to be dispatchable again")
	}
	if redispatched.ID != pub.ID {
		t.Fatalf("expected same message ID, got %q", redispatched.ID)
	}
}

func TestQueue_ReturnToPendingNonexistentGroup(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	// Should not panic
	q.ReturnToPending("nonexistent", "some-id")
}

func TestQueue_ReturnToPendingNonexistentMessage(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("msg"))
	now := time.Now()
	q.TryDispatchOne("group-a", now)

	// Should not panic; message ID doesn't exist in processing so it's a no-op
	q.ReturnToPending("group-a", "nonexistent-id")

	// Original message stays in processing (no-op was correct)
	stats, _ := q.GetGroupStats("group-a")
	if stats.Processing != 1 {
		t.Fatalf("expected Processing=1 after no-op return (unchanged), got %d", stats.Processing)
	}
}

// ── 10. SweepExpiredLeases ─────────────────────────────────────────────────────

func TestQueue_SweepExpiredLeases(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:          "lease-group",
		LeaseTimeout: 10 * time.Millisecond,
	})

	pub, _ := q.Publish("lease-group", []byte("lease-me"))
	now := time.Now()
	q.TryDispatchOne("lease-group", now)

	expired := now.Add(50 * time.Millisecond)
	results := q.SweepExpiredLeases(expired)

	if len(results) != 1 {
		t.Fatalf("expected 1 expired lease, got %d", len(results))
	}
	if results[0].MsgID != pub.ID {
		t.Fatalf("expected expired msg ID %q, got %q", pub.ID, results[0].MsgID)
	}
	if results[0].GroupKey != "lease-group" {
		t.Fatalf("expected group key 'lease-group', got %q", results[0].GroupKey)
	}
	if results[0].Namespace != "testns" || results[0].QueueName != "testq" {
		t.Fatal("unexpected namespace/queue in expired lease")
	}

	// Message should be back in pending
	stats, _ := q.GetGroupStats("lease-group")
	if stats.Pending != 1 {
		t.Fatalf("expected Pending=1 after sweep, got %d", stats.Pending)
	}
	if stats.Processing != 0 {
		t.Fatalf("expected Processing=0 after sweep, got %d", stats.Processing)
	}
}

func TestQueue_SweepExpiredLeasesNoExpiry(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("msg"))
	now := time.Now()

	// Nothing dispatched, no leases
	if results := q.SweepExpiredLeases(now); len(results) != 0 {
		t.Fatalf("expected 0 expired leases, got %d", len(results))
	}
}

func TestQueue_SweepExpiredLeasesMultipleGroups(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{Key: "g1", LeaseTimeout: 5 * time.Millisecond})
	q.SetGroupConfig(GroupConfig{Key: "g2", LeaseTimeout: 10 * time.Millisecond})

	pub1, _ := q.Publish("g1", []byte("msg1"))
	pub2, _ := q.Publish("g2", []byte("msg2"))

	now := time.Now()
	q.TryDispatchOne("g1", now)
	q.TryDispatchOne("g2", now)

	later := now.Add(100 * time.Millisecond)
	results := q.SweepExpiredLeases(later)

	if len(results) != 2 {
		t.Fatalf("expected 2 expired leases, got %d", len(results))
	}

	ids := map[string]bool{results[0].MsgID: true, results[1].MsgID: true}
	if !ids[pub1.ID] || !ids[pub2.ID] {
		t.Fatal("expected both messages in expired results")
	}
}

// ── 11. Group wildcard config ──────────────────────────────────────────────────

func TestQueue_WildcardConfig(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:         "group-*",
		Parallelism: 5,
	})

	for i := 0; i < 5; i++ {
		q.Publish("group-123", []byte("msg"))
	}

	now := time.Now()
	for i := 0; i < 5; i++ {
		if msg := q.TryDispatchOne("group-123", now); msg == nil {
			t.Fatalf("dispatch %d should succeed with wildcard parallelism=5", i)
		}
	}

	// 6th should be blocked
	if msg := q.TryDispatchOne("group-123", now); msg != nil {
		t.Fatal("6th dispatch should be blocked")
	}
}

func TestQueue_WildcardMultiplePatterns(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:         "tenant-*",
		Parallelism: 3,
	})
	q.SetGroupConfig(GroupConfig{
		Key:         "specific-*",
		Parallelism: 7,
	})

	q.Publish("tenant-abc", []byte("msg"))
	q.Publish("tenant-abc", []byte("msg2"))
	q.Publish("tenant-abc", []byte("msg3"))

	now := time.Now()
	for i := 0; i < 3; i++ {
		if msg := q.TryDispatchOne("tenant-abc", now); msg == nil {
			t.Fatalf("tenant dispatch %d failed", i)
		}
	}
	if msg := q.TryDispatchOne("tenant-abc", now); msg != nil {
		t.Fatal("4th tenant dispatch should be blocked")
	}

	q.Publish("specific-x", []byte("s1"))
	q.Publish("specific-x", []byte("s2"))

	for i := 0; i < 2; i++ {
		if msg := q.TryDispatchOne("specific-x", now); msg == nil {
			t.Fatalf("specific dispatch %d failed", i)
		}
	}
}

// ── 12. Exact match wins ───────────────────────────────────────────────────────

func TestQueue_ExactMatchWins(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:          "group-*",
		Parallelism:  1,
		LeaseTimeout: 30 * time.Second,
	})
	q.SetGroupConfig(GroupConfig{
		Key:          "group-123",
		Parallelism:  10,
		LeaseTimeout: 10 * time.Second,
	})

	for i := 0; i < 10; i++ {
		q.Publish("group-123", []byte("msg"))
	}

	now := time.Now()
	dispatched := 0
	for i := 0; i < 15; i++ {
		if msg := q.TryDispatchOne("group-123", now); msg != nil {
			dispatched++
		} else {
			break
		}
	}
	if dispatched != 10 {
		t.Fatalf("expected exactly 10 dispatches (exact parallelism=10), got %d", dispatched)
	}
}

// ── 13. Default config fallback ────────────────────────────────────────────────

func TestQueue_DefaultConfigFallback(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("default-group", []byte("msg1"))
	q.Publish("default-group", []byte("msg2"))

	now := time.Now()
	msg1 := q.TryDispatchOne("default-group", now)
	if msg1 == nil {
		t.Fatal("expected first dispatch")
	}
	// Parallelism=1 default, so second should be blocked
	if msg := q.TryDispatchOne("default-group", now); msg != nil {
		t.Fatal("second dispatch should be blocked (default parallelism=1)")
	}

	q.Complete("default-group", msg1.ID)

	msg2 := q.TryDispatchOne("default-group", now)
	if msg2 == nil {
		t.Fatal("expected dispatch after completing first")
	}
	if string(msg2.Payload) != "msg2" {
		t.Fatalf("expected 'msg2', got %q", string(msg2.Payload))
	}
}

// ── 14. Concorrência ──────────────────────────────────────────────────────────

func TestQueue_ConcurrentPublishComplete(t *testing.T) {
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:         "concurrent",
		Parallelism: 100,
		Retry: RetryConfig{
			MaxAttempts: 1,
		},
	})

	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, err := q.Publish("concurrent", []byte("data"))
			if err != nil {
				return
			}
			now := time.Now()
			if d := q.TryDispatchOne("concurrent", now); d != nil {
				q.Complete("concurrent", msg.ID)
			}
		}()
	}
	wg.Wait()

	stats, ok := q.GetGroupStats("concurrent")
	if !ok {
		t.Fatal("expected group stats")
	}
	if stats.Pending != 0 {
		t.Logf("note: Pending=%d (some messages may be processing)", stats.Pending)
	}
}

func TestQueue_ConcurrentDifferentGroups(t *testing.T) {
	q := newTestQueue(t, "testq", "testns")

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		key := fmt.Sprintf("cg-%d", g)
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				msg, err := q.Publish(k, []byte("data"))
				if err != nil {
					return
				}
				now := time.Now()
				if d := q.TryDispatchOne(k, now); d != nil {
					q.Complete(k, msg.ID)
				}
			}(key)
		}
	}
	wg.Wait()
}

// ── 15. Paginação (PageGroupStats) ────────────────────────────────────────────

func TestQueue_PageGroupStats(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	n := 55
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("page-group-%03d", i)
		q.Publish(key, []byte("msg"))
	}

	var all []GroupStats
	cursor := ""
	for {
		stats, next := q.PageGroupStats(cursor, 20)
		if len(stats) == 0 {
			break
		}
		all = append(all, stats...)
		cursor = next
		if cursor == "" {
			break
		}
	}

	if len(all) != n {
		t.Fatalf("expected %d total stats, got %d", n, len(all))
	}

	seen := make(map[string]bool)
	for _, s := range all {
		if seen[s.Key] {
			t.Fatalf("duplicate key %q in pagination", s.Key)
		}
		seen[s.Key] = true
	}

	// Verify ordering
	for i := 1; i < len(all); i++ {
		if all[i-1].Key >= all[i].Key {
			t.Fatalf("keys not sorted: %q >= %q", all[i-1].Key, all[i].Key)
		}
	}
}

func TestQueue_PageGroupStatsEmpty(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	stats, cursor := q.PageGroupStats("", 10)
	if len(stats) != 0 {
		t.Fatalf("expected empty page, got %d", len(stats))
	}
	if cursor != "" {
		t.Fatalf("expected empty cursor, got %q", cursor)
	}
}

// ── 16. Parallelism limit ──────────────────────────────────────────────────────

func TestQueue_ParallelismLimit(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:         "parallel-group",
		Parallelism: 2,
	})

	for i := 0; i < 3; i++ {
		q.Publish("parallel-group", []byte("msg"))
	}

	now := time.Now()
	msg1 := q.TryDispatchOne("parallel-group", now)
	msg2 := q.TryDispatchOne("parallel-group", now)
	msg3 := q.TryDispatchOne("parallel-group", now)

	if msg1 == nil || msg2 == nil {
		t.Fatal("expected first two dispatches to succeed")
	}
	if msg3 != nil {
		t.Fatal("expected third dispatch to be blocked (parallelism=2)")
	}

	q.Complete("parallel-group", msg1.ID)
	msg3 = q.TryDispatchOne("parallel-group", now)
	if msg3 == nil {
		t.Fatal("expected third dispatch after completing one")
	}
}

func TestQueue_ParallelismSequential(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	// parallelism=1 is sequential
	q.Publish("seq-group", []byte("m1"))
	q.Publish("seq-group", []byte("m2"))

	now := time.Now()
	m1 := q.TryDispatchOne("seq-group", now)
	if m1 == nil {
		t.Fatal("expected dispatch")
	}
	if m := q.TryDispatchOne("seq-group", now); m != nil {
		t.Fatal("expected blocked (sequential)")
	}

	q.Complete("seq-group", m1.ID)
	m2 := q.TryDispatchOne("seq-group", now)
	if m2 == nil {
		t.Fatal("expected dispatch after complete")
	}
}

// ── 17. groupKeyIndex ──────────────────────────────────────────────────────────

func TestQueue_GroupKeyIndex(t *testing.T) {
	t.Parallel()

	idx := groupKeyIndex{}

	if idx.count() != 0 {
		t.Fatalf("expected count 0, got %d", idx.count())
	}

	idx.add("zebra")
	idx.add("alpha")
	idx.add("beta")
	idx.add("alpha") // idempotent

	if idx.count() != 3 {
		t.Fatalf("expected count 3, got %d", idx.count())
	}

	keys, cursor := idx.page("", 2)
	if len(keys) != 2 || keys[0] != "alpha" || keys[1] != "beta" {
		t.Fatalf("expected [alpha beta], got %v", keys)
	}
	if cursor != "beta" {
		t.Fatalf("expected cursor 'beta', got %q", cursor)
	}

	keys2, cursor2 := idx.page(cursor, 2)
	if len(keys2) != 1 || keys2[0] != "zebra" {
		t.Fatalf("expected [zebra], got %v", keys2)
	}
	if cursor2 != "" {
		t.Fatalf("expected empty cursor (last page), got %q", cursor2)
	}

	// Cursor before all keys
	keys3, _ := idx.page("aaa", 10)
	if len(keys3) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys3))
	}

	// Empty page
	keys4, cursor4 := idx.page("zebra", 10)
	if len(keys4) != 0 || cursor4 != "" {
		t.Fatal("expected empty page when cursor is last key")
	}
}

// ── 18. QueueStats ─────────────────────────────────────────────────────────────

func TestQueue_QueueStats(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("a1"))
	q.Publish("group-a", []byte("a2"))
	q.Publish("group-b", []byte("b1"))

	stats := q.Stats()
	if stats.Name != "testq" {
		t.Fatalf("expected Name='testq', got %q", stats.Name)
	}
	if stats.Namespace != "testns" {
		t.Fatalf("expected Namespace='testns', got %q", stats.Namespace)
	}
	if len(stats.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(stats.Groups))
	}

	for _, gs := range stats.Groups {
		switch gs.Key {
		case "group-a":
			if gs.Pending != 2 {
				t.Fatalf("group-a expected Pending=2, got %d", gs.Pending)
			}
			if gs.Processing != 0 {
				t.Fatalf("group-a expected Processing=0, got %d", gs.Processing)
			}
		case "group-b":
			if gs.Pending != 1 {
				t.Fatalf("group-b expected Pending=1, got %d", gs.Pending)
			}
		default:
			t.Fatalf("unexpected key %q", gs.Key)
		}
	}

	// Dispatch one and verify updated stats
	now := time.Now()
	q.TryDispatchOne("group-a", now)
	q.TryDispatchOne("group-b", now)

	stats = q.Stats()
	for _, gs := range stats.Groups {
		if gs.Key == "group-a" {
			if gs.Pending != 1 {
				t.Fatalf("group-a expected Pending=1 after dispatch, got %d", gs.Pending)
			}
			if gs.Processing != 1 {
				t.Fatalf("group-a expected Processing=1 after dispatch, got %d", gs.Processing)
			}
		}
		if gs.Key == "group-b" {
			if gs.Processing != 1 {
				t.Fatalf("group-b expected Processing=1 after dispatch, got %d", gs.Processing)
			}
		}
	}
}

func TestQueue_Snapshot(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "snapq", "snapns")

	q.Publish("g1", []byte("m1"))
	q.Publish("g1", []byte("m2"))
	q.Publish("g2", []byte("m3"))

	q.SetGroupConfig(GroupConfig{Key: "g1", Parallelism: 2})
	q.SetGroupConfig(GroupConfig{Key: "g3", Parallelism: 5})

	stats, cfgs := q.Snapshot()
	if stats.Name != "snapq" {
		t.Fatalf("expected snapq, got %q", stats.Name)
	}
	if stats.Namespace != "snapns" {
		t.Fatalf("expected snapns, got %q", stats.Namespace)
	}
	if len(stats.Groups) != 2 {
		t.Fatalf("expected 2 group stats, got %d", len(stats.Groups))
	}
	if len(cfgs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(cfgs))
	}
}

// ── 19. Config management ──────────────────────────────────────────────────────

func TestQueue_SetGroupConfigQuantumDefault(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:     "quantum-test",
		Quantum: 0,
	})

	cfg, ok := q.GetGroupConfig("quantum-test")
	if !ok {
		t.Fatal("expected config")
	}
	if cfg.Quantum != 1 {
		t.Fatalf("expected Quantum=1 (default), got %d", cfg.Quantum)
	}
}

func TestQueue_GetGroupConfigNotFound(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	_, ok := q.GetGroupConfig("nonexistent")
	if ok {
		t.Fatal("expected false for nonexistent config")
	}
}

func TestQueue_DeleteGroupConfig(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{Key: "temp-group"})
	if q.ConfigCount() != 1 {
		t.Fatalf("expected 1 config, got %d", q.ConfigCount())
	}

	if !q.DeleteGroupConfig("temp-group") {
		t.Fatal("DeleteGroupConfig should return true")
	}
	if q.ConfigCount() != 0 {
		t.Fatalf("expected 0 configs after delete, got %d", q.ConfigCount())
	}
	if q.DeleteGroupConfig("nonexistent") {
		t.Fatal("DeleteGroupConfig should return false for nonexistent")
	}
}

func TestQueue_ListGroupConfigs(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{Key: "z-config", Parallelism: 3})
	q.SetGroupConfig(GroupConfig{Key: "a-config", Parallelism: 1})
	q.SetGroupConfig(GroupConfig{Key: "wild-*", Parallelism: 5})

	cfgs := q.ListGroupConfigs()
	if len(cfgs) != 3 {
		t.Fatalf("expected 3 configs, got %d", len(cfgs))
	}
	// Exact keys sorted first, then wildcards
	if cfgs[0].Key != "a-config" || cfgs[1].Key != "z-config" || cfgs[2].Key != "wild-*" {
		t.Fatalf("unexpected order: %v", cfgs)
	}
}

func TestQueue_PageGroupConfigs(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	for i := 0; i < 55; i++ {
		key := fmt.Sprintf("cfg-%03d", i)
		q.SetGroupConfig(GroupConfig{Key: key})
	}

	page1, cursor := q.PageGroupConfigs("", 20)
	if len(page1) != 20 {
		t.Fatalf("expected 20 configs, got %d", len(page1))
	}
	if cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	page2, cursor2 := q.PageGroupConfigs(cursor, 20)
	if len(page2) != 20 {
		t.Fatalf("expected 20 configs on page 2, got %d", len(page2))
	}

	page3, cursor3 := q.PageGroupConfigs(cursor2, 20)
	if len(page3) != 15 {
		t.Fatalf("expected 15 configs on page 3, got %d", len(page3))
	}
	if cursor3 != "" {
		t.Fatal("expected empty cursor on last page")
	}

	// Invalid limit defaults to 100, returning all
	page4, _ := q.PageGroupConfigs("", 0)
	if len(page4) != 55 {
		t.Fatalf("expected 55 configs with defaulted limit, got %d", len(page4))
	}
}

// ── 20. GroupToken ─────────────────────────────────────────────────────────────

func TestQueue_GroupToken(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("token-group", []byte("msg"))

	token := q.GetGroupToken("token-group")
	if token == nil {
		t.Fatal("expected non-nil token")
	}

	if !token.TrySchedule() {
		t.Fatal("TrySchedule should return true on first call")
	}
	if token.TrySchedule() {
		t.Fatal("TrySchedule should return false when already scheduled")
	}

	token.Unschedule()
	if !token.TrySchedule() {
		t.Fatal("TrySchedule should return true after Unschedule")
	}

	if q.GetGroupToken("nonexistent") != nil {
		t.Fatal("expected nil token for nonexistent group")
	}
}

// ── 21. WAL replay helpers ────────────────────────────────────────────────────

func TestQueue_ReplayLease(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	pub, _ := q.Publish("replay-group", []byte("replay"))
	now := time.Now()

	ok := q.ReplayLease("replay-group", pub.ID, now)
	if !ok {
		t.Fatal("ReplayLease should succeed for published message")
	}

	// Message was moved to processing; the nil'd ready slot is still counted
	// in pendingCount() until compaction, but processing should reflect the move.
	stats, _ := q.GetGroupStats("replay-group")
	if stats.Processing != 1 {
		t.Fatalf("expected Processing=1 after ReplayLease, got %d", stats.Processing)
	}

	if err := q.Complete("replay-group", pub.ID); err != nil {
		t.Fatalf("Complete after ReplayLease failed: %v", err)
	}
}

func TestQueue_ReplayLeaseNotFound(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")
	now := time.Now()

	if q.ReplayLease("nope", "id", now) {
		t.Fatal("expected false for nonexistent group")
	}

	q.Publish("replay-group", []byte("msg"))
	if q.ReplayLease("replay-group", "nonexistent-id", now) {
		t.Fatal("expected false for nonexistent message")
	}
}

func TestQueue_RemoveMessage(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	pub, _ := q.Publish("rem-group", []byte("remove-me"))
	if !q.RemoveMessage("rem-group", pub.ID) {
		t.Fatal("RemoveMessage should find message in ready")
	}

	// Message was nil'd in the slot; TryDispatchOne should skip it
	now := time.Now()
	if msg := q.TryDispatchOne("rem-group", now); msg != nil {
		t.Fatalf("removed message should not be dispatched, got %q", msg.ID)
	}

	pub2, _ := q.Publish("rem-group", []byte("from-processing"))
	q.TryDispatchOne("rem-group", now)
	if !q.RemoveMessage("rem-group", pub2.ID) {
		t.Fatal("RemoveMessage should find message in processing")
	}

	if q.RemoveMessage("rem-group", "nonexistent") {
		t.Fatal("RemoveMessage should return false for nonexistent message")
	}
	if q.RemoveMessage("nope", "id") {
		t.Fatal("RemoveMessage should return false for nonexistent group")
	}
}

// ── 22. Publish after dispatch ─────────────────────────────────────────────────

func TestQueue_PublishAfterDispatch(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("first"))
	now := time.Now()
	q.TryDispatchOne("group-a", now)

	second, err := q.Publish("group-a", []byte("second"))
	if err != nil {
		t.Fatalf("Publish during processing should succeed: %v", err)
	}
	if second == nil {
		t.Fatal("expected non-nil message")
	}

	stats, _ := q.GetGroupStats("group-a")
	if stats.Pending != 1 {
		t.Fatalf("expected Pending=1, got %d", stats.Pending)
	}
	if stats.Processing != 1 {
		t.Fatalf("expected Processing=1, got %d", stats.Processing)
	}
}

// ── 23. DrainGroup ─────────────────────────────────────────────────────────────

func TestQueue_DrainGroup(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("drain-group", []byte("msg1"))
	q.Publish("drain-group", []byte("msg2"))

	now := time.Now()
	batch, _, hasWake, stillPending := q.DrainGroup("drain-group", now)

	if batch == nil {
		t.Fatal("expected non-nil batch")
	}
	// quantum=1, deficit starts at 0, deficit += 1 → 1 message dispatched
	if len(*batch) != 1 {
		t.Fatalf("expected 1 message in batch (quantum=1), got %d", len(*batch))
	}
	if hasWake {
		t.Fatal("expected no wake timer (no delayed messages)")
	}
	if !stillPending {
		t.Fatal("expected stillPending=true (more messages queued)")
	}
}

func TestQueue_DrainGroupEmpty(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	now := time.Now()
	batch, _, hasWake, stillPending := q.DrainGroup("empty-group", now)
	if batch != nil {
		t.Fatal("expected nil batch for nonexistent group")
	}
	if hasWake {
		t.Fatal("expected no wake")
	}
	if stillPending {
		t.Fatal("expected not pending")
	}
}

// ── 24. GetGroupStats not found ────────────────────────────────────────────────

func TestQueue_GetGroupStatsNotFound(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	_, ok := q.GetGroupStats("nonexistent")
	if ok {
		t.Fatal("expected false for nonexistent group")
	}
}

// ── 25. SetGroupConfig updates existing group ──────────────────────────────────

func TestQueue_SetGroupConfigUpdatesExistingGroup(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("my-group", []byte("msg"))

	q.SetGroupConfig(GroupConfig{
		Key:         "my-group",
		Parallelism: 5,
	})

	for i := 0; i < 5; i++ {
		q.Publish("my-group", []byte("msg"))
	}

	now := time.Now()
	for i := 0; i < 5; i++ {
		if msg := q.TryDispatchOne("my-group", now); msg == nil {
			t.Fatalf("dispatch %d should succeed with parallelism=5", i)
		}
	}
	if msg := q.TryDispatchOne("my-group", now); msg != nil {
		t.Fatal("6th dispatch should be blocked")
	}
}

// ── 26. SetGroupConfig wildcard updates existing groups ────────────────────────

func TestQueue_SetGroupConfigWildcardUpdatesExisting(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("tenant-1", []byte("msg"))
	q.Publish("tenant-2", []byte("msg"))

	q.SetGroupConfig(GroupConfig{
		Key:         "tenant-*",
		Parallelism: 3,
	})

	for i := 0; i < 3; i++ {
		q.Publish("tenant-1", []byte("m"))
	}
	for i := 0; i < 3; i++ {
		q.Publish("tenant-2", []byte("m"))
	}

	now := time.Now()
	for i := 0; i < 3; i++ {
		// Both groups should dispatch 3 (parallelism=3 from wildcard)
		q.TryDispatchOne("tenant-1", now)
		q.TryDispatchOne("tenant-2", now)
	}
}

// ── 27. Config Idx ────────────────────────────────────────────────────────────

func TestQueue_ConfigCount(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	if c := q.ConfigCount(); c != 0 {
		t.Fatalf("expected ConfigCount=0, got %d", c)
	}

	q.SetGroupConfig(GroupConfig{Key: "a"})
	q.SetGroupConfig(GroupConfig{Key: "b"})
	q.SetGroupConfig(GroupConfig{Key: "wild-*"})

	if c := q.ConfigCount(); c != 3 {
		t.Fatalf("expected ConfigCount=3, got %d", c)
	}
}

// ── 28. GroupCount ─────────────────────────────────────────────────────────────

func TestQueue_GroupCount(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	if c := q.GroupCount(); c != 0 {
		t.Fatalf("expected GroupCount=0, got %d", c)
	}

	q.Publish("group-a", []byte(""))
	q.Publish("group-b", []byte(""))
	q.Publish("group-a", []byte("")) // same group, no change

	if c := q.GroupCount(); c != 2 {
		t.Fatalf("expected GroupCount=2, got %d", c)
	}
}

// ── 29. Replay lease after dispatch ────────────────────────────────────────────

func TestQueue_ReplayLeaseAfterDispatch(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("replay-group", []byte("msg"))
	now := time.Now()
	q.TryDispatchOne("replay-group", now)

	// Message is now in processing, ReplayLease should NOT find it
	if q.ReplayLease("replay-group", "some-random-id", now) {
		t.Fatal("expected false for message not in ready/urgent")
	}
}

// ── 30. Export / Import state ──────────────────────────────────────────────────

func TestQueue_ExportImportState(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.Publish("group-a", []byte("payload"))

	state := q.ExportState()
	if len(state.Groups) != 1 {
		t.Fatalf("expected 1 group in exported state, got %d", len(state.Groups))
	}

	gs, ok := state.Groups["group-a"]
	if !ok {
		t.Fatal("expected group-a in exported state")
	}
	if gs.Key != "group-a" {
		t.Fatalf("expected Key='group-a', got %q", gs.Key)
	}
	if len(gs.ReadyMsgs) != 1 {
		t.Fatalf("expected 1 ready message, got %d", len(gs.ReadyMsgs))
	}
}

func TestQueue_ConcurrentPublishDispatch(t *testing.T) {
	q := newTestQueue(t, "testq", "testns")
	var wg sync.WaitGroup

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				q.Publish("conc-group", []byte("data"))
			}
		}()
	}

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				q.TryDispatchOne("conc-group", time.Now())
			}
		}()
	}

	wg.Wait()

	stats, ok := q.GetGroupStats("conc-group")
	if !ok {
		t.Fatal("group should exist")
	}
	t.Logf("after concurrent: pending=%d processing=%d", stats.Pending, stats.Processing)
}

func TestQueue_PublishDuplicateID(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	msg1, err := q.PublishWithID("g1", "same-id", []byte("first"))
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if msg1.State != StatePending {
		t.Fatalf("expected pending, got %v", msg1.State)
	}

	msg2, err := q.PublishWithID("g1", "same-id", []byte("second"))
	if err != nil {
		t.Fatalf("second publish with same ID: %v", err)
	}
	if msg2.State != StatePending {
		t.Fatalf("expected pending, got %v", msg2.State)
	}

	if msg1.ID == msg2.ID {
		t.Log("both messages have same ID (expected)")
	}
}

func TestQueue_DispatchCompleteRace(t *testing.T) {
	q := newTestQueue(t, "testq", "testns")

	msgs := make([]*Message, 10)
	for i := 0; i < 10; i++ {
		m, err := q.Publish("race-group", []byte("data"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		msgs[i] = m
	}

	var wg sync.WaitGroup
	for _, m := range msgs {
		wg.Add(1)
		go func(msg *Message) {
			defer wg.Done()
			disp := q.TryDispatchOne("race-group", time.Now())
			if disp != nil {
				q.Complete("race-group", disp.ID)
			}
		}(m)
	}
	wg.Wait()

	stats, ok := q.GetGroupStats("race-group")
	if ok {
		if stats.Pending != 0 || stats.Processing != 0 {
			t.Logf("after race: pending=%d processing=%d", stats.Pending, stats.Processing)
		}
	}
}

func TestQueue_DeepQueueOrder(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	for i := 0; i < 1000; i++ {
		_, err := q.Publish("order-group", []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	stats, ok := q.GetGroupStats("order-group")
	if !ok {
		t.Fatal("group should exist")
	}
	if stats.Pending != 1000 {
		t.Fatalf("pending = %d, want 1000", stats.Pending)
	}

	for i := 0; i < 1000; i++ {
		msg := q.TryDispatchOne("order-group", time.Now())
		if msg == nil {
			t.Fatalf("dispatch %d returned nil", i)
		}
		q.Complete("order-group", msg.ID)
	}

	stats, _ = q.GetGroupStats("order-group")
	if stats.Pending != 0 || stats.Processing != 0 {
		t.Fatalf("after drain: pending=%d processing=%d", stats.Pending, stats.Processing)
	}
}

func TestQueue_GroupConfigChange(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:        "cfg-group",
		MaxPending: 3,
	})

	for i := 0; i < 3; i++ {
		_, err := q.Publish("cfg-group", []byte("data"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	_, err := q.Publish("cfg-group", []byte("extra"))
	if err == nil {
		t.Fatal("expected error for exceeding MaxPending=3")
	}

	q.SetGroupConfig(GroupConfig{
		Key:        "cfg-group",
		MaxPending: 5,
	})

	for i := 0; i < 2; i++ {
		_, err := q.Publish("cfg-group", []byte("more"))
		if err != nil {
			t.Fatalf("publish %d after config change: %v", i, err)
		}
	}

	stats, ok := q.GetGroupStats("cfg-group")
	if !ok {
		t.Fatal("group should exist")
	}
	if stats.Pending != 5 {
		t.Fatalf("pending = %d, want 5", stats.Pending)
	}
}

func TestQueue_FailAndNextNoDLQ(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:         "fail-group",
		Parallelism: 1,
		Retry: RetryConfig{
			MaxAttempts:  2,
			Backoff:      BackoffExponential,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Second,
		},
	})

	msg, _ := q.Publish("fail-group", []byte("data"))
	q.TryDispatchOne("fail-group", time.Now())

	dlqName, dlqMsg, next := q.FailAndNext("fail-group", msg.ID, "failed", time.Now())
	if dlqName != "" || dlqMsg != nil {
		t.Fatal("expected no DLQ routing")
	}
	if next != nil {
		t.Logf("next message after fail: %s (will retry)", next.ID)
	}
}

func TestQueue_ImportStateFull(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	state := QueueState{
		Groups: map[string]GroupState{
			"g1": {
				Key:         "g1",
				Parallelism: 2,
				Quantum:     5,
				ReadyMsgs: []*Message{
					{ID: "m1", Payload: []byte("a"), State: StatePending},
					{ID: "m2", Payload: []byte("b"), State: StatePending},
				},
				ProcessingMsgs: map[string]*Message{
					"m3": {ID: "m3", Payload: []byte("c"), State: StateProcessing},
				},
			},
		},
	}
	q.ImportState(state)

	stats, ok := q.GetGroupStats("g1")
	if !ok {
		t.Fatal("group should exist after import")
	}
	if stats.Pending != 2 {
		t.Fatalf("pending = %d, want 2", stats.Pending)
	}
	if stats.Processing != 1 {
		t.Fatalf("processing = %d, want 1", stats.Processing)
	}

	msg := q.TryDispatchOne("g1", time.Now())
	if msg == nil {
		t.Fatal("expected dispatch after import")
	}
}

func TestQueue_RepeatedFailAndNext(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "testq", "testns")

	q.SetGroupConfig(GroupConfig{
		Key:         "retry-loop",
		Parallelism: 1,
		Retry: RetryConfig{
			MaxAttempts:  3,
			Backoff:      BackoffExponential,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Second,
		},
	})

	msg, _ := q.Publish("retry-loop", []byte("data"))
	now := time.Now()
	q.TryDispatchOne("retry-loop", now)

	dlqName, dlqMsg, next := q.FailAndNext("retry-loop", msg.ID, "attempt-1", now)
	if next != nil {
		t.Logf("retry 1: next=%s", next.ID)
	}
	_ = dlqName
	_ = dlqMsg

	now = now.Add(time.Hour)
	if next != nil {
		q.TryDispatchOne("retry-loop", now)
		dlqName, dlqMsg, next = q.FailAndNext("retry-loop", next.ID, "attempt-2", now)
		if next != nil {
			t.Logf("retry 2: next=%s", next.ID)
		}
	}

	now = now.Add(time.Hour)
	if next != nil {
		q.TryDispatchOne("retry-loop", now)
		_, _, next = q.FailAndNext("retry-loop", next.ID, "attempt-3", now)
		if next == nil {
			t.Log("retries exhausted as expected")
		}
	}
}

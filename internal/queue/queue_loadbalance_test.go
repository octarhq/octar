package queue

import (
	"testing"
	"time"
)

func TestQueue_MultiGroupDRR_RoundRobin(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "drr-rr", "testns")

	groupCount := 10
	msgsPerGroup := 5

	// Publish 5 messages to each of 10 groups.
	for i := range groupCount {
		key := groupKey(i)
		q.SetGroupConfig(GroupConfig{
			Key:         key,
			Parallelism: msgsPerGroup, // allow draining all messages without blocking
			Quantum:     1,
		})
		for range msgsPerGroup {
			if _, err := q.Publish(key, []byte("payload")); err != nil {
				t.Fatalf("Publish %s: %v", key, err)
			}
		}
	}

	// Drain in round‑robin: each group gets 1 msg per tick (quantum=1).
	now := time.Now()
	totalDrained := 0
	for round := 0; round < msgsPerGroup; round++ {
		for i := range groupCount {
			key := groupKey(i)

			batch, _, _, stillPending := q.DrainGroup(key, now)
			if batch == nil {
				t.Fatalf("round %d, group %s: expected non-nil batch", round, key)
			}
			if len(*batch) != 1 {
				t.Fatalf("round %d, group %s: expected 1 msg, got %d", round, key, len(*batch))
			}
			// Last round should have no more pending.
			if round == msgsPerGroup-1 && stillPending {
				t.Fatalf("round %d, group %s: expected no more pending", round, key)
			}
			totalDrained++
		}
	}

	if totalDrained != groupCount*msgsPerGroup {
		t.Fatalf("expected %d drained, got %d", groupCount*msgsPerGroup, totalDrained)
	}
}

func TestQueue_MultiGroupDRR_QuantumFive(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "drr-q5", "testns")

	key := "q5-group"
	q.SetGroupConfig(GroupConfig{
		Key:         key,
		Parallelism: 12,
		Quantum:     5,
	})

	for range 12 {
		if _, err := q.Publish(key, []byte("p")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	now := time.Now()

	// First drain: deficit += 5 → 5 messages.
	batch, _, _, _ := q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 5 {
		t.Fatalf("expected 5 msgs (quantum=5), got %d", len(*batch))
	}

	// Second drain: deficit = 0, adds 5 → 5 messages.
	batch, _, _, _ = q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 5 {
		t.Fatalf("expected 5 msgs (quantum=5) on second drain, got %d", len(*batch))
	}

	// Third drain: deficit = 0, adds 5, but only 2 remaining.
	batch, _, _, _ = q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 2 {
		t.Fatalf("expected 2 msgs (only 2 left), got %d", len(*batch))
	}

	// Fourth drain: empty.
	batch, _, _, _ = q.DrainGroup(key, now)
	if batch != nil {
		t.Fatalf("expected nil batch (empty), got %d msgs", len(*batch))
	}
}

func TestQueue_MultiGroupDRR_DeficitCarryover(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "drr-deficit", "testns")

	key := "deficit-group"
	q.SetGroupConfig(GroupConfig{
		Key:         key,
		Parallelism: 10,
		Quantum:     3,
	})

	// Publish only 1 message — quantum=3 but only 1 available.
	for range 1 {
		if _, err := q.Publish(key, []byte("p")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	now := time.Now()

	// First drain: deficit += 3, 1 msg available → drain 1, deficit = 2.
	batch, _, _, stillPending := q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 1 {
		t.Fatalf("expected 1 msg, got %d", len(*batch))
	}
	if stillPending {
		t.Fatal("expected no more pending")
	}

	// No new messages published.
	// Second drain: deficit = 2 + quantum(3) = 5, but nothing to drain.
	batch, _, _, _ = q.DrainGroup(key, now)
	if batch != nil {
		t.Fatalf("expected nil batch (nothing to drain), got %d", len(*batch))
	}

	// Publish 3 messages.
	for range 3 {
		if _, err := q.Publish(key, []byte("p")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Third drain: deficit = 5 + 3 = 8, drain 3.
	batch, _, _, _ = q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 3 {
		t.Fatalf("expected 3 msgs, got %d", len(*batch))
	}
}

func TestQueue_TryDispatchOne_MultiGroup(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "tdo-multi", "testns")

	groupCount := 10
	ooc := make([]int, groupCount) // out-of-chest count per group

	// Set parallelism high so TryDispatchOne isn't blocked.
	for i := range groupCount {
		q.SetGroupConfig(GroupConfig{
			Key:         groupKey(i),
			Parallelism: 10,
		})
	}

	// Publish 5 messages per group.
	for i := range groupCount {
		key := groupKey(i)
		for range 5 {
			if _, err := q.Publish(key, []byte("p")); err != nil {
				t.Fatalf("Publish %s: %v", key, err)
			}
		}
	}

	now := time.Now()
	// TryDispatchOne across all groups in round-robin fashion.
	for round := 0; round < 5; round++ {
		for i := range groupCount {
			msg := q.TryDispatchOne(groupKey(i), now)
			if msg == nil {
				t.Fatalf("round %d, group %d: expected message", round, i)
			}
			ooc[i]++
		}
	}

	// Verify each group got exactly 5 messages.
	for i, count := range ooc {
		if count != 5 {
			t.Errorf("group %d: expected 5 dispatched, got %d", i, count)
		}
	}
}

func TestQueue_TryDispatchOne_MultiGroup_MixedQuantum(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "tdo-mixed", "testns")

	// Group A: quantum=1, Group B: quantum=3.
	q.SetGroupConfig(GroupConfig{Key: "group-a", Quantum: 1, Parallelism: 10})
	q.SetGroupConfig(GroupConfig{Key: "group-b", Quantum: 3, Parallelism: 10})

	for range 10 {
		_, _ = q.Publish("group-a", []byte("a"))
		_, _ = q.Publish("group-b", []byte("b"))
	}

	// TryDispatchOne drains one at a time, not respecting quantum
	// (that's what DrainGroup / drainRound does). This test just verifies
	// both groups dispatch correctly.
	now := time.Now()

	groupADispatched := 0
	groupBDispatched := 0
	for range 20 {
		msg := q.TryDispatchOne("group-a", now)
		if msg != nil {
			groupADispatched++
		}
		msg = q.TryDispatchOne("group-b", now)
		if msg != nil {
			groupBDispatched++
		}
	}

	if groupADispatched != 10 {
		t.Errorf("group-a: expected 10, got %d", groupADispatched)
	}
	if groupBDispatched != 10 {
		t.Errorf("group-b: expected 10, got %d", groupBDispatched)
	}
}

func TestQueue_MultiGroupDRR_NoisyNeighborFairness(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "drr-noisy", "testns")

	// Group A has 1000 messages; groups B and C have 1 each.
	// DRR must ensure quiet groups get serviced before noisy-a dominates.
	q.SetGroupConfig(GroupConfig{Key: "noisy-a", Quantum: 1, Parallelism: 2000})
	q.SetGroupConfig(GroupConfig{Key: "quiet-b", Quantum: 1, Parallelism: 100})
	q.SetGroupConfig(GroupConfig{Key: "quiet-c", Quantum: 1, Parallelism: 100})

	for range 1000 {
		_, _ = q.Publish("noisy-a", []byte("noisy"))
	}
	_, _ = q.Publish("quiet-b", []byte("b"))
	_, _ = q.Publish("quiet-c", []byte("c"))

	now := time.Now()

	// Round 1: drain all three groups in round-robin order.
	// Each gets 1 msg (quantum=1). After this, quiet-b and quiet-c are empty.
	for _, key := range []string{"noisy-a", "quiet-b", "quiet-c"} {
		batch, _, _, _ := q.DrainGroup(key, now)
		if batch == nil {
			t.Fatalf("round 1, group %s: expected non-nil batch", key)
		}
		if len(*batch) != 1 {
			t.Fatalf("round 1, group %s: expected 1 msg (quantum=1), got %d", key, len(*batch))
		}
	}

	// Rounds 2+: only noisy-a still has messages (999 remaining).
	// Each drain yields exactly 1 msg (quantum=1).
	for range 999 {
		batch, _, _, _ := q.DrainGroup("noisy-a", now)
		if batch == nil || len(*batch) != 1 {
			t.Fatalf("expected 1 msg from noisy-a, got %d", safeLen(batch))
		}
	}

	// All groups exhausted.
	for _, key := range []string{"noisy-a", "quiet-b", "quiet-c"} {
		batch, _, _, _ := q.DrainGroup(key, now)
		if batch != nil {
			t.Fatalf("group %s: expected nil (empty), got %d msgs", key, len(*batch))
		}
	}
}

func safeLen(batch *[]*Message) int {
	if batch == nil {
		return 0
	}
	return len(*batch)
}

func TestQueue_DrainGroup_QuantumWithConcurrentPublish(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t, "drain-conc", "testns")

	key := "conc-group"
	q.SetGroupConfig(GroupConfig{Key: key, Quantum: 5, Parallelism: 10})

	// Publish 3 upfront.
	for range 3 {
		_, _ = q.Publish(key, []byte("pre"))
	}

	now := time.Now()

	// Drain: deficit += 5, drain 3 (only 3 available), deficit = 2.
	batch, _, _, _ := q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 3 {
		t.Fatalf("expected 3 msgs (only 3 available), got %d", len(*batch))
	}

	// Publish 3 more while deficit=2.
	for range 3 {
		_, _ = q.Publish(key, []byte("post"))
	}

	// Drain: deficit = 2 + quantum(5) = 7, drain 3 (only 3 pending), deficit = 4.
	batch, _, _, _ = q.DrainGroup(key, now)
	if batch == nil || len(*batch) != 3 {
		t.Fatalf("expected 3 msgs (3 published), got %d", len(*batch))
	}
}

func groupKey(i int) string {
	return "group-" + string(rune('a'+i))
}

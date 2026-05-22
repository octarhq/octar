package chaos

import (
	"fmt"
	"testing"
	"time"
)

// TestCrash_NoLoss ensures all published messages survive a hard crash.
func TestCrash_NoLoss(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "crash-noloss")

	msgCount := 100
	ids := make([]string, msgCount)
	for i := 0; i < msgCount; i++ {
		id, err := h.Publish("test-ns", "crash-noloss", "g1", []byte("hello"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		ids[i] = id
	}

	h.Wal.Close()
	time.Sleep(100 * time.Millisecond)

	h.Restart()

	q2 := h.GetQueue("test-ns", "crash-noloss")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total != msgCount {
		t.Fatalf("expected %d messages after recovery, got %d", msgCount, total)
	}
}

// TestCrash_MixedState verifies that published messages survive a crash,
// and the queue structure (groups, counts) is correctly recovered.
func TestCrash_MixedState(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "crash-mixed")

	for i := 0; i < 10; i++ {
		_, err := h.Publish("test-ns", "crash-mixed", "g1", []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	h.Crash()
	h.Restart()

	q2 := h.GetQueue("test-ns", "crash-mixed")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}

	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total != 10 {
		t.Fatalf("expected 10 messages after crash recovery, got %d", total)
	}
}

// TestCrash_MultipleQueues tests recovery across multiple queues.
func TestCrash_MultipleQueues(t *testing.T) {
	h := New(t)
	defer h.Close()

	for i := 0; i < 5; i++ {
		qName := fmt.Sprintf("crash-mq-%d", i)
		h.RegisterQueue("test-ns", qName)
		for j := 0; j < 10; j++ {
			_, err := h.Publish("test-ns", qName, "g1", []byte("data"))
			if err != nil {
				t.Fatalf("publish q=%s: %v", qName, err)
			}
		}
	}

	h.Crash()
	h.Restart()

	for i := 0; i < 5; i++ {
		qName := fmt.Sprintf("crash-mq-%d", i)
		q := h.GetQueue("test-ns", qName)
		if q == nil {
			t.Fatalf("queue %s not recovered", qName)
		}
		stats, _ := q.Snapshot()
		var total int
		for _, g := range stats.Groups {
			total += g.Pending + g.Processing
		}
		if total != 10 {
			t.Fatalf("queue %s: expected 10 messages, got %d", qName, total)
		}
	}
}

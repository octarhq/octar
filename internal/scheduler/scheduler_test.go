package scheduler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/queue"
)

func TestRegisterGetQueue(t *testing.T) {
	s := NewScheduler()
	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	got := s.GetQueue("test-ns", "test-q")
	if got != q {
		t.Fatal("expected registered queue, got nil")
	}
}

func TestRegisterGetQueue_NilOnUnknown(t *testing.T) {
	s := NewScheduler()
	got := s.GetQueue("nonexistent", "missing")
	if got != nil {
		t.Fatal("expected nil for unknown queue")
	}
}

func TestUnregisterQueue(t *testing.T) {
	s := NewScheduler()
	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	s.UnregisterQueue("test-ns", "test-q")
	got := s.GetQueue("test-ns", "test-q")
	if got != nil {
		t.Fatal("expected nil after unregister")
	}
}

func TestUnregisterQueue_Nonexistent(t *testing.T) {
	s := NewScheduler()
	s.UnregisterQueue("nonexistent", "missing")
}

func TestListQueues(t *testing.T) {
	s := NewScheduler()

	queues := s.ListQueues()
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues, got %d", len(queues))
	}

	q1 := queue.NewQueue("q1", "ns")
	q2 := queue.NewQueue("q2", "ns")
	q3 := queue.NewQueue("q3", "ns")
	s.RegisterQueue(q1)
	s.RegisterQueue(q2)
	s.RegisterQueue(q3)

	queues = s.ListQueues()
	if len(queues) != 3 {
		t.Fatalf("expected 3 queues, got %d", len(queues))
	}
}

func TestActivate_NonexistentGroup(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	dispatchCalled := false
	s.Run(func(msg *queue.Message) bool {
		dispatchCalled = true
		return true
	})

	s.Activate(q, "nonexistent-group")
	time.Sleep(50 * time.Millisecond)
	if dispatchCalled {
		t.Fatal("dispatch should not be called for nonexistent group")
	}
}

func TestActivate_NilQueue(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	s.Run(func(msg *queue.Message) bool { return true })
	s.Activate(nil, "g1")
}

func TestActivate_EmptyGroupKey(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	s.Run(func(msg *queue.Message) bool { return true })
	s.Activate(q, "")
}

func TestActivateDispatch(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	published, err := q.Publish("group-1", []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	dispatched := make(chan *queue.Message, 1)
	s.Run(func(msg *queue.Message) bool {
		dispatched <- msg
		return true
	})

	s.Activate(q, "group-1")

	select {
	case msg := <-dispatched:
		if msg.ID != published.ID {
			t.Fatalf("wrong message dispatched: got %s, want %s", msg.ID, published.ID)
		}
		if string(msg.Payload) != "hello world" {
			t.Fatalf("wrong payload: got %s, want %s", string(msg.Payload), "hello world")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for dispatch")
	}
}

func TestActivateDispatch_Backpressure(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	_, _ = q.Publish("group-1", []byte("m1"))
	_, _ = q.Publish("group-1", []byte("m2"))

	dispatched := make(chan *queue.Message, 5)
	s.Run(func(msg *queue.Message) bool {
		dispatched <- msg
		return false
	})

	s.Activate(q, "group-1")

	select {
	case msg := <-dispatched:
		if string(msg.Payload) != "m1" {
			t.Fatalf("expected first message, got %s", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first dispatch")
	}

	select {
	case <-dispatched:
		t.Fatal("second message should not be dispatched after backpressure")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMultipleQueues(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q1 := queue.NewQueue("q1", "ns")
	q2 := queue.NewQueue("q2", "ns")
	q3 := queue.NewQueue("q3", "ns")
	s.RegisterQueue(q1)
	s.RegisterQueue(q2)
	s.RegisterQueue(q3)

	_, _ = q1.Publish("g1", []byte("from-q1"))
	_, _ = q2.Publish("g1", []byte("from-q2"))
	_, _ = q3.Publish("g1", []byte("from-q3"))

	var mu sync.Mutex
	var dispatched []string

	s.Run(func(msg *queue.Message) bool {
		mu.Lock()
		dispatched = append(dispatched, msg.QueueName)
		mu.Unlock()
		return true
	})

	s.Activate(q1, "g1")
	s.Activate(q2, "g1")
	s.Activate(q3, "g1")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if len(dispatched) != 3 {
		t.Fatalf("expected 3 dispatches, got %d: %v", len(dispatched), dispatched)
	}
	mu.Unlock()
}

func TestRunStop(t *testing.T) {
	s := NewScheduler()

	s.Run(func(msg *queue.Message) bool { return true })
	s.Stop()
}

func TestRunOnlyOnce(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	var calls atomic.Int64
	s.Run(func(msg *queue.Message) bool {
		calls.Add(1)
		return true
	})
	s.Run(func(msg *queue.Message) bool {
		calls.Add(1)
		return true
	})

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)
	_, _ = q.Publish("g1", []byte("data"))
	s.Activate(q, "g1")

	time.Sleep(100 * time.Millisecond)

	if got := calls.Load(); got != 1 {
		t.Fatalf("dispatch should be called exactly once, got %d calls", got)
	}
}

func TestStopIdempotent(t *testing.T) {
	s := NewScheduler()
	s.Run(func(msg *queue.Message) bool { return true })
	s.Stop()
	s.Stop()
}

func TestMetrics(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	stats := s.Metrics()
	if stats.RegisteredQueues != 0 {
		t.Fatalf("expected 0 registered queues, got %d", stats.RegisteredQueues)
	}
	if stats.WorkerCount != 0 {
		t.Fatalf("expected 0 workers, got %d", stats.WorkerCount)
	}

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)
	_, _ = q.Publish("g1", []byte("data"))

	dispatched := make(chan struct{}, 1)
	s.Run(func(msg *queue.Message) bool {
		dispatched <- struct{}{}
		return true
	})

	s.Activate(q, "g1")

	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	stats = s.Metrics()
	if stats.RegisteredQueues != 1 {
		t.Fatalf("expected 1 registered queue, got %d", stats.RegisteredQueues)
	}
	if stats.ActivationsTotal != 1 {
		t.Fatalf("expected 1 activation total, got %d", stats.ActivationsTotal)
	}
}

func TestConcurrentRegisterActivate(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	var dispatched atomic.Int64
	s.Run(func(msg *queue.Message) bool {
		dispatched.Add(1)
		return true
	})

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			q := queue.NewQueue("q", "ns")
			s.RegisterQueue(q)
			_, _ = q.Publish("g1", []byte("data"))
			s.Activate(q, "g1")
			_ = n
		}(i)
	}
	wg.Wait()

	time.Sleep(200 * time.Millisecond)

	n := dispatched.Load()
	if n == 0 {
		t.Fatal("expected at least some dispatches")
	}
}

func TestConcurrentActivateSameGroup(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	for range 1000 {
		_, _ = q.Publish("g1", []byte("data"))
	}

	var dispatched atomic.Int64
	s.Run(func(msg *queue.Message) bool {
		dispatched.Add(1)
		return true
	})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				s.Activate(q, "g1")
			}
		}()
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)

	n := dispatched.Load()
	if n == 0 {
		t.Fatal("expected dispatches")
	}
	t.Logf("dispatched %d messages from concurrent activations", n)
}

func TestScheduler_QueueKeyCollision(t *testing.T) {
	s := NewScheduler()

	q1 := queue.NewQueue("same", "ns1")
	q2 := queue.NewQueue("same", "ns2")
	s.RegisterQueue(q1)
	s.RegisterQueue(q2)

	got1 := s.GetQueue("ns1", "same")
	got2 := s.GetQueue("ns2", "same")
	if got1 != q1 {
		t.Fatal("expected q1 for ns1/same")
	}
	if got2 != q2 {
		t.Fatal("expected q2 for ns2/same")
	}
}

func TestScheduler_ActivateBeforeRun(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)
	_, _ = q.Publish("g1", []byte("data"))

	s.Activate(q, "g1")

	dispatched := make(chan *queue.Message, 1)
	s.Run(func(msg *queue.Message) bool {
		dispatched <- msg
		return true
	})

	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("timeout — activation before Run should be processed after Run")
	}
}

func TestScheduler_MultipleGroupsSameQueue(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	_, _ = q.Publish("g1", []byte("from-g1"))
	_, _ = q.Publish("g2", []byte("from-g2"))
	_, _ = q.Publish("g3", []byte("from-g3"))

	var mu sync.Mutex
	dispatched := make(map[string]int)

	s.Run(func(msg *queue.Message) bool {
		mu.Lock()
		dispatched[msg.GroupKey]++
		mu.Unlock()
		return true
	})

	s.Activate(q, "g1")
	s.Activate(q, "g2")
	s.Activate(q, "g3")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if len(dispatched) != 3 {
		t.Fatalf("expected 3 groups dispatched, got %d: %v", len(dispatched), dispatched)
	}
	for _, g := range []string{"g1", "g2", "g3"} {
		if dispatched[g] != 1 {
			t.Fatalf("group %s dispatched %d times, expected 1", g, dispatched[g])
		}
	}
	mu.Unlock()
}

func TestScheduler_WakeAfterRetry(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	q := queue.NewQueue("test-q", "test-ns")
	s.RegisterQueue(q)

	q.SetGroupConfig(queue.GroupConfig{
		Key:          "g1",
		Parallelism:  1,
		Quantum:      1,
		LeaseTimeout: time.Minute,
		Retry: queue.RetryConfig{
			MaxAttempts:  3,
			Backoff:      queue.BackoffFixed,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     30 * time.Second,
		},
	})

	_, _ = q.Publish("g1", []byte("data"))

	dispatched := make(chan *queue.Message, 10)
	s.Run(func(msg *queue.Message) bool {
		dispatched <- msg
		return true
	})

	s.Activate(q, "g1")

	select {
	case msg := <-dispatched:
		_, _, _ = q.Fail("g1", msg.ID, "test error")
		s.Activate(q, "g1")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first dispatch")
	}

	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for re-dispatch after retry")
	}
}

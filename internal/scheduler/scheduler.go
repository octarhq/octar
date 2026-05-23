package scheduler

import (
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/xtime"
)

// DispatchFunc is called by the scheduler for each message it decides to send.
// Returns false to signal backpressure; the scheduler returns the message to
// pending and stops draining the group for this activation.
type DispatchFunc func(msg *queue.Message) bool

// SchedulerStats is a point-in-time snapshot of scheduler internals.
type SchedulerStats struct {
	RegisteredQueues     int           `json:"registered_queues"`
	ActivationQueueDepth int           `json:"activation_queue_depth"`
	ActiveGroups         int           `json:"active_groups"`
	WorkerCount          int           `json:"worker_count"`
	BusyWorkers          int           `json:"busy_workers"`
	WorkerUtilization    float64       `json:"worker_utilization"`
	ActivationsTotal     uint64        `json:"activations_total"`
	ActivationsPerSec    float64       `json:"activations_per_sec"`
	ActivationLatencyAvg time.Duration `json:"activation_latency_avg"`
	ActivationLatencyMax time.Duration `json:"activation_latency_max"`
	Uptime               time.Duration `json:"uptime"`
}

// groupActivation is the unit of work queued to a scheduler worker.
type groupActivation struct {
	q          *queue.Queue
	groupKey   string
	token      *queue.GroupToken // scheduling handles without exposing group internals
	enqueuedAt time.Time
}

// Scheduler is event-driven: it does no polling. Groups are activated by
// publishers (on Publish), consumers (on ACK/NACK), and the timing wheel
// (on retry backoff expiry). Each activation drains as many messages as
// the group's quantum allows in one shot.
type Scheduler struct {
	mu         sync.RWMutex
	queues     map[string]*queue.Queue
	active     chan groupActivation
	wheel      *TimingWheel
	dispatch   DispatchFunc
	stop       chan struct{}
	stopOnce   sync.Once
	workerOnce sync.Once
	startedAt  time.Time

	// Cache-line padded atomics to prevent false sharing between scheduler workers.
	workerCount int32
	_           [15]int32
	busyWorkers int32
	_           [15]int32

	activationsTotal            uint64
	_                           [7]uint64
	activationLatencyTotalNanos uint64
	_                           [7]uint64
	activationLatencyMaxNanos   uint64
	_                           [7]uint64
	busyTimeTotalNanos          uint64
	_                           [7]uint64

	logger *slog.Logger
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		queues: make(map[string]*queue.Queue),
		// 65536-slot MPSC channel: large enough to absorb burst activations from
		// 16+ publishers without blocking, without unbounded memory growth.
		active: make(chan groupActivation, 65536),
		wheel:  NewTimingWheel(10*time.Millisecond, 65536),
		stop:   make(chan struct{}),
		logger: slog.Default().With("component", "scheduler"),
	}
}

// RegisterQueue makes the scheduler aware of a queue.
func (s *Scheduler) RegisterQueue(q *queue.Queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[queueKey(q.Namespace, q.Name)] = q
	s.logger.Info("queue registered", "queue", q.Name, "namespace", q.Namespace)
}

// UnregisterQueue removes a queue from scheduling.
func (s *Scheduler) UnregisterQueue(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.queues, queueKey(namespace, name))
	s.logger.Info("queue unregistered", "queue", name, "namespace", namespace)
}

// GetQueue returns a registered queue by namespace and name, or nil.
func (s *Scheduler) GetQueue(namespace, name string) *queue.Queue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queues[queueKey(namespace, name)]
}

// ListQueues returns all registered queues.
func (s *Scheduler) ListQueues() []*queue.Queue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*queue.Queue, 0, len(s.queues))
	for _, q := range s.queues {
		out = append(out, q)
	}
	return out
}

// Run starts the scheduling workers. Safe to call once.
func (s *Scheduler) Run(dispatch DispatchFunc) {
	s.dispatch = dispatch
	s.workerOnce.Do(func() {
		s.mu.Lock()
		if s.startedAt.IsZero() {
			s.startedAt = xtime.Now()
		}
		s.mu.Unlock()

		n := runtime.GOMAXPROCS(0)
		if n < 1 {
			n = 1
		}
		atomic.StoreInt32(&s.workerCount, int32(n))
		for i := 0; i < n; i++ {
			go s.worker()
		}
	})
	s.logger.Info("scheduler running")
}

// Activate enqueues a group for dispatch if it is not already queued.
//
// Why CAS instead of a mutex?
// At 60K msg/s with 16 publishers all calling Activate on the same group,
// a mutex would serialise all callers. CAS lets exactly one caller enqueue
// the group; the rest return immediately in O(1) with no contention.
func (s *Scheduler) Activate(q *queue.Queue, groupKey string) {
	if q == nil || groupKey == "" {
		return
	}
	token := q.GetGroupToken(groupKey)
	if token == nil {
		return
	}
	if token.TrySchedule() {
		atomic.AddUint64(&s.activationsTotal, 1)
		s.logger.Debug("group activated", "queue", q.Name, "namespace", q.Namespace, "group", groupKey)
		select {
		case s.active <- groupActivation{q: q, groupKey: groupKey, token: token, enqueuedAt: xtime.Now()}:
		default:
			// Channel full (safety valve): reset so the group can be re-queued.
			token.Unschedule()
			s.logger.Warn("scheduler active channel full, dropping activation", "group", groupKey)
		}
	}
}

// Stop shuts down the scheduling workers and the timing wheel.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.wheel.Stop()
	})
	s.logger.Info("scheduler stopped")
}

func (s *Scheduler) worker() {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("scheduler worker panicked", "panic", r)
			// Restart the worker — losing one worker reduces throughput but
			// the broker keeps running.
			go s.worker()
		}
	}()

	for {
		select {
		case a := <-s.active:
			started := xtime.Now()
			atomic.AddInt32(&s.busyWorkers, 1)
			s.processActivation(a)
			atomic.AddInt32(&s.busyWorkers, -1)
			atomic.AddUint64(&s.busyTimeTotalNanos, uint64(time.Since(started).Nanoseconds()))
		case <-s.stop:
			return
		}
	}
}

// processActivation drains one group and decides what happens next.
//
// Why reset the scheduling flag BEFORE draining?
// If we reset it after, a publisher that arrives mid-drain cannot re-queue the
// group (it sees flag=1 and skips). Resetting before means any concurrent
// publisher can enqueue a new activation — if new messages arrive they will be
// picked up on the next pass rather than silently lost.
func (s *Scheduler) processActivation(a groupActivation) {
	now := xtime.Now()

	latencyNs := uint64(now.Sub(a.enqueuedAt).Nanoseconds())
	atomic.AddUint64(&s.activationLatencyTotalNanos, latencyNs)
	for {
		cur := atomic.LoadUint64(&s.activationLatencyMaxNanos)
		if latencyNs <= cur {
			break
		}
		if atomic.CompareAndSwapUint64(&s.activationLatencyMaxNanos, cur, latencyNs) {
			break
		}
	}

	a.token.Unschedule() // reset before drain — see comment above

	ready, wakeAt, hasWake, stillPending := a.q.DrainGroup(a.groupKey, now)
	s.logger.Debug("processed activation",
		"queue", a.q.Name,
		"namespace", a.q.Namespace,
		"group", a.groupKey,
		"ready_count", func() int {
			if ready == nil {
				return 0
			}
			return len(*ready)
		}(),
		"has_wake", hasWake,
		"still_pending", stillPending,
		"wake_at", wakeAt,
	)

	if ready != nil {
		delivered := true
		for i, msg := range *ready {
			if s.dispatch != nil && !s.dispatch(msg) {
				delivered = false
				for j := i; j < len(*ready); j++ {
					a.q.ReturnToPending(a.groupKey, (*ready)[j].ID)
				}
				break
			}
		}
		queue.PutMessageBatch(ready)
		if !delivered {
			return
		}
	}

	if hasWake {
		s.scheduleWake(a.q, a.groupKey, wakeAt)
		return
	}
	if stillPending {
		s.Activate(a.q, a.groupKey)
	}
}

// scheduleWake registers a timing wheel callback to re-activate the group when
// its earliest delayed message becomes eligible.
//
// The CAS on wakeScheduledAt prevents duplicate timers: if an earlier wakeup
// is already registered, a later one is ignored. When the timer fires it clears
// the field so new timers can be registered.
func (s *Scheduler) scheduleWake(q *queue.Queue, groupKey string, wakeAt time.Time) {
	token := q.GetGroupToken(groupKey)
	if token == nil {
		return
	}

	wakeNs := wakeAt.UnixNano()
	for {
		curr := token.LoadWake()
		if curr != 0 && curr <= wakeNs {
			return // an earlier or equal wake is already scheduled
		}
		if token.TrySetWake(curr, wakeNs) {
			break
		}
	}

	delay := time.Until(wakeAt)
	if delay < 0 {
		delay = 0
	}
	s.wheel.Add(delay, func() {
		token.ClearWake(wakeNs)
		s.Activate(q, groupKey)
	})
}

// Metrics returns a point-in-time view of scheduler internals.
func (s *Scheduler) Metrics() SchedulerStats {
	now := xtime.Now()
	s.mu.RLock()
	nQueues := len(s.queues)
	startedAt := s.startedAt
	s.mu.RUnlock()

	if startedAt.IsZero() {
		startedAt = now
	}
	uptime := now.Sub(startedAt)
	if uptime < 0 {
		uptime = 0
	}

	wc := int(atomic.LoadInt32(&s.workerCount))
	bw := int(atomic.LoadInt32(&s.busyWorkers))
	total := atomic.LoadUint64(&s.activationsTotal)
	latTotal := atomic.LoadUint64(&s.activationLatencyTotalNanos)
	latMax := atomic.LoadUint64(&s.activationLatencyMaxNanos)
	busyNs := atomic.LoadUint64(&s.busyTimeTotalNanos)

	var util float64
	if wc > 0 && uptime > 0 {
		util = float64(busyNs) / float64(uptime.Nanoseconds()*int64(wc))
	}
	var aps float64
	if uptime > 0 {
		aps = float64(total) / uptime.Seconds()
	}
	var avgLat time.Duration
	if total > 0 {
		avgLat = time.Duration(latTotal / total)
	}

	return SchedulerStats{
		RegisteredQueues:     nQueues,
		ActivationQueueDepth: len(s.active),
		ActiveGroups:         len(s.active),
		WorkerCount:          wc,
		BusyWorkers:          bw,
		WorkerUtilization:    util,
		ActivationsTotal:     total,
		ActivationsPerSec:    aps,
		ActivationLatencyAvg: avgLat,
		ActivationLatencyMax: time.Duration(latMax),
		Uptime:               uptime,
	}
}

func queueKey(namespace, name string) string { return namespace + "/" + name }

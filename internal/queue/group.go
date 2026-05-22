package queue

import (
	"container/heap"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// ── Configuration types ───────────────────────────────────────────────────────

// BackoffStrategy defines how retry delays grow between attempts.
type BackoffStrategy string

const (
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffLinear      BackoffStrategy = "linear"
	BackoffExponential BackoffStrategy = "exponential"
)

// RateLimitConfig defines the sliding-window rate limit for a group.
type RateLimitConfig struct {
	Max    int           `json:"max"`
	Window time.Duration `json:"window"`
}

// RetryConfig defines retry behaviour for failed messages.
type RetryConfig struct {
	MaxAttempts  int             `json:"max_attempts"`
	Backoff      BackoffStrategy `json:"backoff"`
	InitialDelay time.Duration   `json:"initial_delay"`
	MaxDelay     time.Duration   `json:"max_delay"`
}

// DLQConfig routes exhausted messages to a dead-letter queue.
type DLQConfig struct {
	Enabled bool   `json:"enabled"`
	Queue   string `json:"queue"`
}

// GroupConfig is the user-facing configuration for a group key (supports wildcards).
type GroupConfig struct {
	Key          string           `json:"key"`
	Parallelism  int              `json:"parallelism"` // 1 = sequential, N = up to N concurrent
	Quantum      int              `json:"quantum"`     // DRR weight; defaults to 1
	LeaseTimeout time.Duration    `json:"lease_timeout"`
	RateLimit    *RateLimitConfig `json:"rate_limit,omitempty"`
	Retry        RetryConfig      `json:"retry"`
	DLQ          *DLQConfig       `json:"dlq,omitempty"`
	MaxPending   int              `json:"max_pending"` // 0 = unlimited
}

// defaultGroupConfig returns safe defaults for a group that has no explicit config.
func defaultGroupConfig(key string) GroupConfig {
	return GroupConfig{
		Key:          key,
		Parallelism:  1,
		Quantum:      1,
		LeaseTimeout: 5 * time.Minute,
		MaxPending:   10000000, // 10M default, adjustable per config or via DefaultGroupConfig
		Retry: RetryConfig{
			MaxAttempts:  3,
			Backoff:      BackoffExponential,
			InitialDelay: time.Second,
			MaxDelay:     5 * time.Minute,
		},
	}
}

// Matches returns true if this config applies to groupKey.
// Supports glob wildcards (e.g. "group-*" matches "group-123").
func (c *GroupConfig) Matches(key string) bool {
	if c.Key == key {
		return true
	}
	matched, _ := path.Match(c.Key, key)
	return matched
}

// IsSequential returns true when the group processes messages one at a time.
func (c *GroupConfig) IsSequential() bool { return c.Parallelism <= 1 }

// ── Runtime group state ───────────────────────────────────────────────────────

// group is the runtime state for a single consumer group inside a queue.
//
// Messages live in one of three states:
//
//	ready      — eligible for immediate dispatch (FIFO, consumed via readyHead pointer)
//	delayed    — waiting for their ScheduledAt time (retry backoff, min-heap)
//	processing — dispatched and waiting for ACK or NACK (lease-tracked)
//
// Why a head-index instead of slice shifts for ready?
// Popping from the front of a slice with append(s[1:]) is O(N) because it
// copies the entire remaining slice. A readyHead pointer makes every pop O(1)
// and defers the compaction until the slice is fully consumed.
type group struct {
	mu        sync.Mutex
	cfg       GroupConfig
	ready     []*Message          // FIFO eligible queue, consumed via readyHead
	readyHead int                 // index of the next undispatched message
	urgent    []*Message          // prepended messages (expired leases, backpressure), consumed via urgentHead
	urgentHead int                // index of the next undispatched urgent message
	delayed   delayedHeap         // min-heap of retry messages by ScheduledAt
	processing map[string]*Message // in-flight messages keyed by ID
	limiter    *slidingWindowLimiter
	deficit    int // DRR deficit counter (unused until weighted-fairness phase)
	quantum    int // DRR quantum (message budget per scheduler tick)

	// Atomic flags used by the scheduler for lock-free idempotency.
	isScheduled     atomic.Uint32
	wakeScheduledAt atomic.Int64
}

func newGroup(cfg GroupConfig) *group {
	def := defaultGroupConfig(cfg.Key)
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = def.Parallelism
	}
	if cfg.Quantum <= 0 {
		cfg.Quantum = def.Quantum
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = def.Retry.MaxAttempts
	}
	if cfg.Retry.Backoff == "" {
		cfg.Retry.Backoff = def.Retry.Backoff
	}
	if cfg.Retry.InitialDelay == 0 {
		cfg.Retry.InitialDelay = def.Retry.InitialDelay
	}
	if cfg.Retry.MaxDelay == 0 {
		cfg.Retry.MaxDelay = def.Retry.MaxDelay
	}

	var limiter *slidingWindowLimiter
	if cfg.RateLimit != nil {
		limiter = newSlidingWindowLimiter(cfg.RateLimit.Max, cfg.RateLimit.Window)
	}

	g := &group{
		cfg:  cfg,
		ready:      make([]*Message, 0, 4),
		urgent:     make([]*Message, 0, 4),
		delayed:    make(delayedHeap, 0),
		processing: make(map[string]*Message),
		limiter:    limiter,
		quantum:    cfg.Quantum,
	}
	heap.Init(&g.delayed)
	return g
}

// enqueue adds a new message to the back of the ready queue. O(1) amortised.
func (g *group) enqueue(msg *Message) {
	g.ready = append(g.ready, msg)
}

// promoteDelayed moves eligible delayed messages into the ready queue and
// expires stale leases back to pending.
//
// Why reuse nil'd slots for expired leases?
// When next() consumes a slot it sets ready[readyHead] = nil and advances
// readyHead. Prepending with append([]{msg}, g.ready...) inserts at index 0,
// but readyHead is still > 0 — so next() would read a nil slot and panic.
// Instead we decrement readyHead and write into the already-cleared slot,
// which is O(1) and race-free under the shard lock.
func (g *group) promoteDelayed(now time.Time) time.Time {
	earliestWake := time.Time{}

	// 1. Expire leases whose deadline has passed.
	for id, msg := range g.processing {
		if now.After(msg.ScheduledAt) {
			delete(g.processing, id)
			msg.State = StatePending
			g.urgent = append(g.urgent, msg)
		} else {
			if earliestWake.IsZero() || msg.ScheduledAt.Before(earliestWake) {
				earliestWake = msg.ScheduledAt
			}
		}
	}

	// 2. Move delayed messages whose backoff has elapsed into the ready queue.
	for len(g.delayed) > 0 {
		msg := g.delayed[0]
		if msg.ScheduledAt.After(now) {
			if earliestWake.IsZero() || msg.ScheduledAt.Before(earliestWake) {
				earliestWake = msg.ScheduledAt
			}
			break
		}
		heap.Pop(&g.delayed)
		g.ready = append(g.ready, msg)
	}

	return earliestWake
}

func (g *group) nextDelayedAt() (time.Time, bool) {
	if len(g.delayed) == 0 {
		return time.Time{}, false
	}
	return g.delayed[0].ScheduledAt, true
}

func (g *group) nextDelayedAtOrZero() time.Time {
	t, ok := g.nextDelayedAt()
	if !ok {
		return time.Time{}
	}
	return t
}

// ── Dispatch stop reasons ─────────────────────────────────────────────────────

type dispatchStopReason int

const (
	dispatchStopEmpty       dispatchStopReason = iota // no more ready messages
	dispatchStopCapacity                              // parallelism limit reached
	dispatchStopRateLimited                           // rate limiter blocked
)

// drainRound dispatches as many messages as the group's quantum allows in one
// scheduler tick.
//
// Why the deficit/quantum model?
// This is the foundation of Deficit Round Robin (DRR). Each group accumulates a
// credit (deficit) of `quantum` messages per tick. Groups that can't use their
// full quantum carry the deficit forward, preventing starvation of low-traffic
// groups by high-traffic ones. Quantum=1 means strict round-robin: one message
// per group per activation. Set higher via the API for weighted fairness.
//
// Return values:
//
//	ready       — batch of messages to dispatch (caller must call putMessageBatch)
//	wakeAt      — earliest time any delayed/rate-limited message becomes eligible
//	hasWake     — whether wakeAt is meaningful
//	stillPending — whether more messages remain after draining the quantum
func (g *group) drainRound(now time.Time) (*[]*Message, time.Time, bool, bool) {
	wakeAt := g.promoteDelayed(now)
	if g.pendingCount() == 0 {
		return nil, wakeAt, !wakeAt.IsZero(), false
	}

	if g.quantum <= 0 {
		g.quantum = 1
	}
	g.deficit += g.quantum

	ready := getMessageBatch()
	blockedByCapacity := false

	for g.deficit > 0 {
		msg, stop, stopWake := g.nextDispatchable(now)
		if msg != nil {
			*ready = append(*ready, msg)
			g.deficit--
			continue
		}
		switch stop {
		case dispatchStopEmpty:
			if !stopWake.IsZero() {
				if wakeAt.IsZero() || stopWake.Before(wakeAt) {
					wakeAt = stopWake
				}
			}
			return ready, wakeAt, !wakeAt.IsZero(), false
		case dispatchStopCapacity:
			blockedByCapacity = true
		case dispatchStopRateLimited:
			if wakeAt.IsZero() || stopWake.Before(wakeAt) {
				wakeAt = stopWake
			}
		}
		break
	}

	if blockedByCapacity {
		return ready, wakeAt, !wakeAt.IsZero(), false
	}
	if !wakeAt.IsZero() {
		return ready, wakeAt, true, false
	}
	if g.pendingCount() > 0 {
		return ready, time.Time{}, false, true
	}
	return ready, time.Time{}, false, false
}

// next returns the next eligible message, or nil if blocked or empty.
// Used by the direct-dispatch path (TryDispatchOne, CompleteAndNext).
func (g *group) next(now time.Time) *Message {
	msg, _, _ := g.nextDispatchable(now)
	return msg
}

// nextDispatchable returns the next message that passes all constraints
// (parallelism cap, rate limiter) or the reason it cannot proceed.
func (g *group) nextDispatchable(now time.Time) (*Message, dispatchStopReason, time.Time) {
	if len(g.processing) >= g.cfg.Parallelism {
		return nil, dispatchStopCapacity, time.Time{}
	}
	if g.limiter != nil {
		if allowed, wakeAt := g.limiter.allow(now); !allowed {
			return nil, dispatchStopRateLimited, wakeAt
		}
	}
	// Urgent messages (expired leases, backpressure returns) take priority.
	if g.urgentHead < len(g.urgent) {
		msg := g.urgent[g.urgentHead]
		g.urgent[g.urgentHead] = nil
		g.urgentHead++
		msg.State = StateProcessing
		msg.ScheduledAt = now.Add(g.cfg.LeaseTimeout)
		g.processing[msg.ID] = msg
		return msg, dispatchStopEmpty, time.Time{}
	}
	g.urgent = g.urgent[:0]
	g.urgentHead = 0

	// Skip nil'd slots (created by RemoveMessage during WAL replay).
	for g.readyHead < len(g.ready) {
		msg := g.ready[g.readyHead]
		g.ready[g.readyHead] = nil
		g.readyHead++
		if msg != nil {
			msg.State = StateProcessing
			msg.ScheduledAt = now.Add(g.cfg.LeaseTimeout)
			g.processing[msg.ID] = msg
			return msg, dispatchStopEmpty, time.Time{}
		}
	}

	// All ready messages consumed or nil'd.
	g.ready = g.ready[:0]
	g.readyHead = 0
	if len(g.delayed) > 0 {
		return nil, dispatchStopEmpty, g.nextDelayedAtOrZero()
	}
	return nil, dispatchStopEmpty, time.Time{}
}

// ── Message completion and failure ────────────────────────────────────────────

// complete marks a message as successfully processed. O(1).
func (g *group) complete(msgID string) *Message {
	msg, ok := g.processing[msgID]
	if !ok {
		return nil
	}
	delete(g.processing, msgID)
	msg.State = StateDone
	return msg
}

// fail handles a message failure: schedules retry with backoff, or marks it
// for DLQ when retry attempts are exhausted.
// Returns a non-nil *Message only when the message should be routed to the DLQ.
func (g *group) fail(msgID, errMsg string, now time.Time) (dlq *Message) {
	msg, ok := g.processing[msgID]
	if !ok {
		return nil
	}
	delete(g.processing, msgID)

	msg.LastError = errMsg
	msg.Attempts++

	if msg.Attempts < g.cfg.Retry.MaxAttempts {
		msg.ScheduledAt = now.Add(g.retryDelay(msg.Attempts))
		msg.State = StatePending
		heap.Push(&g.delayed, msg) // re-queue with backoff delay
		return nil
	}

	if g.cfg.DLQ != nil && g.cfg.DLQ.Enabled {
		msg.State = StateDLQ
		return msg
	}
	msg.State = StateFailed
	return nil
}

// returnToPending re-queues a dispatched message at the front of the ready queue.
// Used when no consumer is available to accept the message. Uses the urgent list
// which is O(1) amortised — the prepend-versus-append ordering concern is
// irrelevant for backpressure: the message must simply be re-delivered.
func (g *group) returnToPending(msgID string) bool {
	msg, ok := g.processing[msgID]
	if !ok {
		return false
	}
	delete(g.processing, msgID)
	msg.State = StatePending
	g.urgent = append(g.urgent, msg)
	return true
}

// retryDelay computes the backoff duration for a given attempt number.
func (g *group) retryDelay(attempt int) time.Duration {
	base := g.cfg.Retry.InitialDelay
	maxD := g.cfg.Retry.MaxDelay
	var d time.Duration
	switch g.cfg.Retry.Backoff {
	case BackoffExponential:
		d = base * (1 << attempt)
	case BackoffLinear:
		d = base * time.Duration(attempt+1)
	default: // BackoffFixed
		d = base
	}
	if d > maxD {
		return maxD
	}
	return d
}

func (g *group) pendingCount() int { return (len(g.urgent) - g.urgentHead) + (len(g.ready) - g.readyHead) + len(g.delayed) }
func (g *group) processingCount() int { return len(g.processing) }

type GroupState struct {
	Key            string
	Parallelism    int
	Quantum        int
	ReadyMsgs      []*Message
	UrgentMsgs     []*Message
	DelayedMsgs    []*Message
	ProcessingMsgs map[string]*Message
}

func (g *group) ExportState() GroupState {
	g.mu.Lock()
	defer g.mu.Unlock()

	readyMsgs := make([]*Message, 0, len(g.ready)-g.readyHead)
	for i := g.readyHead; i < len(g.ready); i++ {
		if g.ready[i] != nil {
			readyMsgs = append(readyMsgs, g.ready[i])
		}
	}

	urgentMsgs := make([]*Message, 0, len(g.urgent)-g.urgentHead)
	for i := g.urgentHead; i < len(g.urgent); i++ {
		if g.urgent[i] != nil {
			urgentMsgs = append(urgentMsgs, g.urgent[i])
		}
	}

	delayedMsgs := make([]*Message, 0, len(g.delayed))
	for _, msg := range g.delayed {
		delayedMsgs = append(delayedMsgs, msg)
	}

	processingMsgs := make(map[string]*Message, len(g.processing))
	for id, msg := range g.processing {
		processingMsgs[id] = msg
	}

	return GroupState{
		Key:            g.cfg.Key,
		Parallelism:    g.cfg.Parallelism,
		Quantum:        g.quantum,
		ReadyMsgs:      readyMsgs,
		UrgentMsgs:     urgentMsgs,
		DelayedMsgs:    delayedMsgs,
		ProcessingMsgs: processingMsgs,
	}
}

func (g *group) ImportState(state GroupState) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.ready = state.ReadyMsgs
	g.readyHead = 0
	g.urgent = state.UrgentMsgs
	if g.urgent == nil {
		g.urgent = make([]*Message, 0, 4)
	}
	g.urgentHead = 0
	g.delayed = make(delayedHeap, len(state.DelayedMsgs))
	copy(g.delayed, state.DelayedMsgs)
	heap.Init(&g.delayed)
	g.processing = state.ProcessingMsgs
}

// Package scheduler implements the event-driven dispatch scheduler and its
// supporting timing infrastructure.
package scheduler

import (
	"sync"
	"time"

	"github.com/octarhq/octar/internal/xtime"
)

// timerTask is a node in the timing wheel's per-bucket linked list.
type timerTask struct {
	executeAt int64 // Unix nanoseconds
	callback  func()
	next      *timerTask
}

// TimingWheel is a low-allocation, scalable replacement for time.AfterFunc.
//
// The Go runtime timer heap is O(log N) per insert/fire and allocates a
// goroutine per timer. At 15K msg/s with retry backoffs, this creates thousands
// of timers per second. A timing wheel is O(1) per insert, runs on a single
// background goroutine, and reuses linked-list nodes.
//
// Trade-off: resolution is bounded by the tick interval (~10 ms here). For
// retry backoffs (100 ms – 5 min) this is more than adequate.
type TimingWheel struct {
	buckets    []*timerTask
	bucketTime int64 // tick duration in nanoseconds
	current    int
	ticker     *time.Ticker
	stop       chan struct{}
	mu         sync.Mutex
}

// NewTimingWheel creates a wheel with the given tick resolution and slot count.
// Example: 10 ms tick × 1024 slots = 10.24 s full rotation.
func NewTimingWheel(tick time.Duration, slots int) *TimingWheel {
	tw := &TimingWheel{
		buckets:    make([]*timerTask, slots),
		bucketTime: tick.Nanoseconds(),
		ticker:     time.NewTicker(tick),
		stop:       make(chan struct{}),
	}
	go tw.run()
	return tw
}

// Add schedules cb to run after delay. O(1), no OS timer allocation.
func (tw *TimingWheel) Add(delay time.Duration, cb func()) {
	if delay < 0 {
		delay = 0
	}
	executeAt := xtime.UnixNano() + delay.Nanoseconds()
	ticks := delay.Nanoseconds() / tw.bucketTime
	if ticks == 0 {
		ticks = 1
	}

	tw.mu.Lock()
	idx := (tw.current + int(ticks)) % len(tw.buckets)
	tw.buckets[idx] = &timerTask{executeAt: executeAt, callback: cb, next: tw.buckets[idx]}
	tw.mu.Unlock()
}

// Stop halts the background goroutine.
func (tw *TimingWheel) Stop() { close(tw.stop) }

func (tw *TimingWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tick()
		case <-tw.stop:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimingWheel) tick() {
	tw.mu.Lock()
	head := tw.buckets[tw.current]
	tw.buckets[tw.current] = nil
	tw.current = (tw.current + 1) % len(tw.buckets)
	tw.mu.Unlock()

	now := xtime.UnixNano()
	for t := head; t != nil; {
		next := t.next
		if t.executeAt <= now {
			// Callbacks are non-blocking channel sends; run inline to avoid
			// goroutine-per-timer overhead.
			t.callback()
		} else {
			// Wheel wrapped: task is still in the future — re-insert.
			remaining := t.executeAt - now
			ticks := remaining / tw.bucketTime
			if ticks == 0 {
				ticks = 1
			}
			tw.mu.Lock()
			idx := (tw.current + int(ticks) - 1 + len(tw.buckets)) % len(tw.buckets)
			t.next = tw.buckets[idx]
			tw.buckets[idx] = t
			tw.mu.Unlock()
		}
		t = next
	}
}

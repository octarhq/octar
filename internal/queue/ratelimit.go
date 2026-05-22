package queue

import "time"

// slidingWindowLimiter counts events in a rolling time window.
//
// Why int64 timestamps instead of time.Time?
// time.Time is 24 bytes (wall clock + monotonic + location pointer).
// Storing Unix nanoseconds as int64 (8 bytes) cuts per-event memory by 66%.
// At rate limits of thousands per second this matters for the events slice.
//
// Why a slice instead of a ring buffer?
// The slice is evicted on every allow() call (O(expired) amortised O(1) in
// steady state). A ring buffer would be O(1) always but adds index arithmetic
// and edge cases. For typical rate limits (≤ 10K/s) the slice is fast enough.
//
// Initial capacity strategy:
// We cap the initial allocation at 64 entries regardless of max. The backing
// array grows via append doubling only if the group actually hits the rate limit.
// This prevents the "wildcard bomb": a config like { "key": "tenant-*",
// "rate_limit": { "max": 10000 } } previously allocated 78 KB for every new
// tenant group the moment it appeared — including tenants that never send a
// message. With initCap=64, idle tenants cost 512 bytes; active tenants that
// genuinely need 10k slots grow there naturally.
const rateLimitInitCap = 64

type slidingWindowLimiter struct {
	max    int
	window int64   // nanoseconds
	events []int64 // Unix nanoseconds of recent events, always len ≤ max
}

func newSlidingWindowLimiter(max int, window time.Duration) *slidingWindowLimiter {
	initCap := min(max, rateLimitInitCap)
	return &slidingWindowLimiter{
		max:    max,
		window: window.Nanoseconds(),
		events: make([]int64, 0, initCap),
	}
}

// allow returns (true, zero) if the event is within the rate limit, or
// (false, retryAt) indicating the earliest time the caller may retry.
func (l *slidingWindowLimiter) allow(now time.Time) (bool, time.Time) {
	nowNs := now.UnixNano()
	cutoff := nowNs - l.window

	// Evict events that have fallen outside the window.
	i := 0
	for i < len(l.events) && l.events[i] < cutoff {
		i++
	}

	// Compact the backing array when more than half the slots are expired.
	// Without this, a group that once hit its rate limit keeps the high-water-mark
	// allocation forever. append([:0], [i:]...) copies live events to the front
	// and resets the slice, allowing the runtime to GC the dead head.
	if i > 0 {
		if i >= len(l.events)/2+1 {
			l.events = append(l.events[:0], l.events[i:]...)
		} else {
			l.events = l.events[i:]
		}
	}

	if len(l.events) >= l.max {
		// Earliest event + window = the moment the oldest slot frees up.
		wakeAt := l.events[0] + l.window
		if wakeAt < nowNs {
			wakeAt = nowNs
		}
		return false, time.Unix(0, wakeAt)
	}
	l.events = append(l.events, nowNs)
	return true, time.Time{}
}

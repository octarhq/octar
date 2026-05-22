// Package xtime provides a high-throughput coarse clock for hot paths.
//
// time.Now() has non-trivial cost (vDSO + syscall on some platforms). At
// 60K+ messages per second every call adds up. xtime maintains a background
// goroutine that snapshots the real clock every millisecond into an atomic
// int64; callers pay only one atomic load (~1 ns) with ~1 ms accuracy.
//
// This is the same technique used in high-performance systems like nginx and
// Redis for timestamp accounting.
package xtime

import (
	"sync/atomic"
	"time"
)

var coarseNano atomic.Int64

func init() {
	coarseNano.Store(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		for t := range ticker.C {
			coarseNano.Store(t.UnixNano())
		}
	}()
}

// Now returns the current time from the 1 ms coarse ticker.
// Accuracy: ~1 ms. Cost: one atomic load, no syscall.
func Now() time.Time {
	return time.Unix(0, coarseNano.Load())
}

// UnixNano returns the current Unix nanosecond timestamp from the 1 ms ticker.
func UnixNano() int64 {
	return coarseNano.Load()
}

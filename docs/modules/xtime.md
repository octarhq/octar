# XTime — High-Performance Coarse Clock

**Package**: `internal/xtime`

A high-performance coarse clock that provides sub-microsecond timestamp reads without the system call overhead of `time.Now()`.

## Technologies

| Technology | Purpose |
|------------|---------|
| Go `time` | Time base for synchronisation |
| Go `sync/atomic` | Lock-free clock read |

## Why a Coarse Clock?

### The Problem

On the hot dispatch path, `time.Now()` is called for:
- Every message's scheduled time
- Every rate-limit window check
- Every lease timeout comparison
- Every latency measurement

`time.Now()` on Linux involves a `clock_gettime` syscall (~50–100ns). On Windows, the overhead can be higher. At 15,000 msg/s across 32 shards, this adds measurable CPU overhead.

### The Solution

A background goroutine reads `time.Now()` every **1 millisecond** and stores the value in an atomic int64. Readers read the cached value with zero syscall overhead (~2ns, a 25–50x improvement).

```go
var cachedUnixNano atomic.Int64

func init() {
    go func() {
        for {
            cachedUnixNano.Store(time.Now().UnixNano())
            time.Sleep(time.Millisecond)
        }
    }()
}

func UnixNano() int64 {
    return cachedUnixNano.Load()
}
```

### Trade-off

The clock is accurate to within ±1 ms. This is **intentional**: message scheduling, rate limiting, and lease management do not need microsecond precision. The timing wheel's tick resolution is 10 ms — a 1 ms clock granularity is more than sufficient.

## Usage

- `xtime.UnixNano()` — current time in nanoseconds (1ms precision)
- `xtime.Now()` — returns `time.Unix(0, xtime.UnixNano())`

Used throughout the queue engine, scheduler (timing wheel), and broker (lease sweeper).

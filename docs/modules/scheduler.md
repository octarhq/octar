# Scheduler — Event-Driven Dispatch

**Package**: `internal/scheduler`

The scheduler coordinates message dispatch across queues using an **activation-based** model. Groups are only processed when there is work to do — no polling, no busy-waiting.

## Technologies

| Technology | Purpose |
|------------|---------|
| `internal/xtime` | Coarse clock for latency tracking and timing wheel |
| `internal/queue` | Queue and group types |
| Go `sync/atomic` | CAS-based idempotent activation |
| Go channels | MPSC work queue (65536 slots) |

## How It Works

```
Publisher publishes → group becomes non-empty
    ↓
Activate(group) → CAS { isScheduled: false→true }
    ↓
activationChannel ← group key
    ↓
Worker pool (GOMAXPROCS goroutines) consumes channel
    ↓
dispatchFunc(group) → broker sends MESSAGE frames
    ↓
                    ← consumer sends ACK/NACK
                    ↓
              on ACK: nothing (group may be empty now)
              on NACK: Activate(group) again + timing wheel after backoff
```

## Key Design Decisions

### CAS-Based Idempotency

Without idempotency, a group could be activated N times before the worker processes a single activation, causing N redundant dispatch rounds. Each group has an `isScheduled` atomic flag:

- `Activate()` does `atomic.CompareAndSwapInt32(&g.isScheduled, 0, 1)` — only one goroutine succeeds
- After the worker processes the activation, it sets `isScheduled = 0`
- If new work arrived during dispatch, the next `Activate()` will set it again

### Cache-Line Padding

Hot atomics in the scheduler are padded to 64 bytes to prevent false sharing between CPU cores:

```go
type groupActivation struct {
    isScheduled int32
    _           [60]byte // padding to 64 bytes (cache line)
}
```

### MPSC Channel

The activation channel is a single Go channel (`chan string`) with 65536 buffer slots. Multiple producers (publishers, ACK/NACK handlers, timing wheel) can push to it concurrently. A single consumer (the worker pool) drains it.

### Worker Pool

- **Size**: `runtime.GOMAXPROCS(0)` goroutines
- **Panic recovery**: if a worker panics, it logs the error and restarts — the worker pool is never depleted
- **Latency tracking**: each activation records the time between `Activate()` and the worker picking it up

## Timing Wheel

**Package**: `internal/scheduler`

A timing wheel replaces Go's `time.AfterFunc` for retry backoff scheduling.

### Motivation

At 15,000 msg/s with retries, Go's timer heap creates thousands of goroutines (one per `AfterFunc`) and is O(log N) per insert. The timing wheel:

- **O(1) insert**: hash the delay to a bucket
- **O(1) tick**: advance the cursor and fire all timers in the current bucket
- **Single goroutine**: one background ticker, not one per timer
- **Reuses linked-list nodes**: no per-timer allocation after initial creation

### How It Works

```
TimingWheel:
  ┌────┬────┬────┬────┬────┬────┬────┬────┐
  │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │ .. │ N  │
  └─┬──┴─┬──┴─┬──┴─┬──┴─┬──┴─┬──┴─┬──┴─┬──┘
    │    │    │    │    │    │    │    │
   task→task  ∅   task  ∅    ∅   task  ∅
                            ↓
                    tick every 10ms
```

- Configuration: 10 ms tick × 1024 slots = 10.24 s full rotation
- **Tie-breaking**: timers that wrap around (delay > wheel span) are re-inserted after firing
- **Trade-off**: resolution is bounded by the tick interval (~10 ms). For retry backoffs (100 ms – 5 min) this is more than adequate.

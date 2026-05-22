# Queue — In-Memory Queue Engine

**Package**: `internal/queue`

The queue engine manages message storage, consumer group dispatch, retry backoff, rate limiting, and dead-letter routing — all in memory for maximum performance.

## Technologies

| Technology | Purpose |
|------------|---------|
| `internal/xtime` | High-performance coarse clock for timestamps |
| Go `container/heap` | Min-heap for delayed (retry) messages |
| Go `sync.Mutex` | Per-shard locking for group operations |
| Go `sync/atomic` | DRR deficit counters, shard hash keys |

## Why In-Memory?

Message durability comes from the WAL (see [Storage](storage.md)). The queue engine keeps all active messages in memory so dispatch can happen at pointer speed — no disk I/O on the hot path. Recovery after a crash replays the WAL to rebuild in-memory state.

## Architecture

### Lock Striping

Each queue has 32 shards (`queueShardCount = 32`), selected by FNV-1a hash of the group key:

```go
shard := &q.shards[queue.HashKey(groupKey) & (queueShardCount - 1)]
```

This means **concurrent operations on different groups never contend**. Only operations on groups whose keys hash to the same shard contend on that shard's mutex.

### Three-Queue Message Model

Each consumer group has three internal queues:

| Queue | Data Structure | Semantics |
|-------|---------------|-----------|
| **urgent** | Slice with head index (LIFO) | Messages returned by backpressure go here — processed before ready messages |
| **ready** | Slice with head index (FIFO) | Normal dispatch — messages are drained in order |
| **delayed** | Min-heap (container/heap) | Messages waiting for retry backoff or scheduled delivery |

### Message Lifecycle

```
PUBLISH → enqueue(ready)
    ↓
dispatch → MESSAGE frame → client receives
    ↓                              ↓
  ACK (done)                  NACK (retry)
                                  ↓
                         max_attempts reached?
                            ↙         ↘
                          Yes          No
                           ↓           ↓
                         DLQ     delayed heap (backoff)
                                     ↓
                              promoteDelayed() → ready
```

### Deficit Round Robin (DRR) Dispatch

The scheduler calls `drainRound()` on each queue, which iterates over all groups with pending messages:

1. Add `quantum` to the group's deficit
2. Dispatch up to `deficit` messages (one per call to `nextDispatchable()`)
3. Subtract 1 from deficit per dispatched message
4. If the group still has pending messages, it gets a turn in the next round

**Why DRR?**:
- **Fairness**: a busy group cannot starve other groups
- **Weighted**: groups with higher `quantum` get proportionally more throughput
- **Work-conserving**: unused deficit accumulates if a group has no messages

### Group Configuration

Groups are configured via glob patterns. The first matching config wins:

```go
// Example configs
{ "key": "payment-*",            "parallelism": 1, "quantum": 5 }
{ "key": "payment-email",        "parallelism": 10 }
{ "key": "*",                    "parallelism": 1, "quantum": 1 } // default
```

The config index supports:
- **O(1) exact match** for concrete keys
- **Glob-based fallback** for wildcard keys (compiled per lookup)

### Retry Backoff Strategies

| Strategy | Formula | Use Case |
|----------|---------|----------|
| `fixed` | `delay = initial_delay` | Simple retry, predictable timing |
| `linear` | `delay = initial_delay * attempt` | Progressive backoff |
| `exponential` | `delay = min(initial_delay * 2^attempt, max_delay)` | Aggressive backoff with ceiling |

### Sliding Window Rate Limiter

Per-group rate limiting uses a sliding window:

- Events are stored as `int64` Unix nanosecond timestamps
- On each `allow()` call, expired events outside the window are evicted (amortised O(1))
- Initial capacity is `min(max, 64)` to prevent the "wildcard bomb"

**Why int64 instead of time.Time?** `time.Time` is 24 bytes; `int64` is 8 bytes — 66% memory saving for the event slice.

### Message ID Generation

Each message gets a unique 16-character hex ID:

```
[8 hex chars of nanotime] XOR [8 hex chars of monotonic counter]
```

The XOR ensures uniqueness within a process even if two messages arrive in the same nanosecond. The counter wraps at 2^32, but that would take 4 billion messages at the same nanosecond — practically impossible.

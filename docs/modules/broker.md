# Broker — Core Orchestration

**Package**: `internal/broker`

The broker is the central component that ties together the TCP server, queue engine, scheduler, WAL, authentication, and metrics. It registers itself as the connection handler and implements the message lifecycle.

## Technologies

| Technology | Purpose |
|------------|---------|
| All internal packages | Orchestration of every module |
| Go `sync.Map` | Lock-free subscriber registry |
| Go `sync/atomic` | Quota management, dispatch coordination |

## Key Responsibilities

### Connection Handler

When a TCP client connects and authenticates, the broker:

1. Creates a connection context with session, identity, and namespace binding
2. Reads frames from the connection in a loop
3. Dispatches each frame to the appropriate handler:
   - `PUBLISH` → `onPublish()`
   - `SUBSCRIBE` → `onSubscribe()`
   - `ACK` → `onACK()`
   - `NACK` → `onNACK()`

### Message Flow

```
PUBLISH frame
    ↓
onPublish:
  ├── validate queue exists (or auto-create)
  ├── append to WAL (AppendSync — waits for fsync)
  ├── enqueue message in queue engine
  ├── enqueueDispatch → scheduler.Activate(group)
  └── send PUBLISH_OK to client

SCHEDULER → dispatchFunc:
  ├── group.drainRound() → get batch of messages
  └── for each message:
        ├── find subscriber via subRegistry
        ├── acquire credit (CAS)
        ├── write MESSAGE frame to connection
        └── scheduler.Activate(group) if more pending

ACK frame
    ↓
onACK:
  ├── group.complete(msg) → mark done
  ├── release credit (CAS)
  ├── scheduler.Activate(group) — group may have more work
  └── append to WAL (Append — async)

NACK frame
    ↓
onNACK:
  ├── group.fail(msg) → retry or DLQ
  ├── release credit (CAS)
  ├── if retry: add to delayed heap + timing wheel
  ├── scheduler.Activate(group)
  └── append to WAL (Append — async)
```

### Subscriber Registry

The registry (`subRegistry`) maps `(namespace, queue, group)` → list of subscribed connections. It uses `sync.Map` for lock-free reads (the common case — dispatch) and copy-on-write for mutations (subscribe/unsubscribe — rare).

**Round-robin dispatch** across subscribers within the same group uses an atomic cursor. Each dispatch atomically increments the cursor and picks `subscribers[cursor % len]`.

### Dispatch Workers

The broker spawns `GOMAXPROCS` dispatch workers. Each worker reads from the scheduler's activation channel and calls the dispatch function. This means:

- Up to `GOMAXPROCS` groups can be dispatched concurrently
- Workers are CPU-bound — no blocking I/O on the dispatch path
- Work is distributed across cores without explicit load balancing

### Quota Management

`brokerQuota` is an atomic int64 counter for the global in-flight message limit. Every dispatch:

1. Reads the current count
2. CAS-increments (retry if contended)
3. If the new count exceeds `global_max`, CAS-decrements and sends BACKPRESSURE

The CAS loop is lock-free and adaptively retries on contention.

### Lease Sweeper

A background goroutine runs every 1 second and:

1. Iterates all queues → all groups
2. For each message in `processing` state:
   - If `now - dispatched_at > lease_timeout`, return the message to pending
   - This handles consumer crashes without message loss

### Recovery

On startup, `recoverQueues()` performs two-phase recovery:

1. **Load metadata**: read queue/group configs from SQLite
2. **Rebuild state**: for each queue, load the latest snapshot and replay WAL events since that snapshot

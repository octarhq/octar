# Storage — Write-Ahead Log & Snapshots

**Package**: `internal/storage`

Implements durable, queue-scoped write-ahead logging with batching, segmentation, CRC validation, and periodic snapshots for fast crash recovery.

## Technologies

| Technology | Purpose |
|------------|---------|
| Go `os` (File I/O) | File creation, write, fsync, read |
| Go `encoding/binary` | Binary record encoding |
| CRC32-C (Castagnoli) | Hardware-accelerated streaming CRC |
| Go `sync.Pool` | Reusable read/write buffers |
| Go `unsafe` | Zero-copy string→byte slice conversions |

## Why a Custom WAL?

Existing WAL implementations (e.g. etcd/wal, bbolt) are general-purpose and incur overhead for our access pattern: **per-queue, single-writer, append-only, with periodic snapshots**. A custom WAL allows:

- **Per-queue isolation**: one writer goroutine per queue — a slow queue never blocks others
- **Batched flushing**: timer-based (25ms) and size-based (1000 events) — tuneable for latency vs throughput
- **Per-queue durability**: `durable: true` (default) fsyncs after each batch; `durable: false` uses OS buffer only for 5–10x higher throughput on ephemeral queues
- **Segmented files**: bounded per-file size (512 MB) for fast recovery and easy GC
- **CRC32-C hardware acceleration**: modern CPUs (SSE4.2, ARMv8) compute this in a single instruction

## Architecture

```
Broker → per-queue channel (chan Event, buffered 8192)
              ↓
      ┌─ QueueWAL goroutine (single writer)
      │      │
      │      ├── flush timer (10ms)
      │      ├── flush counter (1000 events)
      │      └── on tick or threshold:
      │             ├── write batch to active segment
      │             ├── CRC32-C each record
      │             ├── update index (seq → file offset)
      │             └── optionally fsync
      │
      └── if segment > 512 MB:
             ├── close active segment
             ├── request snapshot
             └── open new segment
```

## WAL Record Format

```
┌────────────────────────────────────────┐
│  CRC32-C (4 bytes)                     │
├────────────────────────────────────────┤
│  Event Type (1 byte)                   │
├────────────────────────────────────────┤
│  Payload (variable, type-dependent)    │
├────────────────────────────────────────┤
│  Timestamp (8 bytes, Unix nanos)       │
├────────────────────────────────────────┤
│  Sequence Number (8 bytes)             │
└────────────────────────────────────────┘
```

### Event Types

| Type | Code | Payload |
|------|------|---------|
| PUBLISH | 0x01 | Queue, Group, MsgID, Payload |
| LEASE | 0x02 | Queue, Group, MsgID, LeaseTimeout |
| ACK | 0x03 | Queue, Group, MsgID |
| NACK | 0x04 | Queue, Group, MsgID, Reason |
| EXPIRE | 0x05 | Queue, Group, MsgID |

### Two Write Modes

| Mode | Behaviour | Used For |
|------|-----------|----------|
| `Append()` | Fire-and-forget (async) | LEASE, ACK, NACK, EXPIRE |
| `AppendSync()` | Block until flush confirmed | PUBLISH (must be durable before ack) |

> When `durable: true` (default), `AppendSync` blocks until fsync completes.  
> When `durable: false`, it blocks only until the OS buffer is flushed — no fsync.

### Per-Queue Durability

Each queue can override the global `durable` setting declared at queue creation:

```bash
# Durable queue (default) — fsync, survives power loss
POST /queues
{ "name": "orders", "namespace": "main" }

# Non-durable queue — OS buffer only, 5-10x faster
POST /queues
{ "name": "realtime-notifications", "namespace": "main", "durable": false }
```

| `durable` | Survives process crash | Survives power loss | Throughput |
|-----------|----------------------|---------------------|------------|
| `true` (default) | ✅ | ✅ | ~25–50k msg/s (Windows) |
| `false` | ✅ | ❌ | ~150–300k msg/s |

## Segments

- **File naming**: `NNNNNNNNN.log` (sequence number, zero-padded to 9 digits)
- **Accompanying files**: `NNNNNNNNN.idx` (sparse index), `NNNNNNNNN.snap` (snapshot)
- **Rotation**: when active segment exceeds `SegmentMaxBytes` (default 512 MB)
- **Cleanup**: old segments are deleted only after a snapshot confirms their data is checkpointed

## Snapshots

**Package**: `internal/storage/snapshot`

Snapshots serialize queue state (group configs, in-flight messages, DLQ state) into a compact binary format:

- **Magic**: `"FSNP"` (4 bytes) — FrameSnap
- **Version**: 1 (uint32)
- **Groups**: count + per-group serialization (config, pending messages, processing messages, delayed messages)
- **Messages**: ID, Queue, Group, Payload, State, Attempts, ScheduledAt, CreatedAt, LastError

Snapshots are:
- Written **asynchronously** so they never block the WAL writer
- Rate-limited with `snapshotSem` (capacity 1) to prevent pileup
- Triggered periodically (`SnapshotInterval`, default 60s) and on segment rotation

## Recovery

**Package**: `internal/storage/recovery`

On startup, the broker performs two-phase recovery:

### Phase 1: Metadata Restore (SQLite)

Load namespace, queue, and group configurations from the SQLite database.

### Phase 2: WAL Replay

1. Scan the WAL directory for the latest snapshot per queue
2. Load the snapshot (fallback to N-1 if primary is corrupt)
3. Replay WAL events since the snapshot sequence number
4. Rebuild in-memory message states (pending/processing/done/failed/DLQ)

### Permanent Failure Detection

If the WAL encounters an unrecoverable I/O error (e.g. disk full, ENOSPC), it marks itself as permanently failed. All subsequent writes return `ErrWALFailed`. The broker will refuse new publishes until the storage issue is resolved and the process restarts.

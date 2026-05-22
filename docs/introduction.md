# OCTAR — High-Performance Message Broker

## The Problem

Modern distributed systems need a message broker that is **fast**, **predictable**, and **operationally simple**. Existing solutions fall short in different ways:

- **Apache Kafka** delivers throughput at the cost of operational complexity (ZooKeeper/KRaft, rebalancing, multi-replica storage). It is overkill for most use cases and hard to tune for latency-sensitive workloads.

- **RabbitMQ** is mature and feature-rich, but its Erlang runtime makes it a black box for most teams, and its performance degrades under high fan-out or large message volumes.

- **Redis Streams** is simple and fast, but offers no persistence guarantees, no real consumer-group retry semantics, and no built-in dead-letter queues.

- **NATS** is fast and lightweight but sacrifices durability (no WAL by default) and advanced routing.

- **AWS SQS / Google PubSub** are managed services — excellent when they fit, but they introduce vendor lock-in, per-message costs, and unpredictable tail latency.

None of these platforms was designed for the **edge/IoT/datacenter** scenario where you need a single lightweight binary that boots in milliseconds, survives crashes with zero data loss, and offers deterministic message delivery with per-group backoff, rate limiting, and dead-letter routing.

## What OCTAR Is

OCTAR is a **wire-format first**, high-throughput message broker written in Go. It is designed for environments where:

- **Latency matters**: sub-millisecond publish-to-deliver for hot queues.
- **Reliability is non-negotiable**: every message goes through a write-ahead log before it is acknowledged.
- **Operational overhead must be zero**: a single static binary, no external dependencies, no JVM, no runtime tuning.
- **Fairness under load is critical**: a slow consumer in one group must not starve consumers in other groups.
- **Predictability at scale**: O(1) algorithms for scheduling, rate limiting, and dispatch — no GC pauses, no goroutine leaks.

## Design Philosophy

### 1. Lock-Free Concurrency by Default

Traditional message brokers use mutex-per-queue designs that collapse under contention. OCTAR uses:

- **Lock striping** (32 shards per queue, hashed by group key) so that concurrent publishers and consumers for different groups never block each other.
- **Atomic compare-and-swap** (CAS) for credit management, in-flight quotas, and activation scheduling — no mutexes on hot paths.
- **Copy-on-write subscriber lists** so that dispatch can read the subscriber set without locking, while connection/disconnection swaps the pointer atomically.
- **Cache-line padding** on hot atomics to prevent false sharing between cores.

### 2. Event-Driven, Not Polling

The scheduler is **activation-based**: a group is placed on a work queue only when there is work to do (a new message arrives, a consumer becomes ready, a retry timer fires). The work queue is an MPSC (multiple-producer, single-consumer) channel with 65536 slots. No polling, no busy-waiting, no wasted CPU.

Retry timers use a **timing wheel** (O(1) insert, O(1) tick, single background goroutine) instead of Go's `time.AfterFunc` (O(log N) insert, one goroutine per timer).

### 3. Zero-Copy Where Possible

- Wire protocol decoding returns byte slices pointing into the read buffer — no copy for the payload until the caller needs to retain it.
- `unsafe.Slice` conversions avoid allocations when writing strings to the WAL.
- `sync.Pool` is used aggressively for byte buffers, message batches, and codec writers, keeping allocation pressure near zero in steady state.

### 4. Predictable Memory Layout

- Messages are stored in dense slices with head-index pointers (O(1) pop), not linked lists.
- Delayed (retry) messages live in a min-heap by scheduled time — only a few entries per group, making this effectively O(1) amortised.
- Rate limiters pre-allocate a small capacity (64 entries) and grow only if the limit is actually hit — preventing the "wildcard bomb" where a config pattern like `tenant-*` allocates 78 KB per idle tenant.

### 5. Durable by Default

Every message is written to a **per-queue Write-Ahead Log** before the publish acknowledgement is sent. The WAL uses:

- **Single-writer goroutine per queue**: a channel-based pipeline serialises writes without locks.
- **Batching**: timer-based (10 ms) and size-based (1000 events) flushing for maximum throughput.
- **CRC32-C (Castagnoli)**: streaming validation on every record with hardware acceleration on modern CPUs.
- **Segmented files**: rotation at 512 MB to bound recovery time.
- **Snapshots**: periodic binary checkpoints (FSNP format) so recovery replays only events since the last snapshot, not the entire WAL.

### 6. Fair Scheduling: Deficit Round Robin

Message dispatch across consumer groups uses **Deficit Round Robin** (DRR). Each group has a quantum (default 1) and a deficit counter. On each dispatch round, the group's deficit is incremented by its quantum; it may send up to `deficit` messages. This guarantees:

- **Weighted fairness**: a group with quantum = 10 gets 10x the throughput of a group with quantum = 1.
- **No starvation**: every group with pending messages gets served in each round.
- **Work-conserving**: if a group has no messages, its deficit accumulates for the next round — unused capacity is not wasted.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                               OCTAR Broker                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐   ┌──────────────────────┐   ┌──────────────────────────┐  │
│  │ HTTP API     │   │ Scheduler            │   │ TCP Server               │  │
│  │ (Huma v2)    │   │ (Event-Driven)       │   │ (Data Plane)             │  │
│  │ :8080        │   │                      │   │                          │  │
│  └──────┬───────┘   │  ┌────────────────┐  │   │  ┌────────────────────┐  │  │
│         │           │  │ Timing Wheel   │  │   │  │ Connections        │  │  │
│  ┌──────▼───────┐   │  │ O(1) Retry     │  │   │  │ + Credit Control   │  │  │
│  │ Auth Service │   │  └────────────────┘  │   │  └────────────────────┘  │  │
│  │ JWT + RBAC   │   │                      │   └──────────┬───────────────┘  │
│  └──────┬───────┘   │  ┌────────────────┐  │              │                  │
│         │           │  │ Worker Pool    │  │              │                  │
│         │           │  │ GOMAXPROCS     │  │              │                  │
│         │           │  │ Goroutines     │  │              │                  │
│         │           │  └────────────────┘  │              │                  │
│         │           └──────────────────────┘              │                  │
│         │                                                 │                  │
│  ┌──────▼─────────────────────────────────────────────────▼───────────────┐  │
│  │                    Queue Engine (In-Memory)                            │  │
│  │                                                                        │  │
│  │  ┌────────────────────┐   ┌─────────────────────────────────────────┐  │  │
│  │  │ Lock Striping      │   │ Deficit Round Robin                     │  │  │
│  │  │ 32 Shards          │   │ + Per-Group Config                      │  │  │
│  │  └────────────────────┘   │ + Retry Backoff (3x Heap)               │  │  │
│  │                           │ + Sliding Window Rate Limiter           │  │  │
│  │                           │ + DLQ Routing                           │  │  │
│  │                           └─────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────┐   ┌─────────────────────────────────────────┐  │
│  │ Write-Ahead Log          │   │ SQLite Store (Metadata)                 │  │
│  │                          │   │                                         │  │
│  │ • Per-Queue WAL          │   │ • Namespaces                            │  │
│  │ • CRC32-C                │   │ • Queues                                │  │
│  │ • Snapshots              │   │ • Users & API Keys                      │  │
│  │ • Segments               │   │ • RBAC Policies                         │  │
│  │                          │   │ • Audit Events & Sessions               │  │
│  │                          │   │ • Groups Config                         │  │
│  └──────────────────────────┘   └─────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Core Abstractions

| Concept | Description |
|---------|-------------|
| **Namespace** | Top-level tenant isolation. Each namespace has its own queues, users, and permissions. |
| **Queue** | A named message channel within a namespace. Holds messages for one or more consumer groups. |
| **Group** | A logical consumer identified by a key (e.g. `orders-email`, `payment-*`). Groups within the same queue compete for messages via DRR. Group keys support glob patterns for config inheritance. |
| **Message** | An opaque byte payload with ID, state, attempt counter, and scheduling metadata. |
| **Subscription** | A TCP connection bound to a specific queue+group. Multiple subscribers can attach to the same group for load-balanced delivery. |
| **Session** | An authenticated TCP connection with in-flight credit tracking. |

## Quick Facts

| Attribute | Value |
|-----------|-------|
| **Language** | Go 1.26 |
| **Wire Protocol** | Custom binary TCP (12 frame types, 5-byte header) |
| **Storage** | Per-queue WAL (segmented, CRC32-C, snapshots) + SQLite (metadata) |
| **Auth** | Password (bcrypt), API Key (SHA-256), JWT (HMAC/RSA/EC/EdDSA), OAuth2, mTLS |
| **Dispatch** | Event-driven DRR with timing wheel for retries |
| **Rate Limiting** | Sliding window per consumer group |
| **Retry Backoff** | Configurable: fixed, linear, exponential (per group) |
| **API** | OpenAPI 3.1 (Huma v2) + CLI (Cobra) |
| **Metrics** | Prometheus (isolated endpoint) |
| **License** | Open source |

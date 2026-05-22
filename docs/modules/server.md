# Server — TCP Server & Connection Management

**Package**: `internal/server`

Implements the TCP listener, per-connection lifecycle, authentication handshake, frame I/O, and credit-based flow control.

## Technologies

| Technology | Purpose |
|------------|---------|
| Go `net` | TCP listener and connections |
| Go `net/http` | TLS configuration via `tls.Config` |
| `internal/auth` | Connection authentication |
| `internal/protocol` | Frame encoding/decoding |
| `internal/config` | Server configuration |
| `internal/metrics` | Connection and flow metrics |

## Architecture

### TCPServer

```
acceptLoop goroutine
    │
    ├── tokenBucket.Allow() → rate limit check
    │
    └── for each accepted connection:
         ├── activeConns.Add(1)
         └── go handleConn(raw)
              ├── conn.Authenticate(db, authSvc)
              ├── handler(conn)              ← registered by broker
              └── defer: close + activeConns.Add(-1)
```

### Connection State

Each authenticated connection has:
- **Session** with a unique ID, subject identity, and namespace binding
- **Credit** counter for in-flight message tracking (CAS-based)
- **Encoder** for writing frames (mutex-protected, bufio-backed)
- **Decoder** for reading frames
- **Writer goroutine**: drains a buffered channel, batches up to 100 frames before flushing

## Flow Control

### Per-Connection Credits

Each connection has a `credit` struct that tracks how many messages may be in-flight:

- `max_inflight` (default 256): per-connection cap
- `global_max` (default 10000): broker-wide cap across all connections

Credits use **atomic CAS** operations — no mutexes on the hot path. A credit is consumed when a message is dispatched and returned when an ACK/NACK is received.

### Backpressure

When a connection reaches its credit limit, the broker stops dispatching messages. If the client continues sending, the broker sends a `BACKPRESSURE` frame (0xF1) with a suggested retry delay.

### Rate Limiting

A **token bucket** rate limiter limits new TCP connections per second (`connRateLimit`, default 1000). This prevents a connection storm from overwhelming the accept loop.

## Key Design Decisions

### Writer Goroutine per Connection

Rather than writing to the socket directly from the dispatch path (which would add latency to the scheduler), each connection has a buffered channel and a dedicated writer goroutine. The writer:

1. Drains the channel (up to 100 frames)
2. Writes all frames to the `bufio.Writer`
3. Calls `Flush()` once

This coalesces multiple dispatch events into a single TCP segment.

### TLS Support

Optional TLS for the data plane. Configured once on the listener — all connections inherit the same TLS config. No per-connection TLS handshake for renegotiation.

### Panic Recovery

Each connection handler has a deferred `recover()` that logs the panic and closes the connection. A single misbehaving handler cannot crash the broker.

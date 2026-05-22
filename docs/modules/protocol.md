# Protocol — Binary Wire Protocol

**Package**: `internal/protocol`

Defines the binary TCP frame format used by the OCTAR data plane. The protocol is simple, efficient, and designed for low-latency parsing.

## Technologies

| Technology | Purpose |
|------------|---------|
| Go `encoding/binary` | Big-endian integer encoding |
| Go `bufio` | Buffered I/O for coalesced writes |
| Go `sync.Pool` | Reuse byte writer buffers |

## Frame Format

Every frame has a 5-byte header followed by a variable-length payload:

```
┌────────────────┬───────────────────────┬──────────────────────┐
│  type (1 byte) │  payload length (4 B) │  payload (N bytes)   │
├────────────────┼───────────────────────┴──────────────────────┤
│     0x10       │  0x0000002A           │  (42 bytes)          │
└────────────────┴──────────────────────────────────────────────┘
```

### Payload Encoding Rules

| Type | Encoding |
|------|----------|
| String | `[uint16 length][UTF-8 bytes]` |
| Byte slice | `[uint32 length][bytes]` |
| uint32 | 4 bytes big-endian |
| uint64 | 8 bytes big-endian |
| int32 | 4 bytes big-endian (cast from uint32) |

### Frame Types

| Frame | Code | Direction | Payload |
|-------|------|-----------|---------|
| `CONNECT` | 0x01 | C→B | Username, Password, Namespace, APIKey, Token |
| `CONNECT_OK` | 0x02 | B→C | SessionID |
| `CONNECT_ERR` | 0x03 | B→C | Reason |
| `PUBLISH` | 0x10 | C→B | Queue, Group, Payload |
| `PUBLISH_OK` | 0x11 | B→C | MsgID, Offset (uint64) |
| `SUBSCRIBE` | 0x20 | C→B | Queue, Group |
| `MESSAGE` | 0x21 | B→C | MsgID, Queue, Group, Payload, Attempts (int32) |
| `ACK` | 0x30 | C→B | MsgID, Queue, Group |
| `NACK` | 0x31 | C→B | MsgID, Queue, Group, Reason |
| `ERROR` | 0xF0 | B→C | Code (uint32), Message |
| `BACKPRESSURE` | 0xF1 | B→C | Reason, RetryAfter (uint32 ms) |
| `HEARTBEAT` | 0xFF | ⇄ | None |

## Architecture Decisions

### Encoder

- **Mutex-protected** so frames from different goroutines (e.g. message dispatch and heartbeat) never interleave on the wire.
- **bufio.Writer** coalesces the header and payload into a single system call, halving syscall overhead.
- **Broker → Client frames do not flush** — the connection's writer loop batches multiple frames and flushes once per drain cycle.

### Decoder

- **64 KB read buffer** to minimise `Read` syscalls for large payloads (up to 16 MB).
- **Payload validation**: rejects frames larger than 16 MB (configurable via `maxPayloadSize`).
- **Identifier validation**: queue names, group keys, and usernames are checked against `isValidIdent()` — alphanumeric with `_-. /*:` allowed.

### Zero-Allocation Reads

The `blob()` method returns a byte slice pointing directly into the decoder's internal buffer. Callers must copy if they need to retain the payload beyond the current frame decode — this avoids a 16 MB allocation for every frame.

### sync.Pool Usage

`byteWriter` buffers are pooled to reduce allocations. Buffers larger than 64 KB after use are discarded (replaced with a fresh 256-byte buffer) to prevent a single large frame from causing memory retention.

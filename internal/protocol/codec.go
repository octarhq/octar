package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// byteWriterPool reuses byteWriter buffers to reduce heap allocations.
var byteWriterPool = sync.Pool{
	New: func() any { return &byteWriter{buf: make([]byte, 0, 256)} },
}

const (
	headerSize     = 5        // type(1) + length(4)
	maxPayloadSize = 16 << 20 // 16 MB hard cap per frame
)

// Encoder writes binary frames to an io.Writer.
// Safe for concurrent use — a mutex serialises writes so frames from different
// goroutines (e.g. handleConnection and the scheduler dispatch) never interleave.
// A bufio.Writer coalesces the 5-byte header and variable payload into a single
// system call, halving syscall overhead compared to two separate Write calls.
type Encoder struct {
	mu sync.Mutex
	bw *bufio.Writer
}

// NewEncoder wraps w for frame encoding (64 KB write buffer).
// Matches the Decoder's buffer size and reduces WSASend/write syscall frequency
// under high-throughput workloads where many small frames are batched together.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{bw: bufio.NewWriterSize(w, 64*1024)}
}

// Decoder reads binary frames from an io.Reader.
// It is not safe for concurrent use.
type Decoder struct {
	r   *bufio.Reader
	buf []byte
}

// NewDecoder wraps r (with 64 KB buffer) for frame decoding.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReaderSize(r, 64*1024), buf: make([]byte, 1024)}
}

// ── Encoder methods (client → broker) ────────────────────────────────────────

// WriteConnect sends the initial auth frame. Used by clients, not the broker.
// Field order: Username, Password, Namespace, APIKey, Token
// (APIKey and Token were previously omitted — this is a wire protocol version bump).
func (e *Encoder) WriteConnect(f ConnectFrame) error {
	return e.write(FrameConnect, func(b *byteWriter) {
		b.str(f.Username)
		b.str(f.Password)
		b.str(f.Namespace)
		b.str(f.APIKey)
		b.str(f.Token)
	}, true)
}

// WritePublish enqueues a message. Used by publisher clients.
func (e *Encoder) WritePublish(f PublishFrame) error {
	return e.write(FramePublish, func(b *byteWriter) {
		b.str(f.Queue)
		b.str(f.Group)
		b.blob(f.Payload)
	}, true)
}

// WriteSubscribe registers as a consumer. Used by subscriber clients.
func (e *Encoder) WriteSubscribe(f SubscribeFrame) error {
	return e.write(FrameSubscribe, func(b *byteWriter) {
		b.str(f.Queue)
		b.str(f.Group)
	}, true)
}

// WriteACK acknowledges successful processing. Used by consumer clients.
func (e *Encoder) WriteACK(f ACKFrame) error {
	return e.write(FrameACK, func(b *byteWriter) {
		b.str(f.MsgID)
		b.str(f.Queue)
		b.str(f.Group)
	}, true)
}

// WriteNACK reports processing failure. Used by consumer clients.
func (e *Encoder) WriteNACK(f NACKFrame) error {
	return e.write(FrameNACK, func(b *byteWriter) {
		b.str(f.MsgID)
		b.str(f.Queue)
		b.str(f.Group)
		b.str(f.Reason)
	}, true)
}

// ── Encoder methods (broker → client) ────────────────────────────────────────
// These do NOT flush. The connection's writerLoop batches writes and flushes
// once per drain cycle, coalescing multiple frames into a single TCP send.

func (e *Encoder) WriteConnectOK(f ConnectOKFrame) error {
	return e.write(FrameConnectOK, func(b *byteWriter) { b.str(f.SessionID) }, false)
}

func (e *Encoder) WriteConnectErr(f ConnectErrFrame) error {
	return e.write(FrameConnectErr, func(b *byteWriter) { b.str(f.Reason) }, false)
}

func (e *Encoder) WritePublishOK(f PublishOKFrame) error {
	return e.write(FramePublishOK, func(b *byteWriter) {
		b.str(f.MsgID)
		b.u64(f.Offset)
	}, false)
}

func (e *Encoder) WriteMessage(f MessageFrame) error {
	return e.write(FrameMessage, func(b *byteWriter) {
		b.str(f.MsgID)
		b.str(f.Queue)
		b.str(f.Group)
		b.blob(f.Payload)
		b.i32(f.Attempts)
	}, false)
}

func (e *Encoder) WriteError(f ErrorFrame) error {
	return e.write(FrameError, func(b *byteWriter) {
		b.u32(f.Code)
		b.str(f.Message)
	}, false)
}

func (e *Encoder) WriteBackpressure(f BackpressureFrame) error {
	return e.write(FrameBackpressure, func(b *byteWriter) {
		b.str(f.Reason)
		b.u32(f.RetryAfter)
	}, false)
}

func (e *Encoder) WriteHeartbeat() error {
	return e.write(FrameHeartbeat, nil, false)
}

func (e *Encoder) write(t FrameType, fn func(*byteWriter), flush bool) error {
	pw := byteWriterPool.Get().(*byteWriter)
	pw.buf = pw.buf[:0]
	if fn != nil {
		fn(pw)
	}

	var header [headerSize]byte
	header[0] = byte(t)
	binary.BigEndian.PutUint32(header[1:], uint32(len(pw.buf)))

	e.mu.Lock()
	_, err := e.bw.Write(header[:])
	if err == nil && len(pw.buf) > 0 {
		_, err = e.bw.Write(pw.buf)
	}
	if err == nil && flush {
		err = e.bw.Flush()
	}
	e.mu.Unlock()

	// Reset oversized buffers before returning to the pool to prevent memory
	// retention from large frames (e.g. 16 MB payload) leaking into subsequent
	// small frames. A fresh sync.Pool entry will grow to any needed size on
	// demand — no allocation is saved by keeping a multi-MB buffer alive.
	if cap(pw.buf) > 64<<10 {
		pw.buf = make([]byte, 0, 256)
	}
	byteWriterPool.Put(pw)
	return err
}

func (e *Encoder) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.bw.Flush()
}

// ── Decoder ───────────────────────────────────────────────────────────────────

// ReadFrame reads one complete frame and returns its type and decoded payload.
// The payload type matches the FrameType:
//
//	FrameConnect   → ConnectFrame
//	FramePublish   → PublishFrame
//	FrameSubscribe → SubscribeFrame
//	FrameACK       → ACKFrame
//	FrameNACK      → NACKFrame
//	FrameHeartbeat → nil
func (d *Decoder) ReadFrame() (FrameType, any, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(d.r, header[:]); err != nil {
		return 0, nil, err
	}

	ft := FrameType(header[0])
	n := binary.BigEndian.Uint32(header[1:])
	if n > maxPayloadSize {
		return 0, nil, fmt.Errorf("frame too large: %d bytes (max %d)", n, maxPayloadSize)
	}

	if uint32(cap(d.buf)) < n {
		d.buf = make([]byte, n)
	}
	payload := d.buf[:n]
	if n > 0 {
		if _, err := io.ReadFull(d.r, payload); err != nil {
			return 0, nil, err
		}
	}

	r := &byteReader{buf: payload}
	switch ft {
	// client → broker
	case FrameConnect:
		var f ConnectFrame
		f.Username, _ = r.str()
		f.Password, _ = r.str()
		f.Namespace, _ = r.str()
		f.APIKey, _ = r.str()
		f.Token, _ = r.str()
		if !isValidIdent(f.Username) {
			return 0, nil, fmt.Errorf("invalid username")
		}
		return ft, f, nil
	case FramePublish:
		var f PublishFrame
		f.Queue, _ = r.str()
		f.Group, _ = r.str()
		f.Payload, _ = r.blob()
		if !isValidIdent(f.Queue) {
			return 0, nil, fmt.Errorf("invalid queue name")
		}
		if !isValidIdent(f.Group) {
			return 0, nil, fmt.Errorf("invalid group key")
		}
		return ft, f, nil
	case FrameSubscribe:
		var f SubscribeFrame
		f.Queue, _ = r.str()
		f.Group, _ = r.str()
		if !isValidIdent(f.Queue) {
			return 0, nil, fmt.Errorf("invalid queue name")
		}
		if !isValidIdent(f.Group) {
			return 0, nil, fmt.Errorf("invalid group key")
		}
		return ft, f, nil
	case FrameACK:
		var f ACKFrame
		f.MsgID, _ = r.str()
		f.Queue, _ = r.str()
		f.Group, _ = r.str()
		if !isValidIdent(f.Queue) {
			return 0, nil, fmt.Errorf("invalid queue name")
		}
		if !isValidIdent(f.Group) {
			return 0, nil, fmt.Errorf("invalid group key")
		}
		return ft, f, nil
	case FrameNACK:
		var f NACKFrame
		f.MsgID, _ = r.str()
		f.Queue, _ = r.str()
		f.Group, _ = r.str()
		f.Reason, _ = r.str()
		if !isValidIdent(f.Queue) {
			return 0, nil, fmt.Errorf("invalid queue name")
		}
		if !isValidIdent(f.Group) {
			return 0, nil, fmt.Errorf("invalid group key")
		}
		return ft, f, nil

	// broker → client
	case FrameConnectOK:
		var f ConnectOKFrame
		f.SessionID, _ = r.str()
		return ft, f, nil
	case FrameConnectErr:
		var f ConnectErrFrame
		f.Reason, _ = r.str()
		return ft, f, nil
	case FramePublishOK:
		var f PublishOKFrame
		f.MsgID, _ = r.str()
		f.Offset = r.u64()
		return ft, f, nil
	case FrameMessage:
		var f MessageFrame
		f.MsgID, _ = r.str()
		f.Queue, _ = r.str()
		f.Group, _ = r.str()
		f.Payload, _ = r.blob()
		f.Attempts = r.i32()
		return ft, f, nil
	case FrameError:
		var f ErrorFrame
		f.Code = r.u32()
		f.Message, _ = r.str()
		return ft, f, nil

	case FrameBackpressure:
		var f BackpressureFrame
		f.Reason, _ = r.str()
		f.RetryAfter = r.u32()
		return ft, f, nil

	case FrameHeartbeat:
		return ft, nil, nil

	default:
		return ft, nil, fmt.Errorf("unknown frame type: 0x%02x", byte(ft))
	}
}

// ── Low-level binary writer ───────────────────────────────────────────────────

type byteWriter struct{ buf []byte }

func (b *byteWriter) str(s string) {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	b.buf = append(b.buf, l[:]...)
	b.buf = append(b.buf, s...) // append([]byte, string...) never allocates
}

func (b *byteWriter) blob(data []byte) {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(data)))
	b.buf = append(b.buf, l[:]...)
	b.buf = append(b.buf, data...)
}

func (b *byteWriter) u32(v uint32) {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], v)
	b.buf = append(b.buf, l[:]...)
}

func (b *byteWriter) u64(v uint64) {
	var l [8]byte
	binary.BigEndian.PutUint64(l[:], v)
	b.buf = append(b.buf, l[:]...)
}

func (b *byteWriter) i32(v int32) { b.u32(uint32(v)) }

// ── Low-level binary reader ───────────────────────────────────────────────────

type byteReader struct {
	buf []byte
	pos int
}

func (r *byteReader) str() (string, error) {
	if r.pos+2 > len(r.buf) {
		return "", io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(r.buf[r.pos:]))
	r.pos += 2
	if r.pos+n > len(r.buf) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.buf[r.pos : r.pos+n])
	r.pos += n
	return s, nil
}

// isValidIdent returns true when s is a non-empty alphanumeric identifier
// (letters, digits, underscores, hyphens, dots, slashes, asterisks, colons).
// Used to validate queue names, group keys, and namespace identifiers.
func isValidIdent(s string) bool {
	if len(s) == 0 || len(s) > 1024 {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case b >= 'a' && b <= 'z':
		case b >= 'A' && b <= 'Z':
		case b >= '0' && b <= '9':
		case b == '_' || b == '-' || b == '.' || b == '/' || b == '*' || b == ':':
		default:
			return false
		}
	}
	return true
}

func (r *byteReader) u32() uint32 {
	if r.pos+4 > len(r.buf) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v
}

func (r *byteReader) u64() uint64 {
	if r.pos+8 > len(r.buf) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v
}

func (r *byteReader) i32() int32 { return int32(r.u32()) }

func (r *byteReader) blob() ([]byte, error) {
	if r.pos+4 > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	if r.pos+n > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	// Zero-allocation: return a slice pointing to the underlying buffer.
	// Callers must copy this if they want to retain the payload beyond the current frame.
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

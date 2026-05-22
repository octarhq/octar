package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
)

// ── Helpers ─────────────────────────────────────────────────────────────────

func encode(tb testing.TB, enc *Encoder, frame any) {
	tb.Helper()
	var err error
	switch f := frame.(type) {
	case ConnectFrame:
		err = enc.WriteConnect(f)
	case PublishFrame:
		err = enc.WritePublish(f)
	case SubscribeFrame:
		err = enc.WriteSubscribe(f)
	case ACKFrame:
		err = enc.WriteACK(f)
	case NACKFrame:
		err = enc.WriteNACK(f)
	case ConnectOKFrame:
		err = enc.WriteConnectOK(f)
	case ConnectErrFrame:
		err = enc.WriteConnectErr(f)
	case PublishOKFrame:
		err = enc.WritePublishOK(f)
	case MessageFrame:
		err = enc.WriteMessage(f)
	case ErrorFrame:
		err = enc.WriteError(f)
	case BackpressureFrame:
		err = enc.WriteBackpressure(f)
	default:
		tb.Fatalf("unsupported frame type: %T", frame)
	}
	if err != nil {
		tb.Fatalf("encode: %v", err)
	}
	// Flush so broker→client frames (which write without auto-flush)
	// reach the underlying buffer before decode.
	if err := enc.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
}

func decode(tb testing.TB, dec *Decoder, wantType FrameType) any {
	tb.Helper()
	ft, v, err := dec.ReadFrame()
	if err != nil {
		tb.Fatalf("decode: %v", err)
	}
	if ft != wantType {
		tb.Fatalf("frame type: got 0x%02x, want 0x%02x", byte(ft), byte(wantType))
	}
	return v
}

func roundTrip(tb testing.TB, frame any, wantType FrameType) any {
	tb.Helper()
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	encode(tb, enc, frame)
	dec := NewDecoder(&buf)
	return decode(tb, dec, wantType)
}

// ── Round-trip tests: all frame types ───────────────────────────────────────

func TestRoundTrip_Connect(t *testing.T) {
	in := ConnectFrame{
		Username:  "alice",
		Password:  "s3cret",
		Namespace: "acme-corp",
		APIKey:    "flw_live_abc123",
		Token:     "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0",
	}
	got := roundTrip(t, in, FrameConnect).(ConnectFrame)
	if got != in {
		t.Fatalf("ConnectFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_Publish(t *testing.T) {
	in := PublishFrame{
		Queue:   "orders",
		Group:   "region-us",
		Payload: []byte(`{"user":"alice","amount":42.50}`),
	}
	got := roundTrip(t, in, FramePublish).(PublishFrame)
	if !bytes.Equal(got.Payload, in.Payload) {
		t.Fatal("payload mismatch")
	}
	if got.Queue != in.Queue || got.Group != in.Group {
		t.Fatalf("PublishFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_Subscribe(t *testing.T) {
	in := SubscribeFrame{Queue: "notifications", Group: "mobile-app"}
	got := roundTrip(t, in, FrameSubscribe).(SubscribeFrame)
	if got != in {
		t.Fatalf("SubscribeFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_ACK(t *testing.T) {
	in := ACKFrame{MsgID: "msg-001", Queue: "orders", Group: "g1"}
	got := roundTrip(t, in, FrameACK).(ACKFrame)
	if got != in {
		t.Fatalf("ACKFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_NACK(t *testing.T) {
	in := NACKFrame{MsgID: "msg-042", Queue: "dlq", Group: "g2", Reason: "processing timeout"}
	got := roundTrip(t, in, FrameNACK).(NACKFrame)
	if got != in {
		t.Fatalf("NACKFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_Heartbeat(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WriteHeartbeat(); err != nil {
		t.Fatalf("WriteHeartbeat: %v", err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	dec := NewDecoder(&buf)
	ft, v, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if ft != FrameHeartbeat {
		t.Fatalf("type: got 0x%02x, want 0x%02x", byte(ft), byte(FrameHeartbeat))
	}
	if v != nil {
		t.Fatalf("heartbeat payload: got %v, want nil", v)
	}
}

func TestRoundTrip_ConnectOK(t *testing.T) {
	in := ConnectOKFrame{SessionID: "sess-00000001"}
	got := roundTrip(t, in, FrameConnectOK).(ConnectOKFrame)
	if got != in {
		t.Fatalf("ConnectOKFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_ConnectErr(t *testing.T) {
	in := ConnectErrFrame{Reason: "invalid credentials"}
	got := roundTrip(t, in, FrameConnectErr).(ConnectErrFrame)
	if got != in {
		t.Fatalf("ConnectErrFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_PublishOK(t *testing.T) {
	in := PublishOKFrame{
		MsgID:  "msg-9001",
		Offset: 18446744073709551615, // max uint64
	}
	got := roundTrip(t, in, FramePublishOK).(PublishOKFrame)
	if got != in {
		t.Fatalf("PublishOKFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_Message(t *testing.T) {
	in := MessageFrame{
		MsgID:    "msg-0xDEAD",
		Queue:    "events",
		Group:    "stream-1",
		Payload:  []byte("hello, world"),
		Attempts: 3,
	}
	got := roundTrip(t, in, FrameMessage).(MessageFrame)
	if got.MsgID != in.MsgID || got.Queue != in.Queue || got.Group != in.Group {
		t.Fatalf("MessageFrame fields mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
	if !bytes.Equal(got.Payload, in.Payload) {
		t.Fatal("MessageFrame payload mismatch")
	}
	if got.Attempts != in.Attempts {
		t.Fatalf("Attempts: got %d, want %d", got.Attempts, in.Attempts)
	}
}

func TestRoundTrip_Error(t *testing.T) {
	in := ErrorFrame{Code: math.MaxUint32, Message: "out of disk space"}
	got := roundTrip(t, in, FrameError).(ErrorFrame)
	if got != in {
		t.Fatalf("ErrorFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

func TestRoundTrip_Backpressure(t *testing.T) {
	in := BackpressureFrame{Reason: "too many messages", RetryAfter: 5000}
	got := roundTrip(t, in, FrameBackpressure).(BackpressureFrame)
	if got != in {
		t.Fatalf("BackpressureFrame mismatch:\n  got:  %+v\n  want: %+v", got, in)
	}
}

// ── Payload boundary tests ──────────────────────────────────────────────────

func TestPayloadBoundaries(t *testing.T) {
	t.Run("zero_bytes", func(t *testing.T) {
		in := PublishFrame{Queue: "q", Group: "g", Payload: []byte{}}
		got := roundTrip(t, in, FramePublish).(PublishFrame)
		if len(got.Payload) != 0 {
			t.Fatalf("expected empty payload, got %d bytes", len(got.Payload))
		}
	})

	t.Run("nil_payload", func(t *testing.T) {
		in := PublishFrame{Queue: "q", Group: "g"}
		got := roundTrip(t, in, FramePublish).(PublishFrame)
		if len(got.Payload) != 0 {
			t.Fatalf("expected nil → empty, got %d bytes", len(got.Payload))
		}
	})

	t.Run("max_allowed_size", func(t *testing.T) {
		// Calculate max blob size: overhead for queue="q", group="g"
		// str("q") = 2+1=3, str("g") = 2+1=3, blob(…) = 4+blen
		const overhead = 2 + 1 + 2 + 1 + 4 // str("q") + str("g") + blob header
		blobLen := maxPayloadSize - overhead
		if blobLen <= 0 {
			t.Fatal("calculated max blob size is <= 0 — overhead too large")
		}

		payload := make([]byte, blobLen)
		for i := range payload {
			payload[i] = byte(i)
		}

		in := PublishFrame{Queue: "q", Group: "g", Payload: payload}
		got := roundTrip(t, in, FramePublish).(PublishFrame)
		if len(got.Payload) != blobLen {
			t.Fatalf("payload length: got %d, want %d", len(got.Payload), blobLen)
		}
		if !bytes.Equal(got.Payload, payload) {
			t.Fatal("payload content mismatch at the limit")
		}
	})

	t.Run("exceeds_max", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewEncoder(&buf)

		in := PublishFrame{Queue: "q", Group: "g", Payload: make([]byte, maxPayloadSize)}
		if err := enc.WritePublish(in); err != nil {
			t.Fatalf("encode should succeed, got: %v", err)
		}

		dec := NewDecoder(&buf)
		_, _, err := dec.ReadFrame()
		if err == nil {
			t.Fatal("expected error for frame exceeding max payload size")
		}
		if !strings.Contains(err.Error(), "frame too large") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// ── Identifier validation ───────────────────────────────────────────────────

func TestIdentValidation(t *testing.T) {
	t.Run("empty_queue_rejected", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewEncoder(&buf)
		if err := enc.WritePublish(PublishFrame{Queue: "", Group: "g", Payload: []byte{1}}); err != nil {
			t.Fatalf("encode: %v", err)
		}
		dec := NewDecoder(&buf)
		_, _, err := dec.ReadFrame()
		if err == nil {
			t.Fatal("expected error for empty queue")
		}
	})

	t.Run("empty_group_rejected", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewEncoder(&buf)
		if err := enc.WritePublish(PublishFrame{Queue: "q", Group: "", Payload: []byte{1}}); err != nil {
			t.Fatalf("encode: %v", err)
		}
		dec := NewDecoder(&buf)
		_, _, err := dec.ReadFrame()
		if err == nil {
			t.Fatal("expected error for empty group")
		}
	})

	t.Run("special_chars_rejected", func(t *testing.T) {
		for _, name := range []string{"hello world", "queue!", "group@", "ns#1", "pay$", "%bad", "^caret", "&amp", "(paren", ")paren", "=equal", "+plus", "[bracket", "{brace", "|pipe", "\\back", "\"quote", "'sq", "~tilde", "`backtick", "<tag", ",comma", "?q", "   ", "\nnewline", "\x00null"} {
			var buf bytes.Buffer
			enc := NewEncoder(&buf)
			if err := enc.WritePublish(PublishFrame{Queue: "q", Group: name, Payload: nil}); err != nil {
				t.Fatalf("encode %q: %v", name, err)
			}
			dec := NewDecoder(&buf)
			_, _, err := dec.ReadFrame()
			if err == nil {
				t.Errorf("expected error for group %q (special chars)", name)
			}
		}
	})

	t.Run("allowed_chars_accepted", func(t *testing.T) {
		for _, name := range []string{
			"a", "Z", "0", "simple-name", "with_underscore",
			"dotted.name", "slash/name", "wildcard-*", "ns:queue",
			"group-*", "payments.dlq", "a/b/c", "UPPERCASE",
			"0123456789", "mix-Ed_Na.me/ver:*",
		} {
			in := PublishFrame{Queue: name, Group: name, Payload: []byte("ok")}
			got := roundTrip(t, in, FramePublish).(PublishFrame)
			if got.Queue != name || got.Group != name {
				t.Errorf("round-trip mismatch for %q", name)
			}
		}
	})

	t.Run("too_long_rejected", func(t *testing.T) {
		name := strings.Repeat("a", 1025)
		var buf bytes.Buffer
		enc := NewEncoder(&buf)
		if err := enc.WritePublish(PublishFrame{Queue: name, Group: "g", Payload: nil}); err != nil {
			t.Fatalf("encode: %v", err)
		}
		dec := NewDecoder(&buf)
		_, _, err := dec.ReadFrame()
		if err == nil {
			t.Fatal("expected error for >1024-char queue name")
		}
	})

	t.Run("max_length_accepted", func(t *testing.T) {
		name := strings.Repeat("b", 1024)
		in := PublishFrame{Queue: "q", Group: name, Payload: []byte("ok")}
		got := roundTrip(t, in, FramePublish).(PublishFrame)
		if got.Group != name {
			t.Fatal("1024-char identifier should be valid")
		}
	})
}

// ── Empty username (CONNECT) ────────────────────────────────────────────────

func TestEmptyUsername(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WriteConnect(ConnectFrame{Username: "", Password: "p", Namespace: "ns"}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error for empty username")
	}
	if !strings.Contains(err.Error(), "invalid username") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── Empty namespace (CONNECT) ───────────────────────────────────────────────

func TestEmptyNamespace(t *testing.T) {
	in := ConnectFrame{
		Username:  "alice",
		Password:  "p",
		Namespace: "",
		APIKey:    "",
		Token:     "",
	}
	got := roundTrip(t, in, FrameConnect).(ConnectFrame)
	if got.Username != in.Username {
		t.Fatal("username should survive round-trip")
	}
	if got.Namespace != "" {
		t.Fatal("empty namespace should survive round-trip")
	}
	if got.APIKey != "" {
		t.Fatal("empty api_key should survive round-trip")
	}
	if got.Token != "" {
		t.Fatal("empty token should survive round-trip")
	}
}

// ── Broker→Client frames (no flush) ─────────────────────────────────────────

func TestBrokerToClientFrames(t *testing.T) {
	t.Run("multiple_frames_no_flush_batched", func(t *testing.T) {
		var buf bytes.Buffer
		enc := NewEncoder(&buf)

		if err := enc.WriteConnectOK(ConnectOKFrame{SessionID: "s1"}); err != nil {
			t.Fatal(err)
		}
		if err := enc.WritePublishOK(PublishOKFrame{MsgID: "m1", Offset: 42}); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteMessage(MessageFrame{MsgID: "m2", Queue: "q", Group: "g", Payload: []byte("data"), Attempts: 1}); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteError(ErrorFrame{Code: 500, Message: "internal"}); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteBackpressure(BackpressureFrame{Reason: "slow down", RetryAfter: 100}); err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteHeartbeat(); err != nil {
			t.Fatal(err)
		}

		// Writes that don't flush are buffered — flush manually
		if err := enc.Flush(); err != nil {
			t.Fatal(err)
		}

		dec := NewDecoder(&buf)
		ft, v, err := dec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if ft != FrameConnectOK {
			t.Fatalf("first frame type: 0x%02x", byte(ft))
		}
		if v.(ConnectOKFrame).SessionID != "s1" {
			t.Fatal("session id mismatch")
		}

		ft, v, err = dec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if ft != FramePublishOK {
			t.Fatalf("second frame type: 0x%02x", byte(ft))
		}
		pok := v.(PublishOKFrame)
		if pok.MsgID != "m1" || pok.Offset != 42 {
			t.Fatal("PublishOK mismatch")
		}

		ft, v, err = dec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if ft != FrameMessage {
			t.Fatalf("third frame type: 0x%02x", byte(ft))
		}
		msg := v.(MessageFrame)
		if msg.MsgID != "m2" || msg.Attempts != 1 || string(msg.Payload) != "data" {
			t.Fatal("MessageFrame mismatch")
		}

		ft, v, err = dec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if ft != FrameError || v.(ErrorFrame).Code != 500 {
			t.Fatal("ErrorFrame mismatch")
		}

		ft, v, err = dec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if ft != FrameBackpressure || v.(BackpressureFrame).RetryAfter != 100 {
			t.Fatal("BackpressureFrame mismatch")
		}

		ft, v, err = dec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if ft != FrameHeartbeat || v != nil {
			t.Fatal("Heartbeat mismatch")
		}
	})
}

// ── Malformed / truncated / unknown ─────────────────────────────────────────

func TestUnknownFrameType(t *testing.T) {
	var buf bytes.Buffer
	var header [5]byte
	header[0] = 0x42 // unknown
	binary.BigEndian.PutUint32(header[1:], 0)
	buf.Write(header[:])

	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error for unknown frame type")
	}
	if !strings.Contains(err.Error(), "unknown frame type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTruncatedHeader(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x01, 0x00}) // only 2 bytes of a 5-byte header

	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error for truncated header")
	}
	if err != io.ErrUnexpectedEOF && err != io.EOF {
		t.Fatalf("expected EOF-related error, got: %v", err)
	}
}

func TestTruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	var header [5]byte
	header[0] = byte(FramePublish)
	binary.BigEndian.PutUint32(header[1:], 100) // claim 100 bytes in payload
	buf.Write(header[:])
	buf.Write([]byte{0x00, 0x01, 0x01}) // only 3 bytes of claimed 100

	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}
}

func TestFrameTooLargeInHeader(t *testing.T) {
	var buf bytes.Buffer
	var header [5]byte
	header[0] = byte(FramePublish)
	binary.BigEndian.PutUint32(header[1:], maxPayloadSize+1)
	buf.Write(header[:])

	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error for frame too large")
	}
	if !strings.Contains(err.Error(), "frame too large") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── Concurrency ─────────────────────────────────────────────────────────────

func TestConcurrentEncode(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			f := PublishFrame{
				Queue:   "concurrent-q",
				Group:   fmt.Sprintf("worker-%d", n%10),
				Payload: []byte(fmt.Sprintf("msg-%d", n)),
			}
			if err := enc.WritePublish(f); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	dec := NewDecoder(&buf)
	for range 100 {
		ft, _, err := dec.ReadFrame()
		if err != nil {
			t.Fatalf("decode after concurrent write: %v", err)
		}
		if ft != FramePublish {
			t.Fatalf("unexpected frame type: 0x%02x", byte(ft))
		}
	}
}

func TestConcurrentRoundTrip(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var buf bytes.Buffer
			enc := NewEncoder(&buf)
			dec := NewDecoder(&buf)

			in := ConnectFrame{
				Username:  fmt.Sprintf("user-%d", n),
				Password:  "pass",
				Namespace: "ns",
			}
			if err := enc.WriteConnect(in); err != nil {
				t.Error(err)
				return
			}
			ft, v, err := dec.ReadFrame()
			if err != nil {
				t.Error(err)
				return
			}
			if ft != FrameConnect {
				t.Errorf("wrong type: 0x%02x", byte(ft))
				return
			}
			got := v.(ConnectFrame)
			if got.Username != in.Username {
				t.Errorf("username mismatch: %q vs %q", got.Username, in.Username)
			}
			if got.Namespace != in.Namespace {
				t.Errorf("namespace mismatch: %q vs %q", got.Namespace, in.Namespace)
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrentEncodeMultipleTypes(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := enc.WriteHeartbeat(); err != nil {
				t.Error(err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := enc.WritePublish(PublishFrame{
				Queue: "q", Group: "g", Payload: []byte("data"),
			}); err != nil {
				t.Error(err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := enc.WriteACK(ACKFrame{MsgID: "m", Queue: "q", Group: "g"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	// Heartbeat frames don't auto-flush; flush once to push them through.
	if err := enc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	dec := NewDecoder(&buf)
	for range 150 {
		if _, _, err := dec.ReadFrame(); err != nil {
			t.Fatalf("decode after mixed concurrent writes: %v", err)
		}
	}
}

// ── isValidIdent unit tests ─────────────────────────────────────────────────

func TestIsValidIdent(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"", false},
		{"a", true},
		{"Z", true},
		{"0", true},
		{"valid-name", true},
		{"with_underscore", true},
		{"dotted.name", true},
		{"slash/name", true},
		{"wildcard-*", true},
		{"ns:queue", true},
		{"Mix-Ed_Na.me/ver:*", true},
		{"UPPERCASE", true},
		{strings.Repeat("a", 1024), true},
		{strings.Repeat("a", 1025), false},
		{"has space", false},
		{"tab\t", false},
		{"newline\n", false},
		{"exclaim!", false},
		{"at@sign", false},
		{"hash#tag", false},
		{"dollar$", false},
		{"percent%", false},
		{"caret^", false},
		{"and&amp", false},
		{"star*", true}, // star IS valid
		{"(paren", false},
		{")paren", false},
		{"plus+", false},
		{"equal=", false},
		{"[bracket", false},
		{"]bracket", false},
		{"{brace", false},
		{"}brace", false},
		{"pipe|", false},
		{"back\\slash", false},
		{"colon:valid", true},
		{"semicolon;", false},
		{"\"quote", false},
		{"<angle", false},
		{">angle", false},
		{"comma,", false},
		{"question?", false},
		{"tilde~", false},
		{"grave`", false},
		{"\x00null", false},
	}
	for _, tc := range tests {
		got := isValidIdent(tc.input)
		if got != tc.valid {
			t.Errorf("isValidIdent(%q) = %v, want %v", tc.input, got, tc.valid)
		}
	}
}

func TestDecodeUnknownFrameType(t *testing.T) {
	unknown := []byte{0x00, 0x00, 0x00, 0x00, 0x00}
	dec := NewDecoder(bytes.NewReader(unknown))
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error decoding unknown frame type")
	}
	t.Logf("unknown frame type error: %v", err)
}

func TestDecodeGarbageBytes(t *testing.T) {
	garbage := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x42, 0x41, 0x44}
	dec := NewDecoder(bytes.NewReader(garbage))
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error decoding garbage bytes")
	}
	t.Logf("garbage error: %v", err)
}

func TestDecodeEmptyReader(t *testing.T) {
	dec := NewDecoder(bytes.NewReader(nil))
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error from empty reader")
	}
}

func TestDecodeResidualData(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WriteHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}
	buf.Write([]byte("JUNK")) // append residual bytes after valid frame

	dec := NewDecoder(&buf)
	ft, _, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("expected valid frame, got error: %v", err)
	}
	if ft != FrameHeartbeat {
		t.Fatalf("expected Heartbeat, got 0x%02x", byte(ft))
	}
}

func TestDecodeFrameAfterResidual(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WriteHeartbeat(); err != nil {
		t.Fatal(err)
	}
	enc.Flush()
	buf.Write([]byte("JUNK"))

	enc2 := NewEncoder(&buf)
	if err := enc2.WriteHeartbeat(); err != nil {
		t.Fatal(err)
	}
	enc2.Flush()

	dec := NewDecoder(&buf)
	dec.ReadFrame()
	dec.ReadFrame()
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error after consuming both frames + residual should fail")
	} else {
		t.Logf("residual error: %v", err)
	}
}

func TestLargePayloadPublishFrame(t *testing.T) {
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WritePublish(PublishFrame{
		Queue:   "large-q",
		Group:   "large-g",
		Payload: payload,
	}); err != nil {
		t.Fatalf("encode large payload: %v", err)
	}

	dec := NewDecoder(&buf)
	ft, frame, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("decode large payload: %v", err)
	}
	if ft != FramePublish {
		t.Fatalf("expected FramePublish, got 0x%02x", byte(ft))
	}
	pf := frame.(PublishFrame)
	if len(pf.Payload) != len(payload) {
		t.Fatalf("payload size mismatch: %d vs %d", len(pf.Payload), len(payload))
	}
}

func TestPublishFrameMaxStrings(t *testing.T) {
	longName := strings.Repeat("x", 1024)

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WritePublish(PublishFrame{
		Queue:   longName,
		Group:   longName,
		Payload: []byte("data"),
	}); err != nil {
		t.Fatalf("encode with long names: %v", err)
	}

	dec := NewDecoder(&buf)
	ft, frame, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("decode with long names: %v", err)
	}
	if ft != FramePublish {
		t.Fatalf("expected FramePublish, got 0x%02x", byte(ft))
	}
	pf := frame.(PublishFrame)
	if pf.Queue != longName {
		t.Fatalf("queue name mismatch: len=%d vs %d", len(pf.Queue), len(longName))
	}
}

func TestPublishFrameOversizedName(t *testing.T) {
	payload := []byte("data")

	longName := strings.Repeat("y", 4096)

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.WritePublish(PublishFrame{
		Queue:   longName,
		Group:   "g",
		Payload: payload,
	}); err != nil {
		t.Fatalf("encode with very long name: %v", err)
	}

	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error decoding oversized queue name")
	} else {
		t.Logf("oversized name error: %v", err)
	}
}

func TestConcurrentEncodeDecodeStress(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			enc.WritePublish(PublishFrame{
				Queue: "stress-q", Group: "stress-g", Payload: []byte("hello"),
			})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			enc.WriteACK(ACKFrame{MsgID: "m", Queue: "stress-q", Group: "stress-g"})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			enc.WriteHeartbeat()
		}()
	}
	wg.Wait()
	enc.Flush()

	dec := NewDecoder(&buf)
	for range 150 {
		dec.ReadFrame()
	}
}

func TestEmptyQueueGroupNamesRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	if err := enc.WriteSubscribe(SubscribeFrame{Queue: "", Group: ""}); err != nil {
		t.Fatalf("encode empty subscribe: %v", err)
	}

	dec := NewDecoder(&buf)
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error decoding empty queue name")
	}
	t.Logf("empty name rejected: %v", err)
}

func TestDecodePartialFrame(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	enc.WriteHeartbeat()
	enc.Flush()

	full := buf.Bytes()

	partial := full[:2]
	dec := NewDecoder(bytes.NewReader(partial))
	_, _, err := dec.ReadFrame()
	if err == nil {
		t.Fatal("expected error from partial frame data")
	}
	t.Logf("partial frame error: %v", err)
}

// Package protocol defines the OCTAR binary wire protocol for the TCP data plane.
//
// All frames share the same 5-byte header:
//
//	┌──────────────┬───────────────────┬────────────────────┐
//	│  type (1 B)  │   length (4 B)    │  payload (N bytes) │
//	└──────────────┴───────────────────┴────────────────────┘
//
// Encoding rules for payload fields:
//   - Strings: [uint16 length][UTF-8 bytes]
//   - Byte slices: [uint32 length][bytes]
//   - uint32 / uint64 / int32: big-endian
//
// Frame types < 0x10 are handshake frames (CONNECT flow).
// 0x10–0x1F are publisher frames; 0x20–0x2F are consumer frames.
// 0x30–0x3F are acknowledgement frames. 0xFF is HEARTBEAT.
package protocol

// FrameType identifies a frame on the wire.
type FrameType uint8

const (
	// Handshake
	FrameConnect    FrameType = 0x01 // client → broker: authenticate
	FrameConnectOK  FrameType = 0x02 // broker → client: auth accepted
	FrameConnectErr FrameType = 0x03 // broker → client: auth rejected

	// Publisher
	FramePublish   FrameType = 0x10 // client → broker: enqueue a message
	FramePublishOK FrameType = 0x11 // broker → client: message durably accepted (returns ID)

	// Consumer
	FrameSubscribe FrameType = 0x20 // client → broker: register as consumer
	FrameMessage   FrameType = 0x21 // broker → client: deliver a message for processing

	// Acknowledgement
	FrameACK  FrameType = 0x30 // client → broker: message processed successfully
	FrameNACK FrameType = 0x31 // client → broker: message processing failed

	// System
	FrameError       FrameType = 0xF0 // broker → client: error response
	FrameBackpressure FrameType = 0xF1 // broker → client: slow down (backpressure signal)
	FrameHeartbeat   FrameType = 0xFF // either direction: keepalive
)

// ── Handshake ─────────────────────────────────────────────────────────────────

// ConnectFrame is sent by the client immediately after TCP connect.
// Supports authentication via password, API key, or token.
type ConnectFrame struct {
	Username  string
	Password  string
	APIKey    string
	Token     string
	Namespace string
}

// ConnectOKFrame is sent by the broker when credentials are valid.
type ConnectOKFrame struct {
	SessionID string
}

// ConnectErrFrame is sent by the broker when authentication fails.
type ConnectErrFrame struct {
	Reason string
}

// ── Publisher ─────────────────────────────────────────────────────────────────

// PublishFrame enqueues a message in the named queue/group.
// The broker responds with PublishOKFrame on success or ErrorFrame on failure.
type PublishFrame struct {
	Queue   string
	Group   string
	Payload []byte
}

// PublishOKFrame confirms the message was written to the WAL and enqueued.
type PublishOKFrame struct {
	MsgID  string // unique message identifier
	Offset uint64 // WAL sequence number
}

// ── Consumer ──────────────────────────────────────────────────────────────────

// SubscribeFrame registers this connection as a consumer for the given queue/group.
// The broker will begin delivering MessageFrames on this connection.
type SubscribeFrame struct {
	Queue string
	Group string
}

// MessageFrame delivers a single message from the broker to the consumer.
// The consumer must reply with ACKFrame or NACKFrame for every MessageFrame.
type MessageFrame struct {
	MsgID    string
	Queue    string
	Group    string
	Payload  []byte
	Attempts int32 // how many times this message has been attempted (1 = first try)
}

// ── Acknowledgement ───────────────────────────────────────────────────────────

// ACKFrame signals that the consumer successfully processed a message.
type ACKFrame struct {
	MsgID string
	Queue string
	Group string
}

// NACKFrame signals that the consumer failed to process a message.
// The broker will schedule a retry (with backoff) or route to DLQ if exhausted.
type NACKFrame struct {
	MsgID  string
	Queue  string
	Group  string
	Reason string // human-readable failure description, stored in the WAL
}

// ── System ────────────────────────────────────────────────────────────────────

// ErrorFrame carries a broker-generated error for the preceding request.
type ErrorFrame struct {
	Code    uint32
	Message string
}

// BackpressureFrame is sent by the broker to signal the client that the broker
// cannot accept more load at this time. The client should back off and retry
// after Reason is resolved or after RetryAfter.
type BackpressureFrame struct {
	Reason     string // human-readable explanation
	RetryAfter uint32 // suggested retry delay in milliseconds (0 = use client default)
}

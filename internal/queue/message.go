package queue

import (
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/83codes/octar/internal/xtime"
)

type MessageState int8

const (
	StatePending MessageState = iota
	StateProcessing
	StateDone
	StateFailed
	StateDLQ
)

func (s MessageState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateProcessing:
		return "processing"
	case StateDone:
		return "done"
	case StateFailed:
		return "failed"
	case StateDLQ:
		return "dlq"
	default:
		return "unknown"
	}
}

type Message struct {
	ID          string
	QueueName   string
	Namespace   string
	GroupKey    string
	Payload     []byte
	State       MessageState
	Attempts    int
	ScheduledAt time.Time // eligibility time — used for retry backoff
	CreatedAt   time.Time
	LastError   string
}

func newMessageAt(queue, namespace, group string, payload []byte, now time.Time) *Message {
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	return &Message{
		ID:          newID(),
		QueueName:   queue,
		Namespace:   namespace,
		GroupKey:    group,
		Payload:     payloadCopy,
		State:       StatePending,
		Attempts:    0,
		ScheduledAt: now,
		CreatedAt:   now,
	}
}

func NewMessageForRecovery(queue, namespace, group string, payload []byte, now time.Time, id string) *Message {
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	return &Message{
		ID:          id,
		QueueName:   queue,
		Namespace:   namespace,
		GroupKey:    group,
		Payload:     payloadCopy,
		State:       StatePending,
		Attempts:    0,
		ScheduledAt: now,
		CreatedAt:   now,
	}
}

// GenerateID creates a new random 16-character hex message ID.
// Exported so the broker can pre-generate IDs before writing to the WAL,
// ensuring the WAL record and the in-memory message carry the same identifier.
func GenerateID() string { return newID() }

var msgIDCounter atomic.Uint64

// initMsgIDPrefix is a per-process-unique prefix ensuring IDs are unique
// across restarts even though the counter resets.
var initMsgIDPrefix = uint64(xtime.UnixNano() + 42)

func newID() string {
	id := initMsgIDPrefix ^ msgIDCounter.Add(1)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return hex.EncodeToString(b[:])
}

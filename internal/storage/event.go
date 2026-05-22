package storage

// EventType identifies the kind of WAL record.
type EventType uint8

const (
	EventPublish EventType = 0x01 // new message written by a publisher
	EventLease   EventType = 0x02 // message dispatched to a consumer (inflight)
	EventACK     EventType = 0x03 // message successfully processed
	EventNACK    EventType = 0x04 // message processing failed (triggers retry)
	EventExpire  EventType = 0x05 // lease timed out (consumer crashed)
)

// Event is a single WAL record.
// Namespace, Queue, Group, and MsgID are variable-length string fields;
// Payload carries the message body only for EventPublish records.
type Event struct {
	Type      EventType
	Namespace string
	Queue     string
	Group     string
	MsgID     string
	Payload   []byte
	Seq       uint64
	Timestamp int64

	// done is set by AppendSync so the writer can signal the caller after the
	// batch containing this event has been flushed (and fsynced if cfg.Sync).
	// nil means fire-and-forget (used by Append for non-critical events like LEASE).
	done chan error
}

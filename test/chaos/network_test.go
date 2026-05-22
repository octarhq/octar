package chaos

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/protocol"
	"github.com/83codes/octar/internal/queue"
	"github.com/83codes/octar/internal/server"
)

// TestNetwork_DisconnectDuringDispatch simulates a consumer disconnecting
// mid-dispatch. The message should return to pending state after lease expiry.
func TestNetwork_DisconnectDuringDispatch(t *testing.T) {
	h := New(t)
	defer h.Close()

	q := h.RegisterQueue("test-ns", "net-disconnect")
	q.SetGroupConfig(queue.GroupConfig{Key: "g1", LeaseTimeout: 100 * time.Millisecond})

	// Simulate a connection that will disconnect
	r, w := net.Pipe()
	conn := server.NewConnection(w, config.InflightConfig{MaxInflight: 100, GlobalMax: 0})
	conn.Session = &server.Session{Username: "chaos", Namespace: "test-ns"}
	dec := protocol.NewDecoder(bufio.NewReader(r))

	// Publish a message
	msgID, err := h.Publish("test-ns", "net-disconnect", "g1", []byte("lease-test"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Dispatch to the connection (simulate what the broker does)
	msg := q.TryDispatchOne("g1", time.Now())
	if msg == nil {
		t.Fatal("expected a message to dispatch")
	}
	conn.WriteMessage(protocol.MessageFrame{
		MsgID:   msg.ID,
		Queue:   "net-disconnect",
		Group:   "g1",
		Payload: msg.Payload,
	})

	// Read the message on the consumer side
	ft, frame, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("read dispatched message: %v", err)
	}
	if ft != protocol.FrameMessage {
		t.Fatalf("expected Message frame, got %v", ft)
	}
	mf := frame.(protocol.MessageFrame)
	if mf.MsgID != msgID {
		t.Fatalf("expected msgID %s, got %s", msgID, mf.MsgID)
	}

	// Consumer disconnects without ACKing
	r.Close()
	conn.Close()

	// Wait for lease to expire
	time.Sleep(300 * time.Millisecond)

	// Sweep expired leases (simulated — normally done by broker)
	expired := q.SweepExpiredLeases(time.Now())
	if len(expired) == 0 {
		t.Fatal("expected lease to expire")
	}
	found := false
	for _, e := range expired {
		if e.MsgID == msgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected msg %s in expired leases", msgID)
	}

	// Message should be back in pending
	stats, ok := q.GetGroupStats("g1")
	if !ok {
		t.Fatal("group g1 not found")
	}
	if stats.Pending != 1 {
		t.Fatalf("expected 1 pending message after lease expiry, got %d", stats.Pending)
	}
}

// TestNetwork_MultipleDisconnects tests repeated connect/disconnect cycles.
func TestNetwork_MultipleDisconnects(t *testing.T) {
	h := New(t)
	defer h.Close()

	q := h.RegisterQueue("test-ns", "net-reconnect")
	q.SetGroupConfig(queue.GroupConfig{Key: "g1", LeaseTimeout: 50 * time.Millisecond})

	for i := 0; i < 5; i++ {
		r, w := net.Pipe()
		conn := server.NewConnection(w, config.InflightConfig{MaxInflight: 100, GlobalMax: 0})
		conn.Session = &server.Session{Username: "chaos", Namespace: "test-ns"}
		dec := protocol.NewDecoder(bufio.NewReader(r))

		_, err := h.Publish("test-ns", "net-reconnect", "g1", []byte("reconnect"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}

		msg := q.TryDispatchOne("g1", time.Now())
		if msg == nil {
			t.Fatalf("iteration %d: no message to dispatch", i)
		}
		if msg.Payload == nil || string(msg.Payload) != "reconnect" {
			t.Fatalf("iteration %d: unexpected payload", i)
		}

		conn.WriteMessage(protocol.MessageFrame{
			MsgID:   msg.ID,
			Queue:   "net-reconnect",
			Group:   "g1",
			Payload: msg.Payload,
		})

		// Read on consumer side
		ft, _, err := dec.ReadFrame()
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if ft != protocol.FrameMessage {
			t.Fatalf("expected FrameMessage, got %v", ft)
		}

		// Disconnect without ACK
		r.Close()
		conn.Close()

		// Wait for lease expiry then sweep
		time.Sleep(150 * time.Millisecond)
		q.SweepExpiredLeases(time.Now())
	}

	// After all disconnects, all 5 messages should be pending again
	stats, ok := q.GetGroupStats("g1")
	if !ok {
		t.Fatal("group g1 not found")
	}
	if stats.Pending != 5 {
		t.Fatalf("expected 5 pending after reconnects, got %d", stats.Pending)
	}
}

// TestNetwork_BackpressureGraceful verifies the broker handles backpressure
// from a slow consumer without crashing.
func TestNetwork_BackpressureGraceful(t *testing.T) {
	h := New(t)
	defer h.Close()

	q := h.RegisterQueue("test-ns", "net-backpressure")
	q.SetGroupConfig(queue.GroupConfig{Key: "g1", LeaseTimeout: 500 * time.Millisecond})

	r, w := net.Pipe()
	conn := server.NewConnection(w, config.InflightConfig{MaxInflight: 10, GlobalMax: 0})
	conn.Session = &server.Session{Username: "chaos", Namespace: "test-ns"}
	defer r.Close()
	defer conn.Close()

	// Publish many messages rapidly
	for i := 0; i < 50; i++ {
		_, err := h.Publish("test-ns", "net-backpressure", "g1", []byte("backpressure"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Try to dispatch all — the connection has limited inflight
	for i := 0; i < 50; i++ {
		msg := q.TryDispatchOne("g1", time.Now())
		if msg == nil {
			break
		}
		conn.WriteMessage(protocol.MessageFrame{
			MsgID:   msg.ID,
			Queue:   "net-backpressure",
			Group:   "g1",
			Payload: msg.Payload,
		})
	}

	// Verify remaining messages are still pending (inflight limit was hit)
	stats, ok := q.GetGroupStats("g1")
	if !ok {
		t.Fatal("group g1 not found")
	}
	if stats.Pending <= 0 {
		t.Fatalf("expected pending > 0 due to inflight limit, got %d", stats.Pending)
	}
}

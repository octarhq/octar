package broker

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

type frameResult struct {
	ft    protocol.FrameType
	frame any
}

func startReader(dec *protocol.Decoder) <-chan frameResult {
	ch := make(chan frameResult, 256)
	go func() {
		for {
			ft, frame, err := dec.ReadFrame()
			if err != nil {
				close(ch)
				return
			}
			ch <- frameResult{ft, frame}
		}
	}()
	return ch
}

func testE2EBroker(t *testing.T) (*Broker, *queue.Queue, func()) {
	t.Helper()
	b, q, cleanup := testBroker(t)
	b.startDispatchWorkers()
	b.Scheduler.Run(b.enqueueDispatch)
	return b, q, func() {
		b.Scheduler.Stop()
		close(b.dispatchStop)
		b.dispatchWG.Wait()
		cleanup()
	}
}

// e2eWriter wraps Encoder and calls Flush after each write.
type e2eWriter struct {
	enc *protocol.Encoder
}

func (w *e2eWriter) WritePublish(f protocol.PublishFrame) error {
	if err := w.enc.WritePublish(f); err != nil {
		return err
	}
	return w.enc.Flush()
}
func (w *e2eWriter) WriteSubscribe(f protocol.SubscribeFrame) error {
	if err := w.enc.WriteSubscribe(f); err != nil {
		return err
	}
	return w.enc.Flush()
}
func (w *e2eWriter) WriteACK(f protocol.ACKFrame) error {
	if err := w.enc.WriteACK(f); err != nil {
		return err
	}
	return w.enc.Flush()
}
func (w *e2eWriter) WriteNACK(f protocol.NACKFrame) error {
	if err := w.enc.WriteNACK(f); err != nil {
		return err
	}
	return w.enc.Flush()
}
func (w *e2eWriter) WriteHeartbeat() error {
	if err := w.enc.WriteHeartbeat(); err != nil {
		return err
	}
	return w.enc.Flush()
}

func testE2EConn(t *testing.T) (*server.Connection, *e2eWriter, <-chan frameResult, func()) {
	t.Helper()
	r, w := net.Pipe()
	conn := server.NewConnection(w, config.InflightConfig{MaxInflight: 100, GlobalMax: 0})
	conn.Session = &server.Session{
		Username:  "testuser",
		Namespace: "test-ns",
	}
	dec := protocol.NewDecoder(bufio.NewReader(r))
	enc := protocol.NewEncoder(r)
	fch := startReader(dec)
	cleanup := func() {
		r.Close()
		conn.Close()
	}
	return conn, &e2eWriter{enc: enc}, fch, cleanup
}

func awaitFrame(t *testing.T, fch <-chan frameResult, timeout time.Duration) (protocol.FrameType, any) {
	t.Helper()
	select {
	case r, ok := <-fch:
		if !ok {
			return 0, nil
		}
		return r.ft, r.frame
	case <-time.After(timeout):
		return 0, nil
	}
}

func TestE2E_PublishSubscribeACK(t *testing.T) {
	b, q, cleanup := testE2EBroker(t)
	defer cleanup()

	conn, w, fch, closeConn := testE2EConn(t)
	defer closeConn()

	q.SetGroupConfig(queue.GroupConfig{
		Key:     "e2e-group",
		Quantum: 1,
	})

	go b.handleConnection(conn)

	w.WritePublish(protocol.PublishFrame{
		Queue:   "test-q",
		Group:   "e2e-group",
		Payload: []byte("e2e-payload"),
	})

	// Consume PublishOK
	ft, _ := awaitFrame(t, fch, time.Second)
	if ft != protocol.FramePublishOK {
		t.Fatalf("expected FramePublishOK, got %v", ft)
	}

	stats, ok := q.GetGroupStats("e2e-group")
	if !ok {
		t.Fatal("expected group stats")
	}
	if stats.Pending != 1 {
		t.Fatalf("expected Pending=1, got %d", stats.Pending)
	}

	w.WriteSubscribe(protocol.SubscribeFrame{Queue: "test-q", Group: "e2e-group"})

	ft, frame := awaitFrame(t, fch, 3*time.Second)
	if ft != protocol.FrameMessage {
		t.Fatalf("expected FrameMessage, got %v", ft)
	}
	msg := frame.(protocol.MessageFrame)
	if string(msg.Payload) != "e2e-payload" {
		t.Errorf("expected 'e2e-payload', got %s", string(msg.Payload))
	}

	w.WriteACK(protocol.ACKFrame{
		MsgID: msg.MsgID, Queue: "test-q", Group: "e2e-group",
	})
	time.Sleep(100 * time.Millisecond)

	stats, _ = q.GetGroupStats("e2e-group")
	if stats.Processing != 0 {
		t.Errorf("expected Processing=0 after ACK, got %d", stats.Processing)
	}
}

func TestE2E_MultiGroupPublishSubscribe(t *testing.T) {
	b, q, cleanup := testE2EBroker(t)
	defer cleanup()

	conn, w, fch, closeConn := testE2EConn(t)
	defer closeConn()

	groupCount := 3
	msgsPerGroup := 2

	for i := range groupCount {
		q.SetGroupConfig(queue.GroupConfig{
			Key:         groupKey(i),
			Parallelism: 5,
			Quantum:     1,
		})
	}

	go b.handleConnection(conn)

	for i := range groupCount {
		for range msgsPerGroup {
			w.WritePublish(protocol.PublishFrame{
				Queue: "test-q", Group: groupKey(i), Payload: []byte("multi"),
			})
		}
	}

	// Consume PublishOK for each publish
	for range groupCount * msgsPerGroup {
		ft, _ := awaitFrame(t, fch, time.Second)
		if ft != protocol.FramePublishOK {
			t.Fatalf("expected FramePublishOK, got %v", ft)
		}
	}

	for i := range groupCount {
		w.WriteSubscribe(protocol.SubscribeFrame{Queue: "test-q", Group: groupKey(i)})
	}

	received := make(map[string]int)
	deadline := time.After(10 * time.Second)
	for receivedTotal := 0; receivedTotal < groupCount*msgsPerGroup; {
		select {
		case <-deadline:
			t.Fatalf("timed out: got %d of %d msgs (received: %v)", receivedTotal, groupCount*msgsPerGroup, received)
		default:
		}
		ft, frame := awaitFrame(t, fch, 500*time.Millisecond)
		if ft == 0 {
			continue
		}
		if ft != protocol.FrameMessage {
			continue
		}
		msg := frame.(protocol.MessageFrame)
		received[msg.Group]++
		receivedTotal++
	}

	for i := range groupCount {
		gkey := groupKey(i)
		if received[gkey] != msgsPerGroup {
			t.Errorf("group %s: expected %d, got %d", gkey, msgsPerGroup, received[gkey])
		}
	}
}

func TestE2E_LoadBalanceTenGroups(t *testing.T) {
	b, q, cleanup := testE2EBroker(t)
	defer cleanup()

	conn, w, fch, closeConn := testE2EConn(t)
	defer closeConn()

	groupCount := 10
	msgsPerGroup := 3

	for i := range groupCount {
		q.SetGroupConfig(queue.GroupConfig{
			Key:         groupKey10(i),
			Parallelism: 5,
			Quantum:     3,
		})
	}

	go b.handleConnection(conn)

	for i := range groupCount {
		for range msgsPerGroup {
			w.WritePublish(protocol.PublishFrame{
				Queue: "test-q", Group: groupKey10(i), Payload: []byte("lb10"),
			})
		}
	}

	for range groupCount * msgsPerGroup {
		ft, _ := awaitFrame(t, fch, 2*time.Second)
		if ft != protocol.FramePublishOK {
			t.Fatalf("expected FramePublishOK, got %v", ft)
		}
	}

	for i := range groupCount {
		w.WriteSubscribe(protocol.SubscribeFrame{Queue: "test-q", Group: groupKey10(i)})
	}

	received := make(map[string]int)
	deadline := time.After(10 * time.Second)
	for receivedTotal := 0; receivedTotal < groupCount*msgsPerGroup; {
		select {
		case <-deadline:
			t.Fatalf("timed out: got %d of %d msgs", receivedTotal, groupCount*msgsPerGroup)
		default:
		}
		ft, frame := awaitFrame(t, fch, 500*time.Millisecond)
		if ft == 0 {
			continue
		}
		if ft != protocol.FrameMessage {
			continue
		}
		msg := frame.(protocol.MessageFrame)
		received[msg.Group]++
		receivedTotal++
	}

	for i := range groupCount {
		gkey := groupKey10(i)
		if received[gkey] != msgsPerGroup {
			t.Errorf("group %s: expected %d, got %d", gkey, msgsPerGroup, received[gkey])
		}
	}
}

func TestE2E_NACKRetryDLQ(t *testing.T) {
	b, q, cleanup := testE2EBroker(t)
	defer cleanup()

	dlqQ := queue.NewQueue("test-q-dlq", "test-ns")
	b.registerQueueWithWAL(dlqQ)

	q.SetGroupConfig(queue.GroupConfig{
		Key:          "retry-group",
		Parallelism:  5,
		LeaseTimeout: 5 * time.Second,
		Retry: queue.RetryConfig{
			MaxAttempts:  2,
			Backoff:      queue.BackoffFixed,
			InitialDelay: 15 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
		},
		DLQ: &queue.DLQConfig{
			Enabled: true,
			Queue:   "test-q-dlq",
		},
	})

	conn, w, fch, closeConn := testE2EConn(t)
	defer closeConn()

	go b.handleConnection(conn)

	w.WritePublish(protocol.PublishFrame{
		Queue: "test-q", Group: "retry-group", Payload: []byte("nack-test"),
	})

	ft, _ := awaitFrame(t, fch, time.Second)
	if ft != protocol.FramePublishOK {
		t.Fatalf("expected FramePublishOK, got %v", ft)
	}

	w.WriteSubscribe(protocol.SubscribeFrame{Queue: "test-q", Group: "retry-group"})

	ft, frame := awaitFrame(t, fch, 3*time.Second)
	if ft != protocol.FrameMessage {
		t.Fatalf("expected FrameMessage, got %v", ft)
	}
	msg := frame.(protocol.MessageFrame)

	w.WriteNACK(protocol.NACKFrame{
		MsgID: msg.MsgID, Queue: "test-q", Group: "retry-group",
	})

	ft, frame = awaitFrame(t, fch, 5*time.Second)
	if ft != protocol.FrameMessage {
		t.Fatalf("expected FrameMessage, got %v", ft)
	}
	msg2 := frame.(protocol.MessageFrame)

	w.WriteNACK(protocol.NACKFrame{
		MsgID: msg2.MsgID, Queue: "test-q", Group: "retry-group",
	})

	time.Sleep(200 * time.Millisecond)
	stats, ok := dlqQ.GetGroupStats("retry-group")
	if !ok {
		t.Fatal("expected DLQ group stats")
	}
	if stats.Pending < 1 {
		t.Errorf("expected at least 1 pending in DLQ, got %d", stats.Pending)
	}
}

func TestE2E_Heartbeat(t *testing.T) {
	b, _, cleanup := testE2EBroker(t)
	defer cleanup()

	conn, w, fch, closeConn := testE2EConn(t)
	defer closeConn()

	go b.handleConnection(conn)

	if err := w.WriteHeartbeat(); err != nil {
		t.Fatalf("WriteHeartbeat: %v", err)
	}

	ft, _ := awaitFrame(t, fch, 2*time.Second)
	if ft != protocol.FrameHeartbeat {
		t.Fatalf("expected FrameHeartbeat, got %v", ft)
	}
}

func TestE2E_MultipleConnections(t *testing.T) {
	b, q, cleanup := testE2EBroker(t)
	defer cleanup()

	q.SetGroupConfig(queue.GroupConfig{
		Key:         "shared-group",
		Parallelism: 3,
		Quantum:     10,
	})

	// Publisher uses its own connection
	pubConn, pubW, pubFch, pubClose := testE2EConn(t)
	defer pubClose()
	subConn1, subW1, subFch1, subClose1 := testE2EConn(t)
	defer subClose1()
	subConn2, subW2, subFch2, subClose2 := testE2EConn(t)
	defer subClose2()

	go b.handleConnection(pubConn)
	go b.handleConnection(subConn1)
	go b.handleConnection(subConn2)

	// Subscribe both via pipe (the real client path)
	subW1.WriteSubscribe(protocol.SubscribeFrame{Queue: "test-q", Group: "shared-group"})
	subW2.WriteSubscribe(protocol.SubscribeFrame{Queue: "test-q", Group: "shared-group"})
	time.Sleep(100 * time.Millisecond)

	// Publish 6 messages via the publisher connection (3× quantum)
	for range 6 {
		pubW.WritePublish(protocol.PublishFrame{
			Queue: "test-q", Group: "shared-group", Payload: []byte("multi-conn"),
		})
	}
	for range 6 {
		ft, _ := awaitFrame(t, pubFch, time.Second)
		if ft != protocol.FramePublishOK {
			t.Fatalf("expected FramePublishOK, got %v", ft)
		}
	}

	// Collect messages from both subscribers concurrently
	got1, got2 := 0, 0
	deadline := time.After(5 * time.Second)
	for total := 0; total < 6; {
		select {
		case <-deadline:
			t.Fatalf("timed out: sub1=%d sub2=%d total=%d", got1, got2, total)
		default:
		}
		select {
		case r := <-subFch1:
			if r.ft == protocol.FrameMessage {
				got1++
				total++
			}
		case r := <-subFch2:
			if r.ft == protocol.FrameMessage {
				got2++
				total++
			}
		case <-time.After(200 * time.Millisecond):
		}
	}

	if got1 == 0 || got2 == 0 {
		t.Errorf("both connections should receive: sub1=%d sub2=%d", got1, got2)
	}
}

func TestE2E_PublishWithoutSubscriber(t *testing.T) {
	b, q, cleanup := testE2EBroker(t)
	defer cleanup()

	conn, w, fch, closeConn := testE2EConn(t)
	defer closeConn()

	go b.handleConnection(conn)

	for range 50 {
		w.WritePublish(protocol.PublishFrame{
			Queue: "test-q", Group: "no-sub-group", Payload: []byte("queued"),
		})
	}

	for range 50 {
		ft, _ := awaitFrame(t, fch, 2*time.Second)
		if ft != protocol.FramePublishOK {
			t.Fatalf("expected FramePublishOK, got %v", ft)
		}
	}

	stats, ok := q.GetGroupStats("no-sub-group")
	if !ok {
		t.Fatal("expected group stats")
	}
	if stats.Pending != 50 {
		t.Errorf("expected Pending=50, got %d", stats.Pending)
	}
}

func groupKey(i int) string {
	return []string{"g-alpha", "g-beta", "g-gamma"}[i]
}

func groupKey10(i int) string {
	return []string{"g-0", "g-1", "g-2", "g-3", "g-4",
		"g-5", "g-6", "g-7", "g-8", "g-9"}[i]
}

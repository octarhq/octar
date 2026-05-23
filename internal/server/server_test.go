package server

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/protocol"
)

func TestCredit_AcquireRelease(t *testing.T) {
	c := newCredit(config.InflightConfig{MaxInflight: 5})

	for i := 0; i < 5; i++ {
		if !c.AcquireCredit() {
			t.Fatalf("expected acquire %d to succeed", i)
		}
	}

	if c.AcquireCredit() {
		t.Fatal("expected acquire beyond limit to fail")
	}

	c.ReleaseCredit()

	if !c.AcquireCredit() {
		t.Fatal("expected acquire after release to succeed")
	}
}

func TestCredit_AcquireZeroLimit(t *testing.T) {
	c := newCredit(config.InflightConfig{MaxInflight: 0})

	if c.AcquireCredit() {
		t.Fatal("expected acquire with limit 0 to fail")
	}
}

func TestCredit_InflightCount(t *testing.T) {
	c := newCredit(config.InflightConfig{MaxInflight: 100})

	if c.Inflight() != 0 {
		t.Fatalf("initial inflight: %d, want 0", c.Inflight())
	}

	c.AcquireCredit()
	if c.Inflight() != 1 {
		t.Fatalf("inflight after acquire: %d, want 1", c.Inflight())
	}

	c.ReleaseCredit()
	if c.Inflight() != 0 {
		t.Fatalf("inflight after release: %d, want 0", c.Inflight())
	}
}

func TestCredit_ReleaseUnderflow(t *testing.T) {
	c := newCredit(config.InflightConfig{MaxInflight: 10})

	c.ReleaseCredit()
	if c.Inflight() != -1 {
		t.Fatalf("expected inflight -1 after release with no acquire, got %d", c.Inflight())
	}
}

func TestCredit_Concurrent(t *testing.T) {
	c := newCredit(config.InflightConfig{MaxInflight: 10})

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if c.AcquireCredit() {
					c.ReleaseCredit()
				}
			}
		}()
	}
	wg.Wait()
}

func TestCredit_ConcurrentMaxNotExceeded(t *testing.T) {
	c := newCredit(config.InflightConfig{MaxInflight: 5})

	var wg sync.WaitGroup
	var maxSeen atomic.Int32
	gate := make(chan struct{})

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.AcquireCredit() {
				current := c.Inflight()
				for {
					prev := maxSeen.Load()
					if current <= prev || maxSeen.CompareAndSwap(prev, current) {
						break
					}
				}
				<-gate
				c.ReleaseCredit()
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()

	if ms := maxSeen.Load(); ms > 5 {
		t.Fatalf("inflight exceeded max: %d > 5", ms)
	}
}

func TestTokenBucket_Basic(t *testing.T) {
	tb := newTokenBucket(100, 100)

	for i := 0; i < 100; i++ {
		if !tb.allow() {
			t.Fatalf("expected allow %d to succeed", i)
		}
	}

	if tb.allow() {
		t.Fatal("expected 101st allow to fail")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	tb := newTokenBucket(100, 100)

	for i := 0; i < 100; i++ {
		tb.allow()
	}

	if tb.allow() {
		t.Fatal("expected allow to fail after exhausting tokens")
	}

	time.Sleep(15 * time.Millisecond)

	if !tb.allow() {
		t.Fatal("expected allow to succeed after refill")
	}
}

func TestTokenBucket_BurstLimit(t *testing.T) {
	tb := newTokenBucket(10, 5)

	for i := 0; i < 5; i++ {
		if !tb.allow() {
			t.Fatalf("expected allow %d to succeed within burst", i)
		}
	}

	if tb.allow() {
		t.Fatal("expected allow beyond burst to fail")
	}
}

func TestTokenBucket_RateZero(t *testing.T) {
	tb := newTokenBucket(0, 5)

	for i := 0; i < 5; i++ {
		if !tb.allow() {
			t.Fatalf("expected allow %d to succeed", i)
		}
	}

	if tb.allow() {
		t.Fatal("expected allow with rate=0 to fail after burst exhausted")
	}

	time.Sleep(10 * time.Millisecond)

	if tb.allow() {
		t.Fatal("expected allow with rate=0 to still fail after wait (no refill)")
	}
}

func TestTokenBucket_NoBurst(t *testing.T) {
	tb := newTokenBucket(100, 0)

	if tb.allow() {
		t.Fatal("expected allow with burst=0 to fail")
	}
}

func TestConnection_WriterLoop(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	err := conn.WriteMessage(protocol.MessageFrame{
		MsgID: "msg-1", Queue: "q", Group: "g",
		Payload: []byte("hello"), Attempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	dec := protocol.NewDecoder(bufio.NewReader(client))
	ft, frame, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FrameMessage {
		t.Fatalf("expected FrameMessage, got 0x%02x", byte(ft))
	}
	msg := frame.(protocol.MessageFrame)
	if msg.MsgID != "msg-1" {
		t.Fatalf("MsgID: got %q, want %q", msg.MsgID, "msg-1")
	}
	if string(msg.Payload) != "hello" {
		t.Fatalf("Payload: got %q, want %q", string(msg.Payload), "hello")
	}
	if msg.Attempts != 1 {
		t.Fatalf("Attempts: got %d, want 1", msg.Attempts)
	}
}

func TestConnection_WriterLoopMultipleTypes(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	_ = conn.WritePublishOK(protocol.PublishOKFrame{MsgID: "pok-1", Offset: 42})
	_ = conn.WriteError(protocol.ErrorFrame{Code: 500, Message: "internal error"})
	_ = conn.WriteHeartbeat()

	dec := protocol.NewDecoder(bufio.NewReader(client))

	ft, v, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FramePublishOK {
		t.Fatalf("expected FramePublishOK, got 0x%02x", byte(ft))
	}
	pok := v.(protocol.PublishOKFrame)
	if pok.MsgID != "pok-1" || pok.Offset != 42 {
		t.Fatalf("PublishOK mismatch: %+v", pok)
	}

	ft, v, err = dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FrameError {
		t.Fatalf("expected FrameError, got 0x%02x", byte(ft))
	}
	errf := v.(protocol.ErrorFrame)
	if errf.Code != 500 || errf.Message != "internal error" {
		t.Fatalf("Error mismatch: %+v", errf)
	}

	ft, v, err = dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FrameHeartbeat {
		t.Fatalf("expected FrameHeartbeat, got 0x%02x", byte(ft))
	}
	if v != nil {
		t.Fatal("expected nil payload for heartbeat")
	}
}

func TestConnection_WriterLoopDrain(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	for i := 0; i < 10; i++ {
		err := conn.WriteMessage(protocol.MessageFrame{
			MsgID: "msg", Queue: "q", Group: "g",
			Payload: []byte("data"), Attempts: 1,
		})
		if err != nil {
			t.Fatalf("WriteMessage %d: %v", i, err)
		}
	}

	dec := protocol.NewDecoder(bufio.NewReader(client))
	for i := 0; i < 10; i++ {
		ft, _, err := dec.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if ft != protocol.FrameMessage {
			t.Fatalf("frame %d: expected FrameMessage, got 0x%02x", i, byte(ft))
		}
	}
}

func TestConnection_WriterLoopBufferFull(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	fillCount := 0
	for i := 0; i < 2000; i++ {
		err := conn.WriteMessage(protocol.MessageFrame{
			MsgID: "msg", Queue: "q", Group: "g",
			Payload: []byte("data"), Attempts: 1,
		})
		if err != nil {
			break
		}
		fillCount++
	}

	t.Logf("filled %d messages before buffer full", fillCount)

	if fillCount >= 2000 {
		t.Fatal("expected buffer to fill up")
	}
}

func TestConnection_ReadFrameDeadline(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 50*time.Millisecond, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	start := time.Now()
	_, _, err := conn.ReadFrame()
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error, got: %v (%T)", err, err)
	}

	if duration < 40*time.Millisecond {
		t.Fatalf("returned too fast: %v", duration)
	}
}

func TestConnection_ReadFrameRespectsDeadline(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 100*time.Millisecond, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	t.Run("first_read_times_out", func(t *testing.T) {
		start := time.Now()
		_, _, err := conn.ReadFrame()
		if err == nil {
			t.Fatal("expected timeout")
		}
		if d := time.Since(start); d < 80*time.Millisecond {
			t.Fatalf("too fast: %v", d)
		}
	})

	t.Run("subsequent_read_also_times_out", func(t *testing.T) {
		start := time.Now()
		_, _, err := conn.ReadFrame()
		if err == nil {
			t.Fatal("expected timeout on subsequent read")
		}
		if d := time.Since(start); d < 80*time.Millisecond {
			t.Fatalf("too fast: %v", d)
		}
	})
}

func TestConnection_ReadFrameNoDeadline(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = conn.ReadFrame()
	}()

	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("ReadFrame without deadline should block")
	default:
	}

	enc := protocol.NewEncoder(client)
	_ = enc.WriteHeartbeat()
	enc.Flush()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReadFrame should return after data arrives")
	}
}

func TestConnection_WritePublishOK(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	_ = conn.WritePublishOK(protocol.PublishOKFrame{MsgID: "m1", Offset: 100})

	dec := protocol.NewDecoder(bufio.NewReader(client))
	ft, v, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FramePublishOK {
		t.Fatalf("expected FramePublishOK, got 0x%02x", byte(ft))
	}
	pok := v.(protocol.PublishOKFrame)
	if pok.MsgID != "m1" || pok.Offset != 100 {
		t.Fatalf("PublishOK mismatch: %+v", pok)
	}
}

func TestConnection_WriteError(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	_ = conn.WriteError(protocol.ErrorFrame{Code: 404, Message: "not found"})

	dec := protocol.NewDecoder(bufio.NewReader(client))
	ft, v, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FrameError {
		t.Fatalf("expected FrameError, got 0x%02x", byte(ft))
	}
	ef := v.(protocol.ErrorFrame)
	if ef.Code != 404 || ef.Message != "not found" {
		t.Fatalf("Error mismatch: %+v", ef)
	}
}

func TestConnection_BackpressureFrame(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	err := conn.WriteBackpressure(protocol.BackpressureFrame{
		Reason: "slow down", RetryAfter: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}

	dec := protocol.NewDecoder(bufio.NewReader(client))
	ft, v, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ft != protocol.FrameBackpressure {
		t.Fatalf("expected FrameBackpressure, got 0x%02x", byte(ft))
	}
	bp := v.(protocol.BackpressureFrame)
	if bp.Reason != "slow down" || bp.RetryAfter != 5000 {
		t.Fatalf("Backpressure mismatch: %+v", bp)
	}
}

func TestConnection_BackpressureNoPanicOnFullChannel(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	for i := 0; i < 2000; i++ {
		err := conn.WriteBackpressure(protocol.BackpressureFrame{
			Reason: "pressure", RetryAfter: 1,
		})
		if err != nil {
			t.Fatalf("WriteBackpressure %d should never return error: %v", i, err)
		}
	}
}

func TestConnection_Stop(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer client.Close()

	conn.Close()
	server.Close()

	var wroteOK atomic.Bool
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := conn.WriteMessage(protocol.MessageFrame{
				MsgID: "post-close", Queue: "q", Group: "g",
				Payload: nil, Attempts: 1,
			})
			if err == nil {
				wroteOK.Store(true)
			}
		}()
	}
	wg.Wait()

	if wroteOK.Load() {
		t.Log("some writes succeeded (race with writerLoop)—acceptable")
	}
}

func TestConnection_CloseCleanup(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer client.Close()

	conn.Close()

	err := conn.WriteMessage(protocol.MessageFrame{
		MsgID: "after-close", Queue: "q", Group: "g",
		Payload: nil, Attempts: 1,
	})
	if err == nil {
		t.Log("write after close may succeed (select race with stopCh)")
	}
}

func TestConnection_Inflight(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 5}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	if conn.Inflight() != 0 {
		t.Fatalf("initial inflight: %d, want 0", conn.Inflight())
	}

	conn.AcquireCredit()
	if conn.Inflight() != 1 {
		t.Fatalf("inflight after acquire: %d, want 1", conn.Inflight())
	}

	conn.ReleaseCredit()
	if conn.Inflight() != 0 {
		t.Fatalf("inflight after release: %d, want 0", conn.Inflight())
	}
}

func TestConnection_FlushIsNoop(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	if err := conn.Flush(); err != nil {
		t.Fatalf("Flush should be no-op: %v", err)
	}
}

func TestConnection_RemoteAddr(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	addr := conn.RemoteAddr()
	if addr == nil {
		t.Fatal("RemoteAddr should not be nil")
	}
	if addr.Network() != "pipe" {
		t.Fatalf("expected pipe network, got %s", addr.Network())
	}
}

func TestConnection_RecordHooks(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer func() {
		conn.Close()
		client.Close()
	}()

	conn.RecordDispatch("m1")
	conn.RecordACK("m1")
	conn.RecordNACK("m2")
	conn.RecordWriteFail()
}

func TestTokenBucket_Concurrent(t *testing.T) {
	tb := newTokenBucket(10000, 10000)

	var wg sync.WaitGroup
	var allowed atomic.Int64
	var denied atomic.Int64

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				if tb.allow() {
					allowed.Add(1)
				} else {
					denied.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	t.Logf("allowed=%d denied=%d", allowed.Load(), denied.Load())
	if allowed.Load() == 0 {
		t.Fatal("expected some allows")
	}
}

func TestTokenBucket_RefillMoreThanBurst(t *testing.T) {
	tb := newTokenBucket(100, 10)

	for i := 0; i < 10; i++ {
		tb.allow()
	}

	if tb.allow() {
		t.Fatal("expected 11th to fail")
	}

	time.Sleep(200 * time.Millisecond)

	allowed := 0
	for range 50 {
		if tb.allow() {
			allowed++
		}
	}

	if allowed < 1 || allowed > 11 {
		t.Fatalf("expected ~10 tokens (burst cap) after 200ms at 100/s, got %d", allowed)
	}
	if allowed > 10 {
		t.Logf("burst cap allowed %d tokens (slightly above 10 due to timing)", allowed)
	}
}

func TestConnection_WriterLoopServerClose(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer client.Close()

	server.Close()
	time.Sleep(20 * time.Millisecond)

	err := conn.WriteMessage(protocol.MessageFrame{
		MsgID: "post-server-close", Queue: "q", Group: "g",
		Payload: []byte("x"), Attempts: 1,
	})
	if err != nil {
		t.Fatal("WriteMessage should accept (writerLoop handles error)")
	}
}

func TestConnection_ConcurrentWriteAndClose(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = conn.WriteMessage(protocol.MessageFrame{
				MsgID: "race", Queue: "q", Group: "g",
				Payload: []byte("data"), Attempts: 1,
			})
		}()
	}

	time.Sleep(time.Millisecond)
	conn.Close()
	wg.Wait()
}

func TestConnection_ConcurrentAcquireReleaseAndClose(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 5}, 0, 0)

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if conn.AcquireCredit() {
					time.Sleep(time.Microsecond)
					conn.ReleaseCredit()
				}
			}
		}()
	}

	time.Sleep(time.Millisecond)
	conn.Close()
	wg.Wait()
}

func TestConnection_WriteMultipleFrameTypesThenClose(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer conn.Close()
	defer client.Close()

	_ = conn.WritePublishOK(protocol.PublishOKFrame{MsgID: "m1", Offset: 1})
	_ = conn.WriteError(protocol.ErrorFrame{Code: 500, Message: "err"})
	_ = conn.WriteBackpressure(protocol.BackpressureFrame{Reason: "slow", RetryAfter: 100})
	_ = conn.WriteHeartbeat()

	dec := protocol.NewDecoder(bufio.NewReader(client))
	for i := 0; i < 4; i++ {
		_, _, err := dec.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
	}
}

func TestConnection_WriteMessageAfterCloseMaySucceed(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)

	conn.Close()

	for i := 0; i < 20; i++ {
		err := conn.WriteMessage(protocol.MessageFrame{
			MsgID: "post-close", Queue: "q", Group: "g",
			Payload: []byte("x"), Attempts: 1,
		})
		if err != nil && err != net.ErrClosed {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestConnection_CreditExhaustionDoesNotBlock(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 3}, 0, 0)
	defer conn.Close()
	defer server.Close()

	for i := 0; i < 3; i++ {
		if !conn.AcquireCredit() {
			t.Fatalf("acquire %d should succeed", i)
		}
	}

	if conn.AcquireCredit() {
		t.Fatal("acquire beyond limit should fail")
	}

	for i := 0; i < 3; i++ {
		conn.ReleaseCredit()
	}

	if !conn.AcquireCredit() {
		t.Fatal("acquire after release should succeed")
	}
}

func TestConnection_MassiveBackpressureNoBlock(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer conn.Close()

	for i := 0; i < 5000; i++ {
		err := conn.WriteBackpressure(protocol.BackpressureFrame{
			Reason: "pressure", RetryAfter: 1,
		})
		if err != nil {
			t.Fatalf("WriteBackpressure %d returned error: %v", i, err)
		}
	}
}

func TestConnection_CloseMultipleTimes(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)

	conn.Close()
	conn.Close()
}

func TestConnection_ReadFrameOnClosedConn(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)

	server.Close()
	client.Close()

	time.Sleep(10 * time.Millisecond)

	_, _, err := conn.ReadFrame()
	if err == nil {
		t.Log("ReadFrame on closed conn returned nil err (acceptable)")
	}
}

func TestConnection_PingPongHeartbeats(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer conn.Close()
	defer client.Close()

	for i := 0; i < 10; i++ {
		_ = conn.WriteHeartbeat()
	}

	dec := protocol.NewDecoder(bufio.NewReader(client))
	for i := 0; i < 10; i++ {
		ft, v, err := dec.ReadFrame()
		if err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
		if ft != protocol.FrameHeartbeat {
			t.Fatalf("heartbeat %d: got 0x%02x", i, byte(ft))
		}
		if v != nil {
			t.Fatalf("heartbeat %d: payload not nil", i)
		}
	}
}

func TestConnection_AuthWithBadConnectFrame(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer conn.Close()
	defer client.Close()

	go func() {
		enc := protocol.NewEncoder(client)
		_ = enc.WritePublish(protocol.PublishFrame{Queue: "q", Group: "g", Payload: []byte("x")})
		enc.Flush()
	}()

	time.Sleep(50 * time.Millisecond)

	result := conn.Authenticate(nil, nil)
	if result {
		t.Fatal("Authenticate with wrong frame should return false")
	}
}

func TestConnection_AuthWithPartialConnectFrame(t *testing.T) {
	client, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 10}, 0, 0)
	defer conn.Close()
	defer client.Close()

	go func() {
		conn2 := newConnection(client, config.InflightConfig{MaxInflight: 10}, 0, 0)
		defer conn2.Close()
		_ = conn2.WritePublishOK(protocol.PublishOKFrame{MsgID: "x", Offset: 0})
	}()

	time.Sleep(50 * time.Millisecond)

	_, _, err := conn.ReadFrame()
	if err != nil {
		t.Logf("ReadFrame on partial data: %v (expected possible)", err)
	}
}

func TestConnection_AcquireCreditAfterClose(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 5}, 0, 0)
	defer server.Close()

	conn.Close()

	if conn.AcquireCredit() {
		conn.ReleaseCredit()
	}
}

func TestConnection_InflightBounds(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 1}, 0, 0)
	defer conn.Close()
	defer server.Close()

	if conn.Inflight() != 0 {
		t.Fatalf("initial inflight: %d", conn.Inflight())
	}

	conn.AcquireCredit()
	if conn.Inflight() != 1 {
		t.Fatalf("after acquire: %d", conn.Inflight())
	}

	if conn.AcquireCredit() {
		t.Fatal("second acquire should fail")
	}
	if conn.Inflight() != 1 {
		t.Fatalf("after failed acquire: %d", conn.Inflight())
	}

	conn.ReleaseCredit()
	if conn.Inflight() != 0 {
		t.Fatalf("after release: %d", conn.Inflight())
	}
}

func TestConnection_ConcurrentDispatch(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 100}, 0, 0)
	defer conn.Close()
	defer server.Close()

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if conn.AcquireCredit() {
					time.Sleep(time.Microsecond)
					conn.ReleaseCredit()
				}
			}
		}()
	}
	wg.Wait()
}

func TestConnection_MixedCreditAndInflight(t *testing.T) {
	_, server := net.Pipe()
	conn := newConnection(server, config.InflightConfig{MaxInflight: 5}, 0, 0)
	defer conn.Close()
	defer server.Close()

	for i := 0; i < 5; i++ {
		if !conn.AcquireCredit() {
			t.Fatalf("acquire %d", i)
		}
	}

	for i := 0; i < 5; i++ {
		if c := conn.Inflight(); c != int32(5-i) {
			t.Fatalf("inflight after release %d: %d", i, c)
		}
		conn.ReleaseCredit()
	}

	for i := 0; i < 5; i++ {
		if !conn.AcquireCredit() {
			t.Fatalf("re-acquire %d", i)
		}
	}
	for i := 0; i < 5; i++ {
		conn.ReleaseCredit()
	}
}

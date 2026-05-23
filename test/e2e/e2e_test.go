package e2e

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/auth"
	"github.com/octarhq/octar/internal/broker"
	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/db"
	"github.com/octarhq/octar/internal/protocol"
	"github.com/octarhq/octar/internal/queue"
)

func startTestBroker(t *testing.T) (*broker.Broker, int, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	dataDir := t.TempDir()

	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:           "127.0.0.1",
			Port:           port,
			MaxConnections: 100,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   5 * time.Second,
			GlobalMaxMsgs:  100000,
			Inflight: config.InflightConfig{
				MaxInflight: 256,
				GlobalMax:   10000,
			},
		},
		Storage: config.StorageConfig{
			DataDir: dataDir,
			WAL: config.WALConfig{
				FlushInterval:    25 * time.Millisecond, // alinhado com o default de produção
				FlushMaxMessages: 1000,                  // era 100 — lotes maiores, menos flushes
				SegmentMaxBytes:  64 << 20,
				Durable:          false, // mantém false em testes para velocidade
				SnapshotInterval: 60 * time.Second,
			},
		},
		Auth: config.AuthConfig{
			Enabled: true,
			DefaultAdmin: config.DefaultAdminConfig{
				Username: "admin",
				Password: "testpass123!",
			},
			Providers: config.ProvidersConfig{
				Password: config.PasswordProviderConfig{
					Enabled:    true,
					Priority:   10,
					BcryptCost: 4, // custo mínimo — ~3ms vs ~300ms com cost=12
					// Em produção usa o default (12). Em teste, bcrypt não é o que queremos medir.
				},
			},
		},
		API:     config.APIConfig{Host: "127.0.0.1", Port: 0},
		Metrics: config.MetricsConfig{Enabled: false},
		PProf:   config.PProfConfig{Enabled: false},
	}

	store, err := db.New(dataDir, cfg.Auth.DefaultAdmin)
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}

	authSvc := auth.NewService(cfg.Auth, store, dataDir)

	b, err := broker.New(cfg, store, authSvc)
	if err != nil {
		store.Close()
		t.Fatalf("broker.New: %v", err)
	}

	if err := b.Start(); err != nil {
		store.Close()
		t.Fatalf("broker.Start: %v", err)
	}

	return b, port, func() {
		_ = b.Stop()
		store.Close()
	}
}

func registerQueue(t *testing.T, b *broker.Broker, name string) {
	t.Helper()
	q := queue.NewQueue(name, "main")
	b.Scheduler.RegisterQueue(q)
}

func dial(t *testing.T, port int) (net.Conn, *protocol.Encoder, *protocol.Decoder) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, protocol.NewEncoder(conn), protocol.NewDecoder(conn)
}

func connect(t *testing.T, enc *protocol.Encoder, dec *protocol.Decoder, username, password string) {
	t.Helper()
	if err := enc.WriteConnect(protocol.ConnectFrame{
		Username:  username,
		Password:  password,
		Namespace: "main",
	}); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	ft, payload, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("read connect response: %v", err)
	}
	if ft != protocol.FrameConnectOK {
		reason := ""
		if ft == protocol.FrameConnectErr {
			reason = payload.(protocol.ConnectErrFrame).Reason
		}
		t.Fatalf("expected ConnectOK, got %02x: %s", byte(ft), reason)
	}
}

func dialAndConnect(t *testing.T, port int, username, password string) (net.Conn, *protocol.Encoder, *protocol.Decoder) {
	t.Helper()
	conn, enc, dec := dial(t, port)
	connect(t, enc, dec, username, password)
	return conn, enc, dec
}

func readFrame(t *testing.T, dec *protocol.Decoder) (protocol.FrameType, any) {
	t.Helper()
	ft, payload, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	return ft, payload
}

func TestE2E_PublishSubscribeACK(t *testing.T) {
	b, port, cleanup := startTestBroker(t)
	defer cleanup()

	registerQueue(t, b, "test-queue")

	_, pubEnc, pubDec := dialAndConnect(t, port, "admin", "testpass123!")
	_, subEnc, subDec := dialAndConnect(t, port, "admin", "testpass123!")

	if err := subEnc.WriteSubscribe(protocol.SubscribeFrame{
		Queue: "test-queue",
		Group: "default",
	}); err != nil {
		t.Fatal(err)
	}

	if err := pubEnc.WritePublish(protocol.PublishFrame{
		Queue:   "test-queue",
		Group:   "default",
		Payload: []byte("hello world"),
	}); err != nil {
		t.Fatal(err)
	}

	ft, p := readFrame(t, pubDec)
	if ft != protocol.FramePublishOK {
		t.Fatalf("expected PublishOK, got %02x", byte(ft))
	}
	pok := p.(protocol.PublishOKFrame)
	if pok.MsgID == "" {
		t.Fatal("expected non-empty MsgID in PublishOK")
	}

	ft, p = readFrame(t, subDec)
	if ft != protocol.FrameMessage {
		t.Fatalf("expected FrameMessage, got %02x", byte(ft))
	}
	msg := p.(protocol.MessageFrame)
	if string(msg.Payload) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(msg.Payload))
	}
	if msg.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", msg.Attempts)
	}
	if msg.MsgID != pok.MsgID {
		t.Fatalf("message MsgID mismatch: publish=%q deliver=%q", pok.MsgID, msg.MsgID)
	}

	if err := subEnc.WriteACK(protocol.ACKFrame{
		MsgID: msg.MsgID,
		Queue: "test-queue",
		Group: "default",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_MultiGroup(t *testing.T) {
	b, port, cleanup := startTestBroker(t)
	defer cleanup()

	registerQueue(t, b, "mg-queue")

	_, pubEnc, pubDec := dialAndConnect(t, port, "admin", "testpass123!")
	_, subEnc, subDec := dialAndConnect(t, port, "admin", "testpass123!")

	if err := subEnc.WriteSubscribe(protocol.SubscribeFrame{Queue: "mg-queue", Group: "group-a"}); err != nil {
		t.Fatal(err)
	}
	if err := subEnc.WriteSubscribe(protocol.SubscribeFrame{Queue: "mg-queue", Group: "group-b"}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := pubEnc.WritePublish(protocol.PublishFrame{Queue: "mg-queue", Group: "group-a", Payload: []byte("a-" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
		if err := pubEnc.WritePublish(protocol.PublishFrame{Queue: "mg-queue", Group: "group-b", Payload: []byte("b-" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 6; i++ {
		ft, _ := readFrame(t, pubDec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("publish %d: expected PublishOK, got %02x", i, byte(ft))
		}
	}

	received := make(map[string]int)
	for i := 0; i < 6; i++ {
		ft, p := readFrame(t, subDec)
		if ft != protocol.FrameMessage {
			t.Fatalf("expected FrameMessage, got %02x", byte(ft))
		}
		msg := p.(protocol.MessageFrame)
		received[string(msg.Payload)]++
		if err := subEnc.WriteACK(protocol.ACKFrame{MsgID: msg.MsgID, Queue: "mg-queue", Group: msg.Group}); err != nil {
			t.Fatal(err)
		}
	}

	if len(received) != 6 {
		t.Fatalf("expected 6 unique messages, got %d", len(received))
	}
	for k, v := range received {
		if v != 1 {
			t.Fatalf("expected each message once, got %s x%d", k, v)
		}
	}
}

func TestE2E_PublishWithoutSubscriber(t *testing.T) {
	b, port, cleanup := startTestBroker(t)
	defer cleanup()

	registerQueue(t, b, "no-sub-queue")

	_, pubEnc, pubDec := dialAndConnect(t, port, "admin", "testpass123!")

	for i := 0; i < 5; i++ {
		if err := pubEnc.WritePublish(protocol.PublishFrame{
			Queue:   "no-sub-queue",
			Group:   "default",
			Payload: []byte("msg-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 5; i++ {
		ft, _ := readFrame(t, pubDec)
		if ft != protocol.FramePublishOK {
			t.Fatalf("msg %d: expected PublishOK, got %02x", i, byte(ft))
		}
	}

	_, subEnc, subDec := dialAndConnect(t, port, "admin", "testpass123!")

	if err := subEnc.WriteSubscribe(protocol.SubscribeFrame{Queue: "no-sub-queue", Group: "default"}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		ft, p := readFrame(t, subDec)
		if ft != protocol.FrameMessage {
			t.Fatalf("expected FrameMessage, got %02x", byte(ft))
		}
		msg := p.(protocol.MessageFrame)
		if err := subEnc.WriteACK(protocol.ACKFrame{MsgID: msg.MsgID, Queue: "no-sub-queue", Group: "default"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestE2E_Heartbeat(t *testing.T) {
	_, port, cleanup := startTestBroker(t)
	defer cleanup()

	conn, enc, dec := dialAndConnect(t, port, "admin", "testpass123!")
	defer conn.Close()

	if err := enc.WriteHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}

	ft, _, err := dec.ReadFrame()
	if err != nil {
		t.Fatalf("read heartbeat echo: %v", err)
	}
	if ft != protocol.FrameHeartbeat {
		t.Fatalf("expected heartbeat, got %02x", byte(ft))
	}
}

func TestE2E_InvalidCredentials(t *testing.T) {
	_, port, cleanup := startTestBroker(t)
	defer cleanup()

	conn, enc, dec := dial(t, port)
	defer conn.Close()

	if err := enc.WriteConnect(protocol.ConnectFrame{
		Username:  "admin",
		Password:  "wrong-password",
		Namespace: "main",
	}); err != nil {
		t.Fatal(err)
	}

	ft, p, err := dec.ReadFrame()
	if err != nil {
		t.Logf("connection closed before error frame flushed (acceptable race): %v", err)
		return
	}
	if ft != protocol.FrameConnectErr {
		t.Fatalf("expected ConnectErr, got %02x", byte(ft))
	}
	errFrame := p.(protocol.ConnectErrFrame)
	if errFrame.Reason == "" {
		t.Fatal("expected non-empty error reason")
	}
}

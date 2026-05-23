package server

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/auth"
	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/db"
	"github.com/octarhq/octar/internal/protocol"
)

func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func testAuthServer(t *testing.T, port int, maxConns int32, handler ConnHandler) *TCPServer {
	t.Helper()
	store, err := db.New(t.TempDir(), config.DefaultAdminConfig{Username: "admin", Password: "testpass123!"})
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	authSvc := auth.NewService(config.AuthConfig{
		Enabled:      true,
		DefaultAdmin: config.DefaultAdminConfig{Username: "admin", Password: "testpass123!"},
		Providers: config.ProvidersConfig{
			Password: config.PasswordProviderConfig{Enabled: true, Priority: 10},
		},
	}, store, "")

	return NewTCPServer("127.0.0.1", port, store, authSvc, handler,
		config.InflightConfig{MaxInflight: 100}, maxConns, time.Second, time.Second, nil, 0)
}

func dialAndSendConnect(t *testing.T, port int) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	enc := protocol.NewEncoder(conn)
	_ = enc.WriteConnect(protocol.ConnectFrame{
		Username: "admin", Password: "testpass123!", Namespace: "main",
	})
	return conn
}

func TestTCPServer_NewStartStop(t *testing.T) {
	s := NewTCPServer("127.0.0.1", findFreePort(t), nil, nil, nil,
		config.InflightConfig{}, 10, time.Second, time.Second, nil, 0)

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if s.ActiveConns() != 0 {
		t.Fatalf("ActiveConns: expected 0, got %d", s.ActiveConns())
	}

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestTCPServer_StopWithoutStart(t *testing.T) {
	s := NewTCPServer("127.0.0.1", 0, nil, nil, nil,
		config.InflightConfig{}, 10, 0, 0, nil, 0)

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop without Start: %v", err)
	}
}

func TestTCPServer_StartPortConflict(t *testing.T) {
	port := findFreePort(t)

	s1 := NewTCPServer("127.0.0.1", port, nil, nil, nil,
		config.InflightConfig{}, 10, time.Second, time.Second, nil, 0)
	if err := s1.Start(); err != nil {
		t.Fatalf("s1 Start: %v", err)
	}
	defer func() { _ = s1.Stop() }()

	s2 := NewTCPServer("127.0.0.1", port, nil, nil, nil,
		config.InflightConfig{}, 10, time.Second, time.Second, nil, 0)
	if err := s2.Start(); err == nil {
		_ = s2.Stop()
		t.Fatal("expected error when port already in use")
	}
}

func TestTCPServer_ActiveConns_AfterConnect(t *testing.T) {
	port := findFreePort(t)
	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})

	s := testAuthServer(t, port, 10, func(conn *Connection) {
		close(handlerStarted)
		<-handlerDone
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop() }()

	conn := dialAndSendConnect(t, port)
	defer conn.Close()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}

	if s.ActiveConns() != 1 {
		t.Fatalf("ActiveConns: expected 1, got %d", s.ActiveConns())
	}

	close(handlerDone)
}

func TestTCPServer_ActiveConnsDecrementedOnDisconnect(t *testing.T) {
	port := findFreePort(t)
	handlerDone := make(chan struct{})

	s := testAuthServer(t, port, 10, func(conn *Connection) {
		<-handlerDone
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop() }()

	conn := dialAndSendConnect(t, port)

	time.Sleep(100 * time.Millisecond)

	if s.ActiveConns() != 1 {
		t.Fatalf("ActiveConns: expected 1, got %d", s.ActiveConns())
	}

	conn.Close()
	close(handlerDone)

	time.Sleep(50 * time.Millisecond)

	if s.ActiveConns() != 0 {
		t.Fatalf("ActiveConns: expected 0 after disconnect, got %d", s.ActiveConns())
	}
}

func TestTCPServer_MaxConnections(t *testing.T) {
	port := findFreePort(t)

	s := testAuthServer(t, port, 1, func(conn *Connection) {
		select {}
	})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop() }()

	conn1 := dialAndSendConnect(t, port)
	defer conn1.Close()

	time.Sleep(100 * time.Millisecond)

	if s.ActiveConns() != 1 {
		t.Fatalf("expected 1 active conn, got %d", s.ActiveConns())
	}

	conn2, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	conn2.Close()

	time.Sleep(100 * time.Millisecond)

	if s.ActiveConns() != 1 {
		t.Fatalf("expected still 1 active conn (second rejected), got %d", s.ActiveConns())
	}
}

func TestTCPServer_HandleConnAuthFailure(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	s := testAuthServer(t, findFreePort(t), 10, func(conn *Connection) {
		t.Fatal("handler should not be called")
	})

	done := make(chan struct{})
	go func() {
		s.handleConn(server)
		close(done)
	}()

	enc := protocol.NewEncoder(client)
	_ = enc.WriteConnect(protocol.ConnectFrame{
		Username: "admin", Password: "wrong-password", Namespace: "main",
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConn did not return after auth failure")
	}
}

func TestTCPServer_HandleConnSuccess(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	handlerCalled := make(chan struct{})

	s := testAuthServer(t, findFreePort(t), 10, func(conn *Connection) {
		close(handlerCalled)
	})

	done := make(chan struct{})
	go func() {
		s.handleConn(server)
		close(done)
	}()

	enc := protocol.NewEncoder(client)
	_ = enc.WriteConnect(protocol.ConnectFrame{
		Username: "admin", Password: "testpass123!", Namespace: "main",
	})

	select {
	case <-handlerCalled:
	case <-time.After(time.Second):
		t.Fatal("handler not called after successful auth")
	}

	client.Close()
	<-done
}

func TestTCPServer_HandleConnHandlerPanic(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	s := testAuthServer(t, findFreePort(t), 10, func(conn *Connection) {
		panic("test panic")
	})

	done := make(chan struct{})
	go func() {
		s.handleConn(server)
		close(done)
	}()

	enc := protocol.NewEncoder(client)
	_ = enc.WriteConnect(protocol.ConnectFrame{
		Username: "admin", Password: "testpass123!", Namespace: "main",
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConn did not return after handler panic")
	}
}

func TestTCPServer_ConnRateLimit(t *testing.T) {
	port := findFreePort(t)

	s := NewTCPServer("127.0.0.1", port, nil, nil, nil,
		config.InflightConfig{}, 10, time.Second, time.Second, nil, 1)

	s.MaxConnections = 100

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop() }()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()
}

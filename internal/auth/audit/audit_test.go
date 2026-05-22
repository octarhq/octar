package audit

import (
	"context"
	"testing"
	"time"
)

func captureHandler() (func() *Event, func(context.Context, *Event)) {
	ch := make(chan *Event, 1)
	return func() *Event {
		select {
		case e := <-ch:
			return e
		case <-time.After(time.Second):
			return nil
		}
	}, func(_ context.Context, e *Event) {
		ch <- e
	}
}

func TestNewLogger(t *testing.T) {
	logger := NewLogger(nil, 100)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestLogAuth_Success(t *testing.T) {
	getEvent, handler := captureHandler()
	logger := NewLogger(handler, 100)

	logger.LogAuth(context.Background(), "user1", "USER", "password", "127.0.0.1", "main", true, "")

	captured := getEvent()
	if captured == nil {
		t.Fatal("expected captured event")
	}
	if captured.Type != EventAuthSuccess {
		t.Errorf("expected AUTH_SUCCESS, got %s", captured.Type)
	}
	if captured.SubjectID != "user1" {
		t.Errorf("expected user1, got %s", captured.SubjectID)
	}
	if !captured.Success {
		t.Error("expected success=true")
	}
}

func TestLogAuth_Failure(t *testing.T) {
	getEvent, handler := captureHandler()
	logger := NewLogger(handler, 100)

	logger.LogAuth(context.Background(), "user1", "USER", "password", "127.0.0.1", "main", false, "bad password")

	captured := getEvent()
	if captured == nil {
		t.Fatal("expected captured event")
	}
	if captured.Type != EventAuthFailure {
		t.Errorf("expected AUTH_FAILURE, got %s", captured.Type)
	}
	if captured.Success {
		t.Error("expected success=false")
	}
	if captured.Reason != "bad password" {
		t.Errorf("expected reason='bad password', got %s", captured.Reason)
	}
}

func TestLogTokenIssued(t *testing.T) {
	getEvent, handler := captureHandler()
	logger := NewLogger(handler, 100)

	logger.LogTokenIssued(context.Background(), "user1", "USER", "password", "127.0.0.1")

	captured := getEvent()
	if captured == nil {
		t.Fatal("expected captured event")
	}
	if captured.Type != EventTokenIssued {
		t.Errorf("expected TOKEN_ISSUED, got %s", captured.Type)
	}
}

func TestLogTokenRevoked(t *testing.T) {
	getEvent, handler := captureHandler()
	logger := NewLogger(handler, 100)

	logger.LogTokenRevoked(context.Background(), "user1", "admin action")

	captured := getEvent()
	if captured == nil {
		t.Fatal("expected captured event")
	}
	if captured.Type != EventTokenRevoked {
		t.Errorf("expected TOKEN_REVOKED, got %s", captured.Type)
	}
	if captured.Reason != "admin action" {
		t.Errorf("expected reason 'admin action', got %s", captured.Reason)
	}
}

func TestLogAPIKeyCreated(t *testing.T) {
	getEvent, handler := captureHandler()
	logger := NewLogger(handler, 100)

	logger.LogAPIKeyCreated(context.Background(), "user1", "key-123", "main")

	captured := getEvent()
	if captured == nil {
		t.Fatal("expected captured event")
	}
	if captured.Type != EventAPIKeyCreated {
		t.Errorf("expected API_KEY_CREATED, got %s", captured.Type)
	}
	if captured.Namespace != "main" {
		t.Errorf("expected namespace main, got %s", captured.Namespace)
	}
}

func TestLogPermissionDenied(t *testing.T) {
	getEvent, handler := captureHandler()
	logger := NewLogger(handler, 100)

	logger.LogPermissionDenied(context.Background(), "user1", "main", "consume", "queue/test")

	captured := getEvent()
	if captured == nil {
		t.Fatal("expected captured event")
	}
	if captured.Type != EventPermissionDenied {
		t.Errorf("expected PERMISSION_DENIED, got %s", captured.Type)
	}
	if captured.Success {
		t.Error("expected success=false")
	}
}

func TestGetEvents(t *testing.T) {
	logger := NewLogger(nil, 100)

	logger.LogAuth(context.Background(), "user1", "USER", "password", "127.0.0.1", "main", true, "")
	logger.LogAuth(context.Background(), "user2", "USER", "password", "127.0.0.1", "main", false, "bad pass")

	events := logger.GetEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestGetEventsByType(t *testing.T) {
	logger := NewLogger(nil, 100)

	logger.LogAuth(context.Background(), "user1", "USER", "password", "127.0.0.1", "main", true, "")
	logger.LogTokenIssued(context.Background(), "user1", "USER", "password", "127.0.0.1")

	authEvents := logger.GetEventsByType(EventAuthSuccess)
	if len(authEvents) != 1 {
		t.Fatalf("expected 1 AUTH_SUCCESS, got %d", len(authEvents))
	}

	tokenEvents := logger.GetEventsByType(EventTokenIssued)
	if len(tokenEvents) != 1 {
		t.Fatalf("expected 1 TOKEN_ISSUED, got %d", len(tokenEvents))
	}
}

func TestGetEventsBySubject(t *testing.T) {
	logger := NewLogger(nil, 100)

	logger.LogAuth(context.Background(), "alice", "USER", "password", "127.0.0.1", "main", true, "")
	logger.LogAuth(context.Background(), "bob", "USER", "password", "127.0.0.1", "main", true, "")
	logger.LogAuth(context.Background(), "alice", "USER", "password", "127.0.0.1", "other", true, "")

	aliceEvents := logger.GetEventsBySubject("alice")
	if len(aliceEvents) != 2 {
		t.Fatalf("expected 2 events for alice, got %d", len(aliceEvents))
	}
}

func TestGetEventsByNamespace(t *testing.T) {
	logger := NewLogger(nil, 100)

	logger.LogAuth(context.Background(), "alice", "USER", "password", "127.0.0.1", "main", true, "")
	logger.LogAuth(context.Background(), "bob", "USER", "password", "127.0.0.1", "other", true, "")

	mainEvents := logger.GetEventsByNamespace("main")
	if len(mainEvents) != 1 {
		t.Fatalf("expected 1 event for main namespace, got %d", len(mainEvents))
	}
}

func TestEventRingBufferMaxLen(t *testing.T) {
	logger := NewLogger(nil, 3)

	for range 10 {
		logger.LogAuth(context.Background(), "user", "USER", "password", "127.0.0.1", "main", true, "")
	}

	events := logger.GetEvents()
	if len(events) > 3 {
		t.Fatalf("expected at most 3 events (maxLen=3), got %d", len(events))
	}
}

func TestLogger_ConcurrencySafe(t *testing.T) {
	logger := NewLogger(nil, 1000)

	done := make(chan struct{})
	go func() {
		for range 100 {
			logger.LogAuth(context.Background(), "user", "USER", "pwd", "127.0.0.1", "ns", true, "")
		}
		done <- struct{}{}
	}()

	go func() {
		for range 100 {
			logger.LogTokenIssued(context.Background(), "user", "USER", "pwd", "10.0.0.1")
		}
		done <- struct{}{}
	}()

	go func() {
		for range 100 {
			logger.GetEvents()
			logger.GetEventsByType(EventAuthSuccess)
			logger.GetEventsBySubject("user")
		}
		done <- struct{}{}
	}()

	for range 3 {
		<-done
	}

	events := logger.GetEvents()
	if len(events) == 0 {
		t.Fatal("expected some events after concurrent access")
	}
}

func TestMarshalJSON(t *testing.T) {
	event := &Event{
		ID:          "evt-1",
		Type:        EventAuthSuccess,
		Timestamp:   time.Now(),
		SubjectID:   "user1",
		SubjectType: "USER",
		AuthMethod:  "password",
		RemoteAddr:  "127.0.0.1",
		Success:     true,
	}

	data, err := event.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON")
	}
}

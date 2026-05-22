package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type EventType string

const (
	EventAuthSuccess      EventType = "AUTH_SUCCESS"
	EventAuthFailure      EventType = "AUTH_FAILURE"
	EventTokenIssued      EventType = "TOKEN_ISSUED"
	EventTokenRevoked     EventType = "TOKEN_REVOKED"
	EventTokenRefreshed   EventType = "TOKEN_REFRESHED"
	EventAPIKeyCreated    EventType = "API_KEY_CREATED"
	EventAPIKeyRevoked    EventType = "API_KEY_REVOKED"
	EventPermissionDenied EventType = "PERMISSION_DENIED"
	EventNamespaceAccess  EventType = "NAMESPACE_ACCESS"
	EventLoginAttempt     EventType = "LOGIN_ATTEMPT"
)

type Event struct {
	ID          string            `json:"id"`
	Type        EventType         `json:"type"`
	Timestamp   time.Time         `json:"timestamp"`
	SubjectID   string            `json:"subject_id"`
	SubjectType string            `json:"subject_type"`
	AuthMethod  string            `json:"auth_method,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	RemoteAddr  string            `json:"remote_addr"`
	Success     bool              `json:"success"`
	Reason      string            `json:"reason,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type Logger struct {
	mu      sync.RWMutex
	events  []Event
	handler EventHandler
	logger  *slog.Logger
	maxLen  int
}

type EventHandler func(ctx context.Context, event *Event)

func NewLogger(handler EventHandler, maxLen int) *Logger {
	return &Logger{
		handler: handler,
		maxLen:  maxLen,
		logger:  slog.Default().With("component", "audit"),
	}
}

func (l *Logger) LogAuth(ctx context.Context, subjectID, subjectType, method, remoteAddr, namespace string, success bool, reason string) {
	event := &Event{
		Type:        EventAuthSuccess,
		Timestamp:   time.Now(),
		SubjectID:   subjectID,
		SubjectType: subjectType,
		AuthMethod:  method,
		RemoteAddr:  remoteAddr,
		Namespace:   namespace,
		Success:     success,
		Reason:      reason,
	}

	if success {
		event.Type = EventAuthSuccess
	} else {
		event.Type = EventAuthFailure
	}

	l.append(event)
}

func (l *Logger) LogTokenIssued(ctx context.Context, subjectID, subjectType, method, remoteAddr string) {
	l.append(&Event{
		Type:        EventTokenIssued,
		Timestamp:   time.Now(),
		SubjectID:   subjectID,
		SubjectType: subjectType,
		AuthMethod:  method,
		RemoteAddr:  remoteAddr,
		Success:     true,
	})
}

func (l *Logger) LogTokenRevoked(ctx context.Context, subjectID, reason string) {
	l.append(&Event{
		Type:      EventTokenRevoked,
		Timestamp: time.Now(),
		SubjectID: subjectID,
		Success:   true,
		Reason:    reason,
	})
}

func (l *Logger) LogTokenRefreshed(ctx context.Context, subjectID, remoteAddr string) {
	l.append(&Event{
		Type:       EventTokenRefreshed,
		Timestamp:  time.Now(),
		SubjectID:  subjectID,
		RemoteAddr: remoteAddr,
		Success:    true,
	})
}

func (l *Logger) LogAPIKeyCreated(ctx context.Context, subjectID, keyID, namespace string) {
	l.append(&Event{
		Type:      EventAPIKeyCreated,
		Timestamp: time.Now(),
		SubjectID: subjectID,
		Namespace: namespace,
		Success:   true,
		Metadata:  map[string]string{"key_id": keyID},
	})
}

func (l *Logger) LogAPIKeyRevoked(ctx context.Context, subjectID, keyID, reason string) {
	l.append(&Event{
		Type:      EventAPIKeyRevoked,
		Timestamp: time.Now(),
		SubjectID: subjectID,
		Success:   true,
		Reason:    reason,
		Metadata:  map[string]string{"key_id": keyID},
	})
}

func (l *Logger) LogPermissionDenied(ctx context.Context, subjectID, namespace, permission, resource string) {
	l.append(&Event{
		Type:      EventPermissionDenied,
		Timestamp: time.Now(),
		SubjectID: subjectID,
		Namespace: namespace,
		Success:   false,
		Reason:    fmt.Sprintf("permission denied: %s on %s", permission, resource),
	})
}

func (l *Logger) LogNamespaceAccess(ctx context.Context, subjectID, namespace, operation string) {
	l.append(&Event{
		Type:      EventNamespaceAccess,
		Timestamp: time.Now(),
		SubjectID: subjectID,
		Namespace: namespace,
		Success:   true,
		Metadata:  map[string]string{"operation": operation},
	})
}

func (l *Logger) append(event *Event) {
	event.ID = generateEventID()

	l.mu.Lock()
	l.events = append(l.events, *event)
	if l.maxLen > 0 && len(l.events) > l.maxLen {
		l.events = l.events[1:]
	}
	l.mu.Unlock()

	if l.handler != nil {
		go l.handler(context.Background(), event)
	}

	l.logger.Info("audit event",
		"type", event.Type,
		"subject", event.SubjectID,
		"namespace", event.Namespace,
		"success", event.Success,
	)
}

func (l *Logger) GetEvents() []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := make([]Event, len(l.events))
	copy(result, l.events)
	return result
}

func (l *Logger) GetEventsByType(eventType EventType) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []Event
	for _, e := range l.events {
		if e.Type == eventType {
			result = append(result, e)
		}
	}
	return result
}

func (l *Logger) GetEventsBySubject(subjectID string) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []Event
	for _, e := range l.events {
		if e.SubjectID == subjectID {
			result = append(result, e)
		}
	}
	return result
}

func (l *Logger) GetEventsByNamespace(namespace string) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []Event
	for _, e := range l.events {
		if e.Namespace == namespace {
			result = append(result, e)
		}
	}
	return result
}

func generateEventID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), randomID(8))
}

func randomID(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i%len(letters)]
	}
	return string(b)
}

func (e *Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID          string            `json:"id"`
		Type        EventType         `json:"type"`
		Timestamp   time.Time         `json:"timestamp"`
		SubjectID   string            `json:"subject_id"`
		SubjectType string            `json:"subject_type"`
		AuthMethod  string            `json:"auth_method,omitempty"`
		Namespace   string            `json:"namespace,omitempty"`
		RemoteAddr  string            `json:"remote_addr"`
		Success     bool              `json:"success"`
		Reason      string            `json:"reason,omitempty"`
		Metadata    map[string]string `json:"metadata,omitempty"`
	}{
		ID:          e.ID,
		Type:        e.Type,
		Timestamp:   e.Timestamp,
		SubjectID:   e.SubjectID,
		SubjectType: e.SubjectType,
		AuthMethod:  e.AuthMethod,
		Namespace:   e.Namespace,
		RemoteAddr:  e.RemoteAddr,
		Success:     e.Success,
		Reason:      e.Reason,
		Metadata:    e.Metadata,
	})
}

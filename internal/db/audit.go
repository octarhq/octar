package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// AuditEvent records a security-relevant action in the system.
type AuditEvent struct {
	ID          string
	Type        string
	Timestamp   time.Time
	SubjectID   string
	SubjectType string
	AuthMethod  string
	Namespace   string
	RemoteAddr  string
	Success     bool
	Reason      string
	Metadata    map[string]string
}

// AuditFilter narrows QueryAuditEvents results.
type AuditFilter struct {
	EventType string
	SubjectID string
	Namespace string
	From      time.Time
	To        time.Time
	Limit     int
}

func (s *Store) AppendAuditEvent(event *AuditEvent) error {
	metadataJSON, _ := json.Marshal(event.Metadata)
	_, err := s.db.Exec(
		`INSERT INTO audit_events
		 (event_id, event_type, timestamp, subject_id, subject_type, auth_method,
		  namespace, remote_addr, success, reason, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, event.Type, event.Timestamp.Format(time.RFC3339),
		event.SubjectID, event.SubjectType, event.AuthMethod,
		event.Namespace, event.RemoteAddr, event.Success, event.Reason,
		string(metadataJSON),
	)
	return err
}

func (s *Store) QueryAuditEvents(ctx context.Context, filter AuditFilter) ([]AuditEvent, error) {
	query := `SELECT event_id, event_type, timestamp, subject_id, subject_type,
	                 auth_method, namespace, remote_addr, success, reason, metadata
	          FROM audit_events WHERE 1=1`
	args := []any{}

	if filter.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, filter.EventType)
	}
	if filter.SubjectID != "" {
		query += " AND subject_id = ?"
		args = append(args, filter.SubjectID)
	}
	if filter.Namespace != "" {
		query += " AND namespace = ?"
		args = append(args, filter.Namespace)
	}
	if !filter.From.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, filter.From.Format(time.RFC3339))
	}
	if !filter.To.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, filter.To.Format(time.RFC3339))
	}

	query += " ORDER BY timestamp DESC"
	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []AuditEvent
	for rows.Next() {
		var e AuditEvent
		var tsStr string
		var metaStr string
		if err := rows.Scan(
			&e.ID, &e.Type, &tsStr, &e.SubjectID, &e.SubjectType,
			&e.AuthMethod, &e.Namespace, &e.RemoteAddr, &e.Success, &e.Reason, &metaStr,
		); err != nil {
			return nil, err
		}
		e.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		if metaStr != "" {
			json.Unmarshal([]byte(metaStr), &e.Metadata)
		}
		events = append(events, e)
	}
	return events, nil
}

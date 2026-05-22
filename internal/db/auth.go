package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// APIKey is a stored hashed API credential.
type APIKey struct {
	ID          int
	KeyHash     string
	SubjectID   string
	Namespace   string
	Permissions string
	ExpiresAt   *string
	RevokedAt   *string
}

// Session tracks an authenticated TCP connection.
type Session struct {
	ID          int
	SessionID   string
	SubjectID   string
	SubjectType string
	RemoteAddr  string
	CreatedAt   string
	ExpiresAt   *string
	LastSeen    *string
}

// ─── API Keys ────────────────────────────────────────────────────────────────

func (s *Store) CreateAPIKey(keyHash, subjectID, namespace, permissions, prefix string, expiresAt *time.Time) error {
	// Verify the namespace exists (if one is scoped).
	if namespace != "" && namespace != "*" {
		if _, err := s.GetNamespace(namespace); err != nil {
			return fmt.Errorf("%w: namespace %q does not exist", ErrNotFound, namespace)
		}
	}

	var expires *string
	if expiresAt != nil {
		exp := expiresAt.Format(time.RFC3339)
		expires = &exp
	}
	permsJSON, _ := json.Marshal(permissions)
	_, err := s.db.Exec(
		"INSERT INTO api_keys (key_hash, subject_id, namespace, permissions, prefix, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
		keyHash, subjectID, namespace, string(permsJSON), prefix, expires,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: API key already exists", ErrConflict)
		}
		return err
	}
	return nil
}

// GetAPIKey looks up a non-revoked API key by its hash and validates that it
// has not expired. Returns ErrNotFound for missing, revoked, or expired keys.
func (s *Store) GetAPIKey(keyHash string) (*APIKey, error) {
	var key APIKey
	err := s.db.QueryRow(
		`SELECT id, key_hash, subject_id, namespace, permissions, expires_at, revoked_at
		 FROM api_keys WHERE key_hash = ? AND revoked_at IS NULL`,
		keyHash,
	).Scan(&key.ID, &key.KeyHash, &key.SubjectID, &key.Namespace, &key.Permissions, &key.ExpiresAt, &key.RevokedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Enforce expiry in the application layer so we can return a typed error.
	if key.ExpiresAt != nil {
		exp, err := time.Parse(time.RFC3339, *key.ExpiresAt)
		if err == nil && time.Now().After(exp) {
			return nil, fmt.Errorf("%w: API key has expired", ErrNotFound)
		}
	}

	return &key, nil
}

func (s *Store) RevokeAPIKey(keyHash string) error {
	res, err := s.db.Exec(
		"UPDATE api_keys SET revoked_at = CURRENT_TIMESTAMP WHERE key_hash = ?", keyHash,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ─── Sessions ────────────────────────────────────────────────────────────────

func (s *Store) CreateSession(sessionID, subjectID, subjectType, remoteAddr string, expiresAt *time.Time) error {
	var expires *string
	if expiresAt != nil {
		exp := expiresAt.Format(time.RFC3339)
		expires = &exp
	}
	_, err := s.db.Exec(
		"INSERT INTO sessions (session_id, subject_id, subject_type, remote_addr, expires_at) VALUES (?, ?, ?, ?, ?)",
		sessionID, subjectID, subjectType, remoteAddr, expires,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: session %q already exists", ErrConflict, sessionID)
		}
		return err
	}
	return nil
}

func (s *Store) GetSession(sessionID string) (*Session, error) {
	var sess Session
	err := s.db.QueryRow(
		`SELECT id, session_id, subject_id, subject_type, remote_addr, created_at, expires_at, last_seen
		 FROM sessions WHERE session_id = ?`,
		sessionID,
	).Scan(&sess.ID, &sess.SessionID, &sess.SubjectID, &sess.SubjectType, &sess.RemoteAddr,
		&sess.CreatedAt, &sess.ExpiresAt, &sess.LastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Enforce session expiry.
	if sess.ExpiresAt != nil {
		exp, err := time.Parse(time.RFC3339, *sess.ExpiresAt)
		if err == nil && time.Now().After(exp) {
			return nil, fmt.Errorf("%w: session has expired", ErrNotFound)
		}
	}

	return &sess, nil
}

func (s *Store) DeleteSession(sessionID string) error {
	res, err := s.db.Exec("DELETE FROM sessions WHERE session_id = ?", sessionID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

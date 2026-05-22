package db

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
)

// queueNamePattern is the allowed format for queue names.
var queueNamePattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

// groupKeyPattern is the allowed format for group keys (supports wildcards like "group-*").
var groupKeyPattern = regexp.MustCompile(`^[a-z0-9_*-]+$`)

// Queue is the persistent declaration of a queue (name + namespace binding).
// Runtime message state lives in the WAL/scheduler; this record survives restarts.
type Queue struct {
	ID          int
	NamespaceID int
	Name        string
	Config      string // reserved for future queue-level config (JSON)
	CreatedAt   string
}

// Group is a named consumer group configuration within a queue.
// Config stores the full queue.GroupConfig as JSON so all user-defined
// settings (parallelism, rate_limit, retry, dlq) survive restarts.
type Group struct {
	ID        int
	QueueID   int
	Key       string
	Config    string // JSON-encoded queue.GroupConfig
	CreatedAt string
}

// ── Queue CRUD ────────────────────────────────────────────────────────────────

func (s *Store) ListQueues(namespaceID int) ([]Queue, error) {
	rows, err := s.db.Query(
		"SELECT id, namespace_id, name, config, created_at FROM queues WHERE namespace_id = ? ORDER BY name",
		namespaceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Queue
	for rows.Next() {
		var q Queue
		if err := rows.Scan(&q.ID, &q.NamespaceID, &q.Name, &q.Config, &q.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, q)
	}
	return result, nil
}

func (s *Store) GetQueueByName(namespaceID int, name string) (*Queue, error) {
	var q Queue
	err := s.db.QueryRow(
		"SELECT id, namespace_id, name, config, created_at FROM queues WHERE namespace_id = ? AND name = ?",
		namespaceID, name,
	).Scan(&q.ID, &q.NamespaceID, &q.Name, &q.Config, &q.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &q, err
}

func (s *Store) CreateQueue(namespaceID int, name, config string) (*Queue, error) {
	if !queueNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: queue name must match ^[a-z0-9_-]+$", ErrValidation)
	}
	if len(name) > 128 {
		return nil, fmt.Errorf("%w: queue name must be ≤ 128 characters", ErrValidation)
	}

	res, err := s.db.Exec(
		"INSERT INTO queues (namespace_id, name, config) VALUES (?, ?, ?)",
		namespaceID, name, config,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("%w: queue %q already exists in this namespace", ErrConflict, name)
		}
		if isFKViolation(err) {
			return nil, fmt.Errorf("%w: namespace with id %d does not exist", ErrNotFound, namespaceID)
		}
		return nil, err
	}
	id, _ := res.LastInsertId()

	var q Queue
	s.db.QueryRow(
		"SELECT id, namespace_id, name, config, created_at FROM queues WHERE id = ?", id,
	).Scan(&q.ID, &q.NamespaceID, &q.Name, &q.Config, &q.CreatedAt)
	return &q, nil
}

// DeleteQueue removes a queue by namespace + name. Returns ErrNotFound if the
// queue does not exist (groups are removed via CASCADE).
func (s *Store) DeleteQueue(namespaceID int, name string) error {
	res, err := s.db.Exec(
		"DELETE FROM queues WHERE namespace_id = ? AND name = ?",
		namespaceID, name,
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

// ── Group config CRUD ─────────────────────────────────────────────────────────

func (s *Store) ListGroups(queueID int) ([]Group, error) {
	rows, err := s.db.Query(
		"SELECT id, queue_id, key, config, created_at FROM groups WHERE queue_id = ? ORDER BY key",
		queueID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.QueueID, &g.Key, &g.Config, &g.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, g)
	}
	return result, nil
}

// UpsertGroupConfig creates or replaces the config for a group key within a queue.
func (s *Store) UpsertGroupConfig(queueID int, key, config string) error {
	if !groupKeyPattern.MatchString(key) {
		return fmt.Errorf("%w: group key must match ^[a-z0-9_*-]+$", ErrValidation)
	}
	_, err := s.db.Exec(
		`INSERT INTO groups (queue_id, key, config) VALUES (?, ?, ?)
		 ON CONFLICT(queue_id, key) DO UPDATE SET config = excluded.config`,
		queueID, key, config,
	)
	if err != nil {
		if isFKViolation(err) {
			return fmt.Errorf("%w: queue with id %d does not exist", ErrNotFound, queueID)
		}
		return err
	}
	return nil
}

// DeleteGroupConfig removes a group config. Returns ErrNotFound if it didn't exist.
func (s *Store) DeleteGroupConfig(queueID int, key string) error {
	res, err := s.db.Exec(
		"DELETE FROM groups WHERE queue_id = ? AND key = ?",
		queueID, key,
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

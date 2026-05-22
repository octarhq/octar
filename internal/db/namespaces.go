package db

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
)

// namePattern is the allowed format for namespace names.
var namePattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

// Namespace is a logical isolation boundary for queues and users.
type Namespace struct {
	ID        int
	Name      string
	Config    string
	CreatedAt string
	UpdatedAt string
}

func (s *Store) ListNamespaces() ([]Namespace, error) {
	rows, err := s.db.Query(
		"SELECT id, name, config, created_at, updated_at FROM namespaces ORDER BY name",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Namespace
	for rows.Next() {
		var ns Namespace
		if err := rows.Scan(&ns.ID, &ns.Name, &ns.Config, &ns.CreatedAt, &ns.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, ns)
	}
	return result, nil
}

func (s *Store) GetNamespace(name string) (*Namespace, error) {
	var ns Namespace
	err := s.db.QueryRow(
		"SELECT id, name, config, created_at, updated_at FROM namespaces WHERE name = ?",
		name,
	).Scan(&ns.ID, &ns.Name, &ns.Config, &ns.CreatedAt, &ns.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &ns, err
}

func (s *Store) CreateNamespace(name, config string) (*Namespace, error) {
	if !namePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: namespace name must match ^[a-z0-9_-]+$", ErrValidation)
	}
	if len(name) > 64 {
		return nil, fmt.Errorf("%w: namespace name must be ≤ 64 characters", ErrValidation)
	}

	_, err := s.db.Exec("INSERT INTO namespaces (name, config) VALUES (?, ?)", name, config)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("%w: namespace %q", ErrConflict, name)
		}
		return nil, err
	}
	return s.GetNamespace(name)
}

func (s *Store) UpdateNamespace(name, config string) (*Namespace, error) {
	res, err := s.db.Exec(
		"UPDATE namespaces SET config = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?",
		config, name,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrNotFound
	}
	return s.GetNamespace(name)
}

// DeleteNamespace permanently removes a namespace and, via CASCADE, all of its
// queues and groups. Returns ErrNotFound if the namespace does not exist.
func (s *Store) DeleteNamespace(name string) error {
	res, err := s.db.Exec("DELETE FROM namespaces WHERE name = ?", name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

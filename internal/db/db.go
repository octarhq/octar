// Package db provides the SQLite-backed metadata store for OCTAR.
//
// File layout:
//
//	db.go           — Store struct, lifecycle (New / Close / init / seed)
//	namespaces.go   — Namespace type + CRUD
//	users.go        — User type + CRUD + password helpers
//	queues.go       — Queue + Group types + CRUD
//	auth.go         — APIKey + Session types + CRUD
//	audit.go        — AuditEvent + AuditFilter + append / query
package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/83codes/octar/internal/config"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

// Store is the single SQLite connection shared by all domain files.
type Store struct {
	db           *sql.DB
	path         string
	defaultAdmin config.DefaultAdminConfig
}

// New opens (or creates) the SQLite database at dataDir/octar.db and runs
// schema migrations + seed data.
func New(dataDir string, defaultAdmin config.DefaultAdminConfig) (*Store, error) {
	path := filepath.Join(dataDir, "octar.db")

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		return nil, err
	}

	s := &Store{db: db, path: path, defaultAdmin: defaultAdmin}
	if err := s.init(); err != nil {
		return nil, err
	}

	return s, nil
}

// Close shuts down the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init() error {
	schemas := []string{
		// ── Core entities ──────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS namespaces (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			name       TEXT UNIQUE NOT NULL,
			config     TEXT,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS users (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			username      TEXT UNIQUE NOT NULL,
			password_hash TEXT NOT NULL,
			email         TEXT,
			role          TEXT NOT NULL DEFAULT 'user',
			created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
			updated_at    TEXT DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS user_namespace_permissions (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id      INTEGER NOT NULL,
			namespace_id INTEGER NOT NULL,
			permissions  TEXT NOT NULL,
			created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id)      REFERENCES users(id)      ON DELETE CASCADE,
			FOREIGN KEY (namespace_id) REFERENCES namespaces(id) ON DELETE CASCADE,
			UNIQUE(user_id, namespace_id)
		)`,
		// ── Queue & Groups ─────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS queues (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace_id INTEGER NOT NULL,
			name         TEXT NOT NULL,
			config       TEXT,
			created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (namespace_id) REFERENCES namespaces(id) ON DELETE CASCADE,
			UNIQUE(namespace_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS groups (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			queue_id   INTEGER NOT NULL,
			key        TEXT NOT NULL,
			config     TEXT,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
			UNIQUE(queue_id, key)
		)`,
		// ── Auth ───────────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS api_keys (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			key_hash    TEXT UNIQUE NOT NULL,
			subject_id  TEXT NOT NULL,
			namespace   TEXT,
			permissions TEXT,
			prefix      TEXT,
			created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
			expires_at  TEXT,
			revoked_at  TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id   TEXT UNIQUE NOT NULL,
			subject_id   TEXT NOT NULL,
			subject_type TEXT,
			remote_addr  TEXT,
			created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
			expires_at   TEXT,
			last_seen    TEXT
		)`,
		// ── RBAC ───────────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS rbac_policies (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			name        TEXT UNIQUE NOT NULL,
			policy_data TEXT NOT NULL,
			created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
			updated_at  TEXT DEFAULT CURRENT_TIMESTAMP
		)`,
		// ── Audit ──────────────────────────────────────────────────────────────
		`CREATE TABLE IF NOT EXISTS audit_events (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id     TEXT UNIQUE NOT NULL,
			event_type   TEXT NOT NULL,
			timestamp    TEXT NOT NULL,
			subject_id   TEXT,
			subject_type TEXT,
			auth_method  TEXT,
			namespace    TEXT,
			remote_addr  TEXT,
			success      INTEGER NOT NULL,
			reason       TEXT,
			metadata     TEXT,
			created_at   TEXT DEFAULT CURRENT_TIMESTAMP
		)`,
		// ── Indexes ────────────────────────────────────────────────────────────
		`CREATE INDEX IF NOT EXISTS idx_audit_timestamp  ON audit_events(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_subject    ON audit_events(subject_id)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_namespace  ON audit_events(namespace)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_type       ON audit_events(event_type)`,
		`CREATE INDEX IF NOT EXISTS idx_api_keys_hash    ON api_keys(key_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_sessions_id      ON sessions(session_id)`,
		`CREATE INDEX IF NOT EXISTS idx_users_username   ON users(username)`,
		`CREATE INDEX IF NOT EXISTS idx_namespaces_name  ON namespaces(name)`,
	}

	for _, schema := range schemas {
		if _, err := s.db.Exec(schema); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}

	return s.seedDefaultData()
}

func (s *Store) seedDefaultData() error {
	logger := slog.Default().With("component", "db")

	var count int

	s.db.QueryRow("SELECT COUNT(*) FROM namespaces").Scan(&count)
	if count == 0 {
		s.db.Exec("INSERT INTO namespaces (name, config) VALUES ('main', '{}')")
		logger.Info("created default namespace", "name", "main")
	}

	s.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if count == 0 {
		username := s.defaultAdmin.Username
		password := s.defaultAdmin.Password
		if username == "" {
			username = "admin"
		}
		if password == "" {
			password = "admin"
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("hash default admin password: %w", err)
		}
		_, err = s.db.Exec(
			"INSERT INTO users (username, password_hash, role) VALUES (?, ?, 'admin')",
			username, string(hash),
		)
		if err != nil {
			return fmt.Errorf("create default admin user: %w", err)
		}
		logger.Info("created default admin user", "username", username, "role", "admin")
	}

	return nil
}

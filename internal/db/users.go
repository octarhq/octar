package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// validRoles is the closed set of RBAC roles that can be assigned to a user.
var validRoles = map[string]bool{
	"admin":    true,
	"producer": true,
	"consumer": true,
	"observer": true,
	"billing":  true,
	"service":  true,
}

// User represents a OCTAR operator account.
type User struct {
	ID           int
	Username     string
	PasswordHash string
	Email        *string
	Role         string
	CreatedAt    string
	UpdatedAt    string
}

func (s *Store) GetUser(username string) (*User, error) {
	var u User
	err := s.db.QueryRow(
		"SELECT id, username, password_hash, email, role, created_at, updated_at FROM users WHERE username = ?",
		username,
	).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Email, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &u, err
}

// CheckPassword returns true when password matches the stored bcrypt hash.
func (s *Store) CheckPassword(username, password string) bool {
	user, err := s.GetUser(username)
	if err != nil {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) == nil
}

func (s *Store) ListUsers() ([]User, error) {
	rows, err := s.db.Query(
		"SELECT id, username, email, role, created_at FROM users",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.Email, &u.Role, &u.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, u)
	}
	return result, nil
}

func (s *Store) CreateUser(username, password, email, role string) error {
	if !validRoles[role] {
		return fmt.Errorf("%w: invalid role %q; must be one of admin, producer, consumer, observer, billing, service", ErrValidation, role)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		"INSERT INTO users (username, password_hash, email, role) VALUES (?, ?, ?, ?)",
		username, string(hash), email, role,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: username %q", ErrConflict, username)
		}
		return err
	}
	return nil
}

// DeleteUser permanently removes a user. Returns ErrNotFound if the user does
// not exist, and ErrForbidden if the user is the last admin.
func (s *Store) DeleteUser(username string) error {
	user, err := s.GetUser(username)
	if err != nil {
		return ErrNotFound
	}

	// Prevent removing the last admin — the system would become unmanageable.
	if user.Role == "admin" {
		var adminCount int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM users WHERE role = 'admin'").Scan(&adminCount); err != nil {
			return err
		}
		if adminCount <= 1 {
			return fmt.Errorf("%w: cannot delete the last admin user", ErrForbidden)
		}
	}

	res, err := s.db.Exec("DELETE FROM users WHERE username = ?", username)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateUser applies partial updates to a user inside a single transaction.
// Pass nil pointers to leave fields unchanged. Returns ErrNotFound if the user
// does not exist, ErrValidation if the role is invalid.
func (s *Store) UpdateUser(username string, email, role, password *string) error {
	if role != nil && !validRoles[*role] {
		return fmt.Errorf("%w: invalid role %q; must be one of admin, producer, consumer, observer, billing, service", ErrValidation, *role)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Verify the user exists before making any changes.
	var count int
	if err := tx.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", username).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}

	if email != nil {
		if _, err := tx.Exec(
			"UPDATE users SET email = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
			*email, username,
		); err != nil {
			return err
		}
	}
	if role != nil {
		if _, err := tx.Exec(
			"UPDATE users SET role = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
			*role, username,
		); err != nil {
			return err
		}
	}
	if password != nil {
		hash, err := bcrypt.GenerateFromPassword([]byte(*password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			"UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
			string(hash), username,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetUserNamespacePermissionsByUsername loads namespace permissions for a user looked up by username.
func (s *Store) GetUserNamespacePermissionsByUsername(username string) (map[string][]string, error) {
	user, err := s.GetUser(username)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(
		`SELECT n.name, unp.permissions
		 FROM user_namespace_permissions unp
		 JOIN namespaces n ON n.id = unp.namespace_id
		 WHERE unp.user_id = ?`,
		user.ID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]string)
	for rows.Next() {
		var nsName, permsJSON string
		if err := rows.Scan(&nsName, &permsJSON); err != nil {
			return nil, err
		}
		var perms []string
		json.Unmarshal([]byte(permsJSON), &perms) //nolint:errcheck
		result[nsName] = perms
	}
	return result, nil
}

// SetUserNamespacePermissionsByName sets namespace permissions for a user and namespace identified by name.
func (s *Store) SetUserNamespacePermissionsByName(username, namespaceName string, permissions []string) error {
	user, err := s.GetUser(username)
	if err != nil {
		return err
	}
	ns, err := s.GetNamespace(namespaceName)
	if err != nil {
		return err
	}
	return s.SetUserNamespacePermissions(user.ID, ns.ID, permissions)
}

func (s *Store) SetUserNamespacePermissions(userID, namespaceID int, permissions []string) error {
	permsJSON, _ := json.Marshal(permissions)
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO user_namespace_permissions (user_id, namespace_id, permissions)
		 VALUES (?, ?, ?)`,
		userID, namespaceID, string(permsJSON),
	)
	return err
}

func (s *Store) GetUserNamespacePermissions(userID int) (map[int][]string, error) {
	rows, err := s.db.Query(
		"SELECT namespace_id, permissions FROM user_namespace_permissions WHERE user_id = ?",
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int][]string)
	for rows.Next() {
		var nsID int
		var permsJSON string
		if err := rows.Scan(&nsID, &permsJSON); err != nil {
			return nil, err
		}
		var perms []string
		json.Unmarshal([]byte(permsJSON), &perms) //nolint:errcheck
		result[nsID] = perms
	}
	return result, nil
}

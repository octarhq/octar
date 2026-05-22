package db

import (
	"errors"
	"strings"
)

// Sentinel errors returned by the Store. Callers use errors.Is to distinguish
// them and map to the appropriate HTTP status or log message.
var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("not found")

	// ErrConflict is returned when a create/update violates a uniqueness constraint.
	ErrConflict = errors.New("already exists")

	// ErrValidation is returned when input fails a business-rule check before
	// hitting the database (e.g. invalid role, missing required field).
	ErrValidation = errors.New("validation error")

	// ErrForbidden is returned when an operation is structurally disallowed
	// (e.g. deleting the last admin user).
	ErrForbidden = errors.New("forbidden")
)

// isUniqueViolation reports whether err is a SQLite UNIQUE constraint failure.
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// isFKViolation reports whether err is a SQLite FOREIGN KEY constraint failure.
func isFKViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}

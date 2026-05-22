package v1

import (
	"errors"

	"github.com/danielgtaylor/huma/v2"

	"github.com/83codes/octar/internal/db"
)

// dbError maps a db-layer sentinel error to the appropriate huma HTTP error.
// resource is used in the 404 message (e.g. "namespace", "user", "queue").
//
// Mapping:
//
//	db.ErrNotFound   → 404 Not Found
//	db.ErrConflict   → 409 Conflict
//	db.ErrValidation → 422 Unprocessable Entity
//	db.ErrForbidden  → 403 Forbidden
//	anything else    → returned as-is (huma renders as 500)
func dbError(err error, resource string) error {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return huma.Error404NotFound(resource + " not found")
	case errors.Is(err, db.ErrConflict):
		return huma.Error409Conflict(err.Error())
	case errors.Is(err, db.ErrValidation):
		return huma.Error422UnprocessableEntity(err.Error())
	case errors.Is(err, db.ErrForbidden):
		return huma.Error403Forbidden(err.Error())
	default:
		return err
	}
}

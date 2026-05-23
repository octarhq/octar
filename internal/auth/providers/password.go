package providers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/octarhq/octar/internal/auth/authenticator"
	"github.com/octarhq/octar/internal/auth/identity"
	"github.com/octarhq/octar/internal/db"
	"golang.org/x/crypto/bcrypt"
)

type PasswordAuthenticator struct {
	db         *db.Store
	bcryptCost int
}

func NewPasswordAuthenticator(store *db.Store) *PasswordAuthenticator {
	return &PasswordAuthenticator{
		db:         store,
		bcryptCost: bcrypt.DefaultCost,
	}
}

func (a *PasswordAuthenticator) Name() string {
	return "password"
}

func (a *PasswordAuthenticator) Priority() int {
	return 10
}

func (a *PasswordAuthenticator) Authenticate(
	ctx context.Context,
	req authenticator.AuthRequest,
) (*identity.Identity, string, error) {
	if req.Username == "" || req.Password == "" {
		return nil, "", nil
	}

	if !a.db.CheckPassword(req.Username, req.Password) {
		return nil, "", nil
	}

	user, err := a.db.GetUser(req.Username)
	if err != nil {
		return nil, "", nil
	}

	role := user.Role
	subjectType := identity.SubjectUser
	if role == "admin" {
		subjectType = identity.SubjectAdmin
	}

	sessionID := generateSessionID()

	id := &identity.Identity{
		SubjectID:   user.Username,
		SubjectType: subjectType,
		AccountID:   user.Username,
		Namespace:   req.Namespace,
		Roles:       []string{role},
		Permissions: a.resolvePermissions(role),
		AuthMethod:  identity.AuthMethodPassword,
		IssuedAt:    time.Now(),
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Metadata: map[string]string{
			"email": func() string {
				if user.Email != nil {
					return *user.Email
				}
				return ""
			}(),
		},
	}

	return id, sessionID, nil
}

func (a *PasswordAuthenticator) resolvePermissions(role string) identity.PermissionSet {
	switch role {
	case "admin":
		return identity.NewPermissionSet(
			identity.PermPublish,
			identity.PermConsume,
			identity.PermAck,
			identity.PermNack,
			identity.PermManageQueues,
			identity.PermManageUsers,
			identity.PermMetricsRead,
			identity.PermAuditRead,
			identity.PermAdmin,
		)
	default:
		return identity.NewPermissionSet(
			identity.PermPublish,
			identity.PermConsume,
			identity.PermAck,
			identity.PermNack,
		)
	}
}

func generateSessionID() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

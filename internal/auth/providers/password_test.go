package providers

import (
	"context"
	"testing"

	"github.com/octarhq/octar/internal/auth/authenticator"
	"github.com/octarhq/octar/internal/auth/identity"
	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/db"
)

func setupTestDB(t *testing.T) *db.Store {
	t.Helper()
	store, err := db.New(t.TempDir(), config.DefaultAdminConfig{Username: "testuser", Password: "testpass123"})
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	return store
}

func TestPasswordAuthenticator_Authenticate_Success(t *testing.T) {
	store := setupTestDB(t)
	defer store.Close()

	auth := NewPasswordAuthenticator(store)

	req := authenticator.AuthRequest{
		Username:  "testuser",
		Password:  "testpass123",
		Namespace: "main",
	}

	id, sessionID, err := auth.Authenticate(context.Background(), req)
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}

	if id == nil {
		t.Fatal("expected identity, got nil")
	}

	if id.SubjectID != "testuser" {
		t.Errorf("expected subject %q, got %q", "testuser", id.SubjectID)
	}

	if id.AuthMethod != identity.AuthMethodPassword {
		t.Errorf("expected auth method %q, got %q", identity.AuthMethodPassword, id.AuthMethod)
	}

	if sessionID == "" {
		t.Fatal("expected session ID, got empty")
	}

	if id.SubjectType != identity.SubjectAdmin {
		t.Errorf("expected subject type admin, got %v", id.SubjectType)
	}
}

func TestPasswordAuthenticator_Authenticate_InvalidPassword(t *testing.T) {
	store := setupTestDB(t)
	defer store.Close()

	auth := NewPasswordAuthenticator(store)

	req := authenticator.AuthRequest{
		Username: "testuser",
		Password: "wrongpassword",
	}

	id, _, err := auth.Authenticate(context.Background(), req)
	if err != nil {
		t.Fatalf("Authenticate should not return error: %v", err)
	}

	if id != nil {
		t.Fatal("expected nil identity for invalid password")
	}
}

func TestPasswordAuthenticator_Authenticate_InvalidUsername(t *testing.T) {
	store := setupTestDB(t)
	defer store.Close()

	auth := NewPasswordAuthenticator(store)

	req := authenticator.AuthRequest{
		Username: "nonexistent",
		Password: "anypassword",
	}

	id, _, err := auth.Authenticate(context.Background(), req)
	if err != nil {
		t.Fatalf("Authenticate should not return error: %v", err)
	}

	if id != nil {
		t.Fatal("expected nil identity for invalid username")
	}
}

func TestPasswordAuthenticator_Authenticate_MissingCredentials(t *testing.T) {
	store := setupTestDB(t)
	defer store.Close()

	auth := NewPasswordAuthenticator(store)

	tests := []struct {
		name string
		req  authenticator.AuthRequest
	}{
		{
			name: "missing username",
			req:  authenticator.AuthRequest{Password: "password"},
		},
		{
			name: "missing password",
			req:  authenticator.AuthRequest{Username: "user"},
		},
		{
			name: "both missing",
			req:  authenticator.AuthRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, _, err := auth.Authenticate(context.Background(), tt.req)
			if err != nil {
				t.Fatalf("Authenticate should not return error: %v", err)
			}
			if id != nil {
				t.Fatal("expected nil identity for missing credentials")
			}
		})
	}
}

func TestPasswordAuthenticator_Name(t *testing.T) {
	store := setupTestDB(t)
	defer store.Close()

	auth := NewPasswordAuthenticator(store)
	if auth.Name() != "password" {
		t.Errorf("expected name 'password', got %q", auth.Name())
	}
}

func TestPasswordAuthenticator_Priority(t *testing.T) {
	store := setupTestDB(t)
	defer store.Close()

	auth := NewPasswordAuthenticator(store)
	if auth.Priority() != 10 {
		t.Errorf("expected priority 10, got %d", auth.Priority())
	}
}

package auth

import (
	"context"
	"testing"

	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
)

func testService(t *testing.T) *Service {
	t.Helper()
	store, err := db.New(t.TempDir(), config.DefaultAdminConfig{
		Username: "admin",
		Password: "testpass123!",
	})
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	return NewService(config.AuthConfig{
		Enabled: true,
		DefaultAdmin: config.DefaultAdminConfig{
			Username: "admin",
			Password: "testpass123!",
		},
		Providers: config.ProvidersConfig{
			Password: config.PasswordProviderConfig{
				Enabled:  true,
				Priority: 10,
			},
			APIKey: config.APIKeyProviderConfig{
				Enabled:  true,
				Priority: 20,
				Prefix:   "fq_",
			},
		},
	}, store, "")
}

func TestService_NewService(t *testing.T) {
	store, err := db.New(t.TempDir(), config.DefaultAdminConfig{
		Username: "admin",
		Password: "testpass123!",
	})
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	defer store.Close()

	svc := NewService(config.AuthConfig{
		Enabled: true,
		DefaultAdmin: config.DefaultAdminConfig{
			Username: "admin",
			Password: "testpass123!",
		},
		Providers: config.ProvidersConfig{
			Password: config.PasswordProviderConfig{Enabled: true, Priority: 10},
			APIKey:   config.APIKeyProviderConfig{Enabled: true, Priority: 20, Prefix: "fq_"},
		},
	}, store, "")

	if svc == nil {
		t.Fatal("expected non-nil service")
	}
	if svc.registry == nil {
		t.Fatal("expected registry to be initialized")
	}
}

func TestService_AuthenticateTCP_Success(t *testing.T) {
	svc := testService(t)

	id, err := svc.AuthenticateTCP(context.Background(), "127.0.0.1:12345", "admin", "testpass123!", "main")
	if err != nil {
		t.Fatalf("AuthenticateTCP: %v", err)
	}
	if id == nil {
		t.Fatal("expected non-nil identity")
	}
	if id.SubjectID != "admin" {
		t.Fatalf("SubjectID: expected admin, got %s", id.SubjectID)
	}
}

func TestService_AuthenticateTCP_WrongPassword(t *testing.T) {
	svc := testService(t)

	id, err := svc.AuthenticateTCP(context.Background(), "127.0.0.1:12345", "admin", "wrong-password", "main")
	if err != nil {
		t.Fatalf("AuthenticateTCP: %v", err)
	}
	if id != nil {
		t.Fatal("expected nil identity for wrong password")
	}
}

func TestService_AuthenticateTCP_UnknownUser(t *testing.T) {
	svc := testService(t)

	id, err := svc.AuthenticateTCP(context.Background(), "127.0.0.1:12345", "nonexistent", "password", "main")
	if err != nil {
		t.Fatalf("AuthenticateTCP: %v", err)
	}
	if id != nil {
		t.Fatal("expected nil identity for unknown user")
	}
}

func TestService_AuthenticateTCPWithKey_Success(t *testing.T) {
	svc := testService(t)

	apiKey := svc.GenerateAPIKey("service-user", "main", []string{"publish", "consume"})

	id, err := svc.AuthenticateTCPWithKey(context.Background(), "127.0.0.1:12345", apiKey, "main")
	if err != nil {
		t.Fatalf("AuthenticateTCPWithKey: %v", err)
	}
	if id == nil {
		t.Fatal("expected non-nil identity")
	}
	if id.SubjectID != "service-user" {
		t.Fatalf("SubjectID: expected service-user, got %s", id.SubjectID)
	}
}

func TestService_AuthenticateTCPWithKey_WrongKey(t *testing.T) {
	svc := testService(t)

	id, err := svc.AuthenticateTCPWithKey(context.Background(), "127.0.0.1:12345", "fq_invalid_key_hash", "main")
	if err != nil {
		t.Fatalf("AuthenticateTCPWithKey: %v", err)
	}
	if id != nil {
		t.Fatal("expected nil identity for invalid key")
	}
}

func TestService_AuthenticateTCPWithKey_EmptyKey(t *testing.T) {
	svc := testService(t)

	id, err := svc.AuthenticateTCPWithKey(context.Background(), "127.0.0.1:12345", "", "main")
	if err != nil {
		t.Fatalf("AuthenticateTCPWithKey: %v", err)
	}
	if id != nil {
		t.Fatal("expected nil identity for empty key")
	}
}

func TestService_GenerateAPIKey(t *testing.T) {
	svc := testService(t)

	key := svc.GenerateAPIKey("test-user", "main", []string{"publish"})
	if key == "" {
		t.Fatal("expected non-empty API key")
	}
}

func TestService_RevokeAPIKey(t *testing.T) {
	svc := testService(t)

	key := svc.GenerateAPIKey("test-user", "main", []string{"publish"})
	svc.RevokeAPIKey(key)

	id, err := svc.AuthenticateTCPWithKey(context.Background(), "127.0.0.1", key, "main")
	if err != nil {
		t.Fatalf("AuthenticateTCPWithKey after revoke: %v", err)
	}
	if id != nil {
		t.Fatal("expected nil identity after revoke")
	}
}

func TestService_ListAPIKeys(t *testing.T) {
	svc := testService(t)

	keys := svc.ListAPIKeys()
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys initially, got %d", len(keys))
	}

	svc.GenerateAPIKey("user-a", "main", []string{"publish"})

	keys = svc.ListAPIKeys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0].SubjectID != "user-a" {
		t.Fatalf("SubjectID: expected user-a, got %s", keys[0].SubjectID)
	}
}

func TestService_GenerateTokens_WithoutJWT(t *testing.T) {
	svc := testService(t)

	user := &db.User{Username: "admin", Role: "admin"}
	tokens, err := svc.GenerateTokens(user)
	if err != nil {
		t.Fatalf("GenerateTokens: %v", err)
	}
	if tokens != nil {
		t.Fatal("expected nil tokens when JWT is not configured")
	}
}

func TestService_RefreshTokens_WithoutJWT(t *testing.T) {
	svc := testService(t)

	tokens, err := svc.RefreshTokens("some-refresh-token")
	if err != nil {
		t.Fatalf("RefreshTokens: %v", err)
	}
	if tokens != nil {
		t.Fatal("expected nil tokens when JWT is not configured")
	}
}

func TestService_VerifyToken_WithoutJWT(t *testing.T) {
	svc := testService(t)

	id, err := svc.VerifyToken("some-token")
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if id != nil {
		t.Fatal("expected nil identity when JWT is not configured")
	}
}

func TestService_GenerateAPIKey_PrefixPresent(t *testing.T) {
	svc := testService(t)

	key := svc.GenerateAPIKey("user", "main", []string{"publish"})
	if len(key) < 3 || key[:3] != "fq_" {
		t.Fatalf("expected key to start with 'fq_', got %q", key)
	}
}

func TestService_AuthenticateTCP_ChecksMultipleProviders(t *testing.T) {
	svc := testService(t)

	id, err := svc.AuthenticateTCP(context.Background(), "127.0.0.1", "admin", "testpass123!", "main")
	if err != nil {
		t.Fatalf("AuthenticateTCP: %v", err)
	}
	if id == nil {
		t.Fatal("expected non-nil identity from password provider")
	}

	apiKey := svc.GenerateAPIKey("svc-user", "main", []string{"publish"})
	id, err = svc.AuthenticateTCPWithKey(context.Background(), "127.0.0.1", apiKey, "main")
	if err != nil {
		t.Fatalf("AuthenticateTCPWithKey: %v", err)
	}
	if id == nil {
		t.Fatal("expected non-nil identity from API key provider")
	}
}

func TestService_LoadDefaultPolicy(t *testing.T) {
	svc := testService(t)

	if svc.policy == nil {
		t.Fatal("expected policy to be initialized")
	}
}

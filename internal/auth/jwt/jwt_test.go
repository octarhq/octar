package jwt

import (
	"testing"
	"time"

	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
)

func testJWTConfig() config.JWTProviderConfig {
	return config.JWTProviderConfig{
		Enabled:        true,
		HMACSecret:     "test-secret-key-for-unit-tests-only",
		AccessTokenTTL: 900,
		KeyType:        "HMAC",
	}
}

func testUser(role string) *db.User {
	return &db.User{
		Username: "testuser",
		Role:     role,
	}
}

func TestNewManager(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
}

func TestGenerateTokens(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")

	tokens, err := mgr.GenerateTokens(testUser("admin"))
	if err != nil {
		t.Fatalf("GenerateTokens: %v", err)
	}
	if tokens == nil {
		t.Fatal("expected non-nil tokens")
	}
	if tokens.AccessToken == "" {
		t.Fatal("expected non-empty access token")
	}
	if tokens.RefreshToken == "" {
		t.Fatal("expected non-empty refresh token")
	}
	if tokens.TokenType != "Bearer" {
		t.Errorf("expected Bearer, got %s", tokens.TokenType)
	}
	if tokens.ExpiresIn <= 0 {
		t.Errorf("expected positive ExpiresIn, got %d", tokens.ExpiresIn)
	}
}

func TestGenerateTokens_RegularUser(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")

	tokens, err := mgr.GenerateTokens(testUser("user"))
	if err != nil {
		t.Fatalf("GenerateTokens: %v", err)
	}
	if tokens == nil {
		t.Fatal("expected non-nil tokens")
	}
	if tokens.AccessToken == "" {
		t.Fatal("expected non-empty access token")
	}
}

func TestVerifyAccessToken(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")

	tokens, err := mgr.GenerateTokens(testUser("admin"))
	if err != nil {
		t.Fatalf("GenerateTokens: %v", err)
	}

	claims, err := mgr.VerifyAccessToken(tokens.AccessToken)
	if err != nil {
		t.Fatalf("VerifyAccessToken: %v", err)
	}
	if claims == nil {
		t.Fatal("expected non-nil claims")
	}
	if claims.Subject != "testuser" {
		t.Errorf("expected subject testuser, got %s", claims.Subject)
	}
	if claims.SubjectTy != "admin" {
		t.Errorf("expected subject type admin, got %s", claims.SubjectTy)
	}
}

func TestVerifyAccessToken_Expired(t *testing.T) {
	cfg := testJWTConfig()
	cfg.AccessTokenTTL = 0 // 0 → uses default 900s in GenerateTokens but we want expired
	mgr := NewManager(cfg, "")

	// Manually create expired claims.
	claims := Claims{
		Subject:   "testuser",
		SubjectTy: "admin",
		IssuedAt:  time.Now().Add(-time.Hour).Unix(),
		ExpiresAt: time.Now().Add(-time.Minute).Unix(),
	}
	token, err := mgr.signHMAC(claims)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	_, err = mgr.VerifyAccessToken(token)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestVerifyAccessToken_InvalidToken(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")

	_, err := mgr.VerifyAccessToken("invalid.token.format")
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}

func TestVerifyAccessToken_WrongKey(t *testing.T) {
	mgr1 := NewManager(testJWTConfig(), "")
	tokens, err := mgr1.GenerateTokens(testUser("admin"))
	if err != nil {
		t.Fatalf("GenerateTokens: %v", err)
	}

	// Verify with a different manager (different secret).
	mgr2 := NewManager(config.JWTProviderConfig{
		Enabled:    true,
		HMACSecret: "different-secret-for-test",
		KeyType:    "HMAC",
	}, "")

	_, err = mgr2.VerifyAccessToken(tokens.AccessToken)
	if err == nil {
		t.Fatal("expected error for wrong signing key")
	}
}

func TestRefreshTokens(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")

	tokens, err := mgr.GenerateTokens(testUser("admin"))
	if err != nil {
		t.Fatalf("GenerateTokens: %v", err)
	}

	refreshed, err := mgr.RefreshTokens(tokens.RefreshToken)
	if err != nil {
		t.Fatalf("RefreshTokens: %v", err)
	}
	if refreshed == nil {
		t.Fatal("expected non-nil refreshed tokens")
	}
	if refreshed.AccessToken == "" {
		t.Fatal("expected non-empty access token after refresh")
	}
	if refreshed.RefreshToken == "" {
		t.Fatal("expected non-empty refresh token after refresh")
	}
	// New access token should be different from old one.
	if refreshed.AccessToken == tokens.AccessToken {
		t.Fatal("refreshed access token should differ from original")
	}
}

func TestRefreshTokens_Invalid(t *testing.T) {
	mgr := NewManager(testJWTConfig(), "")

	_, err := mgr.RefreshTokens("invalid-base64!")
	if err == nil {
		t.Fatal("expected error for invalid refresh token")
	}
}

func TestClaims_ToIdentity(t *testing.T) {
	claims := Claims{
		Subject:   "testuser",
		SubjectTy: "user",
		Roles:     []string{"admin"},
		Perms:     []string{"admin", "publish"},
		Namespaces: map[string][]string{
			"main": {"publish", "consume"},
		},
		Method:    "password",
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}

	id := claims.ToIdentity()
	if id == nil {
		t.Fatal("expected non-nil identity")
	}
	if id.SubjectID != "testuser" {
		t.Errorf("expected testuser, got %s", id.SubjectID)
	}
	if string(id.SubjectType) != "user" {
		t.Errorf("expected 'user', got %s", id.SubjectType)
	}
	if !id.IsAdmin() {
		t.Error("expected admin")
	}
	if !id.CanPublish() {
		t.Error("expected CanPublish")
	}
	if ns, ok := id.Namespaces["main"]; !ok || len(ns) != 2 {
		t.Errorf("expected namespaces main with 2 perms, got %v", ns)
	}
}

func TestPermissionsFromRole(t *testing.T) {
	tests := []struct {
		role     string
		minPerms int
	}{
		{"admin", 8},
		{"user", 5},
		{"unknown", 0},
	}

	for _, tc := range tests {
		t.Run(tc.role, func(t *testing.T) {
			perms := permissionsFromRole(tc.role)
			if len(perms) < tc.minPerms {
				t.Errorf("expected at least %d perms for %s, got %d", tc.minPerms, tc.role, len(perms))
			}
		})
	}
}

func TestSplitToken(t *testing.T) {
	parts := splitToken("header.payload.signature")
	if len(parts) != 3 || parts[0] != "header" || parts[1] != "payload" || parts[2] != "signature" {
		t.Errorf("unexpected split: %v", parts)
	}
}

func TestGenerateTokens_NoJWTConfig(t *testing.T) {
	mgr := NewManager(config.JWTProviderConfig{}, "")
	if mgr == nil {
		t.Fatal("expected non-nil manager even with empty config")
	}

	// No signing key = no tokens.
	_, err := mgr.GenerateTokens(testUser("admin"))
	if err == nil {
		t.Fatal("expected error when no signing key configured")
	}
}

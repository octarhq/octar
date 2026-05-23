package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/octarhq/octar/internal/api/v1"
	"github.com/octarhq/octar/internal/auth"
	"github.com/octarhq/octar/internal/broker"
	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/db"
)

func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func startTestApp(t *testing.T) (*apiv1.Server, string, func()) {
	t.Helper()

	dataDir := t.TempDir()
	apiPort := findFreePort(t)

	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataDir: dataDir,
			WAL: config.WALConfig{
				FlushInterval:    50 * time.Millisecond,
				FlushMaxMessages: 100,
				SegmentMaxBytes:  64 << 20,
				Durable:          false,
			},
		},
		Auth: config.AuthConfig{
			Enabled: true,
			Session: config.SessionConfig{
				Timeout:         24 * time.Hour,
				MaxSessions:     1000,
				CleanupInterval: 5 * time.Minute,
			},
			Providers: config.ProvidersConfig{
				Password: config.PasswordProviderConfig{Enabled: true, Priority: 10},
				APIKey:   config.APIKeyProviderConfig{Enabled: true, Priority: 20},
				JWT:      config.JWTProviderConfig{Enabled: true, KeyType: "RSA"},
			},
			DefaultAdmin: config.DefaultAdminConfig{
				Username: "admin",
				Password: "test123456",
			},
		},
		API:     config.APIConfig{Host: "127.0.0.1", Port: apiPort},
		Metrics: config.MetricsConfig{Enabled: false},
		PProf:   config.PProfConfig{Enabled: false},
	}

	store, err := db.New(dataDir, cfg.Auth.DefaultAdmin)
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}

	authSvc := auth.NewService(cfg.Auth, store, dataDir)

	b, err := broker.New(cfg, store, authSvc)
	if err != nil {
		store.Close()
		t.Fatalf("broker.New: %v", err)
	}

	if err := b.Start(); err != nil {
		store.Close()
		t.Fatalf("broker.Start: %v", err)
	}

	apiSrv := apiv1.NewServer(apiPort, store, authSvc, b.Scheduler, b)

	go func() {
		if err := apiSrv.Start(); err != nil {
			t.Logf("api server error: %v", err)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	return apiSrv, baseURL, func() {
		_ = apiSrv.Stop(t.Context())
		_ = b.Stop()
		store.Close()
	}
}

func TestE2E_LoginFlow(t *testing.T) {
	_, baseURL, cleanup := startTestApp(t)
	defer cleanup()

	// Test successful login
	loginReq := map[string]string{
		"username": "admin",
		"password": "test123456",
	}

	body, _ := json.Marshal(loginReq)
	resp, err := http.Post(baseURL+"/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var loginResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		TokenType    string `json:"token_type"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if loginResp.AccessToken == "" {
		t.Fatal("expected access token")
	}

	if loginResp.TokenType != "Bearer" {
		t.Errorf("expected token type 'Bearer', got %q", loginResp.TokenType)
	}

	// Test invalid credentials
	invalidReq := map[string]string{
		"username": "admin",
		"password": "wrongpassword",
	}

	body, _ = json.Marshal(invalidReq)
	resp, err = http.Post(baseURL+"/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", resp.StatusCode)
	}
}

func TestE2E_InvalidCredentialsViaHTTP(t *testing.T) {
	_, baseURL, cleanup := startTestApp(t)
	defer cleanup()

	testCases := []struct {
		name     string
		username string
		password string
		status   int
	}{
		{
			name:     "wrong password",
			username: "admin",
			password: "wrong",
			status:   http.StatusUnauthorized,
		},
		{
			name:     "wrong username",
			username: "nonexistent",
			password: "test123456",
			status:   http.StatusUnauthorized,
		},
		{
			name:     "empty username",
			username: "",
			password: "test123456",
			status:   http.StatusUnauthorized,
		},
		{
			name:     "empty password",
			username: "admin",
			password: "",
			status:   http.StatusUnauthorized,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loginReq := map[string]string{
				"username": tc.username,
				"password": tc.password,
			}

			body, _ := json.Marshal(loginReq)
			resp, err := http.Post(baseURL+"/auth/login", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("login request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.status {
				t.Errorf("expected status %d, got %d", tc.status, resp.StatusCode)
			}
		})
	}
}

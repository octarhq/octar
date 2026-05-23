package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/octarhq/octar/internal/config"
)

func TestStore_SeedDefaultData_CreatesAdminOnFirstStartup(t *testing.T) {
	// First execution - should create admin user
	tempDir := t.TempDir()
	cfg := config.DefaultAdminConfig{
		Username: "admin",
		Password: "test_password_123",
	}

	store, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Verify admin user was created
	user, err := store.GetUser("admin")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if user == nil {
		t.Fatal("expected admin user to exist")
	}
	if user.Username != "admin" {
		t.Errorf("expected username admin, got %s", user.Username)
	}
	if user.Role != "admin" {
		t.Errorf("expected role admin, got %s", user.Role)
	}

	// Verify password works
	if !store.CheckPassword("admin", "test_password_123") {
		t.Fatal("password check failed for admin user")
	}
}

func TestStore_SeedDefaultData_DoesNotRecreateAdminOnSecondStartup(t *testing.T) {
	// First execution - creates admin
	tempDir := t.TempDir()
	cfg1 := config.DefaultAdminConfig{
		Username: "admin",
		Password: "first_password",
	}

	store1, err := New(tempDir, cfg1)
	if err != nil {
		t.Fatalf("New first time: %v", err)
	}

	// Get first user ID
	user1, err := store1.GetUser("admin")
	if err != nil {
		t.Fatalf("GetUser first time: %v", err)
	}
	firstUserID := user1.ID
	store1.Close()

	// Second execution - should use existing database
	cfg2 := config.DefaultAdminConfig{
		Username: "admin",
		Password: "second_password",
	}

	store2, err := New(tempDir, cfg2)
	if err != nil {
		t.Fatalf("New second time: %v", err)
	}
	defer store2.Close()

	// Verify admin still exists with same ID
	user2, err := store2.GetUser("admin")
	if err != nil {
		t.Fatalf("GetUser second time: %v", err)
	}
	if user2.ID != firstUserID {
		t.Errorf("expected user ID %d, got %d (should not recreate)", firstUserID, user2.ID)
	}

	// Verify first password still works (not changed by second startup)
	if !store2.CheckPassword("admin", "first_password") {
		t.Fatal("first password should still work - admin should not be recreated")
	}

	// Verify second password does NOT work
	if store2.CheckPassword("admin", "second_password") {
		t.Fatal("second password should not work - database was not reset")
	}
}

func TestStore_SeedDefaultData_WithEmptyPassword(t *testing.T) {
	// When password is empty, should use default "admin"
	tempDir := t.TempDir()
	cfg := config.DefaultAdminConfig{
		Username: "admin",
		Password: "", // Empty password
	}

	store, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Verify default password "admin" works
	if !store.CheckPassword("admin", "admin") {
		t.Fatal("default password 'admin' should work when empty password provided")
	}
}

func TestStore_SeedDefaultData_WithCustomUsername(t *testing.T) {
	// Test with custom username
	tempDir := t.TempDir()
	cfg := config.DefaultAdminConfig{
		Username: "custom_admin",
		Password: "custom_password",
	}

	store, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Verify custom username was created
	user, err := store.GetUser("custom_admin")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if user == nil {
		t.Fatal("expected custom_admin user to exist")
	}
	if user.Username != "custom_admin" {
		t.Errorf("expected username custom_admin, got %s", user.Username)
	}

	// Verify password works
	if !store.CheckPassword("custom_admin", "custom_password") {
		t.Fatal("custom password check failed")
	}
}

func TestStore_SeedDefaultData_CreatesDefaultNamespace(t *testing.T) {
	// Verify default namespace is created
	tempDir := t.TempDir()
	cfg := config.DefaultAdminConfig{
		Username: "admin",
		Password: "test",
	}

	store, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Verify "main" namespace exists
	ns, err := store.GetNamespace("main")
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if ns == nil {
		t.Fatal("expected 'main' namespace to exist")
	}
	if ns.Name != "main" {
		t.Errorf("expected namespace name 'main', got %s", ns.Name)
	}
}

func TestStore_SeedDefaultData_IdempotentNamespaceCreation(t *testing.T) {
	// Verify namespace is not duplicated on second startup
	tempDir := t.TempDir()
	cfg := config.DefaultAdminConfig{
		Username: "admin",
		Password: "test",
	}

	store1, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New first time: %v", err)
	}
	store1.Close()

	store2, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New second time: %v", err)
	}
	defer store2.Close()

	// List namespaces - should only have "main" once
	namespaces, err := store2.ListNamespaces()
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}

	mainCount := 0
	for _, ns := range namespaces {
		if ns.Name == "main" {
			mainCount++
		}
	}

	if mainCount != 1 {
		t.Errorf("expected 'main' namespace to appear once, got %d", mainCount)
	}
}

func TestStore_AdminUserProperties(t *testing.T) {
	// Verify admin user has correct properties
	tempDir := t.TempDir()
	cfg := config.DefaultAdminConfig{
		Username: "admin",
		Password: "secure_password",
	}

	store, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	user, err := store.GetUser("admin")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}

	// Verify all properties
	if user.ID == 0 {
		t.Error("user ID should not be 0")
	}
	if user.Role != "admin" {
		t.Errorf("expected admin role, got %s", user.Role)
	}
	if user.PasswordHash == "" {
		t.Error("password hash should not be empty")
	}
	if user.CreatedAt == "" {
		t.Error("created_at should be set")
	}
	if user.UpdatedAt == "" {
		t.Error("updated_at should be set")
	}
}

func TestStore_DatabaseFilePersists(t *testing.T) {
	// Verify database file is created and persists
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "octar.db")

	cfg := config.DefaultAdminConfig{
		Username: "admin",
		Password: "test",
	}

	store1, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	store1.Close()

	// Verify file exists
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("database file should exist at %s, got error: %v", dbPath, err)
	}

	// Second open should use same file
	store2, err := New(tempDir, cfg)
	if err != nil {
		t.Fatalf("New second time: %v", err)
	}
	defer store2.Close()

	// Verify same data is there
	user, err := store2.GetUser("admin")
	if err != nil || user == nil {
		t.Fatal("admin user should persist across database reopens")
	}
}

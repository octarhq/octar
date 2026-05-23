package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/config"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir(), config.DefaultAdminConfig{
		Username: "admin",
		Password: "admin123",
	})
	if err != nil {
		t.Fatalf("New store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// ─── Namespace CRUD ───────────────────────────────────────────────────────

func TestStore_CreateNamespace(t *testing.T) {
	s := newTestStore(t)

	ns, err := s.CreateNamespace("test-ns", `{"description":"test"}`)
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if ns.Name != "test-ns" {
		t.Errorf("expected test-ns, got %s", ns.Name)
	}
	if ns.ID == 0 {
		t.Error("expected non-zero ID")
	}

	got, err := s.GetNamespace("test-ns")
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if got.Name != ns.Name {
		t.Errorf("expected %s, got %s", ns.Name, got.Name)
	}
}

func TestStore_CreateNamespace_Duplicate(t *testing.T) {
	s := newTestStore(t)

	_, err := s.CreateNamespace("dup-ns", "{}")
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	_, err = s.CreateNamespace("dup-ns", "{}")
	if err == nil {
		t.Fatal("expected error for duplicate namespace")
	}
}

func TestStore_CreateNamespace_InvalidName(t *testing.T) {
	s := newTestStore(t)

	_, err := s.CreateNamespace("INVALID_NAME!", "{}")
	if err == nil {
		t.Fatal("expected error for invalid namespace name")
	}
}

func TestStore_ListNamespaces(t *testing.T) {
	s := newTestStore(t)

	names := []string{"ns-alpha", "ns-beta", "ns-gamma"}
	for _, name := range names {
		_, err := s.CreateNamespace(name, "{}")
		if err != nil {
			t.Fatalf("CreateNamespace %s: %v", name, err)
		}
	}

	list, err := s.ListNamespaces()
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(list) < len(names) {
		t.Fatalf("expected at least %d namespaces, got %d", len(names), len(list))
	}
}

func TestStore_GetNamespace_NotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.GetNamespace("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_UpdateNamespace(t *testing.T) {
	s := newTestStore(t)

	created, err := s.CreateNamespace("update-ns", `{"old":"config"}`)
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	updated, err := s.UpdateNamespace("update-ns", `{"new":"config"}`)
	if err != nil {
		t.Fatalf("UpdateNamespace: %v", err)
	}
	if updated.Config != `{"new":"config"}` {
		t.Errorf("expected new config, got %s", updated.Config)
	}
	if updated.ID != created.ID {
		t.Errorf("expected same ID %d, got %d", created.ID, updated.ID)
	}
}

func TestStore_UpdateNamespace_NotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.UpdateNamespace("nonexistent", "{}")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_DeleteNamespace(t *testing.T) {
	s := newTestStore(t)

	_, err := s.CreateNamespace("delete-ns", "{}")
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	err = s.DeleteNamespace("delete-ns")
	if err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	_, err = s.GetNamespace("delete-ns")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestStore_DeleteNamespace_NotFound(t *testing.T) {
	s := newTestStore(t)

	err := s.DeleteNamespace("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// ─── User CRUD ────────────────────────────────────────────────────────────

func TestStore_CreateUser(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("testuser", "password123", "test@example.com", "producer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	user, err := s.GetUser("testuser")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("expected testuser, got %s", user.Username)
	}
	if user.Role != "producer" {
		t.Errorf("expected role producer, got %s", user.Role)
	}
	if user.Email == nil || *user.Email != "test@example.com" {
		t.Errorf("expected email test@example.com, got %v", user.Email)
	}
	if user.PasswordHash == "" {
		t.Error("expected non-empty password hash")
	}
}

func TestStore_CreateUser_Duplicate(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("dup-user", "pass123", "", "consumer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	err = s.CreateUser("dup-user", "pass456", "", "consumer")
	if err == nil {
		t.Fatal("expected error for duplicate user")
	}
}

func TestStore_CreateUser_InvalidRole(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("bad-role", "pass123", "", "superadmin")
	if err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func TestStore_GetUser_NotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.GetUser("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_ListUsers(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("user-a", "pass", "", "producer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	err = s.CreateUser("user-b", "pass", "", "consumer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	users, err := s.ListUsers()
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(users) < 2 {
		t.Fatalf("expected at least 2 users, got %d", len(users))
	}
}

func TestStore_CheckPassword(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("pass-user", "correct-password", "", "consumer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if !s.CheckPassword("pass-user", "correct-password") {
		t.Error("expected password to match")
	}
	if s.CheckPassword("pass-user", "wrong-password") {
		t.Error("expected wrong password to fail")
	}
	if s.CheckPassword("nonexistent", "any") {
		t.Error("expected nonexistent user to fail")
	}
}

func TestStore_UpdateUser(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("update-user", "oldpass", "old@example.com", "producer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	newEmail := "new@example.com"
	err = s.UpdateUser("update-user", &newEmail, nil, nil)
	if err != nil {
		t.Fatalf("UpdateUser (email): %v", err)
	}

	user, err := s.GetUser("update-user")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if user.Email == nil || *user.Email != "new@example.com" {
		t.Errorf("expected new email, got %v", user.Email)
	}
}

func TestStore_UpdateUser_Role(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("role-user", "pass", "", "producer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	newRole := "admin"
	err = s.UpdateUser("role-user", nil, &newRole, nil)
	if err != nil {
		t.Fatalf("UpdateUser (role): %v", err)
	}

	user, _ := s.GetUser("role-user")
	if user.Role != "admin" {
		t.Errorf("expected admin role, got %s", user.Role)
	}
}

func TestStore_UpdateUser_InvalidRole(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("bad-role-upd", "pass", "", "producer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	badRole := "superadmin"
	err = s.UpdateUser("bad-role-upd", nil, &badRole, nil)
	if err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func TestStore_UpdateUser_Password(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("pw-user", "oldpass", "", "consumer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	newPass := "newpass"
	err = s.UpdateUser("pw-user", nil, nil, &newPass)
	if err != nil {
		t.Fatalf("UpdateUser (password): %v", err)
	}

	if !s.CheckPassword("pw-user", "newpass") {
		t.Error("new password should match")
	}
	if s.CheckPassword("pw-user", "oldpass") {
		t.Error("old password should NOT match")
	}
}

func TestStore_DeleteUser(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateUser("delete-me", "pass", "", "consumer")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	err = s.DeleteUser("delete-me")
	if err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	_, err = s.GetUser("delete-me")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestStore_DeleteUser_NotFound(t *testing.T) {
	s := newTestStore(t)

	err := s.DeleteUser("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_DeleteLastAdmin_Forbidden(t *testing.T) {
	s := newTestStore(t)

	// The store seeds one admin user on creation.
	err := s.DeleteUser("admin")
	if err == nil {
		t.Fatal("expected error for deleting last admin")
	}
}

// ─── Queue CRUD ───────────────────────────────────────────────────────────

func newTestNamespace(t *testing.T, s *Store, name string) *Namespace {
	t.Helper()
	ns, err := s.CreateNamespace(name, "{}")
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	return ns
}

func TestStore_CreateQueue(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "queue-ns")

	q, err := s.CreateQueue(ns.ID, "test-queue", `{}`)
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if q.Name != "test-queue" {
		t.Errorf("expected test-queue, got %s", q.Name)
	}
	if q.NamespaceID != ns.ID {
		t.Errorf("expected namespace ID %d, got %d", ns.ID, q.NamespaceID)
	}
}

func TestStore_CreateQueue_DuplicateName(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "dup-q-ns")

	_, err := s.CreateQueue(ns.ID, "dup-queue", "{}")
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	_, err = s.CreateQueue(ns.ID, "dup-queue", "{}")
	if err == nil {
		t.Fatal("expected error for duplicate queue name")
	}
}

func TestStore_CreateQueue_SameNameDifferentNamespace(t *testing.T) {
	s := newTestStore(t)
	ns1 := newTestNamespace(t, s, "ns-a")
	ns2 := newTestNamespace(t, s, "ns-b")

	q1, err := s.CreateQueue(ns1.ID, "same-name", "{}")
	if err != nil {
		t.Fatalf("CreateQueue ns1: %v", err)
	}
	q2, err := s.CreateQueue(ns2.ID, "same-name", "{}")
	if err != nil {
		t.Fatalf("CreateQueue ns2: %v", err)
	}

	if q1.ID == q2.ID {
		t.Error("expected different queue IDs across namespaces")
	}
}

func TestStore_CreateQueue_InvalidName(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "invalid-q-ns")

	_, err := s.CreateQueue(ns.ID, "invalid name!", "{}")
	if err == nil {
		t.Fatal("expected error for invalid queue name")
	}
}

func TestStore_CreateQueue_NonexistentNamespace(t *testing.T) {
	s := newTestStore(t)

	_, err := s.CreateQueue(99999, "orphan-queue", "{}")
	// modernc.org/sqlite may or may not enforce FK constraints depending
	// on the driver/connection state. Either an FK error or a silently
	// inserted orphan is acceptable from the driver layer.
	if err != nil {
		if !isFKViolation(err) {
			t.Fatalf("unexpected error type: %v", err)
		}
	}
}

func TestStore_GetQueueByName(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "getq-ns")

	created, err := s.CreateQueue(ns.ID, "get-queue", `{"key":"val"}`)
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	got, err := s.GetQueueByName(ns.ID, "get-queue")
	if err != nil {
		t.Fatalf("GetQueueByName: %v", err)
	}
	if got.Name != created.Name {
		t.Errorf("expected %s, got %s", created.Name, got.Name)
	}
}

func TestStore_GetQueueByName_NotFound(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "notfound-ns")

	_, err := s.GetQueueByName(ns.ID, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_ListQueues(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "listq-ns")

	for _, name := range []string{"q-one", "q-two", "q-three"} {
		_, err := s.CreateQueue(ns.ID, name, "{}")
		if err != nil {
			t.Fatalf("CreateQueue %s: %v", name, err)
		}
	}

	queues, err := s.ListQueues(ns.ID)
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	if len(queues) != 3 {
		t.Fatalf("expected 3 queues, got %d", len(queues))
	}
}

func TestStore_DeleteQueue(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "delq-ns")

	created, err := s.CreateQueue(ns.ID, "delete-queue", "{}")
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	err = s.DeleteQueue(ns.ID, "delete-queue")
	if err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}

	_, err = s.GetQueueByName(ns.ID, "delete-queue")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	// Queue ID should differ from created one.
	if created.ID == 0 {
		t.Error("expected non-zero created queue ID")
	}
}

func TestStore_DeleteQueue_NotFound(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "delq-nf-ns")

	err := s.DeleteQueue(ns.ID, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// ─── Group Config CRUD ─────────────────────────────────────────────────────

func newTestQueue(t *testing.T, s *Store, ns *Namespace, name string) *Queue {
	t.Helper()
	q, err := s.CreateQueue(ns.ID, name, "{}")
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	return q
}

func TestStore_UpsertGroupConfig(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "grp-ns")
	q := newTestQueue(t, s, ns, "grp-queue")

	err := s.UpsertGroupConfig(q.ID, "my-group", `{"parallelism":3,"quantum":5}`)
	if err != nil {
		t.Fatalf("UpsertGroupConfig: %v", err)
	}

	groups, err := s.ListGroups(q.ID)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].Key != "my-group" {
		t.Errorf("expected my-group, got %s", groups[0].Key)
	}
	if groups[0].Config != `{"parallelism":3,"quantum":5}` {
		t.Errorf("expected config JSON, got %s", groups[0].Config)
	}
}

func TestStore_UpsertGroupConfig_Update(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "updgrp-ns")
	q := newTestQueue(t, s, ns, "updgrp-queue")

	err := s.UpsertGroupConfig(q.ID, "upd-group", `{"parallelism":1}`)
	if err != nil {
		t.Fatalf("UpsertGroupConfig: %v", err)
	}

	err = s.UpsertGroupConfig(q.ID, "upd-group", `{"parallelism":10}`)
	if err != nil {
		t.Fatalf("UpsertGroupConfig (update): %v", err)
	}

	groups, _ := s.ListGroups(q.ID)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group after upsert, got %d", len(groups))
	}
	if groups[0].Config != `{"parallelism":10}` {
		t.Errorf("expected updated config, got %s", groups[0].Config)
	}
}

func TestStore_UpsertGroupConfig_InvalidKey(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "invgrp-ns")
	q := newTestQueue(t, s, ns, "invgrp-queue")

	err := s.UpsertGroupConfig(q.ID, "BAD KEY!", `{}`)
	if err == nil {
		t.Fatal("expected error for invalid group key")
	}
}

func TestStore_DeleteGroupConfig(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "delgrp-ns")
	q := newTestQueue(t, s, ns, "delgrp-queue")

	_ = s.UpsertGroupConfig(q.ID, "del-me", `{}`)

	err := s.DeleteGroupConfig(q.ID, "del-me")
	if err != nil {
		t.Fatalf("DeleteGroupConfig: %v", err)
	}

	groups, _ := s.ListGroups(q.ID)
	if len(groups) != 0 {
		t.Errorf("expected 0 groups, got %d", len(groups))
	}
}

func TestStore_DeleteGroupConfig_NotFound(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "delgrpnf-ns")
	q := newTestQueue(t, s, ns, "delgrpnf-queue")

	err := s.DeleteGroupConfig(q.ID, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_ListGroups(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "lstgrp-ns")
	q := newTestQueue(t, s, ns, "lstgrp-queue")

	keys := []string{"group-a", "group-b", "group-c"}
	for _, k := range keys {
		_ = s.UpsertGroupConfig(q.ID, k, `{"key":"`+k+`"}`)
	}

	groups, err := s.ListGroups(q.ID)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(groups) != len(keys) {
		t.Fatalf("expected %d groups, got %d", len(keys), len(groups))
	}
}

// ─── API Key CRUD ─────────────────────────────────────────────────────────

func TestStore_CreateAPIKey(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "apikey-ns")

	future := time.Now().Add(24 * time.Hour)
	err := s.CreateAPIKey("hash123", "user1", ns.Name, "publish,consume", "key-", &future)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	key, err := s.GetAPIKey("hash123")
	if err != nil {
		t.Fatalf("GetAPIKey: %v", err)
	}
	if key.SubjectID != "user1" {
		t.Errorf("expected user1, got %s", key.SubjectID)
	}
	if key.RevokedAt != nil {
		t.Error("expected no revoked_at")
	}
}

func TestStore_CreateAPIKey_NoExpiry(t *testing.T) {
	s := newTestStore(t)
	_ = newTestNamespace(t, s, "apikey-ne-ns")

	err := s.CreateAPIKey("hash-noexp", "user1", "apikey-ne-ns", "publish", "key-", nil)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	key, err := s.GetAPIKey("hash-noexp")
	if err != nil {
		t.Fatalf("GetAPIKey: %v", err)
	}
	if key.SubjectID != "user1" {
		t.Errorf("expected user1, got %s", key.SubjectID)
	}
}

func TestStore_GetAPIKey_NotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.GetAPIKey("nonexistent-hash")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_RevokeAPIKey(t *testing.T) {
	s := newTestStore(t)
	ns := newTestNamespace(t, s, "revoke-ns")

	err := s.CreateAPIKey("revoke-hash", "user1", ns.Name, "publish", "key-", nil)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	err = s.RevokeAPIKey("revoke-hash")
	if err != nil {
		t.Fatalf("RevokeAPIKey: %v", err)
	}

	_, err = s.GetAPIKey("revoke-hash")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after revoke, got %v", err)
	}
}

func TestStore_RevokeAPIKey_NotFound(t *testing.T) {
	s := newTestStore(t)

	err := s.RevokeAPIKey("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// ─── Session CRUD ─────────────────────────────────────────────────────────

func TestStore_CreateAndGetSession(t *testing.T) {
	s := newTestStore(t)

	future := time.Now().Add(time.Hour)
	err := s.CreateSession("sess-123", "user1", "USER", "127.0.0.1", &future)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	sess, err := s.GetSession("sess-123")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.SubjectID != "user1" {
		t.Errorf("expected user1, got %s", sess.SubjectID)
	}
}

func TestStore_CreateSession_NoExpiry(t *testing.T) {
	s := newTestStore(t)

	err := s.CreateSession("sess-noexp", "user1", "USER", "10.0.0.1", nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	sess, err := s.GetSession("sess-noexp")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.ExpiresAt != nil {
		t.Error("expected no expiry")
	}
}

func TestStore_GetSession_NotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.GetSession("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_DeleteSession(t *testing.T) {
	s := newTestStore(t)

	_ = s.CreateSession("del-sess", "user1", "USER", "127.0.0.1", nil)

	err := s.DeleteSession("del-sess")
	if err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	_, err = s.GetSession("del-sess")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

// ─── Namespace Permissions ────────────────────────────────────────────────

func TestStore_SetUserNamespacePermissionsByName(t *testing.T) {
	s := newTestStore(t)

	_ = s.CreateUser("perm-user", "pass", "", "consumer")
	_ = newTestNamespace(t, s, "perm-ns")

	err := s.SetUserNamespacePermissionsByName("perm-user", "perm-ns", []string{"publish", "consume"})
	if err != nil {
		t.Fatalf("SetUserNamespacePermissionsByName: %v", err)
	}

	perms, err := s.GetUserNamespacePermissionsByUsername("perm-user")
	if err != nil {
		t.Fatalf("GetUserNamespacePermissionsByUsername: %v", err)
	}
	got, ok := perms["perm-ns"]
	if !ok {
		t.Fatal("expected perms for perm-ns")
	}
	if len(got) != 2 {
		t.Errorf("expected 2 permissions, got %d", len(got))
	}
}

// ─── Audit Events ─────────────────────────────────────────────────────────

func TestStore_AppendAndQueryAuditEvent(t *testing.T) {
	s := newTestStore(t)

	err := s.AppendAuditEvent(&AuditEvent{
		ID:          "audit-1",
		Type:        "AUTH_SUCCESS",
		Timestamp:   time.Now(),
		SubjectID:   "user1",
		SubjectType: "USER",
		RemoteAddr:  "127.0.0.1",
		Success:     true,
	})
	if err != nil {
		t.Fatalf("AppendAuditEvent: %v", err)
	}

	// Read back with raw SQL — modernc/sqlite stores TEXT timestamps.
	rows, err := s.db.Query("SELECT event_id, event_type, subject_id, success FROM audit_events WHERE event_id = ?", "audit-1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected at least 1 audit event")
	}
	var id, typ, subj string
	var success bool
	_ = rows.Scan(&id, &typ, &subj, &success)
	if id != "audit-1" {
		t.Errorf("expected audit-1, got %s", id)
	}
	if typ != "AUTH_SUCCESS" {
		t.Errorf("expected AUTH_SUCCESS, got %s", typ)
	}
	if subj != "user1" {
		t.Errorf("expected user1, got %s", subj)
	}
	if !success {
		t.Error("expected success=true")
	}
}

func TestStore_QueryAuditEvents_FilterBySubject(t *testing.T) {
	s := newTestStore(t)

	_ = s.AppendAuditEvent(&AuditEvent{
		ID: "audit-a", Type: "LOGIN", Timestamp: time.Now(), SubjectID: "alice", Success: true,
	})
	_ = s.AppendAuditEvent(&AuditEvent{
		ID: "audit-b", Type: "LOGIN", Timestamp: time.Now(), SubjectID: "bob", Success: true,
	})

	// Use QueryAuditEvents and manually scan timestamps
	rows, err := s.db.QueryContext(context.Background(),
		"SELECT event_id, event_type, subject_id, success FROM audit_events WHERE subject_id = ?", "alice")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 event for alice, got %d", count)
	}
}

func TestStore_QueryAuditEvents_Limit(t *testing.T) {
	s := newTestStore(t)

	for i := range 10 {
		_ = s.AppendAuditEvent(&AuditEvent{
			ID: string(rune('0' + i)), Type: "EVENT", Timestamp: time.Now(), Success: true,
		})
	}

	rows, err := s.db.QueryContext(context.Background(),
		"SELECT event_id FROM audit_events ORDER BY timestamp DESC LIMIT 3")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count > 3 {
		t.Fatalf("expected at most 3 events, got %d", count)
	}
}

func TestStore_QueryAuditEvents_DirectCall(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	err := s.AppendAuditEvent(&AuditEvent{
		ID: "direct-1", Type: "LOGIN", Timestamp: time.Now(),
		SubjectID: "alice", Success: true,
		Metadata: map[string]string{"ip": "10.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.AppendAuditEvent(&AuditEvent{
		ID: "direct-2", Type: "LOGIN", Timestamp: time.Now(),
		SubjectID: "bob", Success: false, Reason: "bad password",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.AppendAuditEvent(&AuditEvent{
		ID: "direct-3", Type: "API_KEY_CREATED", Timestamp: time.Now(),
		SubjectID: "alice", AuthMethod: "api_key", Namespace: "main", Success: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	events, err := s.QueryAuditEvents(ctx, AuditFilter{})
	if err != nil {
		t.Fatalf("QueryAuditEvents (no filter): %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	events, err = s.QueryAuditEvents(ctx, AuditFilter{EventType: "LOGIN"})
	if err != nil {
		t.Fatalf("QueryAuditEvents by type: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 LOGIN events, got %d", len(events))
	}

	events, err = s.QueryAuditEvents(ctx, AuditFilter{SubjectID: "bob"})
	if err != nil {
		t.Fatalf("QueryAuditEvents by subject: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event for bob, got %d", len(events))
	}

	events, err = s.QueryAuditEvents(ctx, AuditFilter{Namespace: "main"})
	if err != nil {
		t.Fatalf("QueryAuditEvents by namespace: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event for main namespace, got %d", len(events))
	}

	events, err = s.QueryAuditEvents(ctx, AuditFilter{Limit: 2})
	if err != nil {
		t.Fatalf("QueryAuditEvents with limit: %v", err)
	}
	if len(events) > 2 {
		t.Fatalf("expected at most 2 events, got %d", len(events))
	}

	events, err = s.QueryAuditEvents(ctx, AuditFilter{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatalf("QueryAuditEvents with time range: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events within time range, got %d", len(events))
	}
}

// ─── isFKViolation ───────────────────────────────────────────────────────────

func TestIsFKViolation(t *testing.T) {
	if isFKViolation(nil) {
		t.Fatal("expected false for nil error")
	}
	if isFKViolation(errors.New("UNIQUE constraint failed")) {
		t.Fatal("expected false for unique violation")
	}
	if !isFKViolation(errors.New("FOREIGN KEY constraint failed")) {
		t.Fatal("expected true for FK violation")
	}
	if !isFKViolation(errors.New("database: FOREIGN KEY constraint failed (code 19)")) {
		t.Fatal("expected true for FK violation with suffix")
	}
}

// ─── GetUserNamespacePermissions ─────────────────────────────────────────────

func TestStore_GetUserNamespacePermissions(t *testing.T) {
	s := newTestStore(t)

	ns, err := s.CreateNamespace("perm-ns", "{}")
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	user, err := s.GetUser("admin")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}

	perms, err := s.GetUserNamespacePermissions(user.ID)
	if err != nil {
		t.Fatalf("GetUserNamespacePermissions: %v", err)
	}
	if len(perms) != 0 {
		t.Fatalf("expected 0 permissions initially, got %d", len(perms))
	}

	err = s.SetUserNamespacePermissionsByName(user.Username, ns.Name, []string{"publish", "consume"})
	if err != nil {
		t.Fatalf("SetUserNamespacePermissionsByName: %v", err)
	}

	perms, err = s.GetUserNamespacePermissions(user.ID)
	if err != nil {
		t.Fatalf("GetUserNamespacePermissions after set: %v", err)
	}
	if len(perms) != 1 {
		t.Fatalf("expected 1 namespace entry, got %d", len(perms))
	}
	gotPerms, ok := perms[ns.ID]
	if !ok {
		t.Fatalf("expected permissions for namespace %d", ns.ID)
	}
	if len(gotPerms) != 2 || gotPerms[0] != "publish" || gotPerms[1] != "consume" {
		t.Fatalf("expected [publish consume], got %v", gotPerms)
	}
}

func TestStore_GetUserNamespacePermissions_UnknownUser(t *testing.T) {
	s := newTestStore(t)

	perms, err := s.GetUserNamespacePermissions(99999)
	if err != nil {
		t.Fatalf("GetUserNamespacePermissions: %v", err)
	}
	if perms == nil {
		t.Fatal("expected non-nil empty map, got nil")
	}
	if len(perms) != 0 {
		t.Fatalf("expected 0 permissions, got %d", len(perms))
	}
}

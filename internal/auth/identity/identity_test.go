package identity

import (
	"testing"
	"time"
)

func TestIdentity_HasPermission(t *testing.T) {
	id := Identity{
		SubjectID:   "test-user",
		SubjectType: SubjectUser,
		Permissions: NewPermissionSet(PermPublish, PermConsume, PermManageQueues),
	}

	tests := []struct {
		name   string
		perm   Permission
		expect bool
	}{
		{"publish", PermPublish, true},
		{"consume", PermConsume, true},
		{"manage queues", PermManageQueues, true},
		{"ack", PermAck, false},
		{"nack", PermNack, false},
		{"admin", PermAdmin, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := id.HasPermission(tc.perm); got != tc.expect {
				t.Errorf("HasPermission(%v) = %v, want %v", tc.perm, got, tc.expect)
			}
		})
	}
}

func TestIdentity_CanHelpers(t *testing.T) {
	pubOnly := Identity{Permissions: NewPermissionSet(PermPublish)}
	all := Identity{Permissions: NewPermissionSet(PermPublish, PermConsume, PermAck, PermNack, PermManageQueues, PermManageUsers)}
	admin := Identity{Permissions: NewPermissionSet(PermAdmin)}
	adminSubject := Identity{SubjectType: SubjectAdmin}

	if !pubOnly.CanPublish() {
		t.Error("pubOnly should CanPublish")
	}
	if pubOnly.CanConsume() {
		t.Error("pubOnly should NOT CanConsume")
	}
	if pubOnly.CanManageQueues() {
		t.Error("pubOnly should NOT CanManageQueues")
	}
	if !all.CanConsume() {
		t.Error("all should CanConsume")
	}
	if !all.CanManageUsers() {
		t.Error("all should CanManageUsers")
	}
	if admin.IsAdmin() != true {
		t.Error("admin permission should be IsAdmin")
	}
	if adminSubject.IsAdmin() != true {
		t.Error("SubjectAdmin should be IsAdmin")
	}
}

func TestIdentity_HasRole(t *testing.T) {
	id := Identity{Roles: []string{"admin", "billing"}}
	if !id.HasRole("admin") {
		t.Error("expected admin role")
	}
	if !id.HasRole("billing") {
		t.Error("expected billing role")
	}
	if id.HasRole("consumer") {
		t.Error("should NOT have consumer role")
	}
}

func TestIdentity_IsExpired(t *testing.T) {
	future := Identity{ExpiresAt: time.Now().Add(time.Hour)}
	if future.IsExpired() {
		t.Error("future expiry should NOT be expired")
	}

	past := Identity{ExpiresAt: time.Now().Add(-time.Hour)}
	if !past.IsExpired() {
		t.Error("past expiry should be expired")
	}

	zero := Identity{}
	if zero.IsExpired() {
		t.Error("zero expiry should NOT be expired")
	}
}

func TestIdentity_CanAccessNamespace(t *testing.T) {
	adminID := Identity{
		SubjectID:   "admin",
		Permissions: NewPermissionSet(PermAdmin),
	}
	if !adminID.CanAccessNamespace("any-namespace") {
		t.Error("admin should access any namespace")
	}

	userID := Identity{
		SubjectID: "user",
		Namespaces: map[string][]string{
			"main": {"publish", "consume"},
		},
	}
	if !userID.CanAccessNamespace("main") {
		t.Error("user should access main namespace")
	}
	if userID.CanAccessNamespace("other") {
		t.Error("user should NOT access other namespace")
	}
}

func TestIdentity_HasNamespacePermission(t *testing.T) {
	id := Identity{
		SubjectID: "user",
		Namespaces: map[string][]string{
			"main": {"publish", "consume", "*"},
		},
	}

	if !id.HasNamespacePermission("main", "consume") {
		t.Error("should have consume in main")
	}
	if !id.HasNamespacePermission("main", "ack") {
		t.Error("should have ack via wildcard in main")
	}
	if id.HasNamespacePermission("other", "publish") {
		t.Error("should NOT have publish in other")
	}

	admin := Identity{Permissions: NewPermissionSet(PermAdmin)}
	if !admin.HasNamespacePermission("any", "anything") {
		t.Error("admin should have any namespace permission")
	}
}

func TestPermissionSetFromStrings(t *testing.T) {
	tests := []struct {
		input  []string
		check  Permission
		expect bool
	}{
		{[]string{"publish"}, PermPublish, true},
		{[]string{"consume"}, PermConsume, true},
		{[]string{"ack"}, PermAck, true},
		{[]string{"nack"}, PermNack, true},
		{[]string{"queues:manage"}, PermManageQueues, true},
		{[]string{"manage_queues"}, PermManageQueues, true},
		{[]string{"users:manage"}, PermManageUsers, true},
		{[]string{"metrics:read"}, PermMetricsRead, true},
		{[]string{"audit:read"}, PermAuditRead, true},
		{[]string{"admin"}, PermAdmin, true},
		{[]string{"unknown"}, PermPublish, false},
	}

	for _, tc := range tests {
		t.Run(tc.input[0], func(t *testing.T) {
			ps := PermissionSetFromStrings(tc.input)
			if ps == nil {
				t.Fatal("unexpected nil PermissionSet")
			}
			if got := ps.Has(tc.check); got != tc.expect {
				t.Errorf("Has(%v) = %v, want %v", tc.check, got, tc.expect)
			}
		})
	}
}

func TestPermissionSet_Merge(t *testing.T) {
	a := NewPermissionSet(PermPublish, PermConsume)
	b := NewPermissionSet(PermAck, PermNack)
	a.Merge(b)

	if !a.Has(PermPublish) || !a.Has(PermConsume) || !a.Has(PermAck) || !a.Has(PermNack) {
		t.Error("Merge should include all permissions")
	}
}

func TestPermissionSet_List(t *testing.T) {
	ps := NewPermissionSet(PermPublish, PermAck)
	list := ps.List()
	seen := make(map[Permission]bool)
	for _, p := range list {
		seen[p] = true
	}
	if !seen[PermPublish] || !seen[PermAck] {
		t.Error("List should include all added permissions")
	}
	if len(list) != 2 {
		t.Errorf("expected 2 permissions, got %d", len(list))
	}
}

func TestIdentity_NilPermissions(t *testing.T) {
	id := Identity{}
	if id.HasPermission(PermPublish) {
		t.Error("nil permissions should return false")
	}
	if id.CanPublish() {
		t.Error("nil permissions should NOT CanPublish")
	}
	if id.IsAdmin() {
		t.Error("nil permissions should NOT be admin")
	}
}

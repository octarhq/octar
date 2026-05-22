package rbac

import (
	"testing"
)

func TestNewPolicy(t *testing.T) {
	p := NewPolicy()
	if p == nil {
		t.Fatal("expected non-nil policy")
	}
	if p.Version != "v1" {
		t.Errorf("expected v1, got %s", p.Version)
	}
	if len(p.Bindings) != 0 {
		t.Errorf("expected 0 bindings, got %d", len(p.Bindings))
	}
}

func TestAddRoleBinding(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("admin-binding", []string{"alice"}, []Role{RoleAdmin}, "")

	role := p.GetRoleForSubject("alice")
	if role != RoleAdmin {
		t.Errorf("expected admin role, got %s", role)
	}

	// Unknown subject returns empty.
	unknown := p.GetRoleForSubject("bob")
	if unknown != "" {
		t.Errorf("expected empty role for unknown, got %s", unknown)
	}
}

func TestGetPermissions(t *testing.T) {
	p := NewPolicy()

	adminPerms := p.GetPermissions(RoleAdmin)
	if len(adminPerms) == 0 {
		t.Fatal("expected non-empty admin permissions")
	}

	unknownPerms := p.GetPermissions("unknown")
	if len(unknownPerms) != 0 {
		t.Errorf("expected empty for unknown role, got %d", len(unknownPerms))
	}
}

func TestHasPermission(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("producer-binding", []string{"producer-user"}, []Role{RoleProducer}, "")

	tests := []struct {
		subject  string
		resource string
		action   string
		expect   bool
	}{
		{"producer-user", "queues", "publish", true},
		{"producer-user", "queues", "consume", false},
		{"producer-user", "namespaces", "namespaces:read", true},
		{"producer-user", "users", "users:manage", false},
		{"unknown-user", "queues", "publish", false},
	}

	for _, tc := range tests {
		t.Run(tc.subject+"/"+tc.action, func(t *testing.T) {
			got := p.HasPermission(tc.subject, tc.resource, tc.action)
			if got != tc.expect {
				t.Errorf("HasPermission(%q, %q, %q) = %v, want %v",
					tc.subject, tc.resource, tc.action, got, tc.expect)
			}
		})
	}
}

func TestHasPermission_Admin(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("admin-binding", []string{"admin-user"}, []Role{RoleAdmin}, "")

	if !p.HasPermission("admin-user", "queues", "publish") {
		t.Error("admin should be able to publish")
	}
	if !p.HasPermission("admin-user", "users", "users:*") {
		t.Error("admin should be able to manage users")
	}
	if !p.HasPermission("admin-user", "namespaces", "namespaces:*") {
		t.Error("admin should be able to manage namespaces")
	}
}

func TestHasPermission_WildcardPattern(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("observer-binding", []string{"observer-user"}, []Role{RoleObserver}, "")

	if !p.HasPermission("observer-user", "queues", "queues:read") {
		t.Error("observer should be able to read queues")
	}
	if p.HasPermission("observer-user", "queues", "queues:write") {
		t.Error("observer should NOT be able to write queues")
	}
}

func TestRoleFromString(t *testing.T) {
	tests := []struct {
		input string
		valid bool
		role  Role
	}{
		{"admin", true, RoleAdmin},
		{"producer", true, RoleProducer},
		{"consumer", true, RoleConsumer},
		{"observer", true, RoleObserver},
		{"billing", true, RoleBilling},
		{"service", true, RoleService},
		{"unknown", false, ""},
		{"", false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			role, err := RoleFromString(tc.input)
			if tc.valid && err != nil {
				t.Errorf("expected no error for %q, got %v", tc.input, err)
			}
			if !tc.valid && err == nil {
				t.Errorf("expected error for %q", tc.input)
			}
			if role != tc.role {
				t.Errorf("expected role %q, got %q", tc.role, role)
			}
		})
	}
}

func TestValidRoles(t *testing.T) {
	roles := ValidRoles()
	expected := []Role{RoleAdmin, RoleProducer, RoleConsumer, RoleObserver, RoleBilling, RoleService}
	if len(roles) != len(expected) {
		t.Fatalf("expected %d roles, got %d", len(expected), len(roles))
	}
	for i, r := range roles {
		if r != expected[i] {
			t.Errorf("role[%d] = %q, want %q", i, r, expected[i])
		}
	}
}

func TestPolicy_MarshalUnmarshal(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("test", []string{"user1"}, []Role{RoleProducer}, "")

	data, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	p2 := NewPolicy()
	if err := p2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if p2.GetRoleForSubject("user1") != RoleProducer {
		t.Error("unmarshalled policy should have producer role for user1")
	}
}

func TestPolicy_Clone(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("test", []string{"user1"}, []Role{RoleAdmin}, "")

	clone := p.Clone()
	if clone.GetRoleForSubject("user1") != RoleAdmin {
		t.Error("clone should have admin role for user1")
	}

	// Modify original, clone should be unaffected.
	p.AddRoleBinding("test2", []string{"user2"}, []Role{RoleConsumer}, "")
	if clone.GetRoleForSubject("user2") != "" {
		t.Error("clone should be independent of original")
	}
}

func TestResourceRule(t *testing.T) {
	p := NewPolicy()
	p.AddRoleBinding("admin-binding", []string{"admin"}, []Role{RoleAdmin}, "")
	p.Resources["secrets"] = ResourceRule{
		Allow: []string{"read"},
		Deny:  []string{"write", "delete"},
	}

	if !p.HasPermission("admin", "secrets", "read") {
		t.Error("admin should read secrets via resource rule")
	}
	if p.HasPermission("admin", "secrets", "write") {
		t.Error("admin should NOT write secrets via resource deny rule")
	}
}

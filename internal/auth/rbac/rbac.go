package rbac

import (
	"encoding/json"
	"fmt"
)

type Role string

const (
	RoleAdmin    Role = "admin"
	RoleProducer Role = "producer"
	RoleConsumer Role = "consumer"
	RoleObserver Role = "observer"
	RoleBilling  Role = "billing"
	RoleService  Role = "service"
)

var RolePermissions = map[Role][]string{
	RoleAdmin: {
		"namespaces:*",
		"queues:*",
		"groups:*",
		"users:*",
		"metrics:*",
		"audit:*",
		"publish",
		"consume",
		"ack",
		"nack",
	},
	RoleProducer: {
		"namespaces:read",
		"queues:write",
		"publish",
	},
	RoleConsumer: {
		"namespaces:read",
		"queues:read",
		"groups:write",
		"consume",
		"ack",
		"nack",
	},
	RoleObserver: {
		"namespaces:read",
		"queues:read",
		"metrics:read",
	},
	RoleBilling: {
		"namespaces:read",
		"metrics:read",
	},
	RoleService: {
		"namespaces:read",
		"publish",
		"consume",
	},
}

type RoleBinding struct {
	Name      string   `json:"name"`      // binding name
	Subjects  []string `json:"subjects"`  // users/service accounts
	Roles     []Role   `json:"roles"`     // roles assigned
	Namespace string   `json:"namespace"` // namespace scope (empty = global)
}

type Policy struct {
	Version   string                  `json:"version"`
	Roles     map[string]Role         `json:"roles"`     // name -> role
	Bindings  []RoleBinding           `json:"bindings"`  // role bindings
	Resources map[string]ResourceRule `json:"resources"` // resource -> allowed actions
}

type ResourceRule struct {
	Allow []string `json:"allow"`
	Deny  []string `json:"deny"`
}

func NewPolicy() *Policy {
	return &Policy{
		Version:   "v1",
		Roles:     make(map[string]Role),
		Bindings:  make([]RoleBinding, 0),
		Resources: make(map[string]ResourceRule),
	}
}

func (p *Policy) AddRoleBinding(name string, subjects []string, roles []Role, namespace string) {
	binding := RoleBinding{
		Name:      name,
		Subjects:  subjects,
		Roles:     roles,
		Namespace: namespace,
	}
	p.Bindings = append(p.Bindings, binding)

	for _, subject := range subjects {
		for _, role := range roles {
			p.Roles[subject] = role
		}
	}
}

func (p *Policy) GetRoleForSubject(subject string) Role {
	if role, ok := p.Roles[subject]; ok {
		return role
	}
	return ""
}

func (p *Policy) GetPermissions(role Role) []string {
	if perms, ok := RolePermissions[role]; ok {
		return perms
	}
	return []string{}
}

func (p *Policy) HasPermission(subject, resource, action string) bool {
	role := p.GetRoleForSubject(subject)
	if role == "" {
		return false
	}

	perms := p.GetPermissions(role)

	// Check resource-specific rules
	if rule, ok := p.Resources[resource]; ok {
		for _, a := range rule.Allow {
			if a == action || a == "*" {
				return true
			}
		}
		for _, a := range rule.Deny {
			if a == action || a == "*" {
				return false
			}
		}
	}

	// Check role permissions
	for _, perm := range perms {
		if perm == action || perm == "*" {
			return true
		}
		// Handle wildcard patterns like "namespaces:*"
		if len(perm) > 2 && perm[len(perm)-1] == '*' {
			prefix := perm[:len(perm)-1]
			if len(action) > len(prefix) && action[:len(prefix)] == prefix {
				return true
			}
		}
	}

	return false
}

func (p *Policy) Marshal() ([]byte, error) {
	return json.MarshalIndent(p, "", "  ")
}

func (p *Policy) Unmarshal(data []byte) error {
	return json.Unmarshal(data, p)
}

func (p *Policy) Clone() *Policy {
	rolesCopy := make(map[string]Role, len(p.Roles))
	for k, v := range p.Roles {
		rolesCopy[k] = v
	}

	bindingsCopy := make([]RoleBinding, len(p.Bindings))
	copy(bindingsCopy, p.Bindings)

	resourcesCopy := make(map[string]ResourceRule, len(p.Resources))
	for k, v := range p.Resources {
		resourcesCopy[k] = v
	}

	return &Policy{
		Version:   p.Version,
		Roles:     rolesCopy,
		Bindings:  bindingsCopy,
		Resources: resourcesCopy,
	}
}

func RoleFromString(s string) (Role, error) {
	role := Role(s)
	switch role {
	case RoleAdmin, RoleProducer, RoleConsumer, RoleObserver, RoleBilling, RoleService:
		return role, nil
	default:
		return "", fmt.Errorf("invalid role: %s", s)
	}
}

func ValidRoles() []Role {
	return []Role{RoleAdmin, RoleProducer, RoleConsumer, RoleObserver, RoleBilling, RoleService}
}

func RoleDescription(role Role) string {
	switch role {
	case RoleAdmin:
		return "Full access to all resources"
	case RoleProducer:
		return "Can publish messages"
	case RoleConsumer:
		return "Can consume and acknowledge messages"
	case RoleObserver:
		return "Read-only access to metrics and data"
	case RoleBilling:
		return "Read access for billing purposes"
	case RoleService:
		return "Service account with specific access"
	default:
		return "Unknown role"
	}
}

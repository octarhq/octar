package identity

import (
	"time"
)

type SubjectType string

const (
	SubjectUser        SubjectType = "USER"
	SubjectService     SubjectType = "SERVICE_ACCOUNT"
	SubjectAPIClient   SubjectType = "API_CLIENT"
	SubjectSystem      SubjectType = "SYSTEM"
	SubjectIntegration SubjectType = "INTEGRATION"
	SubjectAdmin       SubjectType = "ADMIN"
)

type AuthMethod string

const (
	AuthMethodNone      AuthMethod = "NONE"
	AuthMethodPassword  AuthMethod = "PASSWORD"
	AuthMethodAPIKey    AuthMethod = "API_KEY"
	AuthMethodJWT       AuthMethod = "JWT"
	AuthMethodOAuth     AuthMethod = "OAUTH"
	AuthMethodMTLS      AuthMethod = "MTLS"
	AuthMethodAnonymous AuthMethod = "ANONYMOUS"
)

type Permission uint64

const (
	PermNone Permission = 0

	PermPublish      Permission = 1 << iota // 1
	PermConsume                             // 2
	PermAck                                 // 4
	PermNack                                // 8
	PermManageQueues                        // 16
	PermManageUsers                         // 32
	PermMetricsRead                         // 64
	PermAuditRead                           // 128
	PermAdmin                               // 256
)

type PermissionSet map[Permission]bool

func NewPermissionSet(perms ...Permission) PermissionSet {
	ps := make(PermissionSet)
	for _, p := range perms {
		ps[p] = true
	}
	return ps
}

func (ps PermissionSet) Has(p Permission) bool {
	return ps[p]
}

func (ps PermissionSet) Add(p Permission) {
	ps[p] = true
}

func (ps PermissionSet) Remove(p Permission) {
	delete(ps, p)
}

func (ps PermissionSet) Merge(other PermissionSet) {
	for p := range other {
		ps[p] = true
	}
}

func (ps PermissionSet) List() []Permission {
	var result []Permission
	for p := range ps {
		result = append(result, p)
	}
	return result
}

func PermissionSetFromStrings(perms []string) PermissionSet {
	ps := make(PermissionSet)
	for _, p := range perms {
		switch p {
		case "publish":
			ps[PermPublish] = true
		case "consume":
			ps[PermConsume] = true
		case "ack":
			ps[PermAck] = true
		case "nack":
			ps[PermNack] = true
		case "queues:manage", "manage_queues":
			ps[PermManageQueues] = true
		case "users:manage", "manage_users":
			ps[PermManageUsers] = true
		case "metrics:read":
			ps[PermMetricsRead] = true
		case "audit:read":
			ps[PermAuditRead] = true
		case "admin":
			ps[PermAdmin] = true
		}
	}
	return ps
}

type Identity struct {
	SubjectID   string
	SubjectType SubjectType

	AccountID string
	Namespace string

	Roles       []string
	Permissions PermissionSet
	Namespaces  map[string][]string // namespace -> permissions

	AuthMethod AuthMethod

	IssuedAt  time.Time
	ExpiresAt time.Time

	Metadata map[string]string
}

func (i *Identity) HasPermission(p Permission) bool {
	if i.Permissions == nil {
		return false
	}
	return i.Permissions[p]
}

func (i *Identity) HasRole(role string) bool {
	for _, r := range i.Roles {
		if r == role {
			return true
		}
	}
	return false
}

func (i *Identity) IsExpired() bool {
	if i.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(i.ExpiresAt)
}

func (i *Identity) CanPublish() bool {
	return i.HasPermission(PermPublish)
}

func (i *Identity) CanConsume() bool {
	return i.HasPermission(PermConsume)
}

func (i *Identity) CanManageQueues() bool {
	return i.HasPermission(PermManageQueues)
}

func (i *Identity) CanManageUsers() bool {
	return i.HasPermission(PermManageUsers)
}

func (i *Identity) IsAdmin() bool {
	return i.HasPermission(PermAdmin) || i.SubjectType == SubjectAdmin
}

func (i *Identity) CanAccessNamespace(ns string) bool {
	if i.IsAdmin() {
		return true
	}
	if perms, ok := i.Namespaces[ns]; ok && len(perms) > 0 {
		return true
	}
	return false
}

func (i *Identity) HasNamespacePermission(ns, perm string) bool {
	if i.IsAdmin() {
		return true
	}
	perms, ok := i.Namespaces[ns]
	if !ok {
		return false
	}
	for _, p := range perms {
		if p == "*" || p == perm {
			return true
		}
	}
	return false
}

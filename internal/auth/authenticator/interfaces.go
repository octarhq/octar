package authenticator

import (
	"context"

	"github.com/83codes/octar/internal/auth/identity"
)

type AuthRequest struct {
	// Connection metadata
	RemoteAddr string

	// Credentials based on auth method
	Username    string
	Password    string
	APIKey      string
	Token       string
	Certificate string

	// Target
	Namespace string

	// Optional metadata
	Metadata map[string]string
}

type AuthResult struct {
	Identity *identity.Identity
	SessionID string
	Error    error
}

type Authenticator interface {
	// Authenticate validates credentials and returns an Identity
	Authenticate(ctx context.Context, req AuthRequest) (*identity.Identity, string, error)

	// Name returns the auth method name
	Name() string

	// Priority returns the order in which this authenticator is tried
	Priority() int
}

type Registry struct {
	authenticators map[string]Authenticator
}

func NewRegistry() *Registry {
	return &Registry{
		authenticators: make(map[string]Authenticator),
	}
}

func (r *Registry) Register(auth Authenticator) {
	r.authenticators[auth.Name()] = auth
}

func (r *Registry) Get(name string) Authenticator {
	return r.authenticators[name]
}

func (r *Registry) All() []Authenticator {
	var result []Authenticator
	for _, auth := range r.authenticators {
		result = append(result, auth)
	}

	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Priority() > result[j].Priority() {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}
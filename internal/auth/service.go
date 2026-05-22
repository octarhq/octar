package auth

import (
	"context"

	"github.com/83codes/octar/internal/auth/authenticator"
	"github.com/83codes/octar/internal/auth/audit"
	"github.com/83codes/octar/internal/auth/identity"
	"github.com/83codes/octar/internal/auth/jwt"
	"github.com/83codes/octar/internal/auth/providers"
	"github.com/83codes/octar/internal/auth/providers/mtls"
	"github.com/83codes/octar/internal/auth/providers/oauth"
	"github.com/83codes/octar/internal/auth/rbac"
	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
)

type Service struct {
	config     config.AuthConfig
	db         *db.Store
	registry   *authenticator.Registry
	jwtMgr     *jwt.Manager
	apiKeyAuth *providers.APIKeyAuthenticator
	audit      *audit.Logger
	mtlsAuth   *mtls.MTLSAuthenticator
	policy     *rbac.Policy
}

func NewService(cfg config.AuthConfig, store *db.Store, dataDir string) *Service {
	svc := &Service{
		config:     cfg,
		db:         store,
		registry:   authenticator.NewRegistry(),
		apiKeyAuth: providers.NewAPIKeyAuthenticator(cfg.Providers.APIKey),
		audit: audit.NewLogger(func(ctx context.Context, event *audit.Event) {
			dbEvent := &db.AuditEvent{
				ID:          event.ID,
				Type:        string(event.Type),
				Timestamp:   event.Timestamp,
				SubjectID:   event.SubjectID,
				SubjectType: event.SubjectType,
				AuthMethod:  event.AuthMethod,
				Namespace:   event.Namespace,
				RemoteAddr:  event.RemoteAddr,
				Success:     event.Success,
				Reason:      event.Reason,
				Metadata:    event.Metadata,
			}
			store.AppendAuditEvent(dbEvent)
		}, 1000),
		policy: rbac.NewPolicy(),
	}

	svc.loadDefaultPolicy()

	if cfg.Providers.Password.Enabled {
		svc.registry.Register(providers.NewPasswordAuthenticator(store))
	}

	if cfg.Providers.APIKey.Enabled {
		svc.registry.Register(svc.apiKeyAuth)
	}

	if cfg.Providers.OAuth.Enabled {
		oauthProvider := oauth.NewOAuthProvider(cfg.Providers.OAuth)
		svc.registry.Register(oauthProvider)
	}

	if cfg.Providers.MTLS.Enabled {
		svc.mtlsAuth = mtls.NewMTLSAuthenticator(cfg.Providers.MTLS)
		svc.registry.Register(svc.mtlsAuth)
	}

	if cfg.Providers.JWT.Enabled {
		svc.jwtMgr = jwt.NewManager(cfg.Providers.JWT, dataDir)
	}

	return svc
}

func (s *Service) loadDefaultPolicy() {
	s.policy.AddRoleBinding("admin", []string{"admin"}, []rbac.Role{rbac.RoleAdmin}, "")
	s.policy.AddRoleBinding("service", []string{"service"}, []rbac.Role{rbac.RoleService}, "")
}

func (s *Service) GenerateTokens(user *db.User) (*jwt.Tokens, error) {
	if s.jwtMgr == nil {
		return nil, nil
	}
	return s.jwtMgr.GenerateTokens(user)
}

func (s *Service) RefreshTokens(refreshToken string) (*jwt.Tokens, error) {
	if s.jwtMgr == nil {
		return nil, nil
	}
	return s.jwtMgr.RefreshTokens(refreshToken)
}

func (s *Service) VerifyToken(token string) (*identity.Identity, error) {
	if s.jwtMgr == nil {
		return nil, nil
	}
	claims, err := s.jwtMgr.VerifyAccessToken(token)
	if err != nil {
		return nil, err
	}
	return claims.ToIdentity(), nil
}

func (s *Service) AuthenticateTCP(ctx context.Context, remoteAddr, username, password, namespace string) (*identity.Identity, error) {
	for _, auth := range s.registry.All() {
		id, _, err := auth.Authenticate(ctx, authenticator.AuthRequest{
			RemoteAddr: remoteAddr,
			Username:   username,
			Password:   password,
			Namespace:  namespace,
		})
		if err != nil {
			return nil, err
		}
		if id != nil {
			return id, nil
		}
	}
	return nil, nil
}

func (s *Service) AuthenticateTCPWithKey(ctx context.Context, remoteAddr, apiKey, namespace string) (*identity.Identity, error) {
	for _, auth := range s.registry.All() {
		id, _, err := auth.Authenticate(ctx, authenticator.AuthRequest{
			RemoteAddr: remoteAddr,
			APIKey:     apiKey,
			Namespace:  namespace,
		})
		if err != nil {
			return nil, err
		}
		if id != nil {
			return id, nil
		}
	}
	return nil, nil
}

func (s *Service) GenerateAPIKey(subjectID, namespace string, permissions []string) string {
	key := providers.GenerateAPIKey(s.config.Providers.APIKey.Prefix)
	s.apiKeyAuth.AddKey(key, subjectID, namespace, permissions)
	return key
}

func (s *Service) RevokeAPIKey(key string) {
	s.apiKeyAuth.RevokeKey(key)
}

func (s *Service) ListAPIKeys() []providers.APIKeyEntry {
	return s.apiKeyAuth.ListKeys()
}
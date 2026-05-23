package providers

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/octarhq/octar/internal/auth/authenticator"
	"github.com/octarhq/octar/internal/auth/identity"
	"github.com/octarhq/octar/internal/config"
)

type APIKeyAuthenticator struct {
	cfg     config.APIKeyProviderConfig
	keyHash map[string]*APIKeyEntry
}

type APIKeyEntry struct {
	Hash        string
	SubjectID   string
	SubjectType identity.SubjectType
	Namespace   string
	Permissions []string
	Prefix      string
}

func NewAPIKeyAuthenticator(cfg config.APIKeyProviderConfig) *APIKeyAuthenticator {
	return &APIKeyAuthenticator{
		cfg:     cfg,
		keyHash: make(map[string]*APIKeyEntry),
	}
}

func (a *APIKeyAuthenticator) Name() string {
	return "api_key"
}

func (a *APIKeyAuthenticator) Priority() int {
	return a.cfg.Priority
}

func (a *APIKeyAuthenticator) Authenticate(ctx context.Context, req authenticator.AuthRequest) (*identity.Identity, string, error) {
	if req.APIKey == "" {
		return nil, "", nil
	}

	key := strings.TrimSpace(req.APIKey)

	prefix := a.cfg.Prefix
	if prefix != "" && !strings.HasPrefix(key, prefix) {
		return nil, "", nil
	}

	keyHash := hashKey(key)
	entry, ok := a.keyHash[keyHash]
	if !ok {
		return nil, "", nil
	}

	if req.Namespace != "" && entry.Namespace != "" && req.Namespace != entry.Namespace {
		return nil, "", nil
	}

	id := &identity.Identity{
		SubjectID:   entry.SubjectID,
		SubjectType: entry.SubjectType,
		AccountID:   entry.SubjectID,
		Namespace:   entry.Namespace,
		Roles:       []string{"service"},
		Permissions: identity.PermissionSetFromStrings(entry.Permissions),
		AuthMethod:  identity.AuthMethodAPIKey,
		Namespaces:  map[string][]string{entry.Namespace: entry.Permissions},
	}

	return id, "", nil
}

func (a *APIKeyAuthenticator) AddKey(key, subjectID, namespace string, perms []string) {
	entry := &APIKeyEntry{
		Hash:        hashKey(key),
		SubjectID:   subjectID,
		SubjectType: identity.SubjectService,
		Namespace:   namespace,
		Permissions: perms,
		Prefix:      a.cfg.Prefix,
	}
	a.keyHash[entry.Hash] = entry
}

func (a *APIKeyAuthenticator) RevokeKey(key string) {
	keyHash := hashKey(key)
	delete(a.keyHash, keyHash)
}

func (a *APIKeyAuthenticator) ListKeys() []APIKeyEntry {
	var result []APIKeyEntry
	for _, entry := range a.keyHash {
		result = append(result, *entry)
	}
	return result
}

func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

func GenerateAPIKey(prefix string) string {
	return prefix + generateRandomSuffix(32)
}

func generateRandomSuffix(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := 0; i < n; i++ {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b)
}

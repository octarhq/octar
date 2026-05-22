// Package jwt handles JWT token issuance and verification for OCTAR.
// Supports HMAC-SHA256, RSA, ECDSA, and EdDSA signing algorithms.
package jwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/83codes/octar/internal/auth/identity"
	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
)

// ── Types ─────────────────────────────────────────────────────────────────────

// SubjectType aliases identity.SubjectType so callers don't need a separate import.
type SubjectType = identity.SubjectType

// Claims is the JWT payload embedded in an access token.
type Claims struct {
	Subject   string `json:"sub"`
	SubjectTy string `json:"sty,omitempty"`

	AccountID  string `json:"acc,omitempty"`
	Namespace  string `json:"ns,omitempty"`

	Roles      []string            `json:"roles,omitempty"`
	Perms      []string            `json:"perms,omitempty"`
	Namespaces map[string][]string `json:"ns_perms,omitempty"`

	Method string `json:"method,omitempty"`

	IssuedAt  int64 `json:"iat"`
	ExpiresAt int64 `json:"exp"`
}

// ToIdentity converts JWT claims to a runtime Identity.
func (c *Claims) ToIdentity() *identity.Identity {
	id := &identity.Identity{
		SubjectID:   c.Subject,
		SubjectType: identity.SubjectType(c.SubjectTy),
		AccountID:   c.AccountID,
		Namespace:   c.Namespace,
		Roles:       c.Roles,
		AuthMethod:  identity.AuthMethod(c.Method),
		IssuedAt:    time.Unix(c.IssuedAt, 0),
		ExpiresAt:   time.Unix(c.ExpiresAt, 0),
	}

	if len(c.Perms) > 0 {
		id.Permissions = identity.PermissionSetFromStrings(c.Perms)
	} else {
		id.Permissions = identity.NewPermissionSet()
	}

	if c.Namespaces != nil {
		id.Namespaces = c.Namespaces
	} else {
		id.Namespaces = make(map[string][]string)
	}
	return id
}

// Tokens is the pair returned to a successful login or refresh.
type Tokens struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int64
	TokenType    string
}

// RefreshToken is the payload embedded in a refresh token (signed separately).
type RefreshToken struct {
	SubjectID   string                  `json:"sid"`
	SubjectType identity.SubjectType    `json:"sty"`
	AccountID   string                  `json:"acc"`
	Roles       []string                `json:"roles"`
	Namespaces  map[string][]string     `json:"ns_perms"`
	IssuedAt    int64                   `json:"iat"`
	ExpiresAt   int64                   `json:"exp"`
}

// ── Manager ───────────────────────────────────────────────────────────────────

// Manager signs and verifies JWTs. It supports both asymmetric keys (RSA /
// ECDSA / EdDSA) and symmetric HMAC-SHA256.
type Manager struct {
	cfg        config.JWTProviderConfig
	keyStore   *keyStore
	refreshTTL time.Duration
	hmacSecret []byte
}

type keyStore struct {
	privateKey interface{}
	publicKey  interface{}
	keyType    string
	keyID      string
}

func NewManager(cfg config.JWTProviderConfig, dataDir string) *Manager {
	m := &Manager{
		cfg:        cfg,
		refreshTTL: 7 * 24 * time.Hour,
		hmacSecret: []byte(cfg.HMACSecret),
	}
	if cfg.PrivateKey != "" {
		m.loadKeys()
	} else if dataDir != "" {
		m.loadOrGenerateRSAKeys(dataDir)
	}
	return m
}

func (m *Manager) loadKeys() {
	switch m.cfg.KeyType {
	case "RSA":
		m.loadRSAKeys()
	case "EC":
		m.loadECKeys()
	case "EdDSA":
		m.loadEdDSAKeys()
	case "HMAC":
		// HMAC secret is already loaded in hmacSecret
	}
}

func (m *Manager) loadRSAKeys() {
	privBytes, err := base64.StdEncoding.DecodeString(m.cfg.PrivateKey)
	if err != nil {
		return
	}
	priv, err := x509DecodePrivateKey(privBytes)
	if err != nil {
		return
	}
	m.keyStore = &keyStore{
		privateKey: priv,
		publicKey:  &priv.PublicKey,
		keyType:    "RSA",
		keyID:      m.cfg.KeyID,
	}
}

func (m *Manager) loadECKeys() {
	privBytes, err := base64.StdEncoding.DecodeString(m.cfg.PrivateKey)
	if err != nil {
		return
	}
	priv, err := x509DecodeECPrivateKey(privBytes)
	if err != nil {
		return
	}
	m.keyStore = &keyStore{
		privateKey: priv,
		publicKey:  &priv.PublicKey,
		keyType:    "EC",
		keyID:      m.cfg.KeyID,
	}
}

func (m *Manager) loadEdDSAKeys() {
	privBytes, err := base64.StdEncoding.DecodeString(m.cfg.PrivateKey)
	if err != nil {
		return
	}
	if len(privBytes) == 32 {
		priv := ed25519.PrivateKey(privBytes)
		m.keyStore = &keyStore{
			privateKey: priv,
			publicKey:  priv.Public().(ed25519.PublicKey),
			keyType:    "EdDSA",
			keyID:      m.cfg.KeyID,
		}
	}
}

func (m *Manager) loadOrGenerateRSAKeys(dataDir string) {
	keyPath := filepath.Join(dataDir, "jwt_private.pem")

	priv, err := loadRSAPrivateKeyPEM(keyPath)
	if err == nil {
		m.keyStore = &keyStore{
			privateKey: priv,
			publicKey:  &priv.PublicKey,
			keyType:    "RSA",
			keyID:      "auto",
		}
		return
	}

	priv, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return
	}

	if err := saveRSAPrivateKeyPEM(keyPath, priv); err != nil {
		return
	}

	m.keyStore = &keyStore{
		privateKey: priv,
		publicKey:  &priv.PublicKey,
		keyType:    "RSA",
		keyID:      "auto",
	}
}

func loadRSAPrivateKeyPEM(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, fmt.Errorf("invalid PEM block")
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func saveRSAPrivateKeyPEM(path string, key *rsa.PrivateKey) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

// GenerateTokens issues a fresh access + refresh token pair for a user.
func (m *Manager) GenerateTokens(user *db.User) (*Tokens, error) {
	subjectType := "user"
	if user.Role == "admin" {
		subjectType = "admin"
	}

	accessTTL := time.Duration(m.cfg.AccessTokenTTL) * time.Second
	if accessTTL == 0 {
		accessTTL = 15 * time.Minute
	}
	now := time.Now()

	claims := Claims{
		Subject:   user.Username,
		SubjectTy: subjectType,
		AccountID: user.Username,
		Roles:     []string{user.Role},
		Perms:     permissionsFromRole(user.Role),
		Method:    "password",
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(accessTTL).Unix(),
	}

	accessToken, err := m.signClaims(claims)
	if err != nil {
		return nil, err
	}

	refreshToken, err := m.generateRefreshToken(
		user.Username, subjectType, user.Username, []string{user.Role}, nil,
	)
	if err != nil {
		return nil, err
	}

	return &Tokens{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(accessTTL.Seconds()),
		TokenType:    "Bearer",
	}, nil
}

// RefreshTokens validates a refresh token and issues a new token pair.
func (m *Manager) RefreshTokens(refreshToken string) (*Tokens, error) {
	rt, err := m.validateRefreshToken(refreshToken)
	if err != nil {
		return nil, err
	}

	accessTTL := time.Duration(m.cfg.AccessTokenTTL) * time.Second
	if accessTTL == 0 {
		accessTTL = 15 * time.Minute
	}
	now := time.Now()

	accessClaims := Claims{
		Subject:    rt.SubjectID,
		SubjectTy:  string(rt.SubjectType),
		AccountID:  rt.AccountID,
		Roles:      rt.Roles,
		Perms:      permissionsFromRole(rt.Roles[0]),
		Namespaces: rt.Namespaces,
		Method:     "refresh",
		IssuedAt:   now.Unix(),
		ExpiresAt:  now.Add(accessTTL).Unix(),
	}

	accessToken, err := m.signClaims(accessClaims)
	if err != nil {
		return nil, err
	}

	newRT, err := m.generateRefreshToken(
		rt.SubjectID, string(rt.SubjectType), rt.AccountID, rt.Roles, rt.Namespaces,
	)
	if err != nil {
		return nil, err
	}

	return &Tokens{
		AccessToken:  accessToken,
		RefreshToken: newRT,
		ExpiresIn:    int64(accessTTL.Seconds()),
		TokenType:    "Bearer",
	}, nil
}

// VerifyAccessToken parses and validates an access token, returning its claims.
func (m *Manager) VerifyAccessToken(token string) (*Claims, error) {
	if m.keyStore != nil {
		return m.verifyWithKey(token)
	}
	if len(m.hmacSecret) > 0 {
		return m.verifyHMAC(token)
	}
	return nil, fmt.Errorf("no signing key configured")
}

// ── Signing ───────────────────────────────────────────────────────────────────

func (m *Manager) signClaims(claims Claims) (string, error) {
	if m.keyStore != nil {
		return m.signWithKey(claims)
	}
	if len(m.hmacSecret) > 0 {
		return m.signHMAC(claims)
	}
	return "", fmt.Errorf("no signing key configured")
}

func (m *Manager) signWithKey(claims Claims) (string, error) {
	header := map[string]string{
		"alg": m.keyStore.keyType,
		"typ": "JWT",
		"kid": m.keyStore.keyID,
	}
	headerEnc := base64.RawURLEncoding.EncodeToString(mustMarshalJSON(header))
	payloadEnc := base64.RawURLEncoding.EncodeToString(mustMarshalJSON(claims))

	sig, err := m.sign([]byte(headerEnc + "." + payloadEnc))
	if err != nil {
		return "", err
	}
	return headerEnc + "." + payloadEnc + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (m *Manager) sign(data []byte) ([]byte, error) {
	switch k := m.keyStore.privateKey.(type) {
	case *rsa.PrivateKey:
		return rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, data)
	case *ecdsa.PrivateKey:
		return ecdsa.SignASN1(rand.Reader, k, data)
	case ed25519.PrivateKey:
		return k.Sign(rand.Reader, data, nil)
	default:
		return nil, fmt.Errorf("unsupported key type")
	}
}

func (m *Manager) verifyWithKey(token string) (*Claims, error) {
	parts := splitToken(token)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}
	headerEnc, payloadEnc, sigEnc := parts[0], parts[1], parts[2]

	sig, err := base64.RawURLEncoding.DecodeString(sigEnc)
	if err != nil {
		return nil, err
	}
	if err := m.verify([]byte(headerEnc+"."+payloadEnc), sig); err != nil {
		return nil, err
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadEnc)
	if err != nil {
		return nil, err
	}
	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, err
	}
	if claims.ExpiresAt < time.Now().Unix() {
		return nil, fmt.Errorf("token expired")
	}
	return &claims, nil
}

func (m *Manager) verify(data, sig []byte) error {
	switch k := m.keyStore.publicKey.(type) {
	case *rsa.PublicKey:
		return rsa.VerifyPKCS1v15(k, crypto.SHA256, data, sig)
	case *ecdsa.PublicKey:
		if !ecdsa.VerifyASN1(k, data, sig) {
			return fmt.Errorf("ecdsa verification failed")
		}
		return nil
	case ed25519.PublicKey:
		if !ed25519.Verify(k, data, sig) {
			return fmt.Errorf("ed25519 verification failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported key type")
	}
}

func (m *Manager) signHMAC(claims Claims) (string, error) {
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	headerEnc := base64.RawURLEncoding.EncodeToString(mustMarshalJSON(header))
	payloadEnc := base64.RawURLEncoding.EncodeToString(mustMarshalJSON(claims))
	sig := hmacSHA256([]byte(headerEnc+"."+payloadEnc), m.hmacSecret)
	return headerEnc + "." + payloadEnc + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (m *Manager) verifyHMAC(token string) (*Claims, error) {
	parts := splitToken(token)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}
	headerEnc, payloadEnc, sigEnc := parts[0], parts[1], parts[2]

	expected := hmacSHA256([]byte(headerEnc+"."+payloadEnc), m.hmacSecret)
	actual, err := base64.RawURLEncoding.DecodeString(sigEnc)
	if err != nil {
		return nil, err
	}
	if !constTimeEqual(expected, actual) {
		return nil, fmt.Errorf("invalid signature")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadEnc)
	if err != nil {
		return nil, err
	}
	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, err
	}
	if claims.ExpiresAt < time.Now().Unix() {
		return nil, fmt.Errorf("token expired")
	}
	return &claims, nil
}

func (m *Manager) generateRefreshToken(subjectID, subjectType, accountID string, roles []string, namespaces map[string][]string) (string, error) {
	rt := RefreshToken{
		SubjectID:   subjectID,
		SubjectType: identity.SubjectType(subjectType),
		AccountID:   accountID,
		Roles:       roles,
		Namespaces:  namespaces,
		IssuedAt:    time.Now().Unix(),
		ExpiresAt:   time.Now().Add(m.refreshTTL).Unix(),
	}

	data, err := json.Marshal(rt)
	if err != nil {
		return "", err
	}
	if len(m.hmacSecret) > 0 {
		mac := hmacSHA256(data, m.hmacSecret)
		data = append(data, mac...)
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func (m *Manager) validateRefreshToken(token string) (*RefreshToken, error) {
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	if len(m.hmacSecret) > 0 {
		if len(data) < 32 {
			return nil, fmt.Errorf("invalid refresh token")
		}
		mac := data[len(data)-32:]
		body := data[:len(data)-32]
		if !constTimeEqual(hmacSHA256(body, m.hmacSecret), mac) {
			return nil, fmt.Errorf("invalid refresh token signature")
		}
		data = body
	}
	var rt RefreshToken
	if err := json.Unmarshal(data, &rt); err != nil {
		return nil, err
	}
	if rt.ExpiresAt < time.Now().Unix() {
		return nil, fmt.Errorf("refresh token expired")
	}
	return &rt, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func permissionsFromRole(role string) []string {
	switch role {
	case "admin":
		return []string{"admin", "publish", "consume", "ack", "nack", "queues:manage", "users:manage", "metrics:read"}
	case "user":
		return []string{"publish", "consume", "ack", "nack", "queues:manage"}
	default:
		return []string{}
	}
}

func splitToken(token string) []string {
	var parts []string
	var cur []byte
	for _, c := range []byte(token) {
		if c == '.' {
			parts = append(parts, string(cur))
			cur = nil
		} else {
			cur = append(cur, c)
		}
	}
	if cur != nil {
		parts = append(parts, string(cur))
	}
	return parts
}

func hmacSHA256(data, key []byte) []byte {
	h := crypto.SHA256.New()
	h.Write(key)
	h.Write([]byte{0x01})
	k1 := h.Sum(nil)

	h = crypto.SHA256.New()
	h.Write(k1)
	h.Write(data)
	h.Write([]byte{0x01})
	return h.Sum(nil)
}

func constTimeEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

func mustMarshalJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func x509DecodePrivateKey(data []byte) (*rsa.PrivateKey, error) {
	var pkcs1 struct {
		Version int
		Algo    int
		Data    []byte
	}
	if err := json.Unmarshal(data, &pkcs1); err != nil {
		n := new(big.Int)
		n, ok := n.SetString(string(data), 10)
		if !ok {
			return nil, fmt.Errorf("invalid key format")
		}
		d := make([]byte, n.BitLen()/8+1)
		n.FillBytes(d)
		priv := &rsa.PrivateKey{D: new(big.Int).SetBytes(d)}
		priv.Precompute()
		return priv, nil
	}
	return nil, fmt.Errorf("unsupported key format")
}

func x509DecodeECPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	var ec struct {
		D string `json:"d"`
		X string `json:"x"`
		Y string `json:"y"`
	}
	if err := json.Unmarshal(data, &ec); err != nil {
		return nil, err
	}
	d, ok := new(big.Int).SetString(ec.D, 10)
	if !ok {
		return nil, fmt.Errorf("invalid D")
	}
	x, ok := new(big.Int).SetString(ec.X, 10)
	if !ok {
		return nil, fmt.Errorf("invalid X")
	}
	y, ok := new(big.Int).SetString(ec.Y, 10)
	if !ok {
		return nil, fmt.Errorf("invalid Y")
	}
	return &ecdsa.PrivateKey{
		D:         d,
		PublicKey: ecdsa.PublicKey{X: x, Y: y},
	}, nil
}

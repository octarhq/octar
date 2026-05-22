# Auth — Authentication & Authorization

**Package**: `internal/auth`

A modular authentication and authorization system with pluggable providers, JWT token management, RBAC policies, and audit logging.

## Technologies

| Technology | Purpose |
|------------|---------|
| Go `crypto/sha256` | API key hashing |
| Go `crypto/rand` | Cryptographically random suffix generation |
| Go `crypto/hmac` | HMAC-based refresh token signing |
| Go `crypto/rsa`, `crypto/ecdsa`, `crypto/ed25519` | JWT asymmetric key support |
| `golang.org/x/crypto/bcrypt` | Password hashing |

## Architecture

```
Client → TCP/API request
              ↓
         Service.AuthenticateTCP()
              ↓
     ┌──────────────────┐
     │  Authenticator    │
     │  Registry         │
     │  (priority order) │
     ├──────────────────┤
     │  1. Password      │ ← bcrypt hash check
     │  2. API Key       │ ← SHA-256 hash check
     │  3. JWT           │ ← token verification
     │  4. OAuth2/OIDC   │ ← external provider
     │  5. mTLS          │ ← certificate validation
     └──────────────────┘
              ↓
         Identity (SubjectID, Roles, Permissions)
              ↓
         RBAC Policy Check (resource-specific allow/deny)
              ↓
         AuditEvent (AUTH_SUCCESS, PERMISSION_DENIED, etc.)
```

## Authenticators

The authenticator registry maintains a priority-ordered list. On each authentication request, providers are tried in order until one returns success.

### Password Authenticator

- **Priority**: 10 (default)
- **Storage**: bcrypt hash in SQLite `users` table
- **Cost**: configurable (default bcrypt cost 12)
- **Flow**: username + password → bcrypt.CompareHashAndPassword → identity + JWT tokens

### API Key Authenticator

- **Priority**: 20 (default)
- **Storage**: SHA-256 hash in memory (loaded from SQLite on startup)
- **Prefix**: configurable (default `flw_live_`)
- **Flow**: API key → SHA-256 → lookup in hash map → identity with scoped permissions
- **Why SHA-256?**: The actual key is used as a bearer secret; only the hash is stored. If the database is compromised, keys cannot be recovered.

### JWT Authenticator

- **Support for**: HMAC-SHA256, RSA-PKCS1v15-SHA256, ECDSA, EdDSA
- **Custom implementation**: no third-party JWT library — hand-written to avoid dependency overhead
- **Access token TTL**: 15 minutes (configurable)
- **Refresh token TTL**: 7 days (configurable)
- **Refresh token signing**: HMAC-derived from the access token's HMAC secret — no separate database query needed

### OAuth2 / OIDC and mTLS

- **OAuth2**: integration with external OAuth2/OIDC providers
- **mTLS**: mutual TLS certificate-based authentication

## Key Management

### JWT Key Store

The JWT manager (`internal/auth/jwt`) supports multiple key types:

| Key Type | Algorithm | Use Case |
|----------|-----------|----------|
| `HMAC` | HMAC-SHA256 | Simple, single-server deployments |
| `RSA` | RSASSA-PKCS1v15-SHA256 | Multi-service JWT verification (public key distribution) |
| `ECDSA` | ECDSA P-256 | Smaller signatures, hardware security module support |
| `EdDSA` | Ed25519 | Modern, fast, small keys |

### API Key Generation

API keys are generated with a **configurable prefix** (e.g. `flw_live_`) followed by a 32-character cryptographically random suffix from the charset `[a-z0-9]`.

```go
// Key format
flw_live_abcdefghijklmnopqrstuvwxyz012345
```

## RBAC

**Package**: `internal/auth/rbac`

### Roles

| Role | Description |
|------|-------------|
| `admin` | Full access to all resources |
| `producer` | Can publish messages to queues |
| `consumer` | Can subscribe and consume messages |
| `observer` | Read-only access to metadata |
| `billing` | Access to billing-related resources |
| `service` | Service-to-service automation |

### Policy Structure

```json
{
  "roles": {
    "producer": {
      "resources": [
        {
          "namespace": "*",
          "verbs": ["publish"],
          "allow": true
        }
      ]
    }
  }
}
```

Policies support wildcard matching for namespaces, queue names, and group keys.

## Audit

**Package**: `internal/auth/audit`

All authentication and authorization events are logged to an in-memory ring buffer with async persistence to the SQLite `audit_events` table.

### Event Types

| Event | Description |
|-------|-------------|
| `AUTH_SUCCESS` | Successful authentication |
| `AUTH_FAILURE` | Failed authentication attempt |
| `TOKEN_ISSUED` | JWT or refresh token issued |
| `TOKEN_REVOKED` | Token explicitly revoked |
| `API_KEY_CREATED` | New API key generated |
| `API_KEY_REVOKED` | API key deleted |
| `PERMISSION_DENIED` | Authorized but not permitted |
| `LOGIN_ATTEMPT` | Login attempt (success or failure) |

### Rate Limiting

Login attempts are rate-limited per username:

- `max_attempts`: 5 per minute (configurable)
- Lockout duration: 15 minutes (configurable)
- Auth abuse detection: 100 requests/minute from the same source

# DB — SQLite Metadata Store

**Package**: `internal/db`

The metadata store holds durable configuration data that changes infrequently: namespaces, queues, users, API keys, RBAC policies, sessions, and audit events.

## Technologies

| Technology | Purpose |
|------------|---------|
| `modernc.org/sqlite` | Pure-Go SQLite driver (no CGO, no system SQLite) |
| `golang.org/x/crypto/bcrypt` | Password hashing for user credentials |
| Go `database/sql` | Standard database/sql interface |

## Why SQLite?

- **Zero configuration**: no separate database server to install, configure, or tune
- **Single file**: easy to back up, migrate, and inspect
- **Pure Go driver**: no CGO, no cross-compilation issues, works on any platform
- **WAL journal mode**: concurrent reads without blocking — sufficient for metadata access patterns (reads >> writes)
- **ACID transactions**: exactly the guarantees we need for configuration consistency

## Why Not SQLite for Messages?

Messages go through the WAL (see [Storage](storage.md)), not SQLite. SQLite's per-row overhead and B-tree insert cost would add unnecessary latency on the hot path. The metadata store is queried at startup and during management API calls — a few queries per second, not thousands.

## Schema

| Table | Purpose |
|-------|---------|
| `namespaces` | Tenant isolation — each namespace has its own queues and users |
| `users` | Broker users with bcrypt-hashed passwords |
| `user_namespace_permissions` | Per-namespace role and permission bindings |
| `queues` | Queue declarations (name, namespace, created_at) |
| `groups` | Per-queue consumer group configurations |
| `api_keys` | SHA-256 hashed API keys with scope metadata |
| `sessions` | Active JWT session tracking with expiry |
| `rbac_policies` | JSON-serialised RBAC policy definitions |
| `audit_events` | Authentication and security events |

## Key Design Decisions

### Auto-Migration

Tables are created with `CREATE TABLE IF NOT EXISTS` on every startup. Schema changes are additive (new columns have `DEFAULT` values). This avoids migration tooling and keeps upgrades simple.

### Seed Data

On first startup (when no users exist), the store creates:

1. A default `main` namespace
2. A default admin user with credentials from config (or `admin/admin`)

This means the broker is immediately usable after `docker run` with no setup steps.

### WAL Journal Mode

SQLite is configured with `PRAGMA journal_mode=WAL`. This allows:

- Concurrent reads without blocking writers (and vice versa)
- Better read performance (the reader reads from the WAL without blocking on the main database)
- Crash safety without full fsync on every write

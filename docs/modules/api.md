# API — HTTP Management API

**Package**: `internal/api/v1`

The management API provides a RESTful HTTP interface for managing namespaces, queues, consumer groups, users, API keys, authentication, and monitoring.

## Technologies

| Technology | Purpose |
|------------|---------|
| `github.com/danielgtaylor/huma/v2` | OpenAPI 3.1 framework — generates spec from Go types |
| `github.com/go-chi/chi/v5` | HTTP router — fast, idiomatic, middleware-friendly |
| `internal/auth` | JWT auth middleware, permission checks |
| `internal/broker` | Queue and group management |
| `internal/db` | User and namespace CRUD |
| `internal/scheduler` | Metrics and health |

## Why Huma v2?

- **Single source of truth**: Go structs define both the request schema and the OpenAPI spec — no separate spec file to keep in sync
- **Stoplight Elements UI**: interactive API reference served at `/docs` with no configuration
- **Validation**: Huma automatically validates request bodies against the schema
- **No codegen**: works with standard Go types — no annotations, no code generation step

## Endpoints

### Authentication

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/auth/login` | Authenticate with username/password, receive JWT tokens |
| `POST` | `/auth/refresh` | Exchange a refresh token for a new access token |
| `POST` | `/auth/logout` | Invalidate the current session |

### Health & Monitoring

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Broker health (WAL status, scheduler health, disk space) |
| `GET` | `/internal/metrics` | Scheduler metrics (activation latency, queue depth) |
| `GET` | `/permissions` | List all available permissions |

### Namespaces

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces` | List all namespaces |
| `POST` | `/namespaces` | Create a namespace |
| `GET` | `/namespaces/{ns}` | Get namespace details |
| `DELETE` | `/namespaces/{ns}` | Delete a namespace |

### Users

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/users` | List all users |
| `POST` | `/users` | Create a user |
| `GET` | `/users/{id}` | Get user details |
| `PUT` | `/users/{id}` | Update user (role, password) |
| `DELETE` | `/users/{id}` | Delete a user |

### API Keys

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api-keys` | List all API keys |
| `POST` | `/api-keys` | Create an API key (returns the full key once) |
| `DELETE` | `/api-keys/{id}` | Revoke an API key |

### Queues

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/queues` | List all queues |
| `POST` | `/queues` | Create a queue |
| `GET` | `/queues/{ns}/{name}` | Get queue details |
| `DELETE` | `/queues/{ns}/{name}` | Delete a queue |
| `POST` | `/queues/{ns}/{name}/snapshot` | Trigger a storage snapshot |
| `GET` | `/queues/{ns}/{name}/stats` | Queue statistics |
| `GET` | `/queues/{ns}/{name}/stats/{key}` | Per-group statistics |

#### Queue durability

`POST /queues` accepts an optional `durable` field (default: `true`):

```json
{ "name": "my-queue", "namespace": "main", "durable": false }
```

| `durable` | Behaviour | Recommended for |
|-----------|-----------|-----------------|
| `true` (default) | fsync after each WAL batch — survives power loss | Orders, payments, emails |
| `false` | OS buffer only — 5–10x faster, survives process crash | Notifications, cache invalidation, analytics |

The effective value is returned in `GET /queues/{ns}/{name}` responses.

### Consumer Groups

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/queues/{ns}/{name}/groups` | List all groups for a queue |
| `PUT` | `/queues/{ns}/{name}/groups/{key}` | Create or update a group config |
| `GET` | `/queues/{ns}/{name}/groups/{key}` | Get a group's current config |
| `DELETE` | `/queues/{ns}/{name}/groups/{key}` | Delete a group config |

## Middleware Stack

```
chi.Router
  ├── CORS (TODO: configurable origins)
  ├── Request logging
  ├── Rate limiting
  ├── JWT auth (Bearer token from Authorization header)
  └── Permission middleware (per-endpoint check)
```

Responses follow Huma v2's structured error format with HTTP status codes and machine-readable error codes.

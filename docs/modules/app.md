# App — Application Bootstrap

**Package**: `internal/app`

The App module wires all broker components together and manages the process lifecycle (startup, signal handling, graceful shutdown).

## Technologies

| Technology | Purpose |
|------------|---------|
| Go standard library `os/signal` | Signal handling (SIGINT/SIGTERM) |
| `net/http` | Isolated HTTP muxes for metrics and pprof |

## Why This Approach

- **Single `App` struct** as the composition root keeps dependency injection explicit and testable.
- **Separate HTTP muxes** for metrics (`:2112`) and pprof (`:6060`) prevent these endpoints from being exposed on the management API port.
- **Graceful shutdown**: the broker stops accepting new connections, drains in-flight messages, flushes the WAL, and closes the database — in that order.

## Key Types

```go
type App struct {
    Config    *config.Config
    DB        *db.Store
    Broker    *broker.Broker
    APIServer *api.Server
}
```

## Lifecycle

```
New(cfg) → App.Start() → {serveMetrics, servePProf, broker.Start(), api.Start()}
                              ↓
                       ← signal received ←
                              ↓
                       App.Stop() → broker.Stop() → api.Stop()
```

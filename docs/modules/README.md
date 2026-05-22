# Module Reference

OCTAR is organised into internal packages under `internal/`. Each module has a clear responsibility and communicates with others through well-defined interfaces.

| Module | Package | Description |
|--------|---------|-------------|
| [App](app.md) | `internal/app` | Application bootstrap, wiring, and lifecycle |
| [Protocol](protocol.md) | `internal/protocol` | Binary TCP wire protocol (frames + codec) |
| [Server](server.md) | `internal/server` | TCP listener, connection lifecycle, credit-based flow control |
| [Queue](queue.md) | `internal/queue` | In-memory queue engine with DRR dispatch |
| [Scheduler](scheduler.md) | `internal/scheduler` | Event-driven dispatch scheduler + timing wheel |
| [Storage](storage.md) | `internal/storage` | Write-Ahead Log (WAL), snapshots, recovery |
| [Broker](broker.md) | `internal/broker` | Core broker orchestration |
| [DB](db.md) | `internal/db` | SQLite metadata store |
| [Auth](auth.md) | `internal/auth` | Authentication service, providers, RBAC, audit |
| [API](api.md) | `internal/api/v1` | HTTP management API (Huma v2 / OpenAPI) |
| [Config](config.md) | `internal/config` | Configuration loading (Viper) |
| [CLI](cli.md) | `internal/cli` | CLI commands (Cobra) |
| [Metrics](metrics.md) | `internal/metrics` | Prometheus instrumentation |
| [Logger](logger.md) | `internal/logger` | Structured JSON logging |
| [XTime](xtime.md) | `internal/xtime` | High-performance coarse clock |

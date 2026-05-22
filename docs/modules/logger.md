# Logger — Structured Logging

**Package**: `internal/logger`

Structured JSON logging to stderr using Go's standard `log/slog` package.

## Technologies

| Technology | Purpose |
|------------|---------|
| Go `log/slog` | Structured logging (Go 1.21+) |
| `internal/config` | Log level configuration |

## Design Decisions

### JSON Output

Logs are structured JSON written to stderr. This is the standard format for production observability — parsable by every log aggregator (Loki, Datadog, ELK, CloudWatch).

### Level Parsing

Configurable levels: `debug`, `info`, `warn`, `error`. The level is parsed from the config string and mapped to `slog.Level` values.

### Component Tags

Every log line includes a `component` key identifying the source (e.g. `server`, `broker`, `db`, `auth`). This makes filtering in log aggregators straightforward:

```json
{"time":"2026-05-22T12:48:38Z","level":"INFO","msg":"max connections reached, rejecting","component":"server","active":1,"max":1}
```

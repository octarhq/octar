# Config — Configuration System

**Package**: `internal/config`

Centralised configuration loading from YAML files, environment variables, and default values.

## Technologies

| Technology | Purpose |
|------------|---------|
| `github.com/spf13/viper` | Configuration library — multi-source merging |
| `github.com/spf13/cobra` | CLI command framework (config is used by CLI and broker) |

## Configuration Sources (Priority Order)

1. **Environment variables** with `OCTAR_` prefix (highest)
2. **`.env` file** in the working directory
3. **YAML config file** at `configs/config.yaml` (optional — see [config.example.yaml](../config.example.yaml))
4. **Default values** baked into the binary

## Why Viper?

- **Multiple sources**: env vars, files, defaults — merged automatically
- **Key mapping**: `config.Server.Port` maps to `server.port` in YAML and `OCTAR_SERVER_PORT` in env
- **Live reload** (optional): watches config file for changes
- **Widely adopted**: battle-tested in the Go ecosystem

## Configuration Structure

```go
type Config struct {
    Log      LogConfig
    Server   ServerConfig
    API      APIConfig
    Metrics  MetricsConfig
    PProf    PProfConfig
    Auth     AuthConfig
    Storage  StorageConfig
    Defaults DefaultsConfig
}
```

See [config.example.yaml](../config.example.yaml) for the complete configuration reference with documentation for every field.

## Validation

On startup, the config is validated:

- Rejects default JWT HMAC secret in production mode
- Rejects default admin password in production mode
- Validates port ranges (1–65535)
- Validates duration strings are parseable
- Ensures storage paths are writable

## Security

- **JWT secret**: must be at least 32 characters for HMAC; config validation rejects shorter values
- **Admin password**: the default `admin` password is rejected in production — must be explicitly overridden
- **Sensitive values**: never logged or exposed in error messages

# CLI — Command-Line Interface

**Package**: `internal/cli`

The `octar` CLI provides a human-friendly interface for managing the broker via its HTTP API.

## Technologies

| Technology | Purpose |
|------------|---------|
| `github.com/spf13/cobra` | CLI framework — commands, flags, help, autocompletion |
| `github.com/spf13/viper` | Client-side config (stored in `~/.octar/config.yaml`) |
| `go.yaml.in/yaml/v3` | YAML output formatting |

## Commands

### Authentication

```bash
octar login [--username admin] [--password secret]
```

Authenticates and saves the JWT token to `~/.octar/config.yaml`. All subsequent commands use this token.

### Server Management

```bash
octar server         # Start the broker daemon (runs octard)
octar health         # Check broker health status
octar metrics        # Show scheduler metrics
octar permissions    # List available permissions
```

### Namespace Management

```bash
octar namespace list
octar namespace create <name>
octar namespace get <name>
octar namespace delete <name>
```

### Queue Management

```bash
octar queue list
octar queue create <namespace> <name>
octar queue get <namespace> <name>
octar queue delete <namespace> <name>
octar queue stats <namespace> <name>
```

### Consumer Group Management

```bash
octar group list <namespace> <queue>
octar group get <namespace> <queue> <key>
octar group set <namespace> <queue> <key> [flags]
octar group delete <namespace> <queue> <key>
```

Flags for `group set`:
- `--parallelism N` (default 1)
- `--quantum N` (default 1)
- `--lease-timeout duration` (default 30s)
- `--backoff fixed|linear|exponential`
- `--max-attempts N`
- `--initial-delay duration`
- `--max-delay duration`
- `--ratelimit-max N` — enable rate limiting
- `--ratelimit-window duration`

### User Management

```bash
octar user list
octar user create <username> [--password secret] [--role admin]
octar user get <username>
octar user update <username> [--password new] [--role admin]
octar user delete <username>
```

### API Key Management

```bash
octar api-key create <description> [--namespace main]
octar api-key list
octar api-key delete <id>
```

## Output Formats

All list commands support `--json` for machine-readable output:

```bash
octar queue list --json
```

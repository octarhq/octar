# Metrics — Prometheus Instrumentation

**Package**: `internal/metrics`

Prometheus metrics for monitoring broker health, throughput, and resource usage.

## Technologies

| Technology | Purpose |
|------------|---------|
| `github.com/prometheus/client_golang` | Prometheus client library |

## Design Decisions

### Custom Registry

OCTAR uses its own Prometheus registry (not the global default). This prevents conflicts with other libraries' metrics and allows a clean `/metrics` endpoint that only reports OCTAR's metrics.

### Isolated Endpoint

Metrics are served on a dedicated port (default `2112`) with its own HTTP server. This keeps metrics accessible independently of the management API — important for monitoring systems that scrape without authentication.

## Metrics

### WAL / Storage

| Metric | Type | Description |
|--------|------|-------------|
| `octar_wal_written_bytes_total` | Counter | Total bytes written to WAL segments |
| `octar_wal_records_total` | Counter | Total WAL records written |
| `octar_wal_segments` | Gauge | Current number of WAL segment files |
| `octar_wal_failed` | Gauge | 1 if the WAL is in permanent failure state |
| `octar_snapshot_duration_seconds` | Histogram | Time to write a snapshot |

### Queue / Messaging

| Metric | Type | Description |
|--------|------|-------------|
| `octar_queue_messages_pending` | Gauge | Messages pending dispatch (per queue) |
| `octar_queue_messages_processing` | Gauge | Messages currently in-flight (per queue) |
| `octar_messages_published_total` | Counter | Total messages published |
| `octar_messages_acked_total` | Counter | Total messages acknowledged |
| `octar_messages_nacked_total` | Counter | Total messages negatively acknowledged |
| `octar_messages_expired_total` | Counter | Total messages expired (lease timeout) |
| `octar_messages_dlq_total` | Counter | Total messages routed to DLQ |

### Scheduler

| Metric | Type | Description |
|--------|------|-------------|
| `octar_scheduler_activation_latency_ms` | Summary | Time from activate to worker pick-up |
| `octar_scheduler_activations_total` | Counter | Total group activations |
| `octar_scheduler_worker_utilization` | Gauge | Fraction of workers currently busy |
| `octar_scheduler_queue_depth` | Gauge | Current activation channel depth |

### Connections

| Metric | Type | Description |
|--------|------|-------------|
| `octar_connections_active` | Gauge | Currently active TCP connections |
| `octar_connections_total` | Counter | Total connections (including rejected) |
| `octar_connections_rejected_total` | Counter | Connections rejected (rate limit or max) |

### Auth

| Metric | Type | Description |
|--------|------|-------------|
| `octar_auth_attempts_total` | Counter | Auth attempts (tagged by provider, success/failure) |
| `octar_auth_active_sessions` | Gauge | Currently active sessions |

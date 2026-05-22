# OCTAR — Getting Started

## Quick Start (Docker)

```bash
# Zero configuration — no env vars needed
docker run -d \
  --name octar \
  -p 7000:7000 \
  -p 8080:8080 \
  -p 2112:2112 \
  -v octar-data:/data \
  ghcr.io/83codes/octar:latest
```

> On first run, OCTAR auto-generates RSA signing keys and a random admin password.
> Run `docker exec octar cat /data/admin_credentials.txt` to see the credentials.

## Quick Start (Binary)

```bash
# Download and extract
tar xzf octar-linux-amd64.tar.gz
cd octar

# Start the broker — no config file needed
./octard

# Check the auto-generated admin credentials
cat data/admin_credentials.txt

# Use the CLI
./octar login --username admin
./octar health
```

## Configuration

OCTAR runs with **zero configuration** out of the box. When you want to customise,
the `.env` file is the simplest method:

```bash
# Create a .env file in the working directory
echo "OCTAR_SERVER_PORT=9000" >> .env
echo "OCTAR_API_PORT=8081" >> .env

# Start the broker — it reads .env automatically
./octard
```

Copy [.env.example](.env.example) for a full reference of all available variables.

### Configuration priority

1. **`.env` file** — simplest for local overrides
2. **Environment variables** — `OCTAR_*` prefix, override .env
3. **YAML file** — `configs/config.yaml` (see [config.example.yaml](config.example.yaml))
4. **Default values** — baked into the binary

### Production-ready .env example

```bash
# .env
OCTAR_LOG_LEVEL=info
OCTAR_SERVER_PORT=7000
OCTAR_API_PORT=8080
OCTAR_AUTH_DEFAULT_ADMIN_USERNAME=admin
OCTAR_AUTH_DEFAULT_ADMIN_PASSWORD=your-secure-password
OCTAR_STORAGE_DATA_DIR=/data
OCTAR_METRICS_ENABLED=true
OCTAR_METRICS_PORT=2112
```

## CLI Usage

The `octar` CLI connects to the broker's HTTP API. You must authenticate first.

### Authentication

```bash
octar login --username admin --password secure-password
# Token saved to ~/.octar/config.yaml
```

### Namespaces

```bash
octar namespace list
octar namespace create my-app
octar namespace get my-app
octar namespace delete my-app
```

### Queues

```bash
octar queue list
octar queue create my-app my-queue
octar queue get my-app my-queue
octar queue delete my-app my-queue
octar queue stats my-app my-queue
```

### Consumer Groups

```bash
octar group list my-app my-queue
octar group get my-app my-queue my-group
octar group set my-app my-queue my-group --parallelism 5 --backoff exponential
octar group delete my-app my-queue my-group
```

### Users

```bash
octar user list
octar user create alice --password secret
octar user get alice
octar user update alice --role admin
octar user delete alice
```

### API Keys

```bash
octar api-key create my-service --namespace my-app
octar api-key list
octar api-key delete key-id-here
```

### Health & Monitoring

```bash
octar health
octar metrics
octar permissions
```

## Connecting with Clients

OCTAR uses a custom binary TCP protocol. Here is the connection flow:

### 1. TCP Connect

Open a TCP connection to the broker port (default `7000`).

### 2. Authenticate

Send a `CONNECT` frame with one of:
- **Username + password**
- **API key**
- **JWT token** (issued by the `/auth/login` API)

### 3. Publish Messages

Send a `PUBLISH` frame. The broker responds with `PUBLISH_OK` containing the message ID and WAL offset.

### 4. Subscribe to Messages

Send a `SUBSCRIBE` frame. The broker will deliver `MESSAGE` frames on this connection.

### 5. Acknowledge Messages

For every `MESSAGE` received, send either:
- `ACK` — processing succeeded
- `NACK` — processing failed (will be retried with backoff or sent to DLQ)

### Wire Protocol Reference

| Frame | Code | Direction | Description |
|-------|------|-----------|-------------|
| `CONNECT` | 0x01 | Client → Broker | Authentication |
| `CONNECT_OK` | 0x02 | Broker → Client | Auth accepted |
| `CONNECT_ERR` | 0x03 | Broker → Client | Auth rejected |
| `PUBLISH` | 0x10 | Client → Broker | Enqueue a message |
| `PUBLISH_OK` | 0x11 | Broker → Client | Message accepted (returns ID + offset) |
| `SUBSCRIBE` | 0x20 | Client → Broker | Register as consumer |
| `MESSAGE` | 0x21 | Broker → Client | Deliver a message |
| `ACK` | 0x30 | Client → Broker | Processing succeeded |
| `NACK` | 0x31 | Client → Broker | Processing failed |
| `ERROR` | 0xF0 | Broker → Client | Error response |
| `BACKPRESSURE` | 0xF1 | Broker → Client | Slow down signal |
| `HEARTBEAT` | 0xFF | Bidirectional | Keepalive |

### Frame Format

All frames share a 5-byte header:

```
┌────────────────┬───────────────────────┬────────────────────────┐
│  type (1 byte) │  payload length (4 B) │  payload (N bytes)     |
├────────────────┼───────────────────────┴────────────────────────┤
│     0x10       │  0x0000002A           │  (42 bytes)            │
└────────────────┴────────────────────────────────────────────────┘
```

Payload encoding:
- **Strings**: `[uint16 length][UTF-8 bytes]`
- **Byte slices**: `[uint32 length][bytes]`
- **Integers**: big-endian (uint32, uint64, int32)

### Example Clients

See the [examples/](../examples) directory for ready-to-run publisher and subscriber implementations in Go:

- `examples/publish/` — connect, publish a message, print confirmation
- `examples/subscribe/` — connect, subscribe to a group, print received messages
- `examples/stress/` — synthetic load test

## HTTP API

The management API is documented via OpenAPI 3.1. Start the broker and visit:

```
http://localhost:8080/docs
```

This serves Stoplight Elements UI with the full interactive API reference.

### Key Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/auth/login` | Authenticate and receive JWT tokens |
| `POST` | `/auth/refresh` | Refresh an access token |
| `GET` | `/health` | Broker health check |
| `GET` | `/metrics` | Scheduler metrics |
| `GET` | `/permissions` | Permission catalog |
| `GET` | `/namespaces` | List namespaces |
| `POST` | `/namespaces` | Create namespace |
| `GET` | `/users` | List users |
| `POST` | `/users` | Create user |
| `GET` | `/queues` | List queues |
| `POST` | `/queues/{ns}/{name}` | Create queue |
| `GET` | `/queues/{ns}/{name}/groups` | List groups for a queue |
| `POST` | `/queues/{ns}/{name}/snapshot` | Trigger snapshot |

## Production Deployment

### Docker Compose

```yaml
version: "3.8"
services:
  octar:
    image: ghcr.io/83codes/octar:latest
    ports:
      - "7000:7000"
      - "8080:8080"
      - "2112:2112"
    environment:
      OCTAR_AUTH_DEFAULT_ADMIN_PASSWORD: ${OCTAR_ADMIN_PASSWORD}
      OCTAR_STORAGE_DATA_DIR: /data
    volumes:
      - octar-data:/data
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/health"]
      interval: 15s
      timeout: 5s
      retries: 3

volumes:
  octar-data:
```

### Resource Requirements

| Scale | CPU | Memory | Disk |
|-------|-----|--------|------|
| Development | 1 core | 256 MB | 1 GB |
| Production (low) | 2 cores | 512 MB | 10 GB |
| Production (high) | 8+ cores | 4+ GB | SSD |

### TLS

Both the TCP data plane and HTTP API support TLS. Configure via environment variables:

```bash
# TCP
OCTAR_SERVER_TLS_ENABLED=true
OCTAR_SERVER_TLS_CERT_FILE=/certs/server.crt
OCTAR_SERVER_TLS_KEY_FILE=/certs/server.key

# HTTP API
OCTAR_API_TLS_ENABLED=true
OCTAR_API_TLS_CERT_FILE=/certs/api.crt
OCTAR_API_TLS_KEY_FILE=/certs/api.key
```

### Monitoring

Prometheus metrics are available on a dedicated port (default `2112`):

```
http://localhost:2112/metrics
```

Key metrics:
- `octar_wal_written_bytes_total` — WAL throughput
- `octar_queue_messages_pending` — pending message count per queue
- `octar_scheduler_activation_latency_ms` — dispatch latency
- `octar_connections_active` — active TCP connections
- `octar_auth_attempts_total` — authentication rate

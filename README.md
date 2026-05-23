# OCTAR — High-Performance Message Broker

OCTAR is a fast, durable, and operationally simple message broker for modern distributed systems. It features a custom binary TCP protocol, event-driven Deficit Round Robin dispatch, per-queue write-ahead logging, and pluggable authentication.

**[Read the introduction →](docs/introduction.md)**  
**[Get started →](docs/instructions.md)**  
**[Module reference →](docs/modules/README.md)**

## Quick Start

```bash
# Zero configuration — just run it
docker run -d --name octar -p 7000:7000 -p 8080:8080 ghcr.io/83codes/octar:latest
```

> On first run, OCTAR auto-generates RSA signing keys and a random admin password.
> Check `data/admin_credentials.txt` inside the container for the credentials.

## Configuration

OCTAR needs **zero configuration** to run. When you do need to customise:

1. **`.env` file** — copy [docs/.env.example](docs/.env.example) to `.env` and uncomment what you need
2. **Environment variables** — any `OCTAR_*` var overrides everything
3. **YAML file** — copy [docs/config.example.yaml](docs/config.example.yaml) to `configs/config.yaml`

```bash
# .env is the simplest way to configure
echo "OCTAR_SERVER_PORT=9000" >> .env
echo "OCTAR_AUTH_DEFAULT_ADMIN_PASSWORD=my-secret" >> .env
./octard
```

## Features

- **Custom binary TCP protocol** — lightweight, 5-byte header, 12 frame types
- **Event-driven dispatch** — no polling, O(1) timing wheel for retries
- **Deficit Round Robin** — fair, weighted scheduling across consumer groups
- **Per-queue WAL** — durable writes with CRC32-C, snapshots, and fast recovery
- **Pluggable auth** — password (bcrypt), API key, JWT, OAuth2, mTLS
- **Lock-free concurrency** — lock striping, CAS, copy-on-write subscriber lists
- **Prometheus metrics** — isolated endpoint, comprehensive instrumentation
- **OpenAPI 3.1 management API** — interactive docs at `/docs`
- **Zero dependencies** — single static binary, no JVM, no external stores

## Architecture

```
┌──────────┐  ┌──────────────┐  ┌───────────┐  ┌────────────┐
│  TCP     │  │  Scheduler   │  │  Queue    │  │  Storage   │
│  Server  │─▶│  (Event-     │─▶│  Engine   │─▶│  (WAL +    │
│  :7000   │  │   Driven)    │  │  (DRR)    │  │  Snapshot) │
└──────────┘  └──────────────┘  └───────────┘  └────────────┘
     │                                                │
     ▼                                                ▼
┌──────────┐                                  ┌────────────┐
│  Auth    │                                  │  SQLite    │
│  Service │                                  │  (Metadata)│
└──────────┘                                  └────────────┘
     │
     ▼
┌──────────┐
│  HTTP    │
│  API     │
│  :8080   │
└──────────┘
```

## Documentation

- [Introduction & Philosophy](docs/introduction.md)
- [Getting Started & Usage](docs/instructions.md)
- [Module Reference](docs/modules/README.md)
  - [Protocol](docs/modules/protocol.md) — binary wire format
  - [Server](docs/modules/server.md) — TCP connection management
  - [Queue](docs/modules/queue.md) — in-memory engine and DRR
  - [Scheduler](docs/modules/scheduler.md) — event-driven dispatch + timing wheel
  - [Storage](docs/modules/storage.md) — WAL, snapshots, recovery
  - [Broker](docs/modules/broker.md) — core orchestration
  - [Auth](docs/modules/auth.md) — authentication, RBAC, audit
  - [DB](docs/modules/db.md) — SQLite metadata store
  - [API](docs/modules/api.md) — HTTP management API
  - [Config](docs/modules/config.md) — configuration system
  - [CLI](docs/modules/cli.md) — command-line tool
  - [Metrics](docs/modules/metrics.md) — Prometheus instrumentation
  - [XTime](docs/modules/xtime.md) — high-performance clock

## Building from Source

```bash
git clone https://github.com/octarhq/octar.git
cd octar
go build -o octard ./cmd/broker/
go build -o octar  ./cmd/octar/
```

## Testing

```bash
# All tests
go test ./...

# With coverage
go test -coverprofile=coverage.out ./...

# Specific packages
go test ./internal/queue/...
go test ./internal/server/...
go test ./test/e2e/...
```

## License

Open source.

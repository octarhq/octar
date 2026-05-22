# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Cache deps (layer invalidates only on go.mod/go.sum changes).
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build static binaries.
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /octard ./cmd/broker/ && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /octar  ./cmd/octar/

# ── Stage 2: minimal runtime ──────────────────────────────────────────────────
FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 octar

COPY --from=builder /octard /usr/local/bin/octard
COPY --from=builder /octar  /usr/local/bin/octar

# Default config via env vars; a configs/ volume can be mounted for file-based setup.
ENV OCTAR_LOG_LEVEL=info \
    OCTAR_SERVER_HOST=0.0.0.0 \
    OCTAR_SERVER_PORT=7000 \
    OCTAR_API_HOST=0.0.0.0 \
    OCTAR_API_PORT=8080 \
    OCTAR_METRICS_ENABLED=true \
    OCTAR_METRICS_PORT=2112 \
    OCTAR_STORAGE_DATA_DIR=/data

EXPOSE 7000 8080 2112

VOLUME /data

USER octar

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["octard"]

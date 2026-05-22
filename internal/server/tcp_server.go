package server

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/83codes/octar/internal/auth"
	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
	"github.com/83codes/octar/internal/metrics"
)

type tokenBucket struct {
	capacity float64
	tokens   float64
	rate     float64
	last     time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{
		capacity: float64(burst),
		tokens:   float64(burst),
		rate:     rate,
		last:     time.Now(),
	}
}

func (tb *tokenBucket) allow() bool {
	now := time.Now()
	elapsed := now.Sub(tb.last).Seconds()
	tb.last = now
	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

type TCPServer struct {
	Host           string
	Port           int
	MaxConnections int32
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	TLSConfig      *tls.Config
	ConnRateLimit  int
	db             *db.Store
	authSvc        *auth.Service
	handler        ConnHandler
	inflight       config.InflightConfig
	listener       net.Listener
	logger         *slog.Logger
	activeConns    atomic.Int32
}

func NewTCPServer(host string, port int, store *db.Store, authSvc *auth.Service, handler ConnHandler, inflight config.InflightConfig, maxConns int32, readTimeout, writeTimeout time.Duration, tlsCfg *tls.Config, connRateLimit int) *TCPServer {
	return &TCPServer{
		Host:           host,
		Port:           port,
		MaxConnections: maxConns,
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
		TLSConfig:      tlsCfg,
		ConnRateLimit:  connRateLimit,
		db:             store,
		authSvc:        authSvc,
		handler:        handler,
		inflight:       inflight,
		logger:         slog.Default().With("component", "server"),
	}
}

func (s *TCPServer) Start() error {
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)

	var err error
	if s.TLSConfig != nil {
		s.listener, err = tls.Listen("tcp", addr, s.TLSConfig)
		s.logger.Info("tcp server listening with TLS", "addr", addr)
	} else {
		s.listener, err = net.Listen("tcp", addr)
		s.logger.Info("tcp server listening (plaintext)", "addr", addr)
	}
	if err != nil {
		return fmt.Errorf("tcp listen: %w", err)
	}

	go s.acceptLoop()
	return nil
}

func (s *TCPServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *TCPServer) ActiveConns() int32 {
	return s.activeConns.Load()
}

func (s *TCPServer) acceptLoop() {
	const maxBackoff = 5 * time.Second
	backoff := time.Millisecond

	var connLimiter *tokenBucket
	if s.ConnRateLimit > 0 {
		connLimiter = newTokenBucket(float64(s.ConnRateLimit), s.ConnRateLimit)
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				s.logger.Warn("tcp accept transient error, retrying",
					"error", err, "backoff", backoff)
				time.Sleep(backoff)
				if backoff < maxBackoff {
					backoff += backoff
				}
				continue
			}
			return
		}
		backoff = time.Millisecond

		if connLimiter != nil && !connLimiter.allow() {
			s.logger.Warn("connection rate limit exceeded, rejecting",
				"rate", s.ConnRateLimit)
			metrics.ConnRateLimitRejected.Inc()
			conn.Close()
			continue
		}

		if s.MaxConnections > 0 {
			cur := s.activeConns.Load()
			if cur >= s.MaxConnections {
				s.logger.Warn("max connections reached, rejecting",
					"active", cur, "max", s.MaxConnections)
				conn.Close()
				continue
			}
			s.activeConns.Add(1)
		}

		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(raw net.Conn) {
	if s.MaxConnections > 0 {
		defer s.activeConns.Add(-1)
	}

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("connection handler panicked",
				"addr", raw.RemoteAddr(), "panic", r)
			raw.Close()
		}
	}()

	if tcp, ok := raw.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	conn := newConnection(raw, s.inflight, s.ReadTimeout, s.WriteTimeout)

	s.logger.Debug("client connected", "addr", conn.RemoteAddr())

	defer func() {
		s.logger.Debug("client disconnected", "addr", conn.RemoteAddr())
		conn.Close()
	}()

	if !conn.Authenticate(s.db, s.authSvc) {
		s.logger.Warn("auth failed", "addr", conn.RemoteAddr())
		return
	}

	s.logger.Debug("client authenticated",
		"addr", conn.RemoteAddr(),
		"user", conn.Session.Username,
		"namespace", conn.Session.Namespace,
	)

	if s.handler != nil {
		s.handler(conn)
	}
}

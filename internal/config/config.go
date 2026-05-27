// Package config exposes the broker's runtime configuration.
// All values have sensible defaults baked in. Configuration can be overridden
// via a config file (configs/config.yaml), environment variables (OCTAR_*),
// or a .env file. See docs/config.example.yaml for a full reference.
package config

import (
	"time"

	"github.com/spf13/viper"
)

// Config is the top-level configuration for the OCTAR broker.
type Config struct {
	Log      LogConfig      `mapstructure:"log"`
	Server   ServerConfig   `mapstructure:"server"`
	API      APIConfig      `mapstructure:"api"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
	PProf    PProfConfig    `mapstructure:"pprof"`
	Auth     AuthConfig     `mapstructure:"auth"`
	Storage  StorageConfig  `mapstructure:"storage"`
	Defaults DefaultsConfig `mapstructure:"defaults"`
}

// LogConfig controls application logging.
type LogConfig struct {
	Level string `mapstructure:"level"` // debug, info, warn, error
}

// InflightConfig controls per-connection in-flight message limits.
// MaxInflight caps how many messages one connection can have outstanding.
// GlobalMax is the broker-wide hard cap across all connections (0 = unlimited).
type InflightConfig struct {
	MaxInflight int32 `mapstructure:"max_inflight"`
	GlobalMax   int64 `mapstructure:"global_max"`
}

// TLSConfig controls transport-layer security for both TCP and HTTP.
type TLSConfig struct {
	Enabled  bool   `mapstructure:"enabled" doc:"Enable TLS for TCP data plane and HTTP API"`
	CertFile string `mapstructure:"cert_file" doc:"Path to TLS certificate file (PEM)"`
	KeyFile  string `mapstructure:"key_file" doc:"Path to TLS private key file (PEM)"`
	CAFile   string `mapstructure:"ca_file" doc:"Optional path to CA cert for mTLS (TCP)"`
}

// ServerConfig controls the TCP data-plane listener.
type ServerConfig struct {
	Host           string         `mapstructure:"host"`
	Port           int            `mapstructure:"port"`
	MaxConnections int            `mapstructure:"max_connections"`
	ConnRateLimit  int            `mapstructure:"conn_rate_limit"`     // max new connections/s (0 = unlimited)
	GlobalMaxMsgs  int64          `mapstructure:"global_max_messages"` // max pending msgs system-wide (0 = unlimited)
	ReadTimeout    time.Duration  `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration  `mapstructure:"write_timeout"`
	TLS            TLSConfig      `mapstructure:"tls"`
	Inflight       InflightConfig `mapstructure:"inflight"`
}

// APIConfig controls the HTTP management API listener.
type APIConfig struct {
	Host string    `mapstructure:"host"`
	Port int       `mapstructure:"port"`
	TLS  TLSConfig `mapstructure:"tls"` // reuses same TLS config schema
}

// WALAppendTimeout is how long AppendSync waits before timing out.
const WALAppendTimeout = 30 * time.Second

// MetricsConfig controls Prometheus metrics endpoint.
type MetricsConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

// PProfConfig controls pprof profiling endpoint.
type PProfConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

// AuthConfig controls authentication and authorization.
type AuthConfig struct {
	Enabled bool `mapstructure:"enabled"`

	Session      SessionConfig      `mapstructure:"session"`
	Providers    ProvidersConfig    `mapstructure:"providers"`
	RateLimit    RateLimitConfig    `mapstructure:"rate_limit"`
	DefaultAdmin DefaultAdminConfig `mapstructure:"default_admin"`
}

type SessionConfig struct {
	Timeout         time.Duration `mapstructure:"timeout"`
	MaxSessions     int           `mapstructure:"max_sessions"`
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"`
}

// DefaultAdminConfig holds the default admin credentials to be created on first startup.
type DefaultAdminConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type ProvidersConfig struct {
	Password PasswordProviderConfig `mapstructure:"password"`
	APIKey   APIKeyProviderConfig   `mapstructure:"api_key"`
	JWT      JWTProviderConfig      `mapstructure:"jwt"`
	OAuth    OAuthConfig            `mapstructure:"oauth"`
	MTLS     MTLSConfig             `mapstructure:"mtls"`
}

type PasswordProviderConfig struct {
	Enabled    bool `mapstructure:"enabled"`
	Priority   int  `mapstructure:"priority"`
	BcryptCost int  `mapstructure:"bcrypt_cost"`
}

type APIKeyProviderConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Priority   int    `mapstructure:"priority"`
	Prefix     string `mapstructure:"prefix"`
	HashSecret string `mapstructure:"hash_secret"`
}

type JWTProviderConfig struct {
	Enabled         bool   `mapstructure:"enabled"`
	KeyType         string `mapstructure:"key_type"` // RSA, EC, EdDSA, HMAC
	KeyID           string `mapstructure:"key_id"`
	PrivateKey      string `mapstructure:"private_key"`
	PublicKey       string `mapstructure:"public_key"`
	HMACSecret      string `mapstructure:"hmac_secret"`
	AccessTokenTTL  int    `mapstructure:"access_token_ttl"`  // seconds
	RefreshTokenTTL int    `mapstructure:"refresh_token_ttl"` // seconds
}

type OAuthConfig struct {
	Enabled      bool              `mapstructure:"enabled"`
	Provider     string            `mapstructure:"provider"` // google, github, microsoft, oidc
	ClientID     string            `mapstructure:"client_id"`
	ClientSecret string            `mapstructure:"client_secret"`
	RedirectURL  string            `mapstructure:"redirect_url"`
	Scopes       []string          `mapstructure:"scopes"`
	TeamsMap     map[string]string `mapstructure:"teams_map"` // team -> namespace
}

type MTLSConfig struct {
	Enabled      bool     `mapstructure:"enabled"`
	ClientCACert string   `mapstructure:"client_ca_cert"`
	AllowedCNs   []string `mapstructure:"allowed_cns"`
	AllowedSANs  []string `mapstructure:"allowed_sans"`
}

type RateLimitConfig struct {
	LoginAttempts LoginRateLimitConfig `mapstructure:"login_attempts"`
	AuthAbuse     AuthRateLimitConfig  `mapstructure:"auth_abuse"`
}

type LoginRateLimitConfig struct {
	Max     int           `mapstructure:"max"`
	Window  time.Duration `mapstructure:"window"`
	Lockout time.Duration `mapstructure:"lockout"`
}

type AuthRateLimitConfig struct {
	Max    int           `mapstructure:"max"`
	Window time.Duration `mapstructure:"window"`
}

// WALConfig controls write-ahead log batching, segment rotation, and fsync.
type WALConfig struct {
	// FlushInterval is the maximum time buffered events wait before being flushed.
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	// FlushMaxMessages triggers an immediate flush when the buffer reaches this size.
	FlushMaxMessages int `mapstructure:"flush_max_messages"`
	// SegmentMaxBytes rotates the active segment when it reaches this size.
	SegmentMaxBytes int64 `mapstructure:"segment_max_bytes"`
	// Durable calls fsync after each flush, guaranteeing messages survive power loss.
	// Disable per-queue for higher throughput when losing messages on power loss is acceptable.
	// Default: true. Override per-queue via POST /queues with {"durable": false}.
	Durable bool `mapstructure:"durable"`
	// SnapshotInterval writes a snapshot for fast recovery every N seconds.
	SnapshotInterval time.Duration `mapstructure:"snapshot_interval"`
}

// SnapshotConfig controls periodic index snapshots to accelerate recovery.
type SnapshotConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

// StorageConfig groups all persistence settings.
type StorageConfig struct {
	DataDir  string         `mapstructure:"data_dir"`
	WAL      WALConfig      `mapstructure:"wal"`
	Snapshot SnapshotConfig `mapstructure:"snapshot"`
}

// DefaultRetryConfig sets the retry policy applied to groups that don't specify one.
type DefaultRetryConfig struct {
	MaxAttempts  int           `mapstructure:"max_attempts"`
	Backoff      string        `mapstructure:"backoff"` // fixed | linear | exponential
	InitialDelay time.Duration `mapstructure:"initial_delay"`
	MaxDelay     time.Duration `mapstructure:"max_delay"`
}

// DefaultGroupConfig is the fallback configuration for groups with no explicit config.
type DefaultGroupConfig struct {
	Parallelism  int                `mapstructure:"parallelism"`
	LeaseTimeout time.Duration      `mapstructure:"lease_timeout"`
	Retry        DefaultRetryConfig `mapstructure:"retry"`
	MaxPending   int                `mapstructure:"max_pending"` // max messages per group before backpressure (0 = unlimited)
}

// DefaultsConfig groups all per-entity default settings.
type DefaultsConfig struct {
	Group DefaultGroupConfig `mapstructure:"group"`
}

// Load returns a Config with all defaults applied. If a configs/config.yaml
// file exists it is merged on top of the defaults. Environment variables with
// the OCTAR_ prefix override any file values (e.g. OCTAR_SERVER_PORT=9000).
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")

	viper.SetEnvPrefix("OCTAR")
	viper.AutomaticEnv()

	viper.SetDefault("storage.data_dir", "./data")

	// Server defaults
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 7000)
	viper.SetDefault("server.inflight.max_inflight", 256)
	viper.SetDefault("server.inflight.global_max", 0)
	viper.SetDefault("server.max_connections", 10000)
	viper.SetDefault("server.conn_rate_limit", 1000)
	viper.SetDefault("server.read_timeout", 30*time.Second)
	viper.SetDefault("server.write_timeout", 10*time.Second)
	viper.SetDefault("log.level", "info")

	viper.SetDefault("api.host", "0.0.0.0")
	viper.SetDefault("api.port", 8080)

	viper.SetDefault("storage.wal.flush_interval", 25*time.Millisecond)
	viper.SetDefault("storage.wal.flush_max_messages", 1000)
	viper.SetDefault("storage.wal.segment_max_bytes", 512<<20)
	viper.SetDefault("storage.wal.durable", true)
	viper.SetDefault("storage.wal.snapshot_interval", 60*time.Second)
	viper.SetDefault("storage.snapshot.interval", 5*time.Minute)

	viper.SetDefault("defaults.group.parallelism", 1)
	viper.SetDefault("defaults.group.lease_timeout", 5*time.Minute)
	viper.SetDefault("defaults.group.max_pending", 10000)
	viper.SetDefault("defaults.group.retry.max_attempts", 3)
	viper.SetDefault("defaults.group.retry.backoff", "exponential")
	viper.SetDefault("defaults.group.retry.initial_delay", "1s")
	viper.SetDefault("defaults.group.retry.max_delay", "5m")

	viper.SetDefault("metrics.enabled", true)
	viper.SetDefault("metrics.port", 2112)
	viper.SetDefault("pprof.enabled", true)
	viper.SetDefault("pprof.port", 6060)
	viper.SetDefault("auth.enabled", true)
	viper.SetDefault("auth.session.timeout", 24*time.Hour)
	viper.SetDefault("auth.session.max_sessions", 100000)
	viper.SetDefault("auth.session.cleanup_interval", 5*time.Minute)
	viper.SetDefault("auth.providers.password.enabled", true)
	viper.SetDefault("auth.providers.password.priority", 10)
	viper.SetDefault("auth.providers.password.bcrypt_cost", 12)
	viper.SetDefault("auth.providers.api_key.enabled", true)
	viper.SetDefault("auth.providers.api_key.priority", 20)
	viper.SetDefault("auth.providers.api_key.prefix", "flw_live_")
	viper.SetDefault("auth.providers.jwt.enabled", true)
	viper.SetDefault("auth.providers.jwt.key_type", "RSA")
	viper.SetDefault("auth.providers.jwt.access_token_ttl", 900)
	viper.SetDefault("auth.providers.jwt.refresh_token_ttl", 604800)
	viper.SetDefault("auth.rate_limit.login_attempts.max", 5)
	viper.SetDefault("auth.rate_limit.login_attempts.window", 1*time.Minute)
	viper.SetDefault("auth.rate_limit.login_attempts.lockout", 15*time.Minute)
	viper.SetDefault("auth.default_admin.username", "admin")
	viper.SetDefault("auth.default_admin.password", "")

	// Config file is optional — all values have sensible defaults.
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if cfg.Storage.WAL.FlushInterval <= 0 {
		cfg.Storage.WAL.FlushInterval = 10 * time.Millisecond
	}
	if cfg.Storage.WAL.FlushMaxMessages <= 0 {
		cfg.Storage.WAL.FlushMaxMessages = 1000
	}
	if cfg.Storage.WAL.SegmentMaxBytes <= 0 {
		cfg.Storage.WAL.SegmentMaxBytes = 512 << 20
	}
	if cfg.Storage.WAL.SnapshotInterval <= 0 {
		cfg.Storage.WAL.SnapshotInterval = 60 * time.Second
	}
	if cfg.Server.MaxConnections <= 0 {
		cfg.Server.MaxConnections = 10000
	}
	if cfg.Server.ConnRateLimit <= 0 {
		cfg.Server.ConnRateLimit = 1000
	}
	if cfg.Server.ReadTimeout <= 0 {
		cfg.Server.ReadTimeout = 30 * time.Second
	}
	if cfg.Server.WriteTimeout <= 0 {
		cfg.Server.WriteTimeout = 10 * time.Second
	}

	return &cfg, nil
}

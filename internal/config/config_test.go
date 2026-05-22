package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// loadFromYAML creates a temp directory with configs/config.yaml containing the
// given YAML content, changes the working directory, resets viper, and runs
// Load(). The working directory is restored via t.Cleanup.
func loadFromYAML(t *testing.T, yamlContent string) (*Config, error) {
	t.Helper()
	viper.Reset()
	dir := t.TempDir()
	configsDir := filepath.Join(dir, "configs")
	if err := os.MkdirAll(configsDir, 0755); err != nil {
		t.Fatal(err)
	}
	yamlPath := filepath.Join(configsDir, "config.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}
	prevDir, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(prevDir) })
	return Load()
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// validConfig returns a minimal config snippet that passes B6 validation.
// Individual tests can override specific fields on top of this.
const validConfig = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`

// ── Default values ──────────────────────────────────────────────────────────

func TestDefaultsApplied(t *testing.T) {
	t.Run("defaults_when_config_omits_them", func(t *testing.T) {
		cfg, err := loadFromYAML(t, validConfig)
		if err != nil {
			t.Fatalf("Load() returned error: %v", err)
		}
		if cfg == nil {
			t.Fatal("Load() returned nil config")
		}

		if cfg.Log.Level != "info" {
			t.Errorf("log.level = %q, want %q", cfg.Log.Level, "info")
		}
		if cfg.Server.MaxConnections != 10000 {
			t.Errorf("server.max_connections = %d, want %d", cfg.Server.MaxConnections, 10000)
		}
		if cfg.Server.ReadTimeout != 30*time.Second {
			t.Errorf("server.read_timeout = %v, want %v", cfg.Server.ReadTimeout, 30*time.Second)
		}
		if cfg.Server.WriteTimeout != 10*time.Second {
			t.Errorf("server.write_timeout = %v, want %v", cfg.Server.WriteTimeout, 10*time.Second)
		}
		if cfg.Server.Inflight.MaxInflight != 256 {
			t.Errorf("server.inflight.max_inflight = %d, want %d", cfg.Server.Inflight.MaxInflight, 256)
		}
		if cfg.Server.Inflight.GlobalMax != 0 {
			t.Errorf("server.inflight.global_max = %d, want %d", cfg.Server.Inflight.GlobalMax, 0)
		}
		if cfg.Storage.WAL.FlushInterval != 10*time.Millisecond {
			t.Errorf("storage.wal.flush_interval = %v, want %v", cfg.Storage.WAL.FlushInterval, 10*time.Millisecond)
		}
		if cfg.Storage.WAL.FlushMaxMessages != 1000 {
			t.Errorf("storage.wal.flush_max_messages = %d, want %d", cfg.Storage.WAL.FlushMaxMessages, 1000)
		}
		if cfg.Storage.WAL.SegmentMaxBytes != 512<<20 {
			t.Errorf("storage.wal.segment_max_bytes = %d, want %d", cfg.Storage.WAL.SegmentMaxBytes, 512<<20)
		}
		if cfg.Storage.WAL.Sync != true {
			t.Errorf("storage.wal.sync = %v, want true", cfg.Storage.WAL.Sync)
		}
		if cfg.Storage.WAL.SnapshotInterval != 60*time.Second {
			t.Errorf("storage.wal.snapshot_interval = %v, want %v", cfg.Storage.WAL.SnapshotInterval, 60*time.Second)
		}
		if cfg.Defaults.Group.Parallelism != 1 {
			t.Errorf("defaults.group.parallelism = %d, want %d", cfg.Defaults.Group.Parallelism, 1)
		}
		if cfg.Defaults.Group.LeaseTimeout != 5*time.Minute {
			t.Errorf("defaults.group.lease_timeout = %v, want %v", cfg.Defaults.Group.LeaseTimeout, 5*time.Minute)
		}
		if cfg.Defaults.Group.MaxPending != 10000 {
			t.Errorf("defaults.group.max_pending = %d, want %d", cfg.Defaults.Group.MaxPending, 10000)
		}
		if cfg.Defaults.Group.Retry.MaxAttempts != 3 {
			t.Errorf("defaults.group.retry.max_attempts = %d, want %d", cfg.Defaults.Group.Retry.MaxAttempts, 3)
		}
		if cfg.Defaults.Group.Retry.Backoff != "exponential" {
			t.Errorf("defaults.group.retry.backoff = %q, want %q", cfg.Defaults.Group.Retry.Backoff, "exponential")
		}
		if cfg.Defaults.Group.Retry.InitialDelay != 1*time.Second {
			t.Errorf("defaults.group.retry.initial_delay = %v, want %v", cfg.Defaults.Group.Retry.InitialDelay, 1*time.Second)
		}
		if cfg.Defaults.Group.Retry.MaxDelay != 5*time.Minute {
			t.Errorf("defaults.group.retry.max_delay = %v, want %v", cfg.Defaults.Group.Retry.MaxDelay, 5*time.Minute)
		}
		if cfg.Metrics.Enabled != true {
			t.Errorf("metrics.enabled = %v, want true", cfg.Metrics.Enabled)
		}
		if cfg.Metrics.Port != 2112 {
			t.Errorf("metrics.port = %d, want %d", cfg.Metrics.Port, 2112)
		}
		if cfg.PProf.Enabled != true {
			t.Errorf("pprof.enabled = %v, want true", cfg.PProf.Enabled)
		}
		if cfg.PProf.Port != 6060 {
			t.Errorf("pprof.port = %d, want %d", cfg.PProf.Port, 6060)
		}
		if cfg.Auth.Enabled != true {
			t.Errorf("auth.enabled = %v, want true", cfg.Auth.Enabled)
		}
		if cfg.Auth.Session.Timeout != 24*time.Hour {
			t.Errorf("auth.session.timeout = %v, want %v", cfg.Auth.Session.Timeout, 24*time.Hour)
		}
		if cfg.Auth.Session.MaxSessions != 100000 {
			t.Errorf("auth.session.max_sessions = %d, want %d", cfg.Auth.Session.MaxSessions, 100000)
		}
		if cfg.Auth.Session.CleanupInterval != 5*time.Minute {
			t.Errorf("auth.session.cleanup_interval = %v, want %v", cfg.Auth.Session.CleanupInterval, 5*time.Minute)
		}
		if cfg.Auth.Providers.Password.Enabled != true {
			t.Errorf("auth.providers.password.enabled = %v, want true", cfg.Auth.Providers.Password.Enabled)
		}
		if cfg.Auth.Providers.Password.Priority != 10 {
			t.Errorf("auth.providers.password.priority = %d, want %d", cfg.Auth.Providers.Password.Priority, 10)
		}
		if cfg.Auth.Providers.Password.BcryptCost != 12 {
			t.Errorf("auth.providers.password.bcrypt_cost = %d, want %d", cfg.Auth.Providers.Password.BcryptCost, 12)
		}
		if cfg.Auth.Providers.JWT.KeyType != "RSA" {
			t.Errorf("auth.providers.jwt.key_type = %q, want %q", cfg.Auth.Providers.JWT.KeyType, "RSA")
		}
		if cfg.Auth.Providers.JWT.AccessTokenTTL != 900 {
			t.Errorf("auth.providers.jwt.access_token_ttl = %d, want %d", cfg.Auth.Providers.JWT.AccessTokenTTL, 900)
		}
		if cfg.Auth.Providers.JWT.RefreshTokenTTL != 604800 {
			t.Errorf("auth.providers.jwt.refresh_token_ttl = %d, want %d", cfg.Auth.Providers.JWT.RefreshTokenTTL, 604800)
		}
		if cfg.Auth.RateLimit.LoginAttempts.Max != 5 {
			t.Errorf("auth.rate_limit.login_attempts.max = %d, want %d", cfg.Auth.RateLimit.LoginAttempts.Max, 5)
		}
		if cfg.Auth.RateLimit.LoginAttempts.Window != 1*time.Minute {
			t.Errorf("auth.rate_limit.login_attempts.window = %v, want %v", cfg.Auth.RateLimit.LoginAttempts.Window, 1*time.Minute)
		}
		if cfg.Auth.RateLimit.LoginAttempts.Lockout != 15*time.Minute {
			t.Errorf("auth.rate_limit.login_attempts.lockout = %v, want %v", cfg.Auth.RateLimit.LoginAttempts.Lockout, 15*time.Minute)
		}
		if cfg.Auth.DefaultAdmin.Username != "admin" {
			t.Errorf("auth.default_admin.username = %q, want %q", cfg.Auth.DefaultAdmin.Username, "admin")
		}
	})

	t.Run("explicit_values_override_defaults", func(t *testing.T) {
		const custom = `
server:
  port: 7000
  max_connections: 500
  timeout: "15s"
log:
  level: "debug"
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`

		cfg, err := loadFromYAML(t, custom)
		if err != nil {
			t.Fatalf("Load() returned error: %v", err)
		}
		if cfg.Server.MaxConnections != 500 {
			t.Errorf("server.max_connections = %d, want %d", cfg.Server.MaxConnections, 500)
		}
		if cfg.Log.Level != "debug" {
			t.Errorf("log.level = %q, want %q", cfg.Log.Level, "debug")
		}
	})
}

// ── Validation B2: zero → default ──────────────────────────────────────────

func TestB2Validation(t *testing.T) {
	t.Run("defaults_applied_when_zero", func(t *testing.T) {
		const zeroConfig = `
server:
  port: 7000
  max_connections: 0
  read_timeout: "0s"
  write_timeout: "0s"
storage:
  wal:
    flush_interval: "0s"
    flush_max_messages: 0
    segment_max_bytes: 0
    snapshot_interval: "0s"
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, zeroConfig)
		if err != nil {
			t.Fatalf("Load() with zero config: %v", err)
		}

		if cfg.Storage.WAL.FlushInterval != 10*time.Millisecond {
			t.Errorf("FlushInterval = %v, want %v", cfg.Storage.WAL.FlushInterval, 10*time.Millisecond)
		}
		if cfg.Storage.WAL.FlushMaxMessages != 1000 {
			t.Errorf("FlushMaxMessages = %d, want %d", cfg.Storage.WAL.FlushMaxMessages, 1000)
		}
		if cfg.Storage.WAL.SegmentMaxBytes != 512<<20 {
			t.Errorf("SegmentMaxBytes = %d, want %d", cfg.Storage.WAL.SegmentMaxBytes, 512<<20)
		}
		if cfg.Storage.WAL.SnapshotInterval != 60*time.Second {
			t.Errorf("SnapshotInterval = %v, want %v", cfg.Storage.WAL.SnapshotInterval, 60*time.Second)
		}
		if cfg.Server.MaxConnections != 10000 {
			t.Errorf("MaxConnections = %d, want %d", cfg.Server.MaxConnections, 10000)
		}
		if cfg.Server.ReadTimeout != 30*time.Second {
			t.Errorf("ReadTimeout = %v, want %v", cfg.Server.ReadTimeout, 30*time.Second)
		}
		if cfg.Server.WriteTimeout != 10*time.Second {
			t.Errorf("WriteTimeout = %v, want %v", cfg.Server.WriteTimeout, 10*time.Second)
		}
	})

	t.Run("defaults_applied_when_negative", func(t *testing.T) {
		const negConfig = `
server:
  port: 7000
  max_connections: -1
  read_timeout: "-1s"
  write_timeout: "-1s"
storage:
  wal:
    flush_interval: "-1s"
    flush_max_messages: -5
    segment_max_bytes: -512
    snapshot_interval: "-1s"
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, negConfig)
		if err != nil {
			t.Fatalf("Load() with negative config: %v", err)
		}

		if cfg.Storage.WAL.FlushInterval != 10*time.Millisecond {
			t.Errorf("FlushInterval = %v, want %v", cfg.Storage.WAL.FlushInterval, 10*time.Millisecond)
		}
		if cfg.Server.MaxConnections != 10000 {
			t.Errorf("MaxConnections = %d, want %d", cfg.Server.MaxConnections, 10000)
		}
		if cfg.Server.ReadTimeout != 30*time.Second {
			t.Errorf("ReadTimeout = %v, want %v", cfg.Server.ReadTimeout, 30*time.Second)
		}
	})
}

// ── Validation B6: default secrets no longer rejected (zero-config) ────────

func TestB6Validation(t *testing.T) {
	t.Run("default_secrets_are_allowed_now", func(t *testing.T) {
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "octar-jwt-signing-key-change-in-production"
  default_admin:
    password: "admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("expected no error (zero-config allows defaults), got: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected non-nil config")
		}
	})
}

// ── Environment variable overrides ──────────────────────────────────────────

func TestEnvOverrides(t *testing.T) {
	// Viper v1.21.0 uses dots (not underscores) in env var names because
	// Load() does not call SetEnvKeyReplacer. The env var for config key
	// "server.port" is "OCTAR_SERVER.PORT" (uppercased, prefix + "_", dots
	// preserved as-is).

	t.Run("OCTAR_SERVER.PORT", func(t *testing.T) {
		t.Setenv("OCTAR_SERVER.PORT", "9999")
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Server.Port != 9999 {
			t.Errorf("server.port = %d, want 9999 (from env)", cfg.Server.Port)
		}
	})

	t.Run("OCTAR_LOG.LEVEL", func(t *testing.T) {
		t.Setenv("OCTAR_LOG.LEVEL", "debug")
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Log.Level != "debug" {
			t.Errorf("log.level = %q, want %q (from env)", cfg.Log.Level, "debug")
		}
	})

	t.Run("OCTAR_STORAGE.WAL.FLUSH_INTERVAL_overrides_config", func(t *testing.T) {
		t.Setenv("OCTAR_STORAGE.WAL.FLUSH_INTERVAL", "500ms")
		const yaml = `
server:
  port: 7000
storage:
  wal:
    flush_interval: "100ms"
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Storage.WAL.FlushInterval != 500*time.Millisecond {
			t.Errorf("FlushInterval = %v, want 500ms (env should override config file)", cfg.Storage.WAL.FlushInterval)
		}
	})

	t.Run("OCTAR_DEFAULTS.GROUP.RETRY.INITIAL_DELAY", func(t *testing.T) {
		t.Setenv("OCTAR_DEFAULTS.GROUP.RETRY.INITIAL_DELAY", "10s")
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Defaults.Group.Retry.InitialDelay != 10*time.Second {
			t.Errorf("InitialDelay = %v, want 10s (from env)", cfg.Defaults.Group.Retry.InitialDelay)
		}
	})

	t.Run("OCTAR_AUTH.PROVIDERS.JWT.HMAC_SECRET", func(t *testing.T) {
		customSecret := "env-override-secret-that-is-long-enough-for-testing-32"
		t.Setenv("OCTAR_AUTH.PROVIDERS.JWT.HMAC_SECRET", customSecret)
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Auth.Providers.JWT.HMACSecret != customSecret {
			t.Errorf("HMACSecret = %q, want %q (from env)", cfg.Auth.Providers.JWT.HMACSecret, customSecret)
		}
	})
}

// ── Duration parsing ────────────────────────────────────────────────────────

func TestDurationParsing(t *testing.T) {
	t.Run("duration_fields_parse_correctly", func(t *testing.T) {
		const yaml = `
server:
  port: 7000
  read_timeout: "30s"
  write_timeout: "10s"
storage:
  wal:
    flush_interval: "50ms"
    snapshot_interval: "120s"
auth:
  enabled: true
  session:
    timeout: "12h"
    cleanup_interval: "2m"
  rate_limit:
    login_attempts:
      window: "30s"
      lockout: "10m"
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
defaults:
  group:
    lease_timeout: "90s"
    retry:
      initial_delay: "2s"
      max_delay: "10m"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}

		tests := []struct {
			name string
			got  time.Duration
			want time.Duration
		}{
			{"server.read_timeout", cfg.Server.ReadTimeout, 30 * time.Second},
			{"server.write_timeout", cfg.Server.WriteTimeout, 10 * time.Second},
			{"storage.wal.flush_interval", cfg.Storage.WAL.FlushInterval, 50 * time.Millisecond},
			{"storage.wal.snapshot_interval", cfg.Storage.WAL.SnapshotInterval, 120 * time.Second},
			{"auth.session.timeout", cfg.Auth.Session.Timeout, 12 * time.Hour},
			{"auth.session.cleanup_interval", cfg.Auth.Session.CleanupInterval, 2 * time.Minute},
			{"auth.rate_limit.login_attempts.window", cfg.Auth.RateLimit.LoginAttempts.Window, 30 * time.Second},
			{"auth.rate_limit.login_attempts.lockout", cfg.Auth.RateLimit.LoginAttempts.Lockout, 10 * time.Minute},
			{"defaults.group.lease_timeout", cfg.Defaults.Group.LeaseTimeout, 90 * time.Second},
			{"defaults.group.retry.initial_delay", cfg.Defaults.Group.Retry.InitialDelay, 2 * time.Second},
			{"defaults.group.retry.max_delay", cfg.Defaults.Group.Retry.MaxDelay, 10 * time.Minute},
		}
		for _, tc := range tests {
			if tc.got != tc.want {
				t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		}
	})

	t.Run("duration_zero_is_rejected_and_fixed", func(t *testing.T) {
		const yaml = `
server:
  port: 7000
  read_timeout: "0s"
  write_timeout: "0s"
storage:
  wal:
    flush_interval: "0s"
    segment_max_bytes: 512000000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Server.ReadTimeout != 30*time.Second {
			t.Errorf("ReadTimeout = %v, want %v", cfg.Server.ReadTimeout, 30*time.Second)
		}
		if cfg.Server.WriteTimeout != 10*time.Second {
			t.Errorf("WriteTimeout = %v, want %v", cfg.Server.WriteTimeout, 10*time.Second)
		}
		if cfg.Storage.WAL.FlushInterval != 10*time.Millisecond {
			t.Errorf("FlushInterval = %v, want %v", cfg.Storage.WAL.FlushInterval, 10*time.Millisecond)
		}
	})
}

// ── Custom config paths / full config ──────────────────────────────────────

func TestCustomConfigFile(t *testing.T) {
	t.Run("custom_config_file_loaded", func(t *testing.T) {
		const custom = `
log:
  level: "warn"
server:
  host: "127.0.0.1"
  port: 9001
  max_connections: 5000
  read_timeout: "15s"
  write_timeout: "5s"
api:
  host: "127.0.0.1"
  port: 9002
metrics:
  enabled: false
  port: 0
pprof:
  enabled: false
  port: 0
auth:
  enabled: true
  session:
    timeout: "8h"
    max_sessions: 50000
    cleanup_interval: "10m"
  providers:
    password:
      enabled: true
      priority: 10
      bcrypt_cost: 14
    api_key:
      enabled: true
      priority: 20
      prefix: "flw_test_"
    jwt:
      enabled: true
      key_type: "HMAC"
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
      access_token_ttl: 1800
      refresh_token_ttl: 1209600
    oauth:
      enabled: false
    mtls:
      enabled: false
  rate_limit:
    login_attempts:
      max: 10
      window: "2m"
      lockout: "30m"
    auth_abuse:
      max: 200
      window: "5m"
  default_admin:
    username: "root"
    password: "strong-password-not-admin"
storage:
  data_dir: "/var/octar/data"
  wal:
    flush_interval: "25ms"
    flush_max_messages: 500
    segment_max_bytes: 268435456
    sync: false
    snapshot_interval: "30s"
  snapshot:
    interval: "2m"
defaults:
  group:
    parallelism: 2
    lease_timeout: "60s"
    max_pending: 5000
    retry:
      max_attempts: 5
      backoff: "fixed"
      initial_delay: "500ms"
      max_delay: "30s"
`
		cfg, err := loadFromYAML(t, custom)
		if err != nil {
			t.Fatalf("Load() with custom config: %v", err)
		}

		if cfg.Log.Level != "warn" {
			t.Errorf("log.level = %q", cfg.Log.Level)
		}
		if cfg.Server.Host != "127.0.0.1" || cfg.Server.Port != 9001 {
			t.Errorf("server: %+v", cfg.Server)
		}
		if cfg.Server.MaxConnections != 5000 {
			t.Errorf("max_connections = %d", cfg.Server.MaxConnections)
		}
		if cfg.Server.ReadTimeout != 15*time.Second {
			t.Errorf("read_timeout = %v", cfg.Server.ReadTimeout)
		}
		if cfg.API.Host != "127.0.0.1" || cfg.API.Port != 9002 {
			t.Errorf("api: %+v", cfg.API)
		}
		if cfg.Metrics.Enabled != false || cfg.Metrics.Port != 0 {
			t.Errorf("metrics: %+v", cfg.Metrics)
		}
		if cfg.PProf.Enabled != false || cfg.PProf.Port != 0 {
			t.Errorf("pprof: %+v", cfg.PProf)
		}
		if cfg.Auth.Session.Timeout != 8*time.Hour {
			t.Errorf("session timeout = %v", cfg.Auth.Session.Timeout)
		}
		if cfg.Auth.Providers.Password.BcryptCost != 14 {
			t.Errorf("bcrypt_cost = %d", cfg.Auth.Providers.Password.BcryptCost)
		}
		if cfg.Auth.Providers.APIKey.Prefix != "flw_test_" {
			t.Errorf("api_key prefix = %q", cfg.Auth.Providers.APIKey.Prefix)
		}
		if cfg.Auth.Providers.JWT.AccessTokenTTL != 1800 {
			t.Errorf("access_token_ttl = %d", cfg.Auth.Providers.JWT.AccessTokenTTL)
		}
		if cfg.Auth.RateLimit.AuthAbuse.Max != 200 {
			t.Errorf("auth_abuse.max = %d", cfg.Auth.RateLimit.AuthAbuse.Max)
		}
		if cfg.Auth.RateLimit.LoginAttempts.Max != 10 {
			t.Errorf("login_attempts.max = %d", cfg.Auth.RateLimit.LoginAttempts.Max)
		}
		if cfg.Storage.DataDir != "/var/octar/data" {
			t.Errorf("data_dir = %q", cfg.Storage.DataDir)
		}
		if cfg.Storage.WAL.FlushInterval != 25*time.Millisecond {
			t.Errorf("flush_interval = %v", cfg.Storage.WAL.FlushInterval)
		}
		if cfg.Storage.WAL.FlushMaxMessages != 500 {
			t.Errorf("flush_max_messages = %d", cfg.Storage.WAL.FlushMaxMessages)
		}
		if cfg.Storage.WAL.SegmentMaxBytes != 268435456 {
			t.Errorf("segment_max_bytes = %d", cfg.Storage.WAL.SegmentMaxBytes)
		}
		if cfg.Storage.WAL.Sync != false {
			t.Errorf("sync = %v", cfg.Storage.WAL.Sync)
		}
		if cfg.Storage.WAL.SnapshotInterval != 30*time.Second {
			t.Errorf("snapshot_interval = %v", cfg.Storage.WAL.SnapshotInterval)
		}
		if cfg.Storage.Snapshot.Interval != 2*time.Minute {
			t.Errorf("snapshot.interval = %v", cfg.Storage.Snapshot.Interval)
		}
		if cfg.Defaults.Group.Parallelism != 2 {
			t.Errorf("parallelism = %d", cfg.Defaults.Group.Parallelism)
		}
		if cfg.Defaults.Group.LeaseTimeout != 60*time.Second {
			t.Errorf("lease_timeout = %v", cfg.Defaults.Group.LeaseTimeout)
		}
		if cfg.Defaults.Group.Retry.Backoff != "fixed" {
			t.Errorf("backoff = %q", cfg.Defaults.Group.Retry.Backoff)
		}
		if cfg.Defaults.Group.Retry.MaxAttempts != 5 {
			t.Errorf("max_attempts = %d", cfg.Defaults.Group.Retry.MaxAttempts)
		}
		if cfg.Defaults.Group.Retry.InitialDelay != 500*time.Millisecond {
			t.Errorf("initial_delay = %v", cfg.Defaults.Group.Retry.InitialDelay)
		}
	})
}

// ── Missing config file ─────────────────────────────────────────────────────

func TestMissingConfigFile(t *testing.T) {
	t.Run("succeeds_without_config_file", func(t *testing.T) {
	viper.Reset()
		dir := t.TempDir()
		prevDir, _ := os.Getwd()
		if err := os.Chdir(dir); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chdir(prevDir) })

		cfg, err := Load()
		if err != nil {
			t.Fatalf("expected no error (zero-config), got: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected non-nil config")
		}
	})
}

// ── Admin user overrides ────────────────────────────────────────────────────

func TestAdminDefaultsApplied(t *testing.T) {
	t.Run("default_admin_username_from_config", func(t *testing.T) {
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    username: "superadmin"
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Auth.DefaultAdmin.Username != "superadmin" {
			t.Errorf("username = %q, want %q", cfg.Auth.DefaultAdmin.Username, "superadmin")
		}
	})

	t.Run("default_admin_username_from_env", func(t *testing.T) {
		t.Setenv("OCTAR_AUTH.DEFAULT_ADMIN.USERNAME", "env-admin")
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    username: "config-admin"
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Auth.DefaultAdmin.Username != "env-admin" {
			t.Errorf("username = %q, want %q (env should override config)", cfg.Auth.DefaultAdmin.Username, "env-admin")
		}
	})
}

// ── ConnRateLimit default ───────────────────────────────────────────────────

func TestConnRateLimitDefault(t *testing.T) {
	t.Run("conn_rate_limit_defaults_to_1000", func(t *testing.T) {
		const yaml = `
server:
  port: 7000
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Server.ConnRateLimit != 1000 {
			t.Errorf("ConnRateLimit = %d, want 1000 (default)", cfg.Server.ConnRateLimit)
		}
	})

	t.Run("conn_rate_limit_from_env", func(t *testing.T) {
		t.Setenv("OCTAR_SERVER.CONN_RATE_LIMIT", "250")
		const yaml = `
server:
  port: 7000
  conn_rate_limit: 500  # base in config; env overrides to 250
auth:
  providers:
    jwt:
      hmac_secret: "my-custom-secret-at-least-32-chars-long-for-hs256"
  default_admin:
    password: "strong-password-not-admin"
`
		cfg, err := loadFromYAML(t, yaml)
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if cfg.Server.ConnRateLimit != 250 {
			t.Errorf("ConnRateLimit = %d, want 250 (from env)", cfg.Server.ConnRateLimit)
		}
	})
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestWALAppendTimeout is a constant, but verify it compiles and is correct
func TestWALAppendTimeoutConstant(t *testing.T) {
	if WALAppendTimeout != 30*time.Second {
		t.Errorf("WALAppendTimeout = %v, want %v", WALAppendTimeout, 30*time.Second)
	}
}

// ── Verify known config values from the project's config.yaml ───────────────

func TestProjectConfigLoads(t *testing.T) {
	viper.Reset()
	prevDir, _ := os.Getwd()
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(prevDir) })

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected project config to load, got: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
}

func findProjectRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// Walk up to find go.mod
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root (go.mod)")
		}
		dir = parent
	}
}

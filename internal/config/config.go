// Package config loads and validates the process configuration from the
// environment. Configuration is read once at startup; secrets are never
// embedded in the binary or in defaults.
package config

import (
	"errors"
	"fmt"
	"github.com/akshat281204/lakehouse-query-studio/internal/guard"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved runtime configuration for the API and worker
// processes.
type Config struct {
	// HTTPAddr is the listen address for the HTTP server, e.g. ":8080".
	HTTPAddr string
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogFormat is json or text.
	LogFormat string

	// PostgresURL is the PostgreSQL connection URL. Empty means the process
	// starts without a database and reports postgres as not-ready rather than
	// crashing.
	PostgresURL string

	// Object store (MinIO / S3-compatible).
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool

	// APIToken is an optional single deployment token. Empty disables token
	// authentication.
	APIToken string

	// Query holds the query engine, worker and result limits.
	Query QueryConfig

	// AllowedHosts and AllowedOrigins are the closed lists of exact Host
	// header values and Origin values the API accepts (design §15.2).
	AllowedHosts   []string
	AllowedOrigins []string

	// Retention and cleanup (design §9, §10). Zero disables a sweeper.
	Retention RetentionConfig

	// MetricsAddr is where the worker serves /metrics. Empty disables it.
	MetricsAddr string
}

// Load reads configuration from the environment, applies defaults, and
// validates the result.
func Load() (*Config, error) {
	cfg := &Config{
		HTTPAddr:       getenv("HTTP_ADDR", ":8080"),
		LogLevel:       getenv("LOG_LEVEL", "info"),
		LogFormat:      getenv("LOG_FORMAT", "json"),
		PostgresURL:    getenv("POSTGRES_URL", ""),
		MinIOEndpoint:  getenv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey: getenv("MINIO_ACCESS_KEY", ""),
		MinIOSecretKey: getenv("MINIO_SECRET_KEY", ""),
		MinIOBucket:    getenv("MINIO_BUCKET", "lakehouse"),
		APIToken:       getenv("API_TOKEN", ""),
	}

	var err error
	if cfg.ShutdownTimeout, err = getenvDuration("SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return nil, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err)
	}
	if cfg.MinIOUseSSL, err = getenvBool("MINIO_USE_SSL", false); err != nil {
		return nil, fmt.Errorf("MINIO_USE_SSL: %w", err)
	}
	if cfg.Query, err = loadQuery(); err != nil {
		return nil, err
	}
	if cfg.Retention, err = loadRetention(); err != nil {
		return nil, err
	}
	cfg.MetricsAddr = getenv("METRICS_ADDR", "127.0.0.1:9100")
	port := "8080"
	if _, p, perr := net.SplitHostPort(cfg.HTTPAddr); perr == nil && p != "" {
		port = p
	}
	cfg.AllowedHosts = splitList(getenv("ALLOWED_HOSTS", ""), guard.DefaultHosts(port))
	cfg.AllowedOrigins = splitList(getenv("ALLOWED_ORIGINS", ""), guard.DefaultOrigins(port))
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks that the configuration is internally consistent. It does not
// require secrets to be present (those are injected by the deployment).
func (c *Config) Validate() error {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return errors.New("HTTP_ADDR must not be empty")
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return fmt.Errorf("HTTP_ADDR %q is not a valid host:port: %w", c.HTTPAddr, err)
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("SHUTDOWN_TIMEOUT must be positive")
	}
	if !oneOf(c.LogLevel, "debug", "info", "warn", "error") {
		return fmt.Errorf("LOG_LEVEL must be one of debug, info, warn, error (got %q)", c.LogLevel)
	}
	if !oneOf(c.LogFormat, "json", "text") {
		return fmt.Errorf("LOG_FORMAT must be one of json, text (got %q)", c.LogFormat)
	}
	if err := c.validatePostgresURL(); err != nil {
		return err
	}
	if strings.TrimSpace(c.MinIOEndpoint) == "" {
		return errors.New("MINIO_ENDPOINT must not be empty")
	}
	if strings.TrimSpace(c.MinIOBucket) == "" {
		return errors.New("MINIO_BUCKET must not be empty")
	}
	if c.APIToken != "" && len(c.APIToken) < 16 {
		return errors.New("API_TOKEN must be at least 16 characters when set")
	}
	return nil
}

func (c *Config) validatePostgresURL() error {
	if strings.TrimSpace(c.PostgresURL) == "" {
		// Optional: the process reports postgres not-ready instead of failing
		// startup, so a clean local checkout can build and run without a
		// database.
		return nil
	}
	u, err := url.Parse(c.PostgresURL)
	if err != nil {
		// url.Parse's error text embeds the raw input URL, which may carry
		// credentials. Report a fixed message so a malformed POSTGRES_URL never
		// leaks its password into startup output.
		return errors.New("POSTGRES_URL is not a valid URL")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return fmt.Errorf("POSTGRES_URL must use the postgres:// or postgresql:// scheme (got %q)", u.Scheme)
	}
	return nil
}

func oneOf(v string, allowed ...string) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func getenvBool(key string, def bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid boolean %q: %w", v, err)
	}
	return b, nil
}

func getenvDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", v, err)
	}
	return d, nil
}

func splitList(raw string, def []string) []string {
	if strings.TrimSpace(raw) == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

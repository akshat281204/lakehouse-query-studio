package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("POSTGRES_URL", "postgres://user:pass@localhost:5432/db?sslmode=disable")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "access-key")
	t.Setenv("MINIO_SECRET_KEY", "secret-key")
	t.Setenv("MINIO_BUCKET", "bucket")
	t.Setenv("MINIO_USE_SSL", "true")
	t.Setenv("API_TOKEN", "deployment-token")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTPAddr != "127.0.0.1:9090" {
		t.Errorf("HTTPAddr = %q, want 127.0.0.1:9090", cfg.HTTPAddr)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 3s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("log config = (%q, %q), want (debug, text)", cfg.LogLevel, cfg.LogFormat)
	}
	if cfg.PostgresURL != "postgres://user:pass@localhost:5432/db?sslmode=disable" {
		t.Errorf("PostgresURL = %q", cfg.PostgresURL)
	}
	if cfg.MinIOEndpoint != "minio:9000" || cfg.MinIOBucket != "bucket" || !cfg.MinIOUseSSL {
		t.Errorf("minio config = %+v", cfg)
	}
	if cfg.MinIOAccessKey != "access-key" || cfg.MinIOSecretKey != "secret-key" {
		t.Errorf("minio credentials not loaded")
	}
	if cfg.APIToken != "deployment-token" {
		t.Errorf("APIToken = %q", cfg.APIToken)
	}
}

func TestLoadInvalidBool(t *testing.T) {
	t.Setenv("MINIO_USE_SSL", "not-a-bool")
	if _, err := Load(); err == nil {
		t.Fatal("Load() expected error for invalid MINIO_USE_SSL, got nil")
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "eventually")
	if _, err := Load(); err == nil {
		t.Fatal("Load() expected error for invalid SHUTDOWN_TIMEOUT, got nil")
	}
}

func TestValidate(t *testing.T) {
	base := func() *Config {
		return &Config{
			HTTPAddr:        ":8080",
			ShutdownTimeout: 10 * time.Second,
			LogLevel:        "info",
			LogFormat:       "json",
			PostgresURL:     "postgres://u:p@localhost:5432/db?sslmode=disable",
			MinIOEndpoint:   "localhost:9000",
			MinIOBucket:     "lakehouse",
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "valid"},
		{name: "empty addr", mutate: func(c *Config) { c.HTTPAddr = "" }, wantErr: true},
		{name: "bad addr", mutate: func(c *Config) { c.HTTPAddr = "8080" }, wantErr: true},
		{name: "zero shutdown", mutate: func(c *Config) { c.ShutdownTimeout = 0 }, wantErr: true},
		{name: "bad log level", mutate: func(c *Config) { c.LogLevel = "verbose" }, wantErr: true},
		{name: "bad log format", mutate: func(c *Config) { c.LogFormat = "xml" }, wantErr: true},
		{name: "bad postgres scheme", mutate: func(c *Config) { c.PostgresURL = "http://example.com" }, wantErr: true},
		{name: "empty minio endpoint", mutate: func(c *Config) { c.MinIOEndpoint = "" }, wantErr: true},
		{name: "empty bucket", mutate: func(c *Config) { c.MinIOBucket = "" }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			if tt.mutate != nil {
				tt.mutate(cfg)
			}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePostgresURLDoesNotLeakCredentials(t *testing.T) {
	// A malformed, credential-bearing POSTGRES_URL must not leak its password
	// (or the raw URL) into the reported validation error. url.Parse's error
	// text embeds the whole input, so we must sanitize it.
	secret := "s3cr3t-p%zzword"
	cfg := &Config{
		HTTPAddr:        ":8080",
		ShutdownTimeout: time.Second,
		LogLevel:        "info",
		LogFormat:       "json",
		PostgresURL:     "postgres://alice:" + secret + "@db.example.com:5432/app",
		MinIOEndpoint:   "localhost:9000",
		MinIOBucket:     "lakehouse",
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for malformed POSTGRES_URL, got nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Validate() error leaks password %q: %v", secret, err)
	}
	if strings.Contains(err.Error(), cfg.PostgresURL) {
		t.Fatalf("Validate() error leaks raw POSTGRES_URL: %v", err)
	}
}

func TestValidateAllowsEmptyPostgresURL(t *testing.T) {
	cfg := &Config{
		HTTPAddr:        ":8080",
		ShutdownTimeout: time.Second,
		LogLevel:        "info",
		LogFormat:       "json",
		MinIOEndpoint:   "localhost:9000",
		MinIOBucket:     "lakehouse",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with empty PostgresURL = %v, want nil", err)
	}
}

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeEnvFile(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test .env: %v", err)
	}
	return path
}

func makeValidEnv() string {
	return strings.Join([]string{
		"HTTP_PORT=9090",
		"CACHE_CAPACITY=200",
		"LOG_FORMAT=text",
		"JWT_SECRET=secret",
		"DB_HOST=localhost",
		"DB_PORT=5433",
		"DB_USER=testuser",
		"DB_PASSWORD=testpass",
		"DB_NAME=testdb",
		"DB_SSL_MODE=require",
		"MIGRATION_DIR=migrations",
		"KAFKA_BROKERS=broker1:9092,broker2:9092",
		"KAFKA_TOPIC=orders",
	}, "\n")
}

// TestLoad_Full checks that all fields are correct.
func TestLoad_Full(t *testing.T) {
	dir := t.TempDir()
	path := writeEnvFile(t, dir, makeValidEnv())

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	if cfg.HTTPPort != "9090" {
		t.Errorf("HTTPPort: want 9090, got %s", cfg.HTTPPort)
	}
	if cfg.CacheCapacity != 200 {
		t.Errorf("CacheCapacity: want 200, got %d", cfg.CacheCapacity)
	}
	if cfg.LogFormat != LogFormatText {
		t.Errorf("LogFormat: want text, got %s", cfg.LogFormat)
	}
	if cfg.JWTSecret != "secret" {
		t.Errorf("LogFormat: want secret, got %s", cfg.LogFormat)
	}
	if cfg.DBHost != "localhost" {
		t.Errorf("DBHost: want localhost, got %s", cfg.DBHost)
	}
	if cfg.DBPort != "5433" {
		t.Errorf("DBPort: want 5433, got %s", cfg.DBPort)
	}
	if cfg.DBUser != "testuser" {
		t.Errorf("DBUser: want testuser, got %s", cfg.DBUser)
	}
	if cfg.DBPass != "testpass" {
		t.Errorf("DBPass: want testpass, got %s", cfg.DBPass)
	}
	if cfg.DBName != "testdb" {
		t.Errorf("DBName: want testdb, got %s", cfg.DBName)
	}
	if cfg.DBSSLMode != "require" {
		t.Errorf("DBSSLMode: want require, got %s", cfg.DBSSLMode)
	}
	if cfg.MigrationDir != "migrations" {
		t.Errorf("MigrationDir: want migrations, got %s", cfg.MigrationDir)
	}
	if cfg.KafkaBrokers != "broker1:9092,broker2:9092" {
		t.Errorf("KafkaBrokers: want broker1:9092,broker2:9092, got %s", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "orders" {
		t.Errorf("KafkaTopic: want orders, got %s", cfg.KafkaTopic)
	}
}

// TestLoad_HTTPPortDefault checks default HTTP_PORT value.
func TestLoad_Defaults(t *testing.T) {
	dir := t.TempDir()
	path := writeEnvFile(t, dir, makeValidEnv()+
		"\nHTTP_PORT="+
		"\nDB_SSL_MODE="+
		"\nMIGRATION_DIR="+
		"\nJWT_TOKEN=")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if cfg.HTTPPort != "8080" {
		t.Errorf("HTTPPort: want 8080, got %s", cfg.HTTPPort)
	}
	if cfg.DBSSLMode != "disable" {
		t.Errorf("DBSSLMode: want disable, got %s", cfg.DBSSLMode)
	}
	if cfg.MigrationDir != "migrations" {
		t.Errorf("MigrationDir: want migrations, got %s", cfg.MigrationDir)
	}
	if cfg.JWTSecret != "secret" {
		t.Errorf("JWTSecret: want secret, got %s", cfg.JWTSecret)
	}
}

// TestLoad_HTTPPortValidation checks HTTP_PORT validation.
func TestLoad_HTTPPortValidation(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "not a number", value: "abc", wantErr: "HTTP_PORT must be number"},
		{name: "too low", value: "80", wantErr: "between 1024 and 65535"},
		{name: "too high", value: "99999", wantErr: "between 1024 and 65535"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			content := makeValidEnv() + "\nHTTP_PORT=" + tt.value
			path := writeEnvFile(t, dir, content)

			_, err := Load(path)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error should contain %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestLoad_DBPortValidation checks DB_PORT validation.
func TestLoad_DBPortValidation(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "not a number", value: "abc", wantErr: "DB_PORT must be number"},
		{name: "too low", value: "80", wantErr: "between 1024 and 65535"},
		{name: "too high", value: "99999", wantErr: "between 1024 and 65535"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			content := makeValidEnv() + "\nDB_PORT=" + tt.value
			path := writeEnvFile(t, dir, content)

			_, err := Load(path)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error should contain %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestLoad_CacheCapacityValidation checks cache capacity validation.
func TestLoad_CacheCapacityValidation(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		wantErr     bool
		wantDefault int
	}{
		{name: "default on missing", value: "", wantErr: false, wantDefault: 100},
		{name: "default on invalid", value: "notanumber", wantErr: false, wantDefault: 100},
		{name: "zero rejected", value: "0", wantErr: true},
		{name: "negative rejected", value: "-1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			content := makeValidEnv()
			if tt.value != "" {
				content += "\nCACHE_CAPACITY=" + tt.value
			} else {
				content = strings.Replace(content, "CACHE_CAPACITY=200", "CACHE_CAPACITY=", 1)
			}
			path := writeEnvFile(t, dir, content)

			cfg, err := Load(path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("load failed: %v", err)
			}
			if cfg.CacheCapacity != tt.wantDefault {
				t.Errorf("CacheCapacity: want %d, got %d", tt.wantDefault, cfg.CacheCapacity)
			}
		})
	}
}

// TestLoad_LogFormatValidation checks that invalid log format values are rejected.
func TestLoad_LogFormatValidation(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "text accepted", value: LogFormatText, wantErr: ""},
		{name: "json accepted", value: LogFormatJSON, wantErr: ""},
		{name: "invalid", value: "xml", wantErr: "LOG_FORMAT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			content := makeValidEnv() + "\nLOG_FORMAT=" + tt.value
			path := writeEnvFile(t, dir, content)

			_, err := Load(path)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error should contain %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestLoad_RequiredFields checks validation of required fields.
func TestLoad_RequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		skip    string
		wantErr string
	}{
		{name: "DB_HOST", skip: "DB_HOST", wantErr: "DB_HOST is required"},
		{name: "JWT_SECRET", skip: "JWT_SECRET", wantErr: "JWT_SECRET is required"},
		{name: "DB_PORT", skip: "DB_PORT", wantErr: "DB_PORT is required"},
		{name: "DB_USER", skip: "DB_USER", wantErr: "DB_USER is required"},
		{name: "DB_PASSWORD", skip: "DB_PASSWORD", wantErr: "DB_PASSWORD is required"},
		{name: "DB_NAME", skip: "DB_NAME", wantErr: "DB_NAME is required"},
		{name: "KAFKA_BROKERS", skip: "KAFKA_BROKERS", wantErr: "KAFKA_BROKERS is required"},
		{name: "KAFKA_TOPIC", skip: "KAFKA_TOPIC", wantErr: "KAFKA_TOPIC is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			content := makeValidEnv() + "\n" + tt.skip + "="
			path := writeEnvFile(t, dir, content)

			_, err := Load(path)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error should contain %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

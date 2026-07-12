// Package config loads and validates application configuration from environment variables.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	HTTPPort      string
	CacheCapacity int
	LogFormat     string
	JWTSecret     string
	DBHost        string
	DBPort        string
	DBUser        string
	DBPass        string
	DBName        string
	DBSSLMode     string
	MigrationDir  string
	KafkaBrokers  string
	KafkaTopic    string
}

const (
	LogFormatText       = "text"
	LogFormatJSON       = "json"
	DefaultMigrationDir = "migrations"
)

// Load reads the given env file and returns Config.
func Load(path string) (*Config, error) {
	if err := godotenv.Overload(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load .env: %w", err)
		}
		slog.Warn(".env file not found, using OS environment variables")
	}

	cacheCapacityRaw := os.Getenv("CACHE_CAPACITY")

	cacheCapacity, err := strconv.Atoi(cacheCapacityRaw)
	if err != nil {
		cacheCapacity = 100
	}
	if cacheCapacity <= 0 {
		return nil, fmt.Errorf("CACHE_CAPACITY must be more than 0")
	}

	cfg := &Config{
		HTTPPort:      os.Getenv("HTTP_PORT"),
		CacheCapacity: cacheCapacity,
		LogFormat:     os.Getenv("LOG_FORMAT"),
		JWTSecret:     os.Getenv("JWT_SECRET"),
		DBHost:        os.Getenv("DB_HOST"),
		DBPort:        os.Getenv("DB_PORT"),
		DBUser:        os.Getenv("DB_USER"),
		DBPass:        os.Getenv("DB_PASSWORD"),
		DBName:        os.Getenv("DB_NAME"),
		DBSSLMode:     os.Getenv("DB_SSL_MODE"),
		MigrationDir:  os.Getenv("MIGRATION_DIR"),
		KafkaBrokers:  os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:    os.Getenv("KAFKA_TOPIC"),
	}

	if cfg.LogFormat != LogFormatText && cfg.LogFormat != LogFormatJSON {
		return nil, fmt.Errorf("LOG_FORMAT must be text or json")
	}

	if cfg.HTTPPort == "" {
		cfg.HTTPPort = "8080"
	}
	if cfg.LogFormat == "" {
		cfg.LogFormat = LogFormatText
	}
	if cfg.JWTSecret == "" {
		return nil, fmt.Errorf("JWT_SECRET is required")
	}
	if cfg.DBHost == "" {
		return nil, fmt.Errorf("DB_HOST is required")
	}
	if cfg.DBPort == "" {
		return nil, fmt.Errorf("DB_PORT is required")
	}
	if cfg.DBUser == "" {
		return nil, fmt.Errorf("DB_USER is required")
	}
	if cfg.DBPass == "" {
		return nil, fmt.Errorf("DB_PASSWORD is required")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("DB_NAME is required")
	}
	if cfg.DBSSLMode == "" {
		cfg.DBSSLMode = "disable"
	}
	if cfg.MigrationDir == "" {
		cfg.MigrationDir = "migrations"
	}
	if cfg.KafkaBrokers == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.KafkaTopic == "" {
		return nil, fmt.Errorf("KAFKA_TOPIC is required")
	}

	httpPort, err := strconv.Atoi(cfg.HTTPPort)
	if err != nil {
		return nil, fmt.Errorf("HTTP_PORT must be number")
	}
	if httpPort < 1024 || httpPort > 65535 {
		return nil, fmt.Errorf("HTTP_PORT must be between 1024 and 65535")
	}

	dbPort, err := strconv.Atoi(cfg.DBPort)
	if err != nil {
		return nil, fmt.Errorf("DB_PORT must be number")
	}
	if dbPort < 1024 || dbPort > 65535 {
		return nil, fmt.Errorf("DB_PORT must be between 1024 and 65535")
	}

	return cfg, nil
}

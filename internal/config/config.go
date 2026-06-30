package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	HTTPPort      string
	CacheCapacity int
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

// Load reads .env file and returns a populated Config.
func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}

	cacheCapacityRaw := os.Getenv("CACHE_CAPACITY")

	cacheCapacity, err := strconv.Atoi(cacheCapacityRaw)
	if err != nil {
		cacheCapacity = 100
	}

	return &Config{
		HTTPPort:      os.Getenv("HTTP_PORT"),
		CacheCapacity: cacheCapacity,
		DBHost:        os.Getenv("DB_HOST"),
		DBPort:        os.Getenv("DB_PORT"),
		DBUser:        os.Getenv("DB_USER"),
		DBPass:        os.Getenv("DB_PASSWORD"),
		DBName:        os.Getenv("DB_NAME"),
		DBSSLMode:     os.Getenv("DB_SSL_MODE"),
		MigrationDir:  os.Getenv("MIGRATION_DIR"),
		KafkaBrokers:  os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:    os.Getenv("KAFKA_TOPIC"),
	}, nil
}

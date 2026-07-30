package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all application configuration.
type Config struct {
	FirehoseURL      string // WebSocket URL for the Firehose
	DatabaseURL      string // PostgreSQL connection string
	CollectionMode   string // "full" or "moderation-priority"
	StatsIntervalSec int    // Statistics reporting interval
	LogLevel         string // Log level
}

// Load reads configuration from .env file and environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		FirehoseURL:      getEnv("FIREHOSE_URL", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"),
		DatabaseURL:      getEnv("DATABASE_URL", "postgres://postgres:password@localhost:5432/bsky_data?sslmode=disable"),
		CollectionMode:   getEnv("COLLECTION_MODE", "full"),
		StatsIntervalSec: getEnvAsInt("STATS_INTERVAL_SEC", 30),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
		log.Printf("Warning: invalid integer for %s: %s, using fallback %d", key, value, fallback)
	}
	return fallback
}

func getEnvAsSlice(key string, fallback []string) []string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		parts := []string{}
		for _, part := range strings.Split(value, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				parts = append(parts, trimmed)
			}
		}
		if len(parts) > 0 {
			return parts
		}
	}
	return fallback
}

// IsFullCollectionMode returns true if the collection mode is "full".
func (c *Config) IsFullCollectionMode() bool {
	return c.CollectionMode == "full"
}

// IsModerationPriorityMode returns true if the collection mode is "moderation-priority".
func (c *Config) IsModerationPriorityMode() bool {
	return c.CollectionMode == "moderation-priority"
}

package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Collection modes
const (
	FullMode               = "full"
	ModerationPriorityMode = "moderation-priority"
)

// Config holds all application configuration.
type Config struct {
	DatabaseURL       string   // PostgreSQL connection string
	CollectionMode    string   // "full" or "moderation-priority"
	StatsIntervalSec  int      // Statistics reporting interval
	LogLevel          string   // Log level
	WantedCollections []string // (Optional) For future filtering
}

// Load reads configuration from .env file and environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		DatabaseURL:      getEnv("DATABASE_URL", "postgres://username:password@localhost:5432/bsky_data?sslmode=disable"),
		CollectionMode:   getEnv("COLLECTION_MODE", FullMode),
		StatsIntervalSec: getEnvAsInt("STATS_INTERVAL_SEC", 10),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		WantedCollections: getEnvAsSlice("WANTED_COLLECTIONS", []string{
			"app.bsky.feed.post",
			"app.bsky.graph.block",
		}),
	}
}

// IsFullMode returns true if the collection mode is "full".
func (c *Config) IsFullMode() bool {
	return c.CollectionMode == FullMode
}

// IsModerationPriorityMode returns true if the collection mode is "moderation-priority".
func (c *Config) IsModerationPriorityMode() bool {
	return c.CollectionMode == ModerationPriorityMode
}

// --- Helper functions ---
// getEnv returns environment variable value or a fallback.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// getEnvAsInt returns environment variable as integer or a fallback.
func getEnvAsInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
		log.Printf("Warning: invalid integer for %s: %s, using fallback %d", key, value, fallback)
	}
	return fallback
}

// getEnvAsSlice returns environment variable as a string slice or a fallback.
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

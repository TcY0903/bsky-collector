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
	DatabaseURL      string
	StatsIntervalSec int

	// Stats verbose switches
	DBStatsVerbose       bool
	FirehoseStatsVerbose bool

	// Firehose
	FirehoseURL              string
	FirehoseEventChannelSize int
	FirehoseSchedulerWorkers int
	FirehoseSchedulerQueue   int

	// Label aggregator
	LabelAggregatorFirehoseURL        string
	LabelAggregatorCSVPath            string
	LabelAggregatorEventChannelSize   int
	LabelAggregatorMaxConcurrent      int
	LabelAggregatorMaxPerTick         int
	LabelAggregatorMaxResolveFailures int
	LabelAggregatorMaxConnectFailures int

	// Per-processor verbose logging
	PostProcessorVerbose  bool
	ListProcessorVerbose  bool
	LabelProcessorVerbose bool
	BlockProcessorVerbose bool

	// Processors batch configuration
	ProcessorBatchSize int
	ProcessorTickerSec int

	// Per-fetcher verbose logging
	PostFetcherVerbose  bool
	ListFetcherVerbose  bool
	ActorFetcherVerbose bool

	// Post fetcher specific
	PostFetchMaxConcurrent   int
	PostFetchMaxContextDepth int
	ThreadParentHeight       int
	ThreadReplyDepth         int
}

// Load reads configuration from .env file and environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		DatabaseURL:      getEnv("DATABASE_URL", "postgres://username:password@localhost:5432/bsky_data?sslmode=disable"),
		StatsIntervalSec: getEnvAsInt("STATS_INTERVAL_SEC", 10),

		DBStatsVerbose:       getEnvAsBool("DB_STATS_VERBOSE", true),
		FirehoseStatsVerbose: getEnvAsBool("FIREHOSE_STATS_VERBOSE", true),

		FirehoseURL:              getEnv("FIREHOSE_URL", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"),
		FirehoseEventChannelSize: getEnvAsInt("FIREHOSE_EVENT_CHANNEL_SIZE", 10000),
		FirehoseSchedulerWorkers: getEnvAsInt("FIREHOSE_SCHEDULER_WORKERS", 10),
		FirehoseSchedulerQueue:   getEnvAsInt("FIREHOSE_SCHEDULER_QUEUE", 100),

		LabelAggregatorFirehoseURL: getEnv("LABEL_AGGREGATOR_FIREHOSE_URL",
			"wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"),
		LabelAggregatorCSVPath:            getEnv("LABEL_AGGREGATOR_CSV_PATH", "labelers.csv"),
		LabelAggregatorEventChannelSize:   getEnvAsInt("LABEL_AGGREGATOR_EVENT_CHANNEL_SIZE", 10000),
		LabelAggregatorMaxConcurrent:      getEnvAsInt("LABEL_AGGREGATOR_MAX_CONCURRENT", 20),
		LabelAggregatorMaxPerTick:         getEnvAsInt("LABEL_AGGREGATOR_MAX_PER_TICK", 10),
		LabelAggregatorMaxResolveFailures: getEnvAsInt("LABEL_AGGREGATOR_MAX_RESOLVE_FAILURES", 3),
		LabelAggregatorMaxConnectFailures: getEnvAsInt("LABEL_AGGREGATOR_MAX_CONNECT_FAILURES", 5),

		PostProcessorVerbose:  getEnvAsBool("POST_PROCESSOR_VERBOSE", false),
		ListProcessorVerbose:  getEnvAsBool("LIST_PROCESSOR_VERBOSE", false),
		LabelProcessorVerbose: getEnvAsBool("LABEL_PROCESSOR_VERBOSE", false),
		BlockProcessorVerbose: getEnvAsBool("BLOCK_PROCESSOR_VERBOSE", false),

		ProcessorBatchSize: getEnvAsInt("PROCESSOR_BATCH_SIZE", 500),
		ProcessorTickerSec: getEnvAsInt("PROCESSOR_TICKER_SEC", 3),

		PostFetcherVerbose:  getEnvAsBool("POST_FETCHER_VERBOSE", true),
		ListFetcherVerbose:  getEnvAsBool("LIST_FETCHER_VERBOSE", false),
		ActorFetcherVerbose: getEnvAsBool("ACTOR_FETCHER_VERBOSE", false),

		PostFetchMaxConcurrent:   getEnvAsInt("POST_FETCH_MAX_CONCURRENT", 5),
		PostFetchMaxContextDepth: getEnvAsInt("POST_FETCH_MAX_CONTEXT_DEPTH", 10),
		ThreadParentHeight:       getEnvAsInt("THREAD_PARENT_HEIGHT", 1000),
		ThreadReplyDepth:         getEnvAsInt("THREAD_REPLY_DEPTH", 1000),
	}
}

// --- Helper functions ---

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

func getEnvAsBool(key string, fallback bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
		log.Printf("Warning: invalid boolean for %s: %s, using fallback %v", key, value, fallback)
	}
	return fallback
}

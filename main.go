package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bsky-collector/config"
	"bsky-collector/processor"
	"bsky-collector/storage"
	"bsky-collector/stream"
)

func main() {
	cfg := config.Load()
	logger := log.New(os.Stdout, "[BSKY] ", log.LstdFlags)
	logger.Printf("Starting Bluesky collector...")

	db, err := storage.NewDB(cfg.DatabaseURL)
	if err != nil {
		logger.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	logger.Printf("Database connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("Received signal: %v, shutting down...", sig)
		cancel()
	}()

	// Firehose client
	firehoseClient := stream.NewFirehoseClient(cfg, logger)
	go func() {
		logger.Printf("Starting Firehose client with auto-reconnect...")
		firehoseClient.StartWithRetry(ctx)
	}()

	// Label aggregator
	labelAggregator := stream.NewLabelAggregator(cfg, logger, db)
	go func() {
		logger.Printf("Starting label aggregator with auto-reconnect...")
		labelAggregator.StartWithRetry(ctx)
	}()
	defer labelAggregator.Close()

	// Processor (starts its own Firehose stats loop)
	proc := processor.NewProcessor(cfg, db, logger)
	proc.SetContext(ctx)
	defer proc.Close()

	// DB stats loop (independent of Firehose stats)
	if cfg.DBStatsVerbose {
		go reportDBStats(db, logger, cfg.StatsIntervalSec, ctx)
	}

	logger.Printf("Starting event processing...")
	eventCount := 0
	labelCount := 0

	for {
		select {
		case <-ctx.Done():
			logger.Printf("Shutting down. Total events: %d, Total label messages: %d", eventCount, labelCount)
			logger.Printf("Program finished")
			return

		case err := <-firehoseClient.Errors():
			logger.Printf("WARN: Firehose error: %v", err)

		case event, ok := <-firehoseClient.Events():
			if !ok {
				logger.Printf("Firehose event channel closed")
				return
			}
			if err := proc.ProcessRawEvent(event); err != nil {
				logger.Printf("WARN: failed to process event: %v", err)
				continue
			}
			eventCount++

		case message, ok := <-labelAggregator.Events():
			if !ok {
				logger.Printf("Label aggregator channel closed")
				continue
			}
			if err := proc.ProcessLabelMessage(message); err != nil {
				logger.Printf("WARN: failed to process label message: %v", err)
				continue
			}
			labelCount++
		}
	}
}

func reportDBStats(db *storage.DB, logger *log.Logger, interval int, ctx context.Context) {
	if interval <= 0 {
		interval = 10
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := db.GetStats()
			if err != nil {
				logger.Printf("WARN: failed to get DB stats: %v", err)
				continue
			}

			logger.Printf(
				"DB: posts=%d, actors=%d, labels=%d, history=%d, blocks=%d, "+
					"lists=%d, list_items=%d, list_blocks=%d, labelers=%d",
				stats["total_posts"],
				stats["total_actors"],
				stats["total_labels"],
				stats["total_history"],
				stats["total_blocks"],
				stats["total_lists"],
				stats["total_list_items"],
				stats["total_list_blocks"],
				stats["total_labelers"],
			)
		}
	}
}

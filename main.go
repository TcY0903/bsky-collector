package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bsky-collector/config"
	"bsky-collector/firehose"
	"bsky-collector/processor"
	"bsky-collector/storage"
)

func main() {
	cfg := config.Load()
	logger := log.New(os.Stdout, "[BSKY] ", log.LstdFlags)
	logger.Printf("🚀 Starting Bluesky post collector...")

	db, err := storage.NewDB(cfg.DatabaseURL)
	if err != nil {
		logger.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()
	logger.Printf("✅ Database connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("📡 Received signal: %v, shutting down...", sig)
		cancel()
	}()

	firehoseClient := firehose.NewClient(cfg.FirehoseURL, logger)

	go func() {
		logger.Printf("🔌 Starting Firehose client...")
		if err := firehoseClient.Start(ctx); err != nil {
			logger.Printf("❌ Firehose client error: %v", err)
		}
	}()

	proc := processor.NewProcessor(cfg, db, logger)
	proc.SetContext(ctx)
	defer proc.Close()

	go reportStats(db, logger, proc, cfg.StatsIntervalSec, ctx)

	logger.Printf("🔄 Starting event processing...")
	postCount := 0

	for {
		select {
		case <-ctx.Done():
			logger.Printf("📊 Shutting down. Total posts processed: %d", postCount)
			logger.Printf("👋 Program finished")
			return

		case err := <-firehoseClient.Errors():
			logger.Printf("⚠️ Firehose error: %v", err)

		case post, ok := <-firehoseClient.Posts():
			if !ok {
				logger.Printf("📭 Post channel closed")
				return
			}

			if err := proc.ProcessPost(post); err != nil {
				logger.Printf("❌ Failed to process post: %v", err)
				continue
			}

			postCount++

			if postCount%1000 == 0 {
				logger.Printf("📊 Processed %d posts", postCount)
			}
		}
	}
}

func reportStats(db *storage.DB, logger *log.Logger, proc *processor.Processor, interval int, ctx context.Context) {
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := db.GetStats()
			if err != nil {
				logger.Printf("⚠️ Failed to get stats: %v", err)
				continue
			}

			eventsPerSec, _ := proc.GetThroughput()

			logger.Printf("📊 Stats: Posts=%d, Actors=%d, Throughput=%.1f events/sec",
				stats["total_posts"],
				stats["total_actors"],
				eventsPerSec,
			)
		}
	}
}

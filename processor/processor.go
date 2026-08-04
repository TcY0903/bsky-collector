package processor

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/events"

	"bsky-collector/config"
	"bsky-collector/storage"
)

// Processor orchestrates the processing of Firehose events.
type Processor struct {
	cfg    *config.Config
	db     *storage.DB
	logger *log.Logger
	ctx    context.Context

	// Post processor
	postProcessor *PostProcessor

	// Fetchers
	actorFetcher *ActorFetcher
	labelFetcher *LabelFetcher

	// Throughput statistics
	statsMu      sync.RWMutex
	totalEvents  int64
	totalPosts   int64
	lastReset    time.Time
	eventsPerSec float64
}

// NewProcessor creates a new Processor.
func NewProcessor(cfg *config.Config, db *storage.DB, logger *log.Logger) *Processor {
	p := &Processor{
		cfg:       cfg,
		db:        db,
		logger:    logger,
		lastReset: time.Now(),
	}

	// Initialize post processor
	p.postProcessor = NewPostProcessor(p)

	// Initialize fetchers
	p.actorFetcher = NewActorFetcher(p)
	p.actorFetcher.Start()

	p.labelFetcher = NewLabelFetcher(p)
	p.labelFetcher.Start()

	go p.calculateThroughput()

	return p
}

// SetContext sets the context for cancellation.
func (p *Processor) SetContext(ctx context.Context) {
	p.ctx = ctx
}

// ProcessRawEvent processes a raw Firehose event.
func (p *Processor) ProcessRawEvent(evt *events.XRPCStreamEvent) error {
	if evt.RepoCommit == nil {
		return nil
	}

	p.RecordEvent()

	commit := evt.RepoCommit
	for _, op := range commit.Ops {
		if op.Path == "" {
			continue
		}

		// Only process post creation events
		if op.Action == "create" && strings.HasPrefix(op.Path, "app.bsky.feed.post/") {
			if err := p.postProcessor.ProcessPost(commit, op); err != nil {
				p.logger.Printf("⚠️ Failed to handle post: %v", err)
			}
		}
	}

	return nil
}

// Close gracefully shuts down the processor.
func (p *Processor) Close() {
	p.logger.Println("🛑 Processor shutting down...")

	if p.postProcessor != nil {
		p.postProcessor.Close()
	}
	if p.actorFetcher != nil {
		p.actorFetcher.Stop()
	}
	if p.labelFetcher != nil {
		p.labelFetcher.Stop()
	}

	p.logger.Println("✅ Processor closed")
}

// --- Throughput methods ---
func (p *Processor) calculateThroughput() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.statsMu.Lock()
			elapsed := time.Since(p.lastReset).Seconds()
			if elapsed > 0 {
				p.eventsPerSec = float64(p.totalEvents) / elapsed
			} else {
				p.eventsPerSec = 0
			}
			p.totalEvents = 0
			p.lastReset = time.Now()
			p.statsMu.Unlock()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Processor) RecordEvent() {
	p.statsMu.Lock()
	p.totalEvents++
	p.statsMu.Unlock()
}

func (p *Processor) RecordPost() {
	p.statsMu.Lock()
	p.totalPosts++
	p.statsMu.Unlock()
}

// GetThroughput returns the current throughput stats.
func (p *Processor) GetThroughput() (eventsPerSec float64, totalPosts int64) {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.eventsPerSec, p.totalPosts
}

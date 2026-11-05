package processor

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/indigo/events"

	"bsky-collector/config"
	"bsky-collector/storage"
)

// Processor orchestrates the processing of Firehose events and label stream events.
type Processor struct {
	cfg    *config.Config
	db     *storage.DB
	logger *log.Logger
	ctx    context.Context

	postProcessor  *PostProcessor
	blockProcessor *BlockProcessor
	listProcessor  *ListProcessor
	labelProcessor *LabelProcessor

	actorFetcher *ActorFetcher
	listFetcher  *ListFetcher
	postFetcher  *PostFetcher // NEW

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

	p.postProcessor = NewPostProcessor(p)
	p.blockProcessor = NewBlockProcessor(p)
	p.listProcessor = NewListProcessor(p)
	p.labelProcessor = NewLabelProcessor(p)

	p.actorFetcher = NewActorFetcher(p)
	p.actorFetcher.Start()

	p.listFetcher = NewListFetcher(p)
	p.listFetcher.Start()

	p.postFetcher = NewPostFetcher(p) // NEW
	p.postFetcher.Start()

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

		if strings.HasPrefix(op.Path, "app.bsky.feed.post/") {
			if err := p.postProcessor.ProcessPost(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle post: %v", err)
			}
			continue
		}

		if strings.HasPrefix(op.Path, "app.bsky.graph.block/") {
			if err := p.blockProcessor.ProcessBlock(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle block: %v", err)
			}
			continue
		}

		if strings.HasPrefix(op.Path, "app.bsky.graph.list/") ||
			strings.HasPrefix(op.Path, "app.bsky.graph.listitem/") ||
			strings.HasPrefix(op.Path, "app.bsky.graph.listblock/") {
			if err := p.listProcessor.ProcessListEvent(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle list event: %v", err)
			}
			continue
		}
	}

	return nil
}

// ProcessLabelMessage processes a raw message from the label aggregator.
func (p *Processor) ProcessLabelMessage(message []byte) error {
	return p.labelProcessor.ProcessLabelMessage(message)
}

// Close gracefully shuts down the processor.
func (p *Processor) Close() {
	p.logger.Println("Shutting down processor")

	if p.postProcessor != nil {
		p.postProcessor.Close()
	}
	if p.blockProcessor != nil {
		p.blockProcessor.Close()
	}
	if p.listProcessor != nil {
		p.listProcessor.Close()
	}
	if p.labelProcessor != nil {
		p.labelProcessor.Close()
	}
	if p.actorFetcher != nil {
		p.actorFetcher.Stop()
	}
	if p.listFetcher != nil {
		p.listFetcher.Stop()
	}
	if p.postFetcher != nil {
		p.postFetcher.Stop()
	}

	p.logger.Println("Processor closed")
}

// sanitizeText removes NULL bytes, invalid UTF-8 sequences and control characters
func sanitizeText(text string) string {
	if text == "" {
		return ""
	}

	// Remove NULL bytes
	text = strings.ReplaceAll(text, "\x00", "")

	// Ensure valid UTF-8
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, " ")
	}

	// Remove control characters except newline, tab, carriage return
	var builder strings.Builder
	for _, r := range text {
		if r == '\n' || r == '\t' || r == '\r' {
			builder.WriteRune(r)
		} else if r >= 32 {
			builder.WriteRune(r)
		}
	}
	text = builder.String()

	return strings.TrimSpace(text)
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

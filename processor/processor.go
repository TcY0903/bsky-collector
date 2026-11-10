package processor

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/indigo/events"

	"bsky-collector/config"
	"bsky-collector/storage"
)

// ==================== Types ====================

// eventStats tracks per-event-type counts for the current window.
type eventStats struct {
	PostCreate  int64
	PostUpdate  int64
	PostDelete  int64
	BlockCreate int64
	BlockDelete int64
	ListCreate  int64
	ListBlock   int64
	Others      int64
}

func (s eventStats) total() int64 {
	return s.PostCreate + s.PostUpdate + s.PostDelete +
		s.BlockCreate + s.BlockDelete +
		s.ListCreate + s.ListBlock + s.Others
}

func (s eventStats) String() string {
	return fmt.Sprintf(
		"post_create=%d post_update=%d post_delete=%d "+
			"block_create=%d block_delete=%d "+
			"list_create=%d list_block=%d others=%d",
		s.PostCreate, s.PostUpdate, s.PostDelete,
		s.BlockCreate, s.BlockDelete,
		s.ListCreate, s.ListBlock, s.Others,
	)
}

// Processor orchestrates Firehose event processing and label stream processing.
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
	postFetcher  *PostFetcher

	statsMu    sync.RWMutex
	window     eventStats
	cumulative eventStats
	lastReset  time.Time
}

// ==================== Lifecycle ====================

// NewProcessor creates a new Processor and starts all fetchers.
func NewProcessor(cfg *config.Config, db *storage.DB, logger *log.Logger) *Processor {
	p := &Processor{
		cfg:       cfg,
		db:        db,
		logger:    logger,
		lastReset: time.Now(),
	}

	// Event processors
	p.postProcessor = NewPostProcessor(p)
	p.blockProcessor = NewBlockProcessor(p)
	p.listProcessor = NewListProcessor(p)
	p.labelProcessor = NewLabelProcessor(p)

	// Fetchers
	p.actorFetcher = NewActorFetcher(p)
	p.actorFetcher.Start()

	p.listFetcher = NewListFetcher(p)
	p.listFetcher.Start()

	p.postFetcher = NewPostFetcher(p)
	p.postFetcher.Start()

	go p.runStatsLoop()

	return p
}

// SetContext sets the context for cancellation.
func (p *Processor) SetContext(ctx context.Context) {
	p.ctx = ctx
}

// Close gracefully shuts down all processors and fetchers.
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

// ==================== Event Dispatch ====================

// ProcessRawEvent processes a raw Firehose event.
func (p *Processor) ProcessRawEvent(evt *events.XRPCStreamEvent) error {
	if evt.RepoCommit == nil {
		return nil
	}

	commit := evt.RepoCommit
	for _, op := range commit.Ops {
		if op.Path == "" {
			continue
		}

		switch {
		case strings.HasPrefix(op.Path, "app.bsky.feed.post/"):
			p.recordPostEvent(op.Action)
			if err := p.postProcessor.ProcessPost(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle post: %v", err)
			}

		case strings.HasPrefix(op.Path, "app.bsky.graph.block/"):
			p.recordBlockEvent(op.Action)
			if err := p.blockProcessor.ProcessBlock(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle block: %v", err)
			}

		case strings.HasPrefix(op.Path, "app.bsky.graph.list/"),
			strings.HasPrefix(op.Path, "app.bsky.graph.listitem/"),
			strings.HasPrefix(op.Path, "app.bsky.graph.listblock/"):
			p.recordListEvent(op.Path, op.Action)
			if err := p.listProcessor.ProcessListEvent(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle list event: %v", err)
			}

		default:
			p.recordOtherEvent()
		}
	}

	return nil
}

// ProcessLabelMessage processes a raw message from the label aggregator.
func (p *Processor) ProcessLabelMessage(message []byte) error {
	return p.labelProcessor.ProcessLabelMessage(message)
}

// ==================== Event Counters ====================

func (p *Processor) recordPostEvent(action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	switch action {
	case "create":
		p.window.PostCreate++
		p.cumulative.PostCreate++
	case "update":
		p.window.PostUpdate++
		p.cumulative.PostUpdate++
	case "delete":
		p.window.PostDelete++
		p.cumulative.PostDelete++
	}
}

func (p *Processor) recordBlockEvent(action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	switch action {
	case "create":
		p.window.BlockCreate++
		p.cumulative.BlockCreate++
	case "delete":
		p.window.BlockDelete++
		p.cumulative.BlockDelete++
	}
}

func (p *Processor) recordListEvent(path, action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	switch {
	case strings.HasPrefix(path, "app.bsky.graph.list/"):
		if action == "create" {
			p.window.ListCreate++
			p.cumulative.ListCreate++
		}
	case strings.HasPrefix(path, "app.bsky.graph.listblock/"):
		if action == "create" {
			p.window.ListBlock++
			p.cumulative.ListBlock++
		}
	}
	// listitem events are counted as "others" for now
}

func (p *Processor) recordOtherEvent() {
	p.statsMu.Lock()
	p.window.Others++
	p.cumulative.Others++
	p.statsMu.Unlock()
}

// ==================== Stats Loop ====================

func (p *Processor) runStatsLoop() {
	interval := p.cfg.StatsIntervalSec
	if interval <= 0 {
		interval = 10
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if p.cfg.FirehoseStatsVerbose {
				p.printFirehoseStats(interval)
			} else {
				p.resetWindow()
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Processor) resetWindow() {
	p.statsMu.Lock()
	p.window = eventStats{}
	p.lastReset = time.Now()
	p.statsMu.Unlock()
}

func (p *Processor) printFirehoseStats(intervalSec int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	elapsed := time.Since(p.lastReset).Seconds()
	if elapsed <= 0 {
		elapsed = float64(intervalSec)
	}

	w := p.window

	rate := func(n int64) float64 {
		return float64(n) / elapsed
	}

	p.logger.Printf(
		"FIREHOSE:\n"+
			"    post_create=%d, post_update=%d, post_delete=%d |\n"+
			"    block_create=%d, block_delete=%d |\n"+
			"    list_create=%d, list_block=%d |\n"+
			"    others=%d |\n"+
			"    total=%d (%.1f/s)",
		w.PostCreate, w.PostUpdate, w.PostDelete,
		w.BlockCreate, w.BlockDelete,
		w.ListCreate, w.ListBlock,
		w.Others, w.total(), rate(w.total()),
	)

	// Reset window
	p.window = eventStats{}
	p.lastReset = time.Now()
}

// ==================== Sanitize ====================

// sanitizeText removes NULL bytes, invalid UTF-8 sequences and control characters.
func sanitizeText(text string) string {
	if text == "" {
		return ""
	}

	text = strings.ReplaceAll(text, "\x00", "")
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, " ")
	}

	var builder strings.Builder
	for _, r := range text {
		if r == '\n' || r == '\t' || r == '\r' {
			builder.WriteRune(r)
		} else if r >= 32 {
			builder.WriteRune(r)
		}
	}

	return strings.TrimSpace(builder.String())
}

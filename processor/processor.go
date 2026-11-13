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
	PostCreate       int64
	PostUpdate       int64
	PostDelete       int64
	BlockCreate      int64
	BlockDelete      int64
	ListCreate       int64
	ListBlock        int64
	ListItemCreate   int64
	ListItemDelete   int64
	LikeCreate       int64
	LikeDelete       int64
	RepostCreate     int64
	RepostDelete     int64
	FollowCreate     int64
	FollowDelete     int64
	ProfileCreate    int64
	ProfileUpdate    int64
	ProfileDelete    int64
	ThreadgateCreate int64
	ThreadgateDelete int64
	PostgateCreate   int64
	PostgateDelete   int64
	LabelerService   int64
	Others           int64
}

func (s eventStats) total() int64 {
	return s.PostCreate + s.PostUpdate + s.PostDelete +
		s.BlockCreate + s.BlockDelete +
		s.ListCreate + s.ListBlock +
		s.ListItemCreate + s.ListItemDelete +
		s.LikeCreate + s.LikeDelete +
		s.RepostCreate + s.RepostDelete +
		s.FollowCreate + s.FollowDelete +
		s.ProfileCreate + s.ProfileUpdate + s.ProfileDelete +
		s.ThreadgateCreate + s.ThreadgateDelete +
		s.PostgateCreate + s.PostgateDelete +
		s.LabelerService + s.Others
}

func (s eventStats) String() string {
	return fmt.Sprintf(
		"post_c=%d post_u=%d post_d=%d, "+
			"block_c=%d block_d=%d, "+
			"list_c=%d list_b=%d listitem_c=%d listitem_d=%d, "+
			"like_c=%d like_d=%d, "+
			"repost_c=%d repost_d=%d, "+
			"follow_c=%d follow_d=%d, "+
			"profile_c=%d profile_u=%d profile_d=%d, "+
			"threadgate_c=%d threadgate_d=%d, "+
			"postgate_c=%d postgate_d=%d, "+
			"labeler_service=%d, others=%d",
		s.PostCreate, s.PostUpdate, s.PostDelete,
		s.BlockCreate, s.BlockDelete,
		s.ListCreate, s.ListBlock, s.ListItemCreate, s.ListItemDelete,
		s.LikeCreate, s.LikeDelete,
		s.RepostCreate, s.RepostDelete,
		s.FollowCreate, s.FollowDelete,
		s.ProfileCreate, s.ProfileUpdate, s.ProfileDelete,
		s.ThreadgateCreate, s.ThreadgateDelete,
		s.PostgateCreate, s.PostgateDelete,
		s.LabelerService, s.Others,
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

		case strings.HasPrefix(op.Path, "app.bsky.graph.list/"):
			p.recordListEvent("list", op.Action)
			if err := p.listProcessor.ProcessListEvent(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle list event: %v", err)
			}

		case strings.HasPrefix(op.Path, "app.bsky.graph.listitem/"):
			p.recordListEvent("listitem", op.Action)
			if err := p.listProcessor.ProcessListEvent(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle list event: %v", err)
			}

		case strings.HasPrefix(op.Path, "app.bsky.graph.listblock/"):
			p.recordListEvent("listblock", op.Action)
			if err := p.listProcessor.ProcessListEvent(commit, op); err != nil {
				p.logger.Printf("WARN: failed to handle list event: %v", err)
			}

		case strings.HasPrefix(op.Path, "app.bsky.feed.like/"):
			p.recordFeedEvent("like", op.Action)

		case strings.HasPrefix(op.Path, "app.bsky.feed.repost/"):
			p.recordFeedEvent("repost", op.Action)

		case strings.HasPrefix(op.Path, "app.bsky.graph.follow/"):
			p.recordFeedEvent("follow", op.Action)

		case strings.HasPrefix(op.Path, "app.bsky.actor.profile/"):
			p.recordProfileEvent(op.Action)

		case strings.HasPrefix(op.Path, "app.bsky.feed.threadgate/"):
			p.recordGateEvent("threadgate", op.Action)

		case strings.HasPrefix(op.Path, "app.bsky.feed.postgate/"):
			p.recordGateEvent("postgate", op.Action)

		case strings.HasPrefix(op.Path, "app.bsky.labeler.service/"):
			p.recordLabelerService()

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

func (p *Processor) recordListEvent(kind, action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	if action != "create" && action != "delete" {
		return
	}
	switch kind {
	case "list":
		if action == "create" {
			p.window.ListCreate++
			p.cumulative.ListCreate++
		}
	case "listitem":
		if action == "create" {
			p.window.ListItemCreate++
			p.cumulative.ListItemCreate++
		} else {
			p.window.ListItemDelete++
			p.cumulative.ListItemDelete++
		}
	case "listblock":
		if action == "create" {
			p.window.ListBlock++
			p.cumulative.ListBlock++
		}
	}
}

func (p *Processor) recordFeedEvent(kind, action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	if action != "create" && action != "delete" {
		return
	}
	inc := func(dst *int64) {
		*dst++
	}
	switch kind {
	case "like":
		if action == "create" {
			inc(&p.window.LikeCreate)
			inc(&p.cumulative.LikeCreate)
		} else {
			inc(&p.window.LikeDelete)
			inc(&p.cumulative.LikeDelete)
		}
	case "repost":
		if action == "create" {
			inc(&p.window.RepostCreate)
			inc(&p.cumulative.RepostCreate)
		} else {
			inc(&p.window.RepostDelete)
			inc(&p.cumulative.RepostDelete)
		}
	case "follow":
		if action == "create" {
			inc(&p.window.FollowCreate)
			inc(&p.cumulative.FollowCreate)
		} else {
			inc(&p.window.FollowDelete)
			inc(&p.cumulative.FollowDelete)
		}
	}
}

func (p *Processor) recordProfileEvent(action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	switch action {
	case "create":
		p.window.ProfileCreate++
		p.cumulative.ProfileCreate++
	case "update":
		p.window.ProfileUpdate++
		p.cumulative.ProfileUpdate++
	case "delete":
		p.window.ProfileDelete++
		p.cumulative.ProfileDelete++
	}
}

func (p *Processor) recordGateEvent(kind, action string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	switch kind {
	case "threadgate":
		switch action {
		case "create":
			p.window.ThreadgateCreate++
			p.cumulative.ThreadgateCreate++
		case "delete":
			p.window.ThreadgateDelete++
			p.cumulative.ThreadgateDelete++
		}
	case "postgate":
		switch action {
		case "create":
			p.window.PostgateCreate++
			p.cumulative.PostgateCreate++
		case "delete":
			p.window.PostgateDelete++
			p.cumulative.PostgateDelete++
		}
	}
}

func (p *Processor) recordLabelerService() {
	p.statsMu.Lock()
	p.window.LabelerService++
	p.cumulative.LabelerService++
	p.statsMu.Unlock()
}

func (p *Processor) recordOtherEvent() {
	p.statsMu.Lock()
	p.window.Others++
	p.cumulative.Others++
	p.statsMu.Unlock()
}

// ==================== Queue Stats ====================
// FetcherQueueStats returns (depth, capacity) for each fetcher queue.
func (p *Processor) FetcherQueueStats() (postDepth, postCap, listDepth, listCap, actorDepth, actorCap int) {
	if p.postFetcher != nil {
		postDepth = p.postFetcher.QueueDepth()
		postCap = p.postFetcher.QueueCapacity()
	}
	if p.listFetcher != nil {
		listDepth = p.listFetcher.QueueDepth()
		listCap = p.listFetcher.QueueCapacity()
	}
	if p.actorFetcher != nil {
		actorDepth = p.actorFetcher.QueueDepth()
		actorCap = p.actorFetcher.QueueCapacity()
	}
	return
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
	totalRate := float64(w.total()) / elapsed

	p.logger.Printf(
		"\nFIREHOSE in last %d seconds:\n"+
			"    post_create = %d,   post_update = %d,   post_delete = %d,\n"+
			"    block_create = %d,   block_delete = %d,\n"+
			"    list_create = %d,   list_block = %d,   listitem_create = %d,   listitem_delete = %d,\n"+
			"    like_create = %d,   like_delete = %d,\n"+
			"    repost_create = %d,   repost_delete = %d,\n"+
			"    follow_create = %d,   follow_delete = %d,\n"+
			"    profile_create = %d,   profile_update = %d,   profile_delete = %d,\n"+
			"    threadgate_create = %d,   threadgate_delete = %d,\n"+
			"    postgate_create = %d,   postgate_delete = %d,\n"+
			"    labeler_service = %d,\n"+
			"    others = %d,\n"+
			"    total = %d (%.1f/s)",
		intervalSec,
		w.PostCreate, w.PostUpdate, w.PostDelete,
		w.BlockCreate, w.BlockDelete,
		w.ListCreate, w.ListBlock, w.ListItemCreate, w.ListItemDelete,
		w.LikeCreate, w.LikeDelete,
		w.RepostCreate, w.RepostDelete,
		w.FollowCreate, w.FollowDelete,
		w.ProfileCreate, w.ProfileUpdate, w.ProfileDelete,
		w.ThreadgateCreate, w.ThreadgateDelete,
		w.PostgateCreate, w.PostgateDelete,
		w.LabelerService,
		w.Others,
		w.total(), totalRate,
	)

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

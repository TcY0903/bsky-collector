package processor

import (
	"context"
	"log"
	"sync"
	"time"

	"bsky-collector/config"
	"bsky-collector/models"
	"bsky-collector/storage"
)

// Processor orchestrates the processing of Firehose events.
type Processor struct {
	cfg    *config.Config
	db     *storage.DB
	logger *log.Logger
	ctx    context.Context

	// Post batch processing
	postBatch   []*models.Post
	batchMu     sync.Mutex
	batchSize   int
	batchTicker *time.Ticker
	flushChan   chan struct{}
	stopFlusher chan struct{}
	flusherWg   sync.WaitGroup
	flusherDone chan struct{}

	// Actor fetching
	actorQueue chan string
	actorWg    sync.WaitGroup
	actorStop  chan struct{}

	// Actor cache
	actorCache map[string]bool
	cacheMu    sync.Mutex

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
		cfg:         cfg,
		db:          db,
		logger:      logger,
		postBatch:   make([]*models.Post, 0, 500),
		batchSize:   500,
		batchTicker: time.NewTicker(3 * time.Second),
		flushChan:   make(chan struct{}, 1),
		stopFlusher: make(chan struct{}),
		flusherDone: make(chan struct{}),
		actorQueue:  make(chan string, 2000),
		actorStop:   make(chan struct{}),
		actorCache:  make(map[string]bool),
		lastReset:   time.Now(),
	}

	p.flusherWg.Add(1)
	go p.batchFlusher()

	p.actorWg.Add(1)
	go p.actorWorker()

	go p.calculateThroughput()
	go p.monitorActorQueue()

	return p
}

// SetContext sets the context for cancellation.
func (p *Processor) SetContext(ctx context.Context) {
	p.ctx = ctx
}

// ProcessPost processes a single post.
func (p *Processor) ProcessPost(post *models.Post) error {
	p.RecordEvent()
	p.RecordPost()
	p.addPostToBatch(post)
	return nil
}

// Close gracefully shuts down the processor.
func (p *Processor) Close() {
	p.logger.Println("🛑 Processor shutting down...")

	close(p.stopFlusher)

	close(p.actorStop)
	p.actorWg.Wait()

	p.batchMu.Lock()
	if len(p.postBatch) > 0 {
		batch := p.postBatch
		p.postBatch = make([]*models.Post, 0, p.batchSize)
		p.batchMu.Unlock()
		p.enrichPosts(batch)
		go p.queueActorsForBatch(batch)
		if err := p.db.SavePostsBatch(batch); err != nil {
			p.logger.Printf("⚠️ Failed to save final batch: %v", err)
		} else {
			p.logger.Printf("📦 Saved %d posts in final batch", len(batch))
		}
	} else {
		p.batchMu.Unlock()
	}

	select {
	case <-p.flusherDone:
		p.logger.Println("✅ Batch flusher finished")
	case <-time.After(5 * time.Second):
		p.logger.Println("⚠️ Batch flusher timeout")
	}

	p.logger.Println("✅ Processor closed")
}

// --- Internal batch methods ---

func (p *Processor) batchFlusher() {
	defer func() {
		close(p.flusherDone)
		p.flusherWg.Done()
	}()

	for {
		select {
		case <-p.batchTicker.C:
			p.flushBatch()
		case <-p.flushChan:
			p.flushBatch()
		case <-p.stopFlusher:
			p.flushBatch()
			return
		}
	}
}

func (p *Processor) flushBatch() {
	p.batchMu.Lock()
	if len(p.postBatch) == 0 {
		p.batchMu.Unlock()
		return
	}

	batch := p.postBatch
	p.postBatch = make([]*models.Post, 0, p.batchSize)
	p.batchMu.Unlock()

	p.cacheMu.Lock()
	p.actorCache = make(map[string]bool)
	p.cacheMu.Unlock()

	p.enrichPosts(batch)

	if err := p.db.SavePostsBatch(batch); err != nil {
		p.logger.Printf("⚠️ Failed to save batch of %d posts: %v", len(batch), err)
	} else {
		p.logger.Printf("📦 Saved %d posts in batch", len(batch))
	}

	go p.queueActorsForBatch(batch)
}

func (p *Processor) addPostToBatch(post *models.Post) {
	p.batchMu.Lock()
	p.postBatch = append(p.postBatch, post)
	batchSize := len(p.postBatch)
	p.batchMu.Unlock()

	if batchSize >= p.batchSize {
		select {
		case p.flushChan <- struct{}{}:
		default:
		}
	}
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
		case <-p.stopFlusher:
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

func (p *Processor) monitorActorQueue() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			queueLen := len(p.actorQueue)
			if queueLen > 0 {
				p.logger.Printf("📊 Actor queue length: %d", queueLen)
			}
		case <-p.stopFlusher:
			return
		}
	}
}

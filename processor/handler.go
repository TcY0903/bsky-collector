package processor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log"
	"sync"
	"time"

	"bsky-collector/config"
	"bsky-collector/models"
	"bsky-collector/storage"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipld/go-car/v2"
)

// Handler processes posts from the Firehose client.
type Handler struct {
	cfg    *config.Config
	db     *storage.DB
	logger *log.Logger

	// Batch processing fields
	postBatch   []*models.Post
	batchMu     sync.Mutex
	batchSize   int
	batchTicker *time.Ticker
	flushChan   chan struct{}
	stopFlusher chan struct{}
	flusherWg   sync.WaitGroup
	flusherDone chan struct{}

	// Throughput statistics
	statsMu      sync.RWMutex
	totalEvents  int64
	totalPosts   int64
	lastReset    time.Time
	eventsPerSec float64
}

// CommitEvent represents the structure stored in Post.Payload with blocks as base64 string.
type CommitEvent struct {
	Repo string `json:"repo"`
	Rev  string `json:"rev"`
	Seq  int64  `json:"seq"`
	Time string `json:"time"`
	Ops  []struct {
		Action string `json:"action"`
		Path   string `json:"path"`
		Cid    string `json:"cid"`
	} `json:"ops"`
	BlocksBase64 string `json:"blocks_base64"`
}

// NewHandler creates a new event handler with batch processing.
func NewHandler(cfg *config.Config, db *storage.DB, logger *log.Logger) *Handler {
	h := &Handler{
		cfg:         cfg,
		db:          db,
		logger:      logger,
		postBatch:   make([]*models.Post, 0, 1000),
		batchSize:   1000,
		batchTicker: time.NewTicker(5 * time.Second),
		flushChan:   make(chan struct{}, 1),
		stopFlusher: make(chan struct{}),
		flusherDone: make(chan struct{}),
		lastReset:   time.Now(),
	}

	h.flusherWg.Add(1)
	go h.batchFlusher()
	go h.calculateThroughput()

	return h
}

// batchFlusher runs in a goroutine and flushes the batch periodically.
func (h *Handler) batchFlusher() {
	defer func() {
		close(h.flusherDone)
		h.flusherWg.Done()
	}()

	for {
		select {
		case <-h.batchTicker.C:
			h.flushBatch()
		case <-h.flushChan:
			h.flushBatch()
		case <-h.stopFlusher:
			h.flushBatch()
			return
		}
	}
}

// flushBatch writes the current batch of posts to the database.
func (h *Handler) flushBatch() {
	h.batchMu.Lock()
	if len(h.postBatch) == 0 {
		h.batchMu.Unlock()
		return
	}

	batch := h.postBatch
	h.postBatch = make([]*models.Post, 0, h.batchSize)
	h.batchMu.Unlock()

	// Enrich posts with content from CAR files
	h.enrichPosts(batch)

	if err := h.db.SavePostsBatch(batch); err != nil {
		h.logger.Printf("⚠️ Failed to save batch of %d posts: %v", len(batch), err)
	} else {
		h.logger.Printf("📦 Saved %d posts in batch", len(batch))
	}
}

// enrichPosts parses CAR files and extracts post text, createdAt, and reply info.
func (h *Handler) enrichPosts(posts []*models.Post) {
	for _, post := range posts {
		// Parse the commit event from payload
		var commit CommitEvent
		if err := json.Unmarshal(post.Payload, &commit); err != nil {
			h.logger.Printf("⚠️ Failed to unmarshal commit: %v", err)
			continue
		}

		// Skip if no blocks
		if commit.BlocksBase64 == "" {
			continue
		}

		// Decode base64 to []byte
		blocksData, err := base64.StdEncoding.DecodeString(commit.BlocksBase64)
		if err != nil {
			h.logger.Printf("⚠️ Failed to decode base64 blocks: %v", err)
			continue
		}

		// Use the CID from the post
		targetCID := post.Cid
		if targetCID == "" {
			continue
		}

		// Parse CAR file
		blockReader, err := car.NewBlockReader(bytes.NewReader(blocksData))
		if err != nil {
			h.logger.Printf("⚠️ Failed to create block reader: %v", err)
			continue
		}

		// Iterate through blocks
		var text string
		var createdAt time.Time
		var replyParent string
		var replyRoot string

		for {
			block, err := blockReader.Next()
			if err != nil {
				break
			}

			if block.Cid().String() != targetCID {
				continue
			}

			var record map[string]interface{}
			if err := cbor.Unmarshal(block.RawData(), &record); err != nil {
				h.logger.Printf("⚠️ Failed to decode CBOR: %v", err)
				break
			}

			// Extract text
			if t, ok := record["text"].(string); ok {
				text = t
			}

			// Extract createdAt
			if c, ok := record["createdAt"].(string); ok {
				if t, err := time.Parse(time.RFC3339, c); err == nil {
					createdAt = t
				}
			}

			// Extract reply information
			if replyVal, ok := record["reply"]; ok {
				replyMap := make(map[string]interface{})
				switch v := replyVal.(type) {
				case map[string]interface{}:
					replyMap = v
				case map[interface{}]interface{}:
					for key, val := range v {
						if strKey, ok := key.(string); ok {
							replyMap[strKey] = val
						}
					}
				default:
					h.logger.Printf("⚠️ Unknown reply type for %s: %T", post.RKey, replyVal)
				}

				// Extract parent
				if parent, ok := replyMap["parent"]; ok {
					parentMap := make(map[string]interface{})
					switch v := parent.(type) {
					case map[string]interface{}:
						parentMap = v
					case map[interface{}]interface{}:
						for key, val := range v {
							if strKey, ok := key.(string); ok {
								parentMap[strKey] = val
							}
						}
					}
					if uri, ok := parentMap["uri"].(string); ok {
						replyParent = uri
					}
					if cid, ok := parentMap["cid"].(string); ok {
						_ = cid // You could store this if needed
					}
				}

				// Extract root
				if root, ok := replyMap["root"]; ok {
					rootMap := make(map[string]interface{})
					switch v := root.(type) {
					case map[string]interface{}:
						rootMap = v
					case map[interface{}]interface{}:
						for key, val := range v {
							if strKey, ok := key.(string); ok {
								rootMap[strKey] = val
							}
						}
					}
					if uri, ok := rootMap["uri"].(string); ok {
						replyRoot = uri
					}
					if cid, ok := rootMap["cid"].(string); ok {
						_ = cid
					}
				}
			}

			break // Found the target block, exit the loop
		}

		post.Text = text
		post.CreatedAt = createdAt
		post.ReplyParent = replyParent
		post.ReplyRoot = replyRoot

		// Free up memory
		post.Payload = nil
	}
}

// addPostToBatch adds a post to the batch and triggers flush if threshold is met.
func (h *Handler) addPostToBatch(post *models.Post) {
	h.batchMu.Lock()
	h.postBatch = append(h.postBatch, post)
	batchSize := len(h.postBatch)
	h.batchMu.Unlock()

	if batchSize >= h.batchSize {
		select {
		case h.flushChan <- struct{}{}:
		default:
		}
	}
}

// calculateThroughput runs in a goroutine and updates throughput stats every 10 seconds.
func (h *Handler) calculateThroughput() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.statsMu.Lock()
			elapsed := time.Since(h.lastReset).Seconds()
			if elapsed > 0 {
				h.eventsPerSec = float64(h.totalEvents) / elapsed
			} else {
				h.eventsPerSec = 0
			}
			h.totalEvents = 0
			h.lastReset = time.Now()
			h.statsMu.Unlock()
		case <-h.stopFlusher:
			return
		}
	}
}

// RecordEvent increments the event counter.
func (h *Handler) RecordEvent() {
	h.statsMu.Lock()
	h.totalEvents++
	h.statsMu.Unlock()
}

// RecordPost increments the post counter.
func (h *Handler) RecordPost() {
	h.statsMu.Lock()
	h.totalPosts++
	h.statsMu.Unlock()
}

// GetThroughput returns the current throughput stats.
func (h *Handler) GetThroughput() (eventsPerSec float64, totalPosts int64) {
	h.statsMu.RLock()
	defer h.statsMu.RUnlock()
	return h.eventsPerSec, h.totalPosts
}

// ProcessPost processes a single post from the Firehose client.
func (h *Handler) ProcessPost(post *models.Post) error {
	h.RecordEvent()
	h.RecordPost()
	h.addPostToBatch(post)
	return nil
}

// Close waits for all workers to finish.
func (h *Handler) Close() {
	h.logger.Println("🛑 Handler shutting down...")

	close(h.stopFlusher)

	h.batchMu.Lock()
	if len(h.postBatch) > 0 {
		h.logger.Printf("📦 Flushing remaining %d posts", len(h.postBatch))
		batch := h.postBatch
		h.postBatch = make([]*models.Post, 0, h.batchSize)
		h.batchMu.Unlock()
		h.enrichPosts(batch)
		if err := h.db.SavePostsBatch(batch); err != nil {
			h.logger.Printf("⚠️ Failed to save final batch: %v", err)
		} else {
			h.logger.Printf("📦 Saved %d posts in final batch", len(batch))
		}
	} else {
		h.batchMu.Unlock()
	}

	select {
	case <-h.flusherDone:
		h.logger.Println("✅ Batch flusher finished")
	case <-time.After(5 * time.Second):
		h.logger.Println("⚠️ Batch flusher timeout")
	}

	h.logger.Println("✅ Handler closed")
}

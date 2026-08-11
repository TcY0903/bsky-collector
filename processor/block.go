package processor

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/fxamacker/cbor/v2"
	"github.com/ipld/go-car/v2"

	"bsky-collector/models"
)

// BlockProcessor handles batch processing of block events.
type BlockProcessor struct {
	p           *Processor
	blockBatch  []*models.Block
	deleteBatch []string
	batchMu     sync.Mutex
	batchSize   int
	ticker      *time.Ticker
	stopCh      chan struct{}
	closeOnce   sync.Once
}

// NewBlockProcessor creates a new BlockProcessor.
func NewBlockProcessor(p *Processor) *BlockProcessor {
	bp := &BlockProcessor{
		p:           p,
		blockBatch:  make([]*models.Block, 0, 500),
		deleteBatch: make([]string, 0, 100),
		batchSize:   500,
		ticker:      time.NewTicker(3 * time.Second),
		stopCh:      make(chan struct{}),
	}
	go bp.autoFlush()
	return bp
}

func (bp *BlockProcessor) autoFlush() {
	for {
		select {
		case <-bp.ticker.C:
			bp.flushBlocks()
			bp.flushDeletes()
		case <-bp.stopCh:
			return
		}
	}
}

// ProcessBlock processes a block event (create or delete).
func (bp *BlockProcessor) ProcessBlock(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	switch op.Action {
	case "create":
		return bp.handleCreate(commit, op)
	case "delete":
		return bp.handleDelete(commit, op)
	default:
		return nil
	}
}

// handleCreate handles a block creation event.
func (bp *BlockProcessor) handleCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil
	}
	rkey := parts[1]

	var subjectDID string
	var createdAt time.Time
	var cidStr string

	if op.Cid != nil {
		cidStr = op.Cid.String()
	}

	// Parse CAR to extract subject and created_at
	if len(commit.Blocks) > 0 && cidStr != "" {
		blockReader, err := car.NewBlockReader(bytes.NewReader(commit.Blocks))
		if err == nil {
			for {
				block, err := blockReader.Next()
				if err != nil {
					break
				}
				if block.Cid().String() == cidStr {
					var record map[string]interface{}
					if err := cbor.Unmarshal(block.RawData(), &record); err == nil {
						if subject, ok := record["subject"].(string); ok {
							subjectDID = subject
						}
						if createdAtStr, ok := record["createdAt"].(string); ok {
							if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
								createdAt = t.UTC()
							}
						}
					}
					break
				}
			}
		}
	}

	if subjectDID == "" {
		bp.p.logger.Printf("⚠️ Could not extract subject from block event: %s", op.Path)
		return nil
	}

	block := &models.Block{
		URI:        fmt.Sprintf("at://%s/%s", commit.Repo, op.Path),
		DID:        commit.Repo,
		SubjectDID: subjectDID,
		RKey:       rkey,
		CID:        cidStr,
		CreatedAt:  createdAt.UTC(),
		ReceivedAt: time.Now().UTC(),
		Status:     "active",
	}

	// Queue both actors for fetching (blocker and blockee)
	if bp.p.actorFetcher != nil {
		bp.p.actorFetcher.QueueDID(commit.Repo) // The user who performed the block
		bp.p.actorFetcher.QueueDID(subjectDID)  // The user who was blocked
	}

	bp.batchMu.Lock()
	bp.blockBatch = append(bp.blockBatch, block)
	if len(bp.blockBatch) >= bp.batchSize {
		batch := bp.blockBatch
		bp.blockBatch = make([]*models.Block, 0, bp.batchSize)
		bp.batchMu.Unlock()
		bp.flushBlocksWithBatch(batch)
	} else {
		bp.batchMu.Unlock()
	}

	return nil
}

// handleDelete handles a block deletion event.
func (bp *BlockProcessor) handleDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	// Check if the block exists in our database
	existingBlock, err := bp.p.db.GetBlockByURI(uri)
	if err != nil {
		return err
	}

	if existingBlock == nil {
		// Block was created before we started listening
		// bp.p.logger.Printf("📭 Received delete for unknown block: %s", uri)
		return nil
	}

	// Add to delete batch
	bp.batchMu.Lock()
	bp.deleteBatch = append(bp.deleteBatch, uri)
	if len(bp.deleteBatch) >= bp.batchSize/5 {
		batch := bp.deleteBatch
		bp.deleteBatch = make([]string, 0, bp.batchSize/5)
		bp.batchMu.Unlock()
		bp.flushDeletesWithBatch(batch)
	} else {
		bp.batchMu.Unlock()
	}

	return nil
}

// flushBlocks flushes pending block batch.
func (bp *BlockProcessor) flushBlocks() {
	bp.batchMu.Lock()
	if len(bp.blockBatch) == 0 {
		bp.batchMu.Unlock()
		return
	}
	batch := bp.blockBatch
	bp.blockBatch = make([]*models.Block, 0, bp.batchSize)
	bp.batchMu.Unlock()
	bp.flushBlocksWithBatch(batch)
}

// flushBlocksWithBatch saves a batch of blocks to the database.
func (bp *BlockProcessor) flushBlocksWithBatch(batch []*models.Block) {
	if len(batch) == 0 {
		return
	}
	if err := bp.p.db.SaveBlocksBatch(batch); err != nil {
		bp.p.logger.Printf("⚠️ Failed to save batch of %d blocks: %v", len(batch), err)
	} else {
		// bp.p.logger.Printf("🔒 Saved %d blocks in batch", len(batch))
	}
}

// flushDeletes flushes pending delete batch.
func (bp *BlockProcessor) flushDeletes() {
	bp.batchMu.Lock()
	if len(bp.deleteBatch) == 0 {
		bp.batchMu.Unlock()
		return
	}
	batch := bp.deleteBatch
	bp.deleteBatch = make([]string, 0, bp.batchSize/5)
	bp.batchMu.Unlock()
	bp.flushDeletesWithBatch(batch)
}

// flushDeletesWithBatch marks multiple blocks as deleted in a single transaction.
func (bp *BlockProcessor) flushDeletesWithBatch(uris []string) {
	if len(uris) == 0 {
		return
	}
	if err := bp.p.db.DeleteBlocksBatch(uris); err != nil {
		bp.p.logger.Printf("⚠️ Failed to delete batch of %d blocks: %v", len(uris), err)
	} else {
		// bp.p.logger.Printf("🔓 Deleted %d blocks in batch", len(uris))
	}
}

// Close shuts down the BlockProcessor.
func (bp *BlockProcessor) Close() {
	bp.closeOnce.Do(func() {
		close(bp.stopCh)
		bp.ticker.Stop()
		bp.flushBlocks()
		bp.flushDeletes()
	})
}

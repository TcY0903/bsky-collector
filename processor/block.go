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

// ==================== Types ====================

// BlockProcessor handles batch processing of block events.
type BlockProcessor struct {
	p           *Processor
	blockBatch  []*models.Block
	deleteBatch []string
	batchMu     sync.Mutex

	batchSize int // block batch threshold; delete uses batchSize/5
	ticker    *time.Ticker
	stopCh    chan struct{}
	closeOnce sync.Once
	verbose   bool
}

// ==================== Lifecycle ====================

// NewBlockProcessor creates a new BlockProcessor using shared processor config.
func NewBlockProcessor(p *Processor) *BlockProcessor {
	batchSize := p.cfg.ProcessorBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	tickerSec := p.cfg.ProcessorTickerSec
	if tickerSec <= 0 {
		tickerSec = 3
	}

	bp := &BlockProcessor{
		p:           p,
		blockBatch:  make([]*models.Block, 0, batchSize),
		deleteBatch: make([]string, 0, batchSize/5),
		batchSize:   batchSize,
		ticker:      time.NewTicker(time.Duration(tickerSec) * time.Second),
		stopCh:      make(chan struct{}),
		verbose:     p.cfg.BlockProcessorVerbose,
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

// Close shuts down the BlockProcessor.
func (bp *BlockProcessor) Close() {
	bp.closeOnce.Do(func() {
		close(bp.stopCh)
		bp.ticker.Stop()
		bp.flushBlocks()
		bp.flushDeletes()
	})
}

// ==================== Dispatch ====================

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

// ==================== Handlers ====================

// handleCreate handles a block creation event.
func (bp *BlockProcessor) handleCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	block, err := bp.buildBlock(commit, op)
	if err != nil || block == nil {
		return err
	}

	// Queue both actors (blocker and blockee) for fetching
	if bp.p.actorFetcher != nil {
		bp.p.actorFetcher.QueueDID(block.DID)
		bp.p.actorFetcher.QueueDID(block.SubjectDID)
	}

	bp.logf("Block create: blocker=%s blockee=%s rkey=%s", block.DID, block.SubjectDID, block.RKey)
	bp.addToBlockBatch(block)
	return nil
}

// handleDelete handles a block deletion event.
func (bp *BlockProcessor) handleDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existingBlock, err := bp.p.db.GetBlockByURI(uri)
	if err != nil {
		return err
	}
	if existingBlock == nil {
		return nil
	}

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

// addToBlockBatch appends a block to the batch, flushing if full.
func (bp *BlockProcessor) addToBlockBatch(block *models.Block) {
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
}

// ==================== Build ====================

// buildBlock builds a Block model from Firehose data by parsing the CAR file.
// Returns nil if the subject cannot be extracted.
func (bp *BlockProcessor) buildBlock(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) (*models.Block, error) {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil, nil
	}
	rkey := parts[1]

	cidStr := ""
	if op.Cid != nil {
		cidStr = op.Cid.String()
	}

	var subjectDID string
	var createdAt time.Time

	if len(commit.Blocks) > 0 && cidStr != "" {
		blockReader, err := car.NewBlockReader(bytes.NewReader(commit.Blocks))
		if err == nil {
			for {
				carBlock, err := blockReader.Next()
				if err != nil {
					break
				}
				if carBlock.Cid().String() == cidStr {
					var record map[string]interface{}
					if err := cbor.Unmarshal(carBlock.RawData(), &record); err == nil {
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
		bp.warnf("WARN: could not extract subject from block event: %s", op.Path)
		return nil, nil
	}

	return &models.Block{
		URI:        fmt.Sprintf("at://%s/%s", commit.Repo, op.Path),
		DID:        commit.Repo,
		SubjectDID: subjectDID,
		RKey:       rkey,
		CID:        cidStr,
		CreatedAt:  createdAt.UTC(),
		ReceivedAt: time.Now().UTC(),
		Status:     "active",
	}, nil
}

// ==================== Flush ====================

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

func (bp *BlockProcessor) flushBlocksWithBatch(batch []*models.Block) {
	if len(batch) == 0 {
		return
	}
	if err := bp.p.db.SaveBlocksBatch(batch); err != nil {
		bp.warnf("WARN: failed to save batch of %d blocks: %v", len(batch), err)
	} else {
		bp.logf("Saved %d blocks", len(batch))
	}
}

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

func (bp *BlockProcessor) flushDeletesWithBatch(uris []string) {
	if len(uris) == 0 {
		return
	}
	if err := bp.p.db.DeleteBlocksBatch(uris); err != nil {
		bp.warnf("WARN: failed to delete batch of %d blocks: %v", len(uris), err)
	} else {
		bp.logf("Deleted %d blocks", len(uris))
	}
}

// ==================== Logging ====================

func (bp *BlockProcessor) logf(format string, args ...interface{}) {
	if bp.verbose {
		bp.p.logger.Printf(format, args...)
	}
}

func (bp *BlockProcessor) warnf(format string, args ...interface{}) {
	bp.p.logger.Printf(format, args...)
}

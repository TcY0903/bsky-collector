package processor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/fxamacker/cbor/v2"
	"github.com/ipld/go-car/v2"
	"gorm.io/datatypes"

	"bsky-collector/models"
)

// ==================== Types ====================

// PostProcessor handles batch processing of post events.
type PostProcessor struct {
	p           *Processor
	mediaParser *MediaParser

	postBatch   []*models.Post
	deleteBatch []string
	batchMu     sync.Mutex

	batchSize int // post batch threshold; delete uses batchSize/5
	ticker    *time.Ticker
	stopCh    chan struct{}
	closeOnce sync.Once
	verbose   bool
}

// ==================== Lifecycle ====================

// NewPostProcessor creates a new PostProcessor using shared processor config.
func NewPostProcessor(p *Processor) *PostProcessor {
	batchSize := p.cfg.ProcessorBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	tickerSec := p.cfg.ProcessorTickerSec
	if tickerSec <= 0 {
		tickerSec = 3
	}

	pp := &PostProcessor{
		p:           p,
		mediaParser: NewMediaParser(p.logger),
		postBatch:   make([]*models.Post, 0, batchSize),
		deleteBatch: make([]string, 0, batchSize/5),
		batchSize:   batchSize,
		ticker:      time.NewTicker(time.Duration(tickerSec) * time.Second),
		stopCh:      make(chan struct{}),
		verbose:     p.cfg.PostProcessorVerbose,
	}
	go pp.autoFlush()
	return pp
}

func (pp *PostProcessor) autoFlush() {
	for {
		select {
		case <-pp.ticker.C:
			pp.flushPosts()
			pp.flushDeletes()
		case <-pp.stopCh:
			return
		}
	}
}

// Close shuts down the PostProcessor.
func (pp *PostProcessor) Close() {
	pp.closeOnce.Do(func() {
		close(pp.stopCh)
		pp.ticker.Stop()
		pp.flushPosts()
		pp.flushDeletes()
	})
}

// ==================== Dispatch ====================

// ProcessPost processes a post event (create, update, or delete).
func (pp *PostProcessor) ProcessPost(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	switch op.Action {
	case "create":
		return pp.handleCreate(commit, op)
	case "update":
		return pp.handleUpdate(commit, op)
	case "delete":
		return pp.handleDelete(commit, op)
	default:
		return nil
	}
}

// ==================== Handlers ====================

func (pp *PostProcessor) handleCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	post, err := pp.buildPost(commit, op)
	if err != nil {
		return err
	}
	post.Status = "active"
	post.NeedActor = true
	pp.addToPostBatch(post)
	return nil
}

func (pp *PostProcessor) handleUpdate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existingPost, err := pp.p.db.GetPostByURI(uri)
	if err != nil {
		return err
	}

	// Case 1: Post was created before we started listening
	if existingPost == nil {
		post, err := pp.buildPost(commit, op)
		if err != nil {
			return err
		}
		post.Status = "updated"
		post.NeedActor = true
		pp.addToPostBatch(post)
		return nil
	}

	// Case 2: Post exists; archive old version and save new one
	newPost, err := pp.buildPost(commit, op)
	if err != nil {
		return err
	}
	newPost.Status = "updated"
	newPost.NeedActor = false

	history := &models.PostHistory{
		PostURI:     existingPost.URI,
		CID:         existingPost.Cid,
		Text:        existingPost.Text,
		Media:       existingPost.Media,
		ReplyParent: existingPost.ReplyParent,
		ReplyRoot:   existingPost.ReplyRoot,
		CreatedAt:   existingPost.CreatedAt.UTC(),
		ReceivedAt:  existingPost.ReceivedAt.UTC(),
		Status:      "updated",
		ValidTo:     time.Now().UTC(),
	}
	if err := pp.p.db.SavePostHistory(history); err != nil {
		pp.warnf("WARN: failed to save history for %s: %v", uri, err)
	}

	pp.addToPostBatch(newPost)
	return nil
}

func (pp *PostProcessor) handleDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existingPost, err := pp.p.db.GetPostByURI(uri)
	if err != nil {
		return err
	}
	if existingPost == nil {
		return nil
	}

	history := &models.PostHistory{
		PostURI:     existingPost.URI,
		CID:         existingPost.Cid,
		Text:        existingPost.Text,
		Media:       existingPost.Media,
		ReplyParent: existingPost.ReplyParent,
		ReplyRoot:   existingPost.ReplyRoot,
		CreatedAt:   existingPost.CreatedAt.UTC(),
		ReceivedAt:  existingPost.ReceivedAt.UTC(),
		Status:      "deleted",
		ValidTo:     time.Now().UTC(),
	}
	if err := pp.p.db.SavePostHistory(history); err != nil {
		pp.warnf("WARN: failed to save history for %s: %v", uri, err)
	}

	pp.batchMu.Lock()
	pp.deleteBatch = append(pp.deleteBatch, uri)
	if len(pp.deleteBatch) >= pp.batchSize/5 {
		batch := pp.deleteBatch
		pp.deleteBatch = make([]string, 0, pp.batchSize/5)
		pp.batchMu.Unlock()
		pp.flushDeletesWithBatch(batch)
	} else {
		pp.batchMu.Unlock()
	}
	return nil
}

// addToPostBatch appends a post to the batch, flushing if full.
func (pp *PostProcessor) addToPostBatch(post *models.Post) {
	pp.batchMu.Lock()
	pp.postBatch = append(pp.postBatch, post)
	if len(pp.postBatch) >= pp.batchSize {
		batch := pp.postBatch
		pp.postBatch = make([]*models.Post, 0, pp.batchSize)
		pp.batchMu.Unlock()
		pp.flushPostsWithBatch(batch)
	} else {
		pp.batchMu.Unlock()
	}
}

// ==================== Build ====================

func (pp *PostProcessor) buildPost(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) (*models.Post, error) {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid path: %s", op.Path)
	}
	rkey := parts[1]

	cidStr := ""
	if op.Cid != nil {
		cidStr = op.Cid.String()
	}

	post := &models.Post{
		URI:        fmt.Sprintf("at://%s/%s", commit.Repo, op.Path),
		DID:        commit.Repo,
		RKey:       rkey,
		Cid:        cidStr,
		ReceivedAt: time.Now().UTC(),
		NeedActor:  false,
	}

	blocksBase64 := base64.StdEncoding.EncodeToString(commit.Blocks)
	ops := make([]map[string]string, 0, len(commit.Ops))
	for _, o := range commit.Ops {
		oCID := ""
		if o.Cid != nil {
			oCID = o.Cid.String()
		}
		ops = append(ops, map[string]string{
			"action": o.Action,
			"path":   o.Path,
			"cid":    oCID,
		})
	}

	payloadData := map[string]interface{}{
		"repo":          commit.Repo,
		"rev":           commit.Rev,
		"seq":           commit.Seq,
		"time":          commit.Time,
		"ops":           ops,
		"blocks_base64": blocksBase64,
	}
	if payload, err := json.Marshal(payloadData); err == nil {
		post.Payload = payload
	}

	return post, nil
}

// ==================== Enrichment ====================

func (pp *PostProcessor) enrichPosts(posts []*models.Post) {
	for _, post := range posts {
		var commit struct {
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
		if err := json.Unmarshal(post.Payload, &commit); err != nil {
			pp.warnf("WARN: failed to unmarshal commit for post %s: %v", post.RKey, err)
			continue
		}
		if commit.BlocksBase64 == "" {
			continue
		}
		blocksData, err := base64.StdEncoding.DecodeString(commit.BlocksBase64)
		if err != nil {
			pp.warnf("WARN: failed to decode base64 blocks for post %s: %v", post.RKey, err)
			continue
		}
		targetCID := post.Cid
		if targetCID == "" {
			continue
		}
		blockReader, err := car.NewBlockReader(bytes.NewReader(blocksData))
		if err != nil {
			pp.warnf("WARN: failed to create block reader for post %s: %v", post.RKey, err)
			continue
		}

		var text, replyParent, replyRoot string
		var createdAt time.Time
		var mediaData []byte

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
				pp.warnf("WARN: failed to decode CBOR for post %s: %v", post.RKey, err)
				break
			}
			if t, ok := record["text"].(string); ok {
				text = sanitizeText(t)
			}
			if c, ok := record["createdAt"].(string); ok {
				if t, err := time.Parse(time.RFC3339, c); err == nil {
					createdAt = t.UTC()
				}
			}

			replyParent, replyRoot = extractReplyRefs(record)
			if embedVal, ok := record["embed"]; ok {
				mediaData = pp.mediaParser.ParseEmbed(embedVal, post.DID)
			}
			break
		}

		post.Text = text
		post.CreatedAt = createdAt.UTC()
		post.ReplyParent = replyParent
		post.ReplyRoot = replyRoot
		if mediaData != nil {
			post.Media = datatypes.JSON(mediaData)
		}
		post.Payload = nil
	}
}

// extractReplyRefs extracts parent and root URIs from a post record.
func extractReplyRefs(record map[string]interface{}) (parent, root string) {
	replyVal, ok := record["reply"]
	if !ok {
		return "", ""
	}

	replyMap := toMapStringInterface(replyVal)
	if replyMap == nil {
		return "", ""
	}

	if pv := toMapStringInterface(replyMap["parent"]); pv != nil {
		if uri, ok := pv["uri"].(string); ok {
			parent = sanitizeText(uri)
		}
	}
	if rv := toMapStringInterface(replyMap["root"]); rv != nil {
		if uri, ok := rv["uri"].(string); ok {
			root = sanitizeText(uri)
		}
	}
	return parent, root
}

// ==================== Flush ====================

func (pp *PostProcessor) flushPosts() {
	pp.batchMu.Lock()
	if len(pp.postBatch) == 0 {
		pp.batchMu.Unlock()
		return
	}
	batch := pp.postBatch
	pp.postBatch = make([]*models.Post, 0, pp.batchSize)
	pp.batchMu.Unlock()

	pp.flushPostsWithBatch(batch)
}

func (pp *PostProcessor) flushPostsWithBatch(batch []*models.Post) {
	if len(batch) == 0 {
		return
	}
	pp.enrichPosts(batch)

	if err := pp.p.db.SavePostsBatch(batch); err != nil {
		pp.warnf("WARN: failed to save batch of %d posts: %v", len(batch), err)
		return
	}
	pp.logf("Saved %d posts", len(batch))

	if pp.p.actorFetcher != nil {
		needActor := make([]*models.Post, 0)
		for _, post := range batch {
			if post.NeedActor {
				needActor = append(needActor, post)
			}
		}
		if len(needActor) > 0 {
			pp.p.actorFetcher.QueueActorsForPosts(needActor)
		}
	}
}

func (pp *PostProcessor) flushDeletes() {
	pp.batchMu.Lock()
	if len(pp.deleteBatch) == 0 {
		pp.batchMu.Unlock()
		return
	}
	batch := pp.deleteBatch
	pp.deleteBatch = make([]string, 0, pp.batchSize/5)
	pp.batchMu.Unlock()

	pp.flushDeletesWithBatch(batch)
}

func (pp *PostProcessor) flushDeletesWithBatch(uris []string) {
	if len(uris) == 0 {
		return
	}
	if err := pp.p.db.DeletePostsBatch(uris); err != nil {
		pp.warnf("WARN: failed to delete batch of %d posts: %v", len(uris), err)
	} else {
		pp.logf("Deleted %d posts", len(uris))
	}
}

// ==================== Logging ====================

func (pp *PostProcessor) logf(format string, args ...interface{}) {
	if pp.verbose {
		pp.p.logger.Printf(format, args...)
	}
}

func (pp *PostProcessor) warnf(format string, args ...interface{}) {
	pp.p.logger.Printf(format, args...)
}

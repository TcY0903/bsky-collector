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

// PostProcessor handles batch processing of post events.
type PostProcessor struct {
	p           *Processor
	mediaParser *MediaParser
	postBatch   []*models.Post
	deleteBatch []string
	batchMu     sync.Mutex
	batchSize   int
	ticker      *time.Ticker
	stopCh      chan struct{}
	closeOnce   sync.Once
}

// NewPostProcessor creates a new PostProcessor.
func NewPostProcessor(p *Processor) *PostProcessor {
	pp := &PostProcessor{
		p:           p,
		mediaParser: NewMediaParser(p.logger),
		postBatch:   make([]*models.Post, 0, 500),
		deleteBatch: make([]string, 0, 100),
		batchSize:   500,
		ticker:      time.NewTicker(3 * time.Second),
		stopCh:      make(chan struct{}),
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

// handleCreate handles a post creation event.
func (pp *PostProcessor) handleCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	post, err := pp.buildPost(commit, op)
	if err != nil {
		return err
	}
	post.Status = "active"
	post.NeedActor = true

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

	return nil
}

// handleUpdate handles a post update event.
func (pp *PostProcessor) handleUpdate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	// Check if the post exists in our database
	existingPost, err := pp.p.db.GetPostByURI(uri)
	if err != nil {
		return err
	}

	if existingPost == nil {
		// Post was created before we started listening.
		//pp.p.logger.Printf("Update unknown post (created before startup): %s", uri)
		post, err := pp.buildPost(commit, op)
		if err != nil {
			return err
		}
		post.Status = "updated"
		post.NeedActor = true

		// Add to batch for enrichment and saving
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
		return nil
	}

	// Build the new version
	newPost, err := pp.buildPost(commit, op)
	if err != nil {
		return err
	}
	newPost.Status = "updated"
	newPost.NeedActor = false

	// Archive the old version to history
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
		pp.p.logger.Printf("⚠️ Failed to save history for %s: %v", uri, err)
	}

	// Add the new version to batch
	pp.batchMu.Lock()
	pp.postBatch = append(pp.postBatch, newPost)
	if len(pp.postBatch) >= pp.batchSize {
		batch := pp.postBatch
		pp.postBatch = make([]*models.Post, 0, pp.batchSize)
		pp.batchMu.Unlock()
		pp.flushPostsWithBatch(batch)
	} else {
		pp.batchMu.Unlock()
	}

	//pp.p.logger.Printf("Updated post: %s", uri)
	return nil
}

// handleDelete handles a post deletion event.
func (pp *PostProcessor) handleDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existingPost, err := pp.p.db.GetPostByURI(uri)
	if err != nil {
		return err
	}

	if existingPost == nil {
		// Post was deleted but created before startup
		return nil
	}

	// Archive the deleted version to history
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
		pp.p.logger.Printf("⚠️ Failed to save history for %s: %v", uri, err)
	}

	// Add to delete batch
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

// buildPost creates a Post model from Firehose data (without enriching).
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

	// Store payload for later enrichment
	blocksBase64 := base64.StdEncoding.EncodeToString(commit.Blocks)
	ops := []map[string]string{}
	for _, o := range commit.Ops {
		o_cid := ""
		if o.Cid != nil {
			o_cid = o.Cid.String()
		}
		ops = append(ops, map[string]string{
			"action": o.Action,
			"path":   o.Path,
			"cid":    o_cid,
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

// enrichPosts parses CAR files and extracts post content.
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
			pp.p.logger.Printf("⚠️ Failed to unmarshal commit for post %s: %v", post.RKey, err)
			continue
		}
		if commit.BlocksBase64 == "" {
			continue
		}
		blocksData, err := base64.StdEncoding.DecodeString(commit.BlocksBase64)
		if err != nil {
			pp.p.logger.Printf("⚠️ Failed to decode base64 blocks for post %s: %v", post.RKey, err)
			continue
		}
		targetCID := post.Cid
		if targetCID == "" {
			continue
		}
		blockReader, err := car.NewBlockReader(bytes.NewReader(blocksData))
		if err != nil {
			pp.p.logger.Printf("⚠️ Failed to create block reader for post %s: %v", post.RKey, err)
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
				pp.p.logger.Printf("⚠️ Failed to decode CBOR for post %s: %v", post.RKey, err)
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

			// Parse reply information
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
					break
				}
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
						replyParent = sanitizeText(uri)
					}
				}
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
						replyRoot = sanitizeText(uri)
					}
				}
			}

			// Parse media (embed)
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

// flushPosts flushes pending post batch.
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

// flushPostsWithBatch saves a batch of posts to the database.
func (pp *PostProcessor) flushPostsWithBatch(batch []*models.Post) {
	if len(batch) == 0 {
		return
	}
	pp.enrichPosts(batch)

	if err := pp.p.db.SavePostsBatch(batch); err != nil {
		pp.p.logger.Printf("Warning: Failed to save batch of %d posts: %v", len(batch), err)
	} else {
		//pp.p.logger.Printf("Saved %d posts in batch", len(batch))
	}

	// Queue actors only for posts that need it (create events or new actors)
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

// flushDeletes flushes pending delete batch.
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

// flushDeletesWithBatch marks multiple posts as deleted in a single transaction.
func (pp *PostProcessor) flushDeletesWithBatch(uris []string) {
	if len(uris) == 0 {
		return
	}

	if err := pp.p.db.DeletePostsBatch(uris); err != nil {
		pp.p.logger.Printf("⚠️ Failed to delete batch of %d posts: %v", len(uris), err)
	} else {
		//pp.p.logger.Printf("🗑️ Deleted %d posts in batch", len(uris))
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

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

	"bsky-collector/models"
)

// PostProcessor handles batch processing of post events.
type PostProcessor struct {
	p         *Processor
	postBatch []*models.Post
	batchMu   sync.Mutex
	batchSize int
	ticker    *time.Ticker
	stopCh    chan struct{}
	closeOnce sync.Once
}

// NewPostProcessor creates a new PostProcessor.
func NewPostProcessor(p *Processor) *PostProcessor {
	pp := &PostProcessor{
		p:         p,
		postBatch: make([]*models.Post, 0, 500),
		batchSize: 500,
		ticker:    time.NewTicker(3 * time.Second),
		stopCh:    make(chan struct{}),
	}
	go pp.autoFlush()
	return pp
}

func (pp *PostProcessor) autoFlush() {
	for {
		select {
		case <-pp.ticker.C:
			pp.flushPosts()
		case <-pp.stopCh:
			return
		}
	}
}

// ProcessPost processes a post creation event.
func (pp *PostProcessor) ProcessPost(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil
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
		ReceivedAt: time.Now(),
	}

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
		pp.p.logger.Printf("⚠️ Failed to save batch of %d posts: %v", len(batch), err)
	} else {
		pp.p.logger.Printf("📦 Saved %d posts in batch", len(batch))
	}

	// Queue actors for this batch
	if pp.p.actorFetcher != nil {
		pp.p.actorFetcher.QueueActorsForPosts(batch)
	}

	// Queue labels for this batch
	if pp.p.labelFetcher != nil {
		go func() {
			for _, post := range batch {
				if post.URI != "" {
					pp.p.labelFetcher.QueueURI(post.URI)
				}
			}
		}()
	}
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
			continue
		}
		if commit.BlocksBase64 == "" {
			continue
		}
		blocksData, err := base64.StdEncoding.DecodeString(commit.BlocksBase64)
		if err != nil {
			continue
		}
		targetCID := post.Cid
		if targetCID == "" {
			continue
		}
		blockReader, err := car.NewBlockReader(bytes.NewReader(blocksData))
		if err != nil {
			continue
		}
		var text, replyParent, replyRoot string
		var createdAt time.Time
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
				break
			}
			if t, ok := record["text"].(string); ok {
				text = t
			}
			if c, ok := record["createdAt"].(string); ok {
				if t, err := time.Parse(time.RFC3339, c); err == nil {
					createdAt = t
				}
			}
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
						replyParent = uri
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
						replyRoot = uri
					}
				}
			}
			break
		}
		post.Text = text
		post.CreatedAt = createdAt
		post.ReplyParent = replyParent
		post.ReplyRoot = replyRoot
		post.Payload = nil
	}
}

func (pp *PostProcessor) Close() {
	pp.closeOnce.Do(func() {
		close(pp.stopCh)
		pp.ticker.Stop()
		pp.flushPosts()
	})
}

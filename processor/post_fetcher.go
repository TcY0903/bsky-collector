package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"bsky-collector/models"
)

const (
	maxPostFetchConcurrent = 10
	maxContextDepth        = 10
)

// PostFetcher fetches missing posts from the public API by URI.
type PostFetcher struct {
	p         *Processor
	queue     chan fetchTask
	stopCh    chan struct{}
	wg        sync.WaitGroup
	started   bool
	mu        sync.Mutex
	batchSize int
	ticker    *time.Ticker
	client    *http.Client
	sem       chan struct{}
	pending   map[string]bool
}

type fetchTask struct {
	URI   string
	Depth int
}

func NewPostFetcher(p *Processor) *PostFetcher {
	return &PostFetcher{
		p:         p,
		queue:     make(chan fetchTask, 5000),
		stopCh:    make(chan struct{}),
		batchSize: 10,
		ticker:    time.NewTicker(3 * time.Second),
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		sem:     make(chan struct{}, maxPostFetchConcurrent),
		pending: make(map[string]bool),
	}
}

func (pf *PostFetcher) Start() {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	if pf.started {
		return
	}
	pf.started = true
	pf.wg.Add(1)
	go pf.worker()
}

func (pf *PostFetcher) Stop() {
	pf.mu.Lock()
	if !pf.started {
		pf.mu.Unlock()
		return
	}
	pf.started = false
	close(pf.stopCh)
	pf.ticker.Stop()
	pf.mu.Unlock()

	done := make(chan struct{})
	go func() {
		pf.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		pf.p.logger.Printf("WARN: PostFetcher stop timed out, forcing exit")
	}
}

func (pf *PostFetcher) QueueURI(uri string) {
	pf.QueueURIWithDepth(uri, 0)
}

func (pf *PostFetcher) QueueURIWithDepth(uri string, depth int) {
	if uri == "" || depth > maxContextDepth {
		return
	}
	if !strings.Contains(uri, "/app.bsky.feed.post/") {
		return
	}

	pf.mu.Lock()
	if pf.pending[uri] {
		pf.mu.Unlock()
		return
	}
	pf.pending[uri] = true
	pf.mu.Unlock()

	select {
	case pf.queue <- fetchTask{URI: uri, Depth: depth}:
	default:
		pf.mu.Lock()
		delete(pf.pending, uri)
		pf.mu.Unlock()
		pf.p.logger.Printf("WARN: post fetch queue full, skipping %s", uri)
	}
}

func (pf *PostFetcher) worker() {
	defer pf.wg.Done()

	batch := make([]fetchTask, 0, pf.batchSize)
	ticker := pf.ticker

	for {
		select {
		case <-pf.stopCh:
			return
		case task := <-pf.queue:
			batch = append(batch, task)
			if len(batch) >= pf.batchSize {
				pf.fetchBatch(batch)
				batch = make([]fetchTask, 0, pf.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				pf.fetchBatch(batch)
				batch = make([]fetchTask, 0, pf.batchSize)
			}
		}
	}
}

func (pf *PostFetcher) fetchBatch(tasks []fetchTask) {
	for _, task := range tasks {
		select {
		case <-pf.stopCh:
			return
		default:
		}
		go func(t fetchTask) {
			select {
			case pf.sem <- struct{}{}:
				defer func() { <-pf.sem }()
			case <-pf.stopCh:
				pf.mu.Lock()
				delete(pf.pending, t.URI)
				pf.mu.Unlock()
				return
			}
			pf.fetchPost(t.URI, t.Depth)
		}(task)
	}
}

func (pf *PostFetcher) fetchPost(postURI string, depth int) {
	defer func() {
		pf.mu.Lock()
		delete(pf.pending, postURI)
		pf.mu.Unlock()
	}()

	parts := strings.Split(strings.TrimPrefix(postURI, "at://"), "/")
	if len(parts) != 3 {
		return
	}
	repo := parts[0]
	collection := parts[1]
	rkey := parts[2]

	if collection != "app.bsky.feed.post" {
		return
	}

	apiURL := fmt.Sprintf(
		"https://public.api.bsky.app/xrpc/com.atproto.repo.getRecord?repo=%s&collection=%s&rkey=%s",
		url.QueryEscape(repo), url.QueryEscape(collection), url.QueryEscape(rkey),
	)

	req, err := http.NewRequestWithContext(pf.p.ctx, "GET", apiURL, nil)
	if err != nil {
		return
	}

	resp, err := pf.client.Do(req)
	if err != nil {
		if pf.p.ctx.Err() == context.Canceled {
			return
		}
		pf.p.logger.Printf("WARN: failed to fetch post %s: %v", postURI, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return
	}
	if resp.StatusCode != http.StatusOK {
		pf.p.logger.Printf("WARN: post API returned %d for %s", resp.StatusCode, postURI)
		return
	}

	var result struct {
		URI   string `json:"uri"`
		CID   string `json:"cid"`
		Value struct {
			Type      string `json:"$type"`
			Text      string `json:"text"`
			CreatedAt string `json:"createdAt"`
			Reply     *struct {
				Parent struct {
					URI string `json:"uri"`
				} `json:"parent"`
				Root struct {
					URI string `json:"uri"`
				} `json:"root"`
			} `json:"reply"`
			Embed json.RawMessage `json:"embed"`
		} `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		pf.p.logger.Printf("WARN: failed to decode post %s: %v", postURI, err)
		return
	}

	post := &models.Post{
		URI:        result.URI,
		DID:        repo,
		RKey:       rkey,
		Cid:        result.CID,
		Text:       sanitizeText(result.Value.Text),
		ReceivedAt: time.Now().UTC(),
		Status:     "active",
		NeedActor:  false,
	}

	if result.Value.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339, result.Value.CreatedAt); err == nil {
			post.CreatedAt = t.UTC()
		}
	}

	if result.Value.Reply != nil {
		post.ReplyParent = sanitizeText(result.Value.Reply.Parent.URI)
		post.ReplyRoot = sanitizeText(result.Value.Reply.Root.URI)
	}

	// Parse embed (media + quoted post)
	var quotedURI string
	if len(result.Value.Embed) > 0 {
		var embedVal interface{}
		if err := json.Unmarshal(result.Value.Embed, &embedVal); err == nil {
			if mediaData := pf.p.postProcessor.mediaParser.ParseEmbed(embedVal, repo); mediaData != nil {
				post.Media = mediaData
			}
			// Extract quoted post URI
			if embedMap, ok := embedVal.(map[string]interface{}); ok {
				quotedURI = extractQuotedPostURI(embedMap)
			}
		}
	}

	if err := pf.p.db.SavePost(post); err != nil {
		pf.p.logger.Printf("WARN: failed to save fetched post %s: %v", postURI, err)
		return
	}

	/*
		if depth > 0 {
			pf.p.logger.Printf("Fetched context post (depth=%d): %s", depth, postURI)
		} else {
			pf.p.logger.Printf("Fetched missing post: %s", postURI)
		}
	*/

	// Queue actor
	if pf.p.actorFetcher != nil {
		pf.p.actorFetcher.QueueDID(repo)
	}

	// Recursive: fetch parent chain
	if post.ReplyParent != "" && depth < maxContextDepth {
		pf.QueueURIWithDepth(post.ReplyParent, depth+1)
	}

	// Fetch root (if different from parent)
	if post.ReplyRoot != "" && post.ReplyRoot != post.ReplyParent && depth < maxContextDepth {
		pf.QueueURIWithDepth(post.ReplyRoot, depth+1)
	}

	// Fetch quoted post (not part of reply chain, depth 0)
	if quotedURI != "" {
		pf.QueueURIWithDepth(quotedURI, 0)
	}
}

// extractQuotedPostURI extracts the quoted post URI from an embed structure.
func extractQuotedPostURI(embed map[string]interface{}) string {
	embedType, _ := embed["$type"].(string)

	switch {
	case embedType == "app.bsky.embed.record" || embedType == "app.bsky.embed.record#view":
		if record, ok := embed["record"].(map[string]interface{}); ok {
			if uri, ok := record["uri"].(string); ok && strings.Contains(uri, "/app.bsky.feed.post/") {
				return uri
			}
		}
	case embedType == "app.bsky.embed.recordWithMedia" || embedType == "app.bsky.embed.recordWithMedia#view":
		if record, ok := embed["record"].(map[string]interface{}); ok {
			// Try direct uri
			if uri, ok := record["uri"].(string); ok && strings.Contains(uri, "/app.bsky.feed.post/") {
				return uri
			}
			// Try nested record.uri
			if innerRecord, ok := record["record"].(map[string]interface{}); ok {
				if uri, ok := innerRecord["uri"].(string); ok && strings.Contains(uri, "/app.bsky.feed.post/") {
					return uri
				}
			}
		}
	}
	return ""
}

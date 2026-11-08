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

// ==================== Constants ====================

const (
	maxPostFetchConcurrent = 5
	maxContextDepth        = 10   // Fallback safety for recursive fetchPost
	threadParentHeight     = 1000 // Max parent chain depth for getPostThread
	threadReplyDepth       = 1000 // Max reply tree depth for getPostThread
)

// ==================== Types ====================

// fetchTask describes a single fetch job.
type fetchTask struct {
	URI     string
	Depth   int
	Context bool // true = fetch full thread context via getPostThread
}

// PostFetcher fetches missing posts and thread context from the public API.
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

// ==================== Lifecycle ====================

// NewPostFetcher creates a new PostFetcher.
func NewPostFetcher(p *Processor) *PostFetcher {
	return &PostFetcher{
		p:         p,
		queue:     make(chan fetchTask, 5000),
		stopCh:    make(chan struct{}),
		batchSize: 10,
		ticker:    time.NewTicker(3 * time.Second),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		sem:     make(chan struct{}, maxPostFetchConcurrent),
		pending: make(map[string]bool),
	}
}

// Start starts the fetcher worker.
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

// Stop stops the fetcher worker with a timeout to avoid hanging.
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

// ==================== Public API ====================

// QueueURI queues a single post for fetching (no context).
func (pf *PostFetcher) QueueURI(uri string) {
	pf.enqueue(fetchTask{URI: uri, Depth: 0, Context: false})
}

// QueueURIWithDepth queues a single post for fetching with an explicit depth.
func (pf *PostFetcher) QueueURIWithDepth(uri string, depth int) {
	pf.enqueue(fetchTask{URI: uri, Depth: depth, Context: false})
}

// FetchThreadContext queues a post for full thread-context fetching via getPostThread.
func (pf *PostFetcher) FetchThreadContext(uri string) {
	pf.enqueue(fetchTask{URI: uri, Depth: 0, Context: true})
}

// ==================== Queue Management ====================

func (pf *PostFetcher) enqueue(task fetchTask) {
	if task.URI == "" || !strings.Contains(task.URI, "/app.bsky.feed.post/") {
		return
	}
	if task.Depth > maxContextDepth {
		return
	}

	pf.mu.Lock()
	if pf.pending[task.URI] {
		pf.mu.Unlock()
		return
	}
	pf.pending[task.URI] = true
	pf.mu.Unlock()

	select {
	case pf.queue <- task:
	default:
		pf.clearPending(task.URI)
		pf.p.logger.Printf("WARN: post fetch queue full, skipping %s", task.URI)
	}
}

func (pf *PostFetcher) clearPending(uri string) {
	pf.mu.Lock()
	delete(pf.pending, uri)
	pf.mu.Unlock()
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
			if task.Context {
				pf.runAsync(task.URI, func() { pf.fetchThreadContext(task.URI) })
			} else {
				batch = append(batch, task)
				if len(batch) >= pf.batchSize {
					pf.dispatchBatch(batch)
					batch = make([]fetchTask, 0, pf.batchSize)
				}
			}
		case <-ticker.C:
			if len(batch) > 0 {
				pf.dispatchBatch(batch)
				batch = make([]fetchTask, 0, pf.batchSize)
			}
		}
	}
}

// dispatchBatch launches all tasks in a batch concurrently (bounded by sem).
func (pf *PostFetcher) dispatchBatch(tasks []fetchTask) {
	for _, task := range tasks {
		t := task
		pf.runAsync(t.URI, func() { pf.fetchPost(t.URI, t.Depth) })
	}
}

// runAsync acquires the semaphore and runs fn in a new goroutine.
func (pf *PostFetcher) runAsync(uri string, fn func()) {
	go func() {
		select {
		case pf.sem <- struct{}{}:
			defer func() { <-pf.sem }()
		case <-pf.stopCh:
			pf.clearPending(uri)
			return
		}
		fn()
	}()
}

// ==================== HTTP Helper ====================

// getJSON performs a GET request and decodes the JSON response into out.
// Returns false on any failure (including 404/429/etc.).
func (pf *PostFetcher) getJSON(apiURL string, out interface{}) bool {
	req, err := http.NewRequestWithContext(pf.p.ctx, "GET", apiURL, nil)
	if err != nil {
		return false
	}
	resp, err := pf.client.Do(req)
	if err != nil {
		if pf.p.ctx.Err() != context.Canceled {
			pf.p.logger.Printf("WARN: HTTP request failed for %s: %v", apiURL, err)
		}
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		pf.p.logger.Printf("WARN: HTTP %d for %s", resp.StatusCode, apiURL)
		return false
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		pf.p.logger.Printf("WARN: failed to decode response for %s: %v", apiURL, err)
		return false
	}
	return true
}

// ==================== Single Post Fetch ====================

func (pf *PostFetcher) fetchPost(postURI string, depth int) {
	defer pf.clearPending(postURI)

	repo, rkey, ok := parsePostURI(postURI)
	if !ok {
		return
	}

	apiURL := fmt.Sprintf(
		"https://public.api.bsky.app/xrpc/com.atproto.repo.getRecord?repo=%s&collection=app.bsky.feed.post&rkey=%s",
		url.QueryEscape(repo), url.QueryEscape(rkey),
	)

	var result struct {
		URI   string `json:"uri"`
		CID   string `json:"cid"`
		Value struct {
			Text      string          `json:"text"`
			CreatedAt string          `json:"createdAt"`
			Reply     *replyRef       `json:"reply"`
			Embed     json.RawMessage `json:"embed"`
		} `json:"value"`
	}
	if !pf.getJSON(apiURL, &result) {
		return
	}

	post := pf.buildPostFromRecord(repo, rkey, result.URI, result.CID, result.Value.Text,
		result.Value.CreatedAt, result.Value.Reply, result.Value.Embed)
	if post == nil {
		return
	}

	var quotedURI string
	if len(result.Value.Embed) > 0 {
		quotedURI = extractQuotedPostURIFromRaw(result.Value.Embed)
	}

	if err := pf.p.db.SavePost(post); err != nil {
		pf.p.logger.Printf("WARN: failed to save fetched post %s: %v", postURI, err)
		return
	}

	if depth > 0 {
		pf.p.logger.Printf("Fetched context post (depth=%d): %s", depth, postURI)
	} else {
		pf.p.logger.Printf("Fetched missing post: %s", postURI)
	}

	if pf.p.actorFetcher != nil {
		pf.p.actorFetcher.QueueDID(repo)
	}

	// Recursively fetch parent chain (fallback path)
	if post.ReplyParent != "" && depth < maxContextDepth {
		pf.QueueURIWithDepth(post.ReplyParent, depth+1)
	}
	if post.ReplyRoot != "" && post.ReplyRoot != post.ReplyParent && depth < maxContextDepth {
		pf.QueueURIWithDepth(post.ReplyRoot, depth+1)
	}
	if quotedURI != "" {
		pf.QueueURIWithDepth(quotedURI, 0)
	}
}

// ==================== Thread Context Fetch ====================

func (pf *PostFetcher) fetchThreadContext(postURI string) {
	defer pf.clearPending(postURI)

	pf.p.logger.Printf("Fetching thread context for: %s", postURI)

	apiURL := fmt.Sprintf(
		"https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread?uri=%s&depth=%d&parentHeight=%d",
		url.QueryEscape(postURI), threadReplyDepth, threadParentHeight,
	)

	var result struct {
		Thread json.RawMessage `json:"thread"`
	}
	if !pf.getJSON(apiURL, &result) || len(result.Thread) == 0 {
		return
	}

	var tv threadViewNode
	if err := json.Unmarshal(result.Thread, &tv); err != nil {
		return
	}

	if tv.Type != "app.bsky.feed.defs#threadViewPost" {
		pf.p.logger.Printf("Thread context not found (type=%s) for: %s", tv.Type, postURI)
		return
	}

	visited := make(map[string]bool)

	// Target post
	if len(tv.Post) > 0 {
		pf.saveThreadPostView(tv.Post, 0, postURI, visited)
	}

	// Parent chain (upward)
	pf.walkParentChain(tv.Parent, 1, visited)

	// Replies (downward)
	for _, reply := range tv.Replies {
		pf.walkReplies(reply, 1, visited)
	}
}

// threadViewNode is a unified struct for both parent and reply nodes.
type threadViewNode struct {
	Type    string            `json:"$type"`
	Post    json.RawMessage   `json:"post"`
	Parent  json.RawMessage   `json:"parent"`
	Replies []json.RawMessage `json:"replies"`
}

// walkParentChain recursively saves parent posts (upward).
func (pf *PostFetcher) walkParentChain(raw json.RawMessage, depth int, visited map[string]bool) {
	if len(raw) == 0 || depth > threadParentHeight {
		return
	}
	var node threadViewNode
	if err := json.Unmarshal(raw, &node); err != nil {
		return
	}
	if node.Type != "app.bsky.feed.defs#threadViewPost" {
		return
	}
	if len(node.Post) > 0 {
		pf.saveThreadPostView(node.Post, depth, "", visited)
	}
	pf.walkParentChain(node.Parent, depth+1, visited)
}

// walkReplies recursively saves reply posts (downward).
func (pf *PostFetcher) walkReplies(raw json.RawMessage, depth int, visited map[string]bool) {
	if len(raw) == 0 || depth > threadReplyDepth {
		return
	}
	var node threadViewNode
	if err := json.Unmarshal(raw, &node); err != nil {
		return
	}
	if node.Type != "app.bsky.feed.defs#threadViewPost" {
		return
	}
	if len(node.Post) > 0 {
		pf.saveThreadPostView(node.Post, depth, "", visited)
	}
	for _, child := range node.Replies {
		pf.walkReplies(child, depth+1, visited)
	}
}

// saveThreadPostView parses and saves a threadViewPost.post object.
func (pf *PostFetcher) saveThreadPostView(raw json.RawMessage, depth int, expectedURI string, visited map[string]bool) {
	var pv struct {
		URI    string `json:"uri"`
		CID    string `json:"cid"`
		Author struct {
			DID string `json:"did"`
		} `json:"author"`
		Record struct {
			Text      string          `json:"text"`
			CreatedAt string          `json:"createdAt"`
			Reply     *replyRef       `json:"reply"` // ← 改成 *replyRef
			Embed     json.RawMessage `json:"embed"`
		} `json:"record"`
	}
	if err := json.Unmarshal(raw, &pv); err != nil {
		return
	}
	if pv.URI == "" || pv.Author.DID == "" {
		return
	}

	// Per-request dedup
	if visited[pv.URI] {
		return
	}
	visited[pv.URI] = true

	// DB dedup (except explicit target)
	if expectedURI == "" || pv.URI != expectedURI {
		if existing, _ := pf.p.db.GetPostByURI(pv.URI); existing != nil {
			return
		}
	}

	parts := strings.Split(strings.TrimPrefix(pv.URI, "at://"), "/")
	if len(parts) != 3 {
		return
	}

	post := pf.buildPostFromRecord(pv.Author.DID, parts[2], pv.URI, pv.CID,
		pv.Record.Text, pv.Record.CreatedAt, pv.Record.Reply, pv.Record.Embed)
	if post == nil {
		return
	}

	if err := pf.p.db.SavePost(post); err != nil {
		pf.p.logger.Printf("WARN: failed to save context post %s: %v", pv.URI, err)
		return
	}

	if depth > 0 {
		pf.p.logger.Printf("Fetched thread context post (depth=%d): %s", depth, pv.URI)
	} else if expectedURI != "" {
		pf.p.logger.Printf("Fetched labeled post: %s", pv.URI)
	}

	if pf.p.actorFetcher != nil {
		pf.p.actorFetcher.QueueDID(pv.Author.DID)
	}
}

// ==================== Build Helpers ====================

// replyRef is the reply reference embedded in a post record.
type replyRef struct {
	Parent struct {
		URI string `json:"uri"`
	} `json:"parent"`
	Root struct {
		URI string `json:"uri"`
	} `json:"root"`
}

// buildPostFromRecord builds a models.Post from record fields.
// Returns nil if the record is invalid.
func (pf *PostFetcher) buildPostFromRecord(
	did, rkey, uri, cid, text, createdAt string,
	reply *replyRef,
	embed json.RawMessage,
) *models.Post {
	post := &models.Post{
		URI:        uri,
		DID:        did,
		RKey:       rkey,
		Cid:        cid,
		Text:       sanitizeText(text),
		ReceivedAt: time.Now().UTC(),
		Status:     "active",
	}

	if createdAt != "" {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			post.CreatedAt = t.UTC()
		}
	}
	if reply != nil {
		post.ReplyParent = sanitizeText(reply.Parent.URI)
		post.ReplyRoot = sanitizeText(reply.Root.URI)
	}
	if len(embed) > 0 {
		var embedVal interface{}
		if err := json.Unmarshal(embed, &embedVal); err == nil {
			if mediaData := pf.p.postProcessor.mediaParser.ParseEmbed(embedVal, did); mediaData != nil {
				post.Media = mediaData
			}
		}
	}
	return post
}

// ==================== URI Helpers ====================

// parsePostURI parses "at://did/collection/rkey" into (did, rkey).
func parsePostURI(uri string) (did, rkey string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(uri, "at://"), "/")
	if len(parts) != 3 || parts[1] != "app.bsky.feed.post" {
		return "", "", false
	}
	return parts[0], parts[2], true
}

// extractQuotedPostURIFromRaw extracts the quoted post URI from a raw embed JSON.
func extractQuotedPostURIFromRaw(embed json.RawMessage) string {
	var embedMap map[string]interface{}
	if err := json.Unmarshal(embed, &embedMap); err != nil {
		return ""
	}
	return extractQuotedPostURI(embedMap)
}

// extractQuotedPostURI extracts the quoted post URI from an embed map.
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
			if uri, ok := record["uri"].(string); ok && strings.Contains(uri, "/app.bsky.feed.post/") {
				return uri
			}
			if innerRecord, ok := record["record"].(map[string]interface{}); ok {
				if uri, ok := innerRecord["uri"].(string); ok && strings.Contains(uri, "/app.bsky.feed.post/") {
					return uri
				}
			}
		}
	}
	return ""
}

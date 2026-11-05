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

const maxPostFetchConcurrent = 5

// PostFetcher fetches missing posts from the public API by URI.
type PostFetcher struct {
	p         *Processor
	queue     chan string
	stopCh    chan struct{}
	wg        sync.WaitGroup
	started   bool
	mu        sync.Mutex
	batchSize int
	ticker    *time.Ticker
	client    *http.Client
	sem       chan struct{}
}

// NewPostFetcher creates a new PostFetcher.
func NewPostFetcher(p *Processor) *PostFetcher {
	return &PostFetcher{
		p:         p,
		queue:     make(chan string, 5000),
		stopCh:    make(chan struct{}),
		batchSize: 100,
		ticker:    time.NewTicker(3 * time.Second),
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		sem: make(chan struct{}, maxPostFetchConcurrent),
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

// Stop stops the fetcher worker.
func (pf *PostFetcher) Stop() {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	if !pf.started {
		return
	}
	pf.started = false
	close(pf.stopCh)
	pf.ticker.Stop()
	pf.wg.Wait()
}

// QueueURI adds a post URI to the fetch queue.
func (pf *PostFetcher) QueueURI(uri string) {
	if uri == "" {
		return
	}
	select {
	case pf.queue <- uri:
	default:
		pf.p.logger.Printf("WARN: post fetch queue full, skipping %s", uri)
	}
}

func (pf *PostFetcher) worker() {
	defer pf.wg.Done()

	batch := make([]string, 0, pf.batchSize)
	ticker := pf.ticker

	for {
		select {
		case <-pf.stopCh:
			if len(batch) > 0 {
				pf.fetchBatch(batch)
			}
			return
		case uri := <-pf.queue:
			batch = append(batch, uri)
			if len(batch) >= pf.batchSize {
				pf.fetchBatch(batch)
				batch = make([]string, 0, pf.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				pf.fetchBatch(batch)
				batch = make([]string, 0, pf.batchSize)
			}
		}
	}
}

func (pf *PostFetcher) fetchBatch(uris []string) {
	var wg sync.WaitGroup
	for _, uri := range uris {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			pf.sem <- struct{}{}
			defer func() { <-pf.sem }()
			pf.fetchPost(u)
		}(uri)
	}
	wg.Wait()
}

// fetchPost fetches a single post by URI via com.atproto.repo.getRecord.
func (pf *PostFetcher) fetchPost(postURI string) {
	// Parse URI: at://did/collection/rkey
	parts := strings.Split(strings.TrimPrefix(postURI, "at://"), "/")
	if len(parts) != 3 {
		pf.p.logger.Printf("WARN: invalid post URI: %s", postURI)
		return
	}
	repo := parts[0]
	collection := parts[1]
	rkey := parts[2]

	if collection != "app.bsky.feed.post" {
		return // Not a post
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

	// Parse embed (media) using existing MediaParser
	if len(result.Value.Embed) > 0 {
		var embedVal interface{}
		if err := json.Unmarshal(result.Value.Embed, &embedVal); err == nil {
			if mediaData := pf.p.postProcessor.mediaParser.ParseEmbed(embedVal, repo); mediaData != nil {
				post.Media = mediaData
			}
		}
	}

	if err := pf.p.db.SavePost(post); err != nil {
		pf.p.logger.Printf("WARN: failed to save fetched post %s: %v", postURI, err)
		return
	}

	pf.p.logger.Printf("Fetched missing post: %s", postURI)

	// Queue actor for fetching
	if pf.p.actorFetcher != nil {
		pf.p.actorFetcher.QueueDID(repo)
	}
}

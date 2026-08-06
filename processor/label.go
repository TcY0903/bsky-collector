package processor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"time"

	"bsky-collector/models"
)

const maxURLLength = 7000

// LabelFetcher handles fetching labels for posts via the queryLabels API.
type LabelFetcher struct {
	p         *Processor
	queue     chan string
	stopCh    chan struct{}
	wg        sync.WaitGroup
	started   bool
	mu        sync.Mutex
	batchSize int
	ticker    *time.Ticker
	client    *http.Client
}

func NewLabelFetcher(p *Processor) *LabelFetcher {
	lf := &LabelFetcher{
		p:         p,
		queue:     make(chan string, 5000),
		stopCh:    make(chan struct{}),
		batchSize: 250,
		ticker:    time.NewTicker(3 * time.Second),
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
	return lf
}

func (lf *LabelFetcher) Start() {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.started {
		return
	}
	lf.started = true
	lf.wg.Add(1)
	go lf.worker()
}

func (lf *LabelFetcher) Stop() {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if !lf.started {
		return
	}
	lf.started = false
	close(lf.stopCh)
	lf.ticker.Stop()
	lf.wg.Wait()
}

func (lf *LabelFetcher) QueueURI(uri string) {
	if uri == "" {
		return
	}
	select {
	case lf.queue <- uri:
	default:
		lf.p.logger.Printf("⚠️ Label queue full, dropping %s", uri)
	}
}

func (lf *LabelFetcher) worker() {
	defer lf.wg.Done()

	batch := make([]string, 0, lf.batchSize)
	ticker := lf.ticker

	for {
		select {
		case <-lf.stopCh:
			if len(batch) > 0 {
				lf.fetchBatch(batch)
			}
			return
		case uri := <-lf.queue:
			batch = append(batch, uri)
			if len(batch) >= lf.batchSize {
				lf.fetchBatch(batch)
				batch = make([]string, 0, lf.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				lf.fetchBatch(batch)
				batch = make([]string, 0, lf.batchSize)
			}
		}
	}
}

// fetchBatch splits URIs into groups by URL length and fetches each group.
func (lf *LabelFetcher) fetchBatch(uris []string) {
	if len(uris) == 0 {
		return
	}

	// Build base URL for length estimation
	baseURL := "https://mod.bsky.app/xrpc/com.atproto.label.queryLabels?limit=250&uriPatterns="
	baseLen := len(baseURL)

	var groups [][]string
	var currentGroup []string
	currentLen := baseLen

	for _, uri := range uris {
		// Each URI adds "&uriPatterns=" prefix + URI length
		addedLen := len(uri) + len("&uriPatterns=")
		if currentLen+addedLen > maxURLLength && len(currentGroup) > 0 {
			groups = append(groups, currentGroup)
			currentGroup = []string{}
			currentLen = baseLen
		}
		currentGroup = append(currentGroup, uri)
		currentLen += addedLen
	}
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	// Fetch each group
	for _, group := range groups {
		lf.fetchGroup(group)
	}
}

// fetchGroup fetches labels for a single group of URIs.
func (lf *LabelFetcher) fetchGroup(uris []string) {
	if len(uris) == 0 {
		return
	}

	params := url.Values{}
	for _, uri := range uris {
		params.Add("uriPatterns", uri)
	}
	params.Add("limit", "250")

	reqURL := "https://mod.bsky.app/xrpc/com.atproto.label.queryLabels?" + params.Encode()

	req, err := http.NewRequestWithContext(lf.p.ctx, "GET", reqURL, nil)
	if err != nil {
		lf.p.logger.Printf("⚠️ Failed to create label request: %v", err)
		return
	}

	resp, err := lf.client.Do(req)
	if err != nil {
		if lf.p.ctx.Err() == context.Canceled {
			lf.p.logger.Printf("⚠️ Label request cancelled (shutdown)")
		} else {
			lf.p.logger.Printf("⚠️ Failed to fetch labels: %v", err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		lf.p.logger.Printf("⚠️ Label API returned status %d", resp.StatusCode)
		return
	}

	var result struct {
		Labels []struct {
			Src string `json:"src"`
			URI string `json:"uri"`
			CID string `json:"cid,omitempty"`
			Val string `json:"val"`
			Neg bool   `json:"neg,omitempty"`
			CTS string `json:"cts"`
		} `json:"labels"`
		Cursor *string `json:"cursor,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		lf.p.logger.Printf("⚠️ Failed to decode label response: %v", err)
		return
	}

	labels := make([]*models.Label, 0, len(result.Labels))
	for _, l := range result.Labels {
		cts, err := time.Parse(time.RFC3339, l.CTS)
		if err != nil {
			cts = time.Now().UTC()
		}
		labels = append(labels, &models.Label{
			Src:        l.Src,
			URI:        l.URI,
			CID:        l.CID,
			Val:        l.Val,
			Neg:        l.Neg,
			CTS:        cts.UTC(),
			ReceivedAt: time.Now().UTC(),
		})
	}

	if len(labels) > 0 {
		if err := lf.p.db.SaveLabelsBatch(labels); err != nil {
			lf.p.logger.Printf("⚠️ Failed to save %d labels: %v", len(labels), err)
		}
	}
}

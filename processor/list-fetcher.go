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

// ListFetcher handles fetching list information via the API.
type ListFetcher struct {
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

// NewListFetcher creates a new ListFetcher.
func NewListFetcher(p *Processor) *ListFetcher {
	lf := &ListFetcher{
		p:         p,
		queue:     make(chan string, 1000),
		stopCh:    make(chan struct{}),
		batchSize: 10,
		ticker:    time.NewTicker(5 * time.Second),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	return lf
}

// Start starts the ListFetcher worker.
func (lf *ListFetcher) Start() {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.started {
		return
	}
	lf.started = true
	lf.wg.Add(1)
	go lf.worker()
}

// Stop stops the ListFetcher worker.
func (lf *ListFetcher) Stop() {
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

// QueueListURI adds a list URI to the fetch queue.
func (lf *ListFetcher) QueueListURI(uri string) {
	if uri == "" {
		return
	}
	select {
	case lf.queue <- uri:
	default:
		lf.p.logger.Printf("⚠️ List queue full, skipping %s", uri)
	}
}

func (lf *ListFetcher) worker() {
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

func (lf *ListFetcher) fetchBatch(uris []string) {
	if len(uris) == 0 {
		return
	}
	for _, uri := range uris {
		lf.fetchList(uri)
	}
}

// fetchList fetches a single list via the API and saves it along with its members.
func (lf *ListFetcher) fetchList(listURI string) {
	// Parse the URI to extract rkey
	// Format: at://did:plc:xxx/app.bsky.graph.list/rkey
	parts := strings.Split(strings.TrimPrefix(listURI, "at://"), "/")
	if len(parts) < 3 {
		lf.p.logger.Printf("⚠️ Invalid list URI: %s", listURI)
		return
	}
	rkey := parts[len(parts)-1]

	// Build API URL
	apiURL := fmt.Sprintf("https://public.api.bsky.app/xrpc/app.bsky.graph.getList?list=%s", url.QueryEscape(listURI))

	req, err := http.NewRequestWithContext(lf.p.ctx, "GET", apiURL, nil)
	if err != nil {
		lf.p.logger.Printf("⚠️ Failed to create list request: %v", err)
		return
	}

	resp, err := lf.client.Do(req)
	if err != nil {
		if lf.p.ctx.Err() == context.Canceled {
			lf.p.logger.Printf("⚠️ List request cancelled (shutdown)")
		} else {
			lf.p.logger.Printf("⚠️ Failed to fetch list %s: %v", listURI, err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		lf.p.logger.Printf("⚠️ List API returned status %d for %s", resp.StatusCode, listURI)
		return
	}

	var result struct {
		List struct {
			URI         string `json:"uri"`
			CID         string `json:"cid"`
			Name        string `json:"name"`
			Purpose     string `json:"purpose"`
			Description string `json:"description"`
			Creator     struct {
				DID string `json:"did"`
			} `json:"creator"`
			CreatedAt string `json:"createdAt"`
		} `json:"list"`
		Items []struct {
			URI     string `json:"uri"`
			Subject struct {
				DID string `json:"did"`
			} `json:"subject"`
			CreatedAt string `json:"createdAt"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		lf.p.logger.Printf("⚠️ Failed to decode list response: %v", err)
		return
	}

	// Save the list with sanitized text fields
	list := &models.List{
		URI:         result.List.URI,
		DID:         result.List.Creator.DID,
		RKey:        rkey,
		CID:         result.List.CID,
		Name:        sanitizeText(result.List.Name),
		Purpose:     result.List.Purpose,
		Description: sanitizeText(result.List.Description),
		ReceivedAt:  time.Now().UTC(),
		Status:      "active",
	}

	if t, err := time.Parse(time.RFC3339, result.List.CreatedAt); err == nil {
		list.CreatedAt = t.UTC()
	}

	if err := lf.p.db.SaveList(list); err != nil {
		lf.p.logger.Printf("⚠️ Failed to save list %s: %v", list.URI, err)
		return
	}
	// lf.p.logger.Printf("📋 Fetched list via API: %s (creator: %s, members: %d)", list.Name, list.DID, len(result.Items))

	// Queue the creator for actor fetching
	if lf.p.actorFetcher != nil {
		lf.p.actorFetcher.QueueDID(list.DID)
	}

	// Save all list items and queue their subjects for actor fetching
	if len(result.Items) > 0 {
		listItems := make([]*models.ListItem, 0, len(result.Items))
		for _, item := range result.Items {
			if item.Subject.DID == "" {
				continue
			}

			// Parse the URI to extract rkey
			itemParts := strings.Split(strings.TrimPrefix(item.URI, "at://"), "/")
			itemRkey := ""
			if len(itemParts) >= 3 {
				itemRkey = itemParts[len(itemParts)-1]
			}

			listItem := &models.ListItem{
				URI:        item.URI,
				ListURI:    list.URI,
				DID:        list.DID,
				SubjectDID: item.Subject.DID,
				RKey:       itemRkey,
				ReceivedAt: time.Now().UTC(),
				Status:     "active",
			}

			if t, err := time.Parse(time.RFC3339, item.CreatedAt); err == nil {
				listItem.CreatedAt = t.UTC()
			}

			listItems = append(listItems, listItem)

			// Queue the member for actor fetching
			if lf.p.actorFetcher != nil {
				lf.p.actorFetcher.QueueDID(item.Subject.DID)
			}
		}

		if len(listItems) > 0 {
			if err := lf.p.db.SaveListItemsBatch(listItems); err != nil {
				lf.p.logger.Printf("⚠️ Failed to save %d list items for %s: %v", len(listItems), list.URI, err)
			} else {
				// lf.p.logger.Printf("👥 Saved %d list items for list: %s", len(listItems), list.Name)
			}
		}
	}
}

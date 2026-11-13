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

// ==================== Types ====================

// ListFetcher handles fetching list information via the API.
type ListFetcher struct {
	p       *Processor
	queue   chan string
	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
	client  *http.Client

	sem     chan struct{}
	verbose bool
}

// ==================== Lifecycle ====================

// NewListFetcher creates a new ListFetcher.
func NewListFetcher(p *Processor) *ListFetcher {
	return &ListFetcher{
		p:       p,
		queue:   make(chan string, 10000),
		stopCh:  make(chan struct{}),
		client:  &http.Client{Timeout: 10 * time.Second},
		sem:     make(chan struct{}, 5),
		verbose: p.cfg.ListFetcherVerbose,
	}
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

// Stop stops the ListFetcher worker with a timeout.
func (lf *ListFetcher) Stop() {
	lf.mu.Lock()
	if !lf.started {
		lf.mu.Unlock()
		return
	}
	lf.started = false
	close(lf.stopCh)
	lf.mu.Unlock()

	done := make(chan struct{})
	go func() {
		lf.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		lf.p.logger.Printf("WARN: ListFetcher stop timed out, forcing exit")
	}
}

// ==================== Public API ====================

// QueueListURI adds a list URI to the fetch queue.
func (lf *ListFetcher) QueueListURI(uri string) {
	if uri == "" {
		return
	}
	select {
	case lf.queue <- uri:
	default:
		lf.warnf("WARN: list queue full, skipping %s", uri)
	}
}

// QueueDepth returns the number of pending list fetch URIs.
func (lf *ListFetcher) QueueDepth() int {
	return len(lf.queue)
}

// QueueCapacity returns the capacity of the list fetch queue.
func (lf *ListFetcher) QueueCapacity() int {
	return cap(lf.queue)
}

// ==================== Logging ====================

func (lf *ListFetcher) logf(format string, args ...interface{}) {
	if lf.verbose {
		lf.p.logger.Printf(format, args...)
	}
}

func (lf *ListFetcher) warnf(format string, args ...interface{}) {
	lf.p.logger.Printf(format, args...)
}

// ==================== Worker ====================

func (lf *ListFetcher) worker() {
	defer lf.wg.Done()

	for {
		select {
		case <-lf.stopCh:
			return
		case uri := <-lf.queue:
			go func(u string) {
				select {
				case lf.sem <- struct{}{}:
					defer func() { <-lf.sem }()
				case <-lf.stopCh:
					return
				}
				lf.fetchList(u)
			}(uri)
		}
	}
}

// ==================== Fetch ====================

// fetchList fetches a single list via the API (with pagination) and saves
// the list metadata plus all its members.
func (lf *ListFetcher) fetchList(listURI string) {
	_, rkey, ok := parseListURI(listURI)
	if !ok {
		lf.warnf("WARN: invalid list URI: %s", listURI)
		return
	}

	var (
		cursor     string
		totalItems int
		list       *models.List
		page       int
	)

	for {
		page++

		apiURL := fmt.Sprintf(
			"https://public.api.bsky.app/xrpc/app.bsky.graph.getList?list=%s&limit=100",
			url.QueryEscape(listURI),
		)
		if cursor != "" {
			apiURL += "&cursor=" + url.QueryEscape(cursor)
		}

		req, err := http.NewRequestWithContext(lf.p.ctx, "GET", apiURL, nil)
		if err != nil {
			return
		}
		resp, err := lf.client.Do(req)
		if err != nil {
			if lf.p.ctx.Err() == context.Canceled {
				return
			}
			lf.warnf("WARN: failed to fetch list %s (page %d): %v", listURI, page, err)
			return
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lf.warnf("WARN: list API returned %d for %s (page %d)", resp.StatusCode, listURI, page)
			return
		}

		var result listAPIResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			lf.warnf("WARN: failed to decode list response for %s (page %d): %v", listURI, page, err)
			return
		}

		// First page: save list metadata
		if list == nil {
			list = lf.buildList(rkey, &result)
			if err := lf.p.db.SaveList(list); err != nil {
				lf.warnf("WARN: failed to save list %s: %v", list.URI, err)
				return
			}
			lf.logf("Fetched list: %s (creator: %s)", list.Name, list.DID)

			if lf.p.actorFetcher != nil {
				lf.p.actorFetcher.QueueDID(list.DID)
			}
		}

		// Save this page's items
		if len(result.Items) > 0 {
			lf.saveListItems(list, result.Items)
			totalItems += len(result.Items)
			lf.logf("Page %d: saved %d list items (running total: %d)", page, len(result.Items), totalItems)
		}

		// Termination: no cursor or empty page
		if result.Cursor == nil || *result.Cursor == "" {
			break
		}
		if len(result.Items) == 0 {
			break
		}
		cursor = *result.Cursor
	}

	if list != nil {
		lf.logf("Finished list %s: %s (creator: %s, total members: %d)",
			list.URI, list.Name, list.DID, totalItems)
	}
}

// ==================== Build Helpers ====================

type listAPIResponse struct {
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
	Cursor *string `json:"cursor,omitempty"`
}

func (lf *ListFetcher) buildList(rkey string, result *listAPIResponse) *models.List {
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
	return list
}

func (lf *ListFetcher) saveListItems(list *models.List, items []struct {
	URI     string `json:"uri"`
	Subject struct {
		DID string `json:"did"`
	} `json:"subject"`
	CreatedAt string `json:"createdAt"`
}) {
	if len(items) == 0 {
		return
	}

	listItems := make([]*models.ListItem, 0, len(items))
	for _, item := range items {
		if item.Subject.DID == "" {
			continue
		}

		_, itemRkey, _ := parseListURI(item.URI)

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

		if lf.p.actorFetcher != nil {
			lf.p.actorFetcher.QueueDID(item.Subject.DID)
		}
	}

	if len(listItems) > 0 {
		if err := lf.p.db.SaveListItemsBatch(listItems); err != nil {
			lf.warnf("WARN: failed to save %d list items for %s: %v", len(listItems), list.URI, err)
		}
	}
}

// ==================== URI Helpers ====================

// parseListURI parses "at://did/collection/rkey" into (repo, rkey, ok).
func parseListURI(uri string) (repo, rkey string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(uri, "at://"), "/")
	if len(parts) < 3 {
		return "", "", false
	}
	return parts[0], parts[len(parts)-1], true
}

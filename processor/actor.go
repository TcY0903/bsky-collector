package processor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"bsky-collector/models"
)

var (
	didRegexp = regexp.MustCompile(`^did:plc:[a-zA-Z0-9]{24}$`)
)

// ActorFetcher handles fetching actor profiles via the batch API.
type ActorFetcher struct {
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

func NewActorFetcher(p *Processor) *ActorFetcher {
	af := &ActorFetcher{
		p:         p,
		queue:     make(chan string, 2000),
		stopCh:    make(chan struct{}),
		batchSize: 25,
		ticker:    time.NewTicker(3 * time.Second),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	return af
}

func (af *ActorFetcher) Start() {
	af.mu.Lock()
	defer af.mu.Unlock()
	if af.started {
		return
	}
	af.started = true
	af.wg.Add(1)
	go af.worker()
}

func (af *ActorFetcher) Stop() {
	af.mu.Lock()
	defer af.mu.Unlock()
	if !af.started {
		return
	}
	af.started = false
	close(af.stopCh)
	af.ticker.Stop()
	af.wg.Wait()
}

func (af *ActorFetcher) QueueDID(did string) {
	if did == "" || !didRegexp.MatchString(did) {
		return
	}
	select {
	case af.queue <- did:
	default:
		af.p.logger.Printf("⚠️ Actor queue full, skipping %s", did)
	}
}

// QueueActorsForPosts extracts DIDs from posts and queues them for fetching.
func (af *ActorFetcher) QueueActorsForPosts(posts []*models.Post) {
	for _, post := range posts {
		if post.DID != "" {
			af.QueueDID(post.DID)
		}
	}
}

func (af *ActorFetcher) worker() {
	defer af.wg.Done()

	batch := make([]string, 0, af.batchSize)
	ticker := af.ticker

	for {
		select {
		case <-af.stopCh:
			if len(batch) > 0 {
				af.fetchBatch(batch)
			}
			return
		case did := <-af.queue:
			batch = append(batch, did)
			if len(batch) >= af.batchSize {
				af.fetchBatch(batch)
				batch = make([]string, 0, af.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				af.fetchBatch(batch)
				batch = make([]string, 0, af.batchSize)
			}
		}
	}
}

func (af *ActorFetcher) fetchBatch(dids []string) {
	validDIDs := []string{}
	for _, did := range dids {
		if didRegexp.MatchString(did) {
			validDIDs = append(validDIDs, did)
		}
	}
	if len(validDIDs) == 0 {
		return
	}

	params := url.Values{}
	for _, did := range validDIDs {
		params.Add("actors", did)
	}
	reqURL := "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfiles?" + params.Encode()

	req, err := http.NewRequestWithContext(af.p.ctx, "GET", reqURL, nil)
	if err != nil {
		af.p.logger.Printf("⚠️ Failed to create actor request: %v", err)
		return
	}

	resp, err := af.client.Do(req)
	if err != nil {
		if af.p.ctx.Err() == context.Canceled {
			af.p.logger.Printf("⚠️ Actor request cancelled (shutdown)")
		} else {
			af.p.logger.Printf("⚠️ Failed to fetch actors: %v", err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		af.p.logger.Printf("⚠️ Actor API returned status %d", resp.StatusCode)
		return
	}

	var result struct {
		Profiles []models.Actor `json:"profiles"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		af.p.logger.Printf("⚠️ Failed to decode actor response: %v", err)
		return
	}

	actors := make([]*models.Actor, 0, len(result.Profiles))
	for i := range result.Profiles {
		actor := &result.Profiles[i]
		if actor.Status == "" {
			actor.Status = "active"
		}
		actor.ReceivedAt = time.Now()
		actors = append(actors, actor)
	}

	if len(actors) > 0 {
		if err := af.p.db.SaveActorsBatch(actors); err != nil {
			af.p.logger.Printf("⚠️ Failed to save %d actors: %v", len(actors), err)
		}
	}
}

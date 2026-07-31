package processor

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"bsky-collector/models"
)

var (
	didRegexp = regexp.MustCompile(`^did:plc:[a-zA-Z0-9]{24}$`)
)

// queueActorsForBatch queues DIDs for asynchronous actor fetching.
func (p *Processor) queueActorsForBatch(posts []*models.Post) {
	didSet := make(map[string]bool)
	for _, post := range posts {
		if post.DID != "" && didRegexp.MatchString(post.DID) {
			didSet[post.DID] = true
		} else if post.DID != "" {
			p.logger.Printf("⚠️ Invalid DID format: %s", post.DID)
		}
	}

	for did := range didSet {
		select {
		case p.actorQueue <- did:
		default:
			p.logger.Printf("⚠️ Actor queue full, skipping %s", did)
		}
	}
}

// actorWorker runs in a goroutine and processes actor fetch requests.
func (p *Processor) actorWorker() {
	defer p.actorWg.Done()

	const batchSize = 25
	batch := make([]string, 0, batchSize)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.actorStop:
			if len(batch) > 0 {
				p.fetchActorsBatch(batch)
			}
			return
		case did := <-p.actorQueue:
			batch = append(batch, did)
			if len(batch) >= batchSize {
				p.fetchActorsBatch(batch)
				batch = make([]string, 0, batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.fetchActorsBatch(batch)
				batch = make([]string, 0, batchSize)
			}
		}
	}
}

// fetchActorsBatch fetches actor profiles for a batch of DIDs using the batch API.
func (p *Processor) fetchActorsBatch(dids []string) {
	if len(dids) == 0 {
		return
	}

	validDIDs := []string{}
	for _, did := range dids {
		if didRegexp.MatchString(did) {
			validDIDs = append(validDIDs, did)
		} else {
			p.logger.Printf("⚠️ Skipping invalid DID: %s", did)
		}
	}

	if len(validDIDs) == 0 {
		return
	}

	if len(validDIDs) > 25 {
		for i := 0; i < len(validDIDs); i += 25 {
			end := i + 25
			if end > len(validDIDs) {
				end = len(validDIDs)
			}
			p.fetchActorsBatch(validDIDs[i:end])
		}
		return
	}

	baseURL := "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfiles"
	params := url.Values{}
	for _, did := range validDIDs {
		params.Add("actors", did)
	}

	reqURL := baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(p.ctx, "GET", reqURL, nil)
	if err != nil {
		p.logger.Printf("⚠️ Failed to create request for batch: %v", err)
		return
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		p.logger.Printf("⚠️ Failed to fetch batch profiles: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.logger.Printf("⚠️ Batch API returned status %d", resp.StatusCode)
		return
	}

	var result struct {
		Profiles []models.Actor `json:"profiles"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		p.logger.Printf("⚠️ Failed to decode batch response: %v", err)
		return
	}

	if len(result.Profiles) == 0 {
		return
	}

	actors := make([]*models.Actor, 0, len(result.Profiles))
	for i := range result.Profiles {
		actor := &result.Profiles[i]
		if actor.Status == "" {
			actor.Status = "active"
		}
		actor.IndexedAt = time.Now()
		actors = append(actors, actor)
	}

	if len(actors) > 0 {
		if err := p.db.SaveActorsBatch(actors); err != nil {
			p.logger.Printf("⚠️ Failed to save %d actors: %v", len(actors), err)
		}
	}
}

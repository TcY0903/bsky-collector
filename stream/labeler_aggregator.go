package stream

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"

	"bsky-collector/models"
	"bsky-collector/storage"
)

const (
	labelFirehoseURL         = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	maxConcurrentConnections = 20
	maxConnectPerTick        = 10
	maxResolveFailures       = 3
	maxConnectFailures       = 5
)

// LabelAggregator connects directly to many labelers and aggregates their streams.
type LabelAggregator struct {
	logger   *log.Logger
	db       *storage.DB
	csvPath  string
	eventCh  chan []byte
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewLabelAggregator creates a new LabelAggregator.
func NewLabelAggregator(logger *log.Logger, db *storage.DB, csvPath string) *LabelAggregator {
	return &LabelAggregator{
		logger:  logger,
		db:      db,
		csvPath: csvPath,
		eventCh: make(chan []byte, 10000),
		stopCh:  make(chan struct{}),
	}
}

// Events returns the channel of raw label stream messages.
func (a *LabelAggregator) Events() <-chan []byte {
	return a.eventCh
}

// Close shuts down the aggregator.
func (a *LabelAggregator) Close() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
}

// StartWithRetry starts the aggregator and blocks until the context is cancelled.
func (a *LabelAggregator) StartWithRetry(ctx context.Context) {
	var (
		mu           sync.Mutex
		labelers     = make(map[string]bool)
		orderedDIDs  []string
		resolveFails = make(map[string]int)
		connectFails = make(map[string]int)
	)

	sem := make(chan struct{}, maxConcurrentConnections)

	// 1. Load labelers from database
	dbLabelers, err := a.db.GetAllLabelers()
	if err != nil {
		a.logger.Printf("WARN: failed to load labelers from database: %v", err)
	} else {
		loaded := 0
		skipped := 0
		for _, l := range dbLabelers {
			// Skip permanently failed labelers
			if l.Status == "resolve_failed" {
				skipped++
				continue
			}
			labelers[l.DID] = false
			orderedDIDs = append(orderedDIDs, l.DID)
			loaded++
		}
		a.logger.Printf("Loaded %d labelers from database (%d skipped as resolve_failed)",
			loaded, skipped)
	}

	// 2. Load seed labelers from CSV
	knownDIDs, err := loadKnownLabelers(a.csvPath)
	if err != nil {
		a.logger.Printf("WARN: failed to load seed labelers from %s: %v", a.csvPath, err)
	} else {
		added := 0
		for _, did := range knownDIDs {
			if _, exists := labelers[did]; exists {
				continue
			}
			labelers[did] = false
			orderedDIDs = append(orderedDIDs, did)
			now := time.Now().UTC()
			if err := a.db.SaveLabeler(&models.Labeler{
				DID:       did,
				Status:    "unknown",
				FirstSeen: now,
				UpdatedAt: now,
			}); err != nil {
				a.logger.Printf("WARN: failed to save seed labeler %s: %v", did, err)
			}
			added++
		}
		if added > 0 {
			a.logger.Printf("Added %d new seed labelers from %s", added, a.csvPath)
		}
	}

	a.logger.Printf("Starting with %d labelers in queue", len(orderedDIDs))

	// 3. Firehose listener
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			default:
			}
			if err := a.listenFirehose(ctx, labelers, &orderedDIDs, &mu); err != nil {
				a.logger.Printf("WARN: label firehose error: %v, reconnecting in 5s", err)
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				case <-a.stopCh:
					return
				}
			}
		}
	}()

	// 4. Periodic connection loop
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			case <-ticker.C:
				mu.Lock()
				var toConnect []string
				for _, did := range orderedDIDs {
					if len(toConnect) >= maxConnectPerTick {
						break
					}
					if !labelers[did] {
						toConnect = append(toConnect, did)
					}
				}
				mu.Unlock()

				for _, did := range toConnect {
					select {
					case sem <- struct{}{}:
						go func(d string) {
							defer func() { <-sem }()
							a.connectToLabeler(ctx, d, labelers, &orderedDIDs, resolveFails, connectFails, &mu)
						}(did)
					default:
					}
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-a.stopCh:
	}
}

func (a *LabelAggregator) listenFirehose(ctx context.Context, labelers map[string]bool, orderedDIDs *[]string, mu *sync.Mutex) error {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, labelFirehoseURL, nil)
	if err != nil {
		return fmt.Errorf("dial firehose: %w", err)
	}
	defer conn.Close()
	a.logger.Println("Connected to Firehose (label discovery)")

	handler := &labelFirehoseHandler{
		logger:      a.logger,
		db:          a.db,
		labelers:    labelers,
		orderedDIDs: orderedDIDs,
		mu:          mu,
	}
	sched := parallel.NewScheduler(10, 100, "label-aggregator", handler.HandleEvent)
	return events.HandleRepoStream(ctx, conn, sched, nil)
}

type labelFirehoseHandler struct {
	logger      *log.Logger
	db          *storage.DB
	labelers    map[string]bool
	orderedDIDs *[]string
	mu          *sync.Mutex
}

func (h *labelFirehoseHandler) HandleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	if evt.RepoCommit == nil {
		return nil
	}
	commit := evt.RepoCommit
	for _, op := range commit.Ops {
		if op.Path == "" || !strings.HasPrefix(op.Path, "app.bsky.labeler.service/") {
			continue
		}
		did := commit.Repo
		h.mu.Lock()
		switch op.Action {
		case "create":
			if _, exists := h.labelers[did]; !exists {
				h.labelers[did] = false
				*h.orderedDIDs = append(*h.orderedDIDs, did)
				h.logger.Printf("Discovered new labeler: %s", did)
				now := time.Now().UTC()
				if err := h.db.SaveLabeler(&models.Labeler{
					DID:       did,
					Status:    "unknown",
					FirstSeen: now,
					UpdatedAt: now,
				}); err != nil {
					h.logger.Printf("WARN: failed to save discovered labeler %s: %v", did, err)
				}
			}
		case "delete":
			if _, exists := h.labelers[did]; exists {
				delete(h.labelers, did)
				removeFromSlice(h.orderedDIDs, did)
				h.logger.Printf("Labeler removed service declaration: %s", did)
				if err := h.db.DeleteLabeler(did); err != nil {
					h.logger.Printf("WARN: failed to delete labeler %s: %v", did, err)
				}
			}
		}
		h.mu.Unlock()
	}
	return nil
}

func (a *LabelAggregator) connectToLabeler(
	ctx context.Context,
	did string,
	labelers map[string]bool,
	orderedDIDs *[]string,
	resolveFails map[string]int,
	connectFails map[string]int,
	mu *sync.Mutex,
) {
	// Resolve DID document
	endpoint, err := resolveLabelerEndpoint(ctx, did)
	if err != nil {
		a.recordFailure(did, "resolve", err.Error(), orderedDIDs, resolveFails, mu, maxResolveFailures)
		return
	}
	if endpoint == "" {
		a.recordFailure(did, "resolve", "no #atproto_labeler service found", orderedDIDs, resolveFails, mu, maxResolveFailures)
		return
	}

	// Attempt WebSocket connection
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		a.recordFailure(did, "connect", err.Error(), orderedDIDs, connectFails, mu, maxConnectFailures)
		return
	}
	defer conn.Close()

	mu.Lock()
	labelers[did] = true
	delete(resolveFails, did)
	delete(connectFails, did)
	mu.Unlock()

	if err := a.db.UpdateLabelerConnected(did, endpoint); err != nil {
		a.logger.Printf("WARN: failed to update labeler %s as connected: %v", did, err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			mu.Lock()
			labelers[did] = false
			mu.Unlock()
			return
		}

		select {
		case a.eventCh <- message:
		default:
			a.logger.Printf("WARN: label channel full, dropping message from %s", did)
		}
	}
}

// recordFailure handles connection failures with type-specific logic.
// failureType: "resolve" (permanent) or "connect" (temporary).
func (a *LabelAggregator) recordFailure(
	did string,
	failureType string,
	lastError string,
	orderedDIDs *[]string,
	failures map[string]int,
	mu *sync.Mutex,
	maxFailures int,
) {
	mu.Lock()
	defer mu.Unlock()

	failures[did]++
	failCount := failures[did]

	// Determine status based on failure type and count
	var status string
	if failCount >= maxFailures {
		if failureType == "resolve" {
			status = "resolve_failed"
		} else {
			status = "connect_failed"
		}
	} else {
		status = "unknown"
	}

	if err := a.db.UpdateLabelerStatus(did, status, failureType, lastError, failCount); err != nil {
		a.logger.Printf("WARN: failed to update labeler %s status: %v", did, err)
	}

	if failCount >= maxFailures {
		delete(failures, did)
		// Move to end of queue (not removed entirely, so it stays in DB)
		moveToEnd(did, orderedDIDs)
		return
	}

	moveToEnd(did, orderedDIDs)
}

// --- helpers ---

func loadKnownLabelers(csvPath string) ([]string, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var dids []string
	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) > 0 && strings.HasPrefix(record[0], "did:") {
			dids = append(dids, record[0])
		}
	}
	return dids, nil
}

func resolveLabelerEndpoint(ctx context.Context, did string) (string, error) {
	url := fmt.Sprintf("https://plc.directory/%s", did)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	var didDoc struct {
		Service []struct {
			ID              string `json:"id"`
			Type            string `json:"type"`
			ServiceEndpoint string `json:"serviceEndpoint"`
		} `json:"service"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&didDoc); err != nil {
		return "", err
	}

	for _, svc := range didDoc.Service {
		if svc.ID == "#atproto_labeler" || strings.Contains(svc.Type, "AtprotoLabeler") {
			wsURL := strings.Replace(svc.ServiceEndpoint, "https://", "wss://", 1)
			wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
			wsURL = strings.TrimSuffix(wsURL, "/") + "/xrpc/com.atproto.label.subscribeLabels"
			return wsURL, nil
		}
	}

	return "", nil
}

func moveToEnd(did string, orderedDIDs *[]string) {
	for i, d := range *orderedDIDs {
		if d == did {
			*orderedDIDs = append((*orderedDIDs)[:i], (*orderedDIDs)[i+1:]...)
			*orderedDIDs = append(*orderedDIDs, did)
			return
		}
	}
}

func removeFromSlice(slice *[]string, did string) {
	for i, d := range *slice {
		if d == did {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			return
		}
	}
}

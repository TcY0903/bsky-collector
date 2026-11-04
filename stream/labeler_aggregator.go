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
	csvPath  string
	eventCh  chan []byte
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewLabelAggregator creates a new LabelAggregator.
func NewLabelAggregator(logger *log.Logger, csvPath string) *LabelAggregator {
	return &LabelAggregator{
		logger:  logger,
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

	// Cold start: load known labelers from CSV
	knownDIDs, err := loadKnownLabelers(a.csvPath)
	if err != nil {
		a.logger.Printf("⚠️ Failed to load labelers from %s: %v", a.csvPath, err)
	} else {
		for _, did := range knownDIDs {
			if _, exists := labelers[did]; !exists {
				labelers[did] = false
				orderedDIDs = append(orderedDIDs, did)
			}
		}
		a.logger.Printf("📋 Loaded %d known labelers from %s", len(knownDIDs), a.csvPath)
	}

	// Firehose listener for discovery + deletion
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
				a.logger.Printf("⚠️ Label firehose error: %v, reconnecting in 5s...", err)
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

	// Periodic connection loop
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

	// Block until shutdown
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

	handler := &labelFirehoseHandler{
		logger:      a.logger,
		labelers:    labelers,
		orderedDIDs: orderedDIDs,
		mu:          mu,
	}
	sched := parallel.NewScheduler(10, 100, "label-aggregator", handler.HandleEvent)
	return events.HandleRepoStream(ctx, conn, sched, nil)
}

type labelFirehoseHandler struct {
	logger      *log.Logger
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
				h.logger.Printf("🔍 Discovered new labeler: %s", did)
			}
		case "delete":
			if _, exists := h.labelers[did]; exists {
				delete(h.labelers, did)
				removeFromSlice(h.orderedDIDs, did)
				h.logger.Printf("🗑️ Labeler removed service declaration: %s", did)
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
	endpoint, err := resolveLabelerEndpoint(ctx, did)
	if err != nil {
		recordFailure(did, orderedDIDs, resolveFails, mu, maxResolveFailures)
		return
	}
	if endpoint == "" {
		recordFailure(did, orderedDIDs, resolveFails, mu, maxResolveFailures)
		return
	}

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		recordFailure(did, orderedDIDs, connectFails, mu, maxConnectFailures)
		return
	}
	defer conn.Close()

	mu.Lock()
	labelers[did] = true
	delete(resolveFails, did)
	delete(connectFails, did)
	mu.Unlock()

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
			a.logger.Printf("⚠️ Label channel full, dropping message from %s", did)
		}
	}
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

func recordFailure(did string, orderedDIDs *[]string, failures map[string]int, mu *sync.Mutex, maxFailures int) {
	mu.Lock()
	defer mu.Unlock()

	failures[did]++
	if failures[did] >= maxFailures {
		delete(failures, did)
		removeFromSlice(orderedDIDs, did)
		return
	}

	// Move to end of queue
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

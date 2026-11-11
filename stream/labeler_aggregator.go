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

	"bsky-collector/config"
	"bsky-collector/models"
	"bsky-collector/storage"
)

// ==================== Types ====================

// LabelAggregator connects directly to many labelers and aggregates their streams.
type LabelAggregator struct {
	cfg    *config.Config
	logger *log.Logger
	db     *storage.DB

	eventCh  chan []byte
	stopCh   chan struct{}
	stopOnce sync.Once
}

// labelerState tracks the in-memory state of each labeler.
type labelerState struct {
	mu           sync.Mutex
	labelers     map[string]bool
	orderedDIDs  []string
	resolveFails map[string]int
	connectFails map[string]int
	sem          chan struct{}
}

func newLabelerState(maxConcurrent int) *labelerState {
	if maxConcurrent <= 0 {
		maxConcurrent = 20
	}
	return &labelerState{
		labelers:     make(map[string]bool),
		resolveFails: make(map[string]int),
		connectFails: make(map[string]int),
		sem:          make(chan struct{}, maxConcurrent),
	}
}

// ==================== Lifecycle ====================

// NewLabelAggregator creates a new LabelAggregator using config.
func NewLabelAggregator(cfg *config.Config, logger *log.Logger, db *storage.DB) *LabelAggregator {
	eventChannelSize := cfg.LabelAggregatorEventChannelSize
	if eventChannelSize <= 0 {
		eventChannelSize = 10000
	}
	return &LabelAggregator{
		cfg:     cfg,
		logger:  logger,
		db:      db,
		eventCh: make(chan []byte, eventChannelSize),
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

// ==================== Startup ====================

// StartWithRetry starts the aggregator and blocks until the context is cancelled.
func (a *LabelAggregator) StartWithRetry(ctx context.Context) {
	st := newLabelerState(a.cfg.LabelAggregatorMaxConcurrent)

	a.loadFromDatabase(st)
	a.loadFromCSV(st)

	// Firehose listener for discovery (with exponential backoff)
	go a.runFirehoseListener(ctx, st)

	// Periodic connection loop
	go a.runConnectionLoop(ctx, st)

	// Block until shutdown
	select {
	case <-ctx.Done():
	case <-a.stopCh:
	}
}

func (a *LabelAggregator) loadFromDatabase(st *labelerState) {
	dbLabelers, err := a.db.GetAllLabelers()
	if err != nil {
		a.logger.Printf("WARN: failed to load labelers from database: %v", err)
		return
	}

	loaded, skipped := 0, 0
	for _, l := range dbLabelers {
		if l.Status == "resolve_failed" {
			skipped++
			continue
		}
		st.labelers[l.DID] = false
		st.orderedDIDs = append(st.orderedDIDs, l.DID)
		loaded++
	}
	a.logger.Printf("Loaded %d labelers from database (%d skipped as resolve_failed)", loaded, skipped)
}

func (a *LabelAggregator) loadFromCSV(st *labelerState) {
	knownDIDs, err := loadKnownLabelers(a.cfg.LabelAggregatorCSVPath)
	if err != nil {
		a.logger.Printf("WARN: failed to load seed labelers from %s: %v", a.cfg.LabelAggregatorCSVPath, err)
		return
	}

	added := 0
	for _, did := range knownDIDs {
		if _, exists := st.labelers[did]; exists {
			continue
		}
		st.labelers[did] = false
		st.orderedDIDs = append(st.orderedDIDs, did)

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
		a.logger.Printf("Added %d new seed labelers from %s", added, a.cfg.LabelAggregatorCSVPath)
	}
}

// ==================== Firehose Listener ====================

// runFirehoseListener runs the discovery Firehose with exponential backoff.
func (a *LabelAggregator) runFirehoseListener(ctx context.Context, st *labelerState) {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		default:
		}

		err := a.listenFirehoseOnce(ctx, st)

		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		default:
		}

		if err != nil {
			a.logger.Printf("WARN: label firehose error: %v", err)
			a.logger.Printf("Reconnecting label firehose in %v...", backoff)

			select {
			case <-time.After(backoff):
				backoff = minDuration(backoff*2, maxBackoff)
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			}
		} else {
			backoff = 1 * time.Second
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			}
		}
	}
}

func (a *LabelAggregator) listenFirehoseOnce(ctx context.Context, st *labelerState) error {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, a.cfg.LabelAggregatorFirehoseURL, nil)
	if err != nil {
		return fmt.Errorf("dial firehose: %w", err)
	}
	defer conn.Close()
	a.logger.Println("Connected to Firehose (label discovery)")

	handler := &labelFirehoseHandler{
		logger: a.logger,
		db:     a.db,
		state:  st,
	}
	sched := parallel.NewScheduler(10, 100, "label-aggregator", handler.HandleEvent)
	return events.HandleRepoStream(ctx, conn, sched, nil)
}

// ==================== Firehose Handler ====================

type labelFirehoseHandler struct {
	logger *log.Logger
	db     *storage.DB
	state  *labelerState
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

		h.state.mu.Lock()
		switch op.Action {
		case "create":
			if _, exists := h.state.labelers[did]; !exists {
				h.state.labelers[did] = false
				h.state.orderedDIDs = append(h.state.orderedDIDs, did)
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
			if _, exists := h.state.labelers[did]; exists {
				delete(h.state.labelers, did)
				removeFromSlice(&h.state.orderedDIDs, did)
				h.logger.Printf("Labeler removed service declaration: %s", did)
				if err := h.db.DeleteLabeler(did); err != nil {
					h.logger.Printf("WARN: failed to delete labeler %s: %v", did, err)
				}
			}
		}
		h.state.mu.Unlock()
	}
	return nil
}

// ==================== Connection Loop ====================

func (a *LabelAggregator) runConnectionLoop(ctx context.Context, st *labelerState) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-ticker.C:
			a.connectNextBatch(ctx, st)
		}
	}
}

func (a *LabelAggregator) connectNextBatch(ctx context.Context, st *labelerState) {
	maxPerTick := a.cfg.LabelAggregatorMaxPerTick
	if maxPerTick <= 0 {
		maxPerTick = 10
	}

	st.mu.Lock()
	var toConnect []string
	for _, did := range st.orderedDIDs {
		if len(toConnect) >= maxPerTick {
			break
		}
		if !st.labelers[did] {
			toConnect = append(toConnect, did)
		}
	}
	st.mu.Unlock()

	for _, did := range toConnect {
		select {
		case st.sem <- struct{}{}:
			go func(d string) {
				defer func() { <-st.sem }()
				a.connectToLabeler(ctx, st, d)
			}(did)
		default:
		}
	}
}

// ==================== Labeler Connection ====================

func (a *LabelAggregator) connectToLabeler(ctx context.Context, st *labelerState, did string) {
	endpoint, err := resolveLabelerEndpoint(ctx, did)
	if err != nil {
		a.recordFailure(st, did, "resolve", err.Error(), a.cfg.LabelAggregatorMaxResolveFailures)
		return
	}
	if endpoint == "" {
		a.recordFailure(st, did, "resolve", "no #atproto_labeler service found", a.cfg.LabelAggregatorMaxResolveFailures)
		return
	}

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		a.recordFailure(st, did, "connect", err.Error(), a.cfg.LabelAggregatorMaxConnectFailures)
		return
	}
	defer conn.Close()

	st.mu.Lock()
	st.labelers[did] = true
	delete(st.resolveFails, did)
	delete(st.connectFails, did)
	st.mu.Unlock()

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
			st.mu.Lock()
			st.labelers[did] = false
			st.mu.Unlock()
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
func (a *LabelAggregator) recordFailure(st *labelerState, did, failureType, lastError string, maxFailures int) {
	st.mu.Lock()
	defer st.mu.Unlock()

	var failures map[string]int
	if failureType == "resolve" {
		failures = st.resolveFails
	} else {
		failures = st.connectFails
	}

	failures[did]++
	failCount := failures[did]

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
	}

	moveToEnd(did, &st.orderedDIDs)
}

// ==================== Helpers ====================

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

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

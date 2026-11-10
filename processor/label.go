package processor

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"

	"bsky-collector/models"
)

// ==================== Types ====================

// LabelProcessor handles batch processing of labels from the aggregator.
type LabelProcessor struct {
	p          *Processor
	labelBatch []*models.Label
	batchMu    sync.Mutex

	batchSize int
	ticker    *time.Ticker
	stopCh    chan struct{}
	closeOnce sync.Once
	verbose   bool
}

// ==================== Lifecycle ====================

// NewLabelProcessor creates a new LabelProcessor using shared processor config.
func NewLabelProcessor(p *Processor) *LabelProcessor {
	batchSize := p.cfg.ProcessorBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	tickerSec := p.cfg.ProcessorTickerSec
	if tickerSec <= 0 {
		tickerSec = 3
	}

	lp := &LabelProcessor{
		p:          p,
		labelBatch: make([]*models.Label, 0, batchSize),
		batchSize:  batchSize,
		ticker:     time.NewTicker(time.Duration(tickerSec) * time.Second),
		stopCh:     make(chan struct{}),
		verbose:    p.cfg.LabelProcessorVerbose,
	}
	go lp.autoFlush()
	return lp
}

func (lp *LabelProcessor) autoFlush() {
	for {
		select {
		case <-lp.ticker.C:
			lp.flushLabels()
		case <-lp.stopCh:
			return
		}
	}
}

// Close shuts down the LabelProcessor.
func (lp *LabelProcessor) Close() {
	lp.closeOnce.Do(func() {
		close(lp.stopCh)
		lp.ticker.Stop()
		lp.flushLabels()
	})
}

// ==================== Processing ====================

// ProcessLabelMessage parses a raw message from the label aggregator.
func (lp *LabelProcessor) ProcessLabelMessage(message []byte) error {
	dec := cbor.NewDecoder(bytes.NewReader(message))

	var header map[string]interface{}
	if err := dec.Decode(&header); err != nil {
		return fmt.Errorf("header decode failed: %w", err)
	}

	if t, _ := header["t"].(string); t != "#labels" {
		return nil
	}

	var payload interface{}
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("payload decode failed: %w", err)
	}

	m, ok := payload.(map[interface{}]interface{})
	if !ok {
		return nil
	}

	seq := toInt64(m["seq"])

	labelsRaw, ok := m["labels"]
	if !ok {
		return nil
	}
	labelsSlice, ok := labelsRaw.([]interface{})
	if !ok {
		return nil
	}

	now := time.Now().UTC()
	batch := make([]*models.Label, 0, len(labelsSlice))

	for _, labelRaw := range labelsSlice {
		labelMap, ok := labelRaw.(map[interface{}]interface{})
		if !ok {
			continue
		}

		label := &models.Label{
			Seq:        seq,
			Src:        getString(labelMap, "src"),
			URI:        getString(labelMap, "uri"),
			Val:        getString(labelMap, "val"),
			Ver:        int(toInt64(labelMap["ver"])),
			Neg:        getBool(labelMap, "neg"),
			ReceivedAt: now,
		}
		if label.Ver == 0 {
			label.Ver = 1
		}

		if ctsStr := getString(labelMap, "cts"); ctsStr != "" {
			if t, err := time.Parse(time.RFC3339, ctsStr); err == nil {
				label.CTS = t.UTC()
			}
		}

		if sigRaw, ok := labelMap["sig"].([]byte); ok {
			label.Sig = sigRaw
		}

		if label.URI == "" || label.Val == "" || label.Src == "" {
			continue
		}
		batch = append(batch, label)

		// Ensure the target of this label exists (post thread context or actor)
		if !label.Neg {
			lp.ensureTargetExists(label.URI)
		}
	}

	if len(batch) > 0 {
		lp.batchMu.Lock()
		lp.labelBatch = append(lp.labelBatch, batch...)
		if len(lp.labelBatch) >= lp.batchSize {
			b := lp.labelBatch
			lp.labelBatch = make([]*models.Label, 0, lp.batchSize)
			lp.batchMu.Unlock()
			lp.flushLabelsWithBatch(b)
		} else {
			lp.batchMu.Unlock()
		}
	}

	return nil
}

// ensureTargetExists fetches the labeled content's context.
//   - Post: fetch full thread context via getPostThread (regardless of local existence)
//   - Profile: fetch actor
func (lp *LabelProcessor) ensureTargetExists(uri string) {
	switch {
	case strings.Contains(uri, "/app.bsky.feed.post/"):
		if lp.p.postFetcher != nil {
			lp.p.postFetcher.FetchThreadContext(uri)
		}
	case strings.Contains(uri, "/app.bsky.actor.profile/"):
		did := extractDIDFromURI(uri)
		if did != "" && lp.p.actorFetcher != nil {
			lp.p.actorFetcher.QueueDID(did)
		}
	}
}

// extractDIDFromURI extracts the DID from an AT-URI.
// Example: "at://did:plc:xxx/app.bsky.actor.profile/self" -> "did:plc:xxx"
func extractDIDFromURI(uri string) string {
	parts := strings.Split(strings.TrimPrefix(uri, "at://"), "/")
	if len(parts) >= 1 && strings.HasPrefix(parts[0], "did:") {
		return parts[0]
	}
	return ""
}

// ==================== Flush ====================

func (lp *LabelProcessor) flushLabels() {
	lp.batchMu.Lock()
	if len(lp.labelBatch) == 0 {
		lp.batchMu.Unlock()
		return
	}
	batch := lp.labelBatch
	lp.labelBatch = make([]*models.Label, 0, lp.batchSize)
	lp.batchMu.Unlock()
	lp.flushLabelsWithBatch(batch)
}

func (lp *LabelProcessor) flushLabelsWithBatch(batch []*models.Label) {
	if len(batch) == 0 {
		return
	}
	if err := lp.p.db.SaveLabelsBatch(batch); err != nil {
		lp.warnf("WARN: failed to save batch of %d labels: %v", len(batch), err)
	} else {
		lp.logf("Saved %d labels", len(batch))
	}
}

// ==================== Logging ====================

func (lp *LabelProcessor) logf(format string, args ...interface{}) {
	if lp.verbose {
		lp.p.logger.Printf(format, args...)
	}
}

func (lp *LabelProcessor) warnf(format string, args ...interface{}) {
	lp.p.logger.Printf(format, args...)
}

// ==================== Helpers ====================

func getString(m map[interface{}]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[interface{}]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case uint:
		return int64(n)
	}
	return 0
}

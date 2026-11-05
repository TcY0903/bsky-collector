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

// LabelProcessor handles batch processing of labels from the aggregator.
type LabelProcessor struct {
	p          *Processor
	labelBatch []*models.Label
	batchMu    sync.Mutex
	batchSize  int
	ticker     *time.Ticker
	stopCh     chan struct{}
	closeOnce  sync.Once
}

// NewLabelProcessor creates a new LabelProcessor.
func NewLabelProcessor(p *Processor) *LabelProcessor {
	lp := &LabelProcessor{
		p:          p,
		labelBatch: make([]*models.Label, 0, 500),
		batchSize:  500,
		ticker:     time.NewTicker(3 * time.Second),
		stopCh:     make(chan struct{}),
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

		// If this label points to a post, ensure the post exists
		if strings.Contains(label.URI, "/app.bsky.feed.post/") {
			lp.ensurePostExists(label.URI)
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

// ensurePostExists checks if the post referenced by the label exists locally;
// if not, queues it for fetching.
func (lp *LabelProcessor) ensurePostExists(postURI string) {
	post, err := lp.p.db.GetPostByURI(postURI)
	if err != nil {
		lp.p.logger.Printf("WARN: failed to check post existence %s: %v", postURI, err)
		return
	}
	if post != nil {
		return // Already exists
	}

	// Post not found, queue for fetching
	if lp.p.postFetcher != nil {
		lp.p.postFetcher.QueueURI(postURI)
	}
}

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
		lp.p.logger.Printf("WARN: failed to save batch of %d labels: %v", len(batch), err)
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

// --- helpers ---

func getString(m map[interface{}]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
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

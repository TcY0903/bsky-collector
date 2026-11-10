package processor

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/fxamacker/cbor/v2"
	"github.com/ipld/go-car/v2"

	"bsky-collector/models"
)

// ==================== Types ====================

// ListProcessor handles batch processing of list events.
type ListProcessor struct {
	p *Processor

	listBatch      []*models.List
	listItemBatch  []*models.ListItem
	listBlockBatch []*models.ListBlock
	deleteBatch    map[string][]string
	batchMu        sync.Mutex

	batchSize int
	ticker    *time.Ticker
	stopCh    chan struct{}
	closeOnce sync.Once
	verbose   bool
}

// ==================== Lifecycle ====================

// NewListProcessor creates a new ListProcessor using shared processor config.
func NewListProcessor(p *Processor) *ListProcessor {
	batchSize := p.cfg.ProcessorBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	tickerSec := p.cfg.ProcessorTickerSec
	if tickerSec <= 0 {
		tickerSec = 3
	}

	lp := &ListProcessor{
		p:              p,
		listBatch:      make([]*models.List, 0, batchSize),
		listItemBatch:  make([]*models.ListItem, 0, batchSize),
		listBlockBatch: make([]*models.ListBlock, 0, batchSize),
		deleteBatch:    make(map[string][]string),
		batchSize:      batchSize,
		ticker:         time.NewTicker(time.Duration(tickerSec) * time.Second),
		stopCh:         make(chan struct{}),
		verbose:        p.cfg.ListProcessorVerbose,
	}
	go lp.autoFlush()
	return lp
}

func (lp *ListProcessor) autoFlush() {
	for {
		select {
		case <-lp.ticker.C:
			lp.flushLists()
			lp.flushListItems()
			lp.flushListBlocks()
			lp.flushDeletes()
		case <-lp.stopCh:
			return
		}
	}
}

// Close shuts down the ListProcessor.
func (lp *ListProcessor) Close() {
	lp.closeOnce.Do(func() {
		close(lp.stopCh)
		lp.ticker.Stop()
		lp.flushLists()
		lp.flushListItems()
		lp.flushListBlocks()
		lp.flushDeletes()
	})
}

// ==================== Dispatch ====================

// ProcessListEvent processes a list event based on the collection.
func (lp *ListProcessor) ProcessListEvent(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	switch {
	case strings.HasPrefix(op.Path, "app.bsky.graph.list/"):
		return lp.processList(commit, op)
	case strings.HasPrefix(op.Path, "app.bsky.graph.listitem/"):
		return lp.processListItem(commit, op)
	case strings.HasPrefix(op.Path, "app.bsky.graph.listblock/"):
		return lp.processListBlock(commit, op)
	default:
		return nil
	}
}

// ==================== List ====================

func (lp *ListProcessor) processList(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	switch op.Action {
	case "create":
		return lp.handleListCreate(commit, op)
	case "update":
		return lp.handleListUpdate(commit, op)
	case "delete":
		return lp.handleListDelete(commit, op)
	default:
		return nil
	}
}

func (lp *ListProcessor) handleListCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	list, err := lp.buildList(commit, op)
	if err != nil {
		return err
	}
	list.Status = "active"

	lp.logf("List create event: did=%s rkey=%s", list.DID, list.RKey)

	if lp.p.actorFetcher != nil {
		lp.p.actorFetcher.QueueDID(list.DID)
	}

	lp.addToListBatch(list)
	return nil
}

func (lp *ListProcessor) handleListUpdate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListByURI(uri)
	if err != nil {
		return err
	}

	list, err := lp.buildList(commit, op)
	if err != nil {
		return err
	}
	list.Status = "active"

	if existing == nil {
		lp.logf("List update event (NEW, created before startup): did=%s rkey=%s", list.DID, list.RKey)
		if lp.p.actorFetcher != nil {
			lp.p.actorFetcher.QueueDID(list.DID)
		}
	} else {
		lp.logf("List update event (EXISTING): did=%s rkey=%s", list.DID, list.RKey)
	}

	lp.addToListBatch(list)
	return nil
}

func (lp *ListProcessor) handleListDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListByURI(uri)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}

	lp.addDelete("list", uri)
	return nil
}

func (lp *ListProcessor) addToListBatch(list *models.List) {
	lp.batchMu.Lock()
	lp.listBatch = append(lp.listBatch, list)
	if len(lp.listBatch) >= lp.batchSize {
		batch := lp.listBatch
		lp.listBatch = make([]*models.List, 0, lp.batchSize)
		lp.batchMu.Unlock()
		lp.flushListsWithBatch(batch)
	} else {
		lp.batchMu.Unlock()
	}
}

func (lp *ListProcessor) buildList(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) (*models.List, error) {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid path: %s", op.Path)
	}
	rkey := parts[1]

	cidStr := ""
	if op.Cid != nil {
		cidStr = op.Cid.String()
	}

	list := &models.List{
		URI:        fmt.Sprintf("at://%s/%s", commit.Repo, op.Path),
		DID:        commit.Repo,
		RKey:       rkey,
		CID:        cidStr,
		ReceivedAt: time.Now().UTC(),
	}

	if len(commit.Blocks) > 0 && cidStr != "" {
		blockReader, err := car.NewBlockReader(bytes.NewReader(commit.Blocks))
		if err == nil {
			for {
				carBlock, err := blockReader.Next()
				if err != nil {
					break
				}
				if carBlock.Cid().String() == cidStr {
					var record map[string]interface{}
					if err := cbor.Unmarshal(carBlock.RawData(), &record); err == nil {
						if name, ok := record["name"].(string); ok {
							list.Name = name
						}
						if purpose, ok := record["purpose"].(string); ok {
							list.Purpose = purpose
						}
						if desc, ok := record["description"].(string); ok {
							list.Description = desc
						}
						if createdAtStr, ok := record["createdAt"].(string); ok {
							if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
								list.CreatedAt = t.UTC()
							}
						}
					}
					break
				}
			}
		}
	}

	return list, nil
}

// ==================== ListItem ====================

func (lp *ListProcessor) processListItem(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	switch op.Action {
	case "create":
		return lp.handleListItemCreate(commit, op)
	case "delete":
		return lp.handleListItemDelete(commit, op)
	default:
		return nil
	}
}

func (lp *ListProcessor) handleListItemCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	item, err := lp.buildListItem(commit, op)
	if err != nil || item == nil {
		return err
	}
	item.Status = "active"

	existingList, err := lp.p.db.GetListByURI(item.ListURI)
	if err != nil {
		return err
	}
	if existingList == nil {
		return nil
	}

	if lp.p.actorFetcher != nil {
		lp.p.actorFetcher.QueueDID(item.SubjectDID)
	}

	lp.batchMu.Lock()
	lp.listItemBatch = append(lp.listItemBatch, item)
	if len(lp.listItemBatch) >= lp.batchSize {
		batch := lp.listItemBatch
		lp.listItemBatch = make([]*models.ListItem, 0, lp.batchSize)
		lp.batchMu.Unlock()
		lp.flushListItemsWithBatch(batch)
	} else {
		lp.batchMu.Unlock()
	}

	return nil
}

func (lp *ListProcessor) handleListItemDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListItemByURI(uri)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}

	lp.addDelete("listitem", uri)
	return nil
}

func (lp *ListProcessor) buildListItem(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) (*models.ListItem, error) {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid path: %s", op.Path)
	}
	rkey := parts[1]

	cidStr := ""
	if op.Cid != nil {
		cidStr = op.Cid.String()
	}

	item := &models.ListItem{
		URI:        fmt.Sprintf("at://%s/%s", commit.Repo, op.Path),
		DID:        commit.Repo,
		RKey:       rkey,
		CID:        cidStr,
		ReceivedAt: time.Now().UTC(),
	}

	if len(commit.Blocks) > 0 && cidStr != "" {
		blockReader, err := car.NewBlockReader(bytes.NewReader(commit.Blocks))
		if err == nil {
			for {
				carBlock, err := blockReader.Next()
				if err != nil {
					break
				}
				if carBlock.Cid().String() == cidStr {
					var record map[string]interface{}
					if err := cbor.Unmarshal(carBlock.RawData(), &record); err == nil {
						if listURI, ok := record["list"].(string); ok {
							item.ListURI = listURI
						}
						if subject, ok := record["subject"].(string); ok {
							item.SubjectDID = subject
						}
						if createdAtStr, ok := record["createdAt"].(string); ok {
							if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
								item.CreatedAt = t.UTC()
							}
						}
					}
					break
				}
			}
		}
	}

	if item.ListURI == "" || item.SubjectDID == "" {
		lp.warnf("WARN: could not extract list or subject from listitem event: %s", op.Path)
		return nil, nil
	}

	return item, nil
}

// ==================== ListBlock ====================

func (lp *ListProcessor) processListBlock(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	switch op.Action {
	case "create":
		return lp.handleListBlockCreate(commit, op)
	case "delete":
		return lp.handleListBlockDelete(commit, op)
	default:
		return nil
	}
}

func (lp *ListProcessor) handleListBlockCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	listBlock, err := lp.buildListBlock(commit, op)
	if err != nil || listBlock == nil {
		return err
	}
	listBlock.Status = "active"

	existingList, err := lp.p.db.GetListByURI(listBlock.SubjectList)
	if err != nil {
		return err
	}
	if existingList == nil && lp.p.listFetcher != nil {
		lp.logf("Listblock triggers list fetch: subject_list=%s (blocker=%s)", listBlock.SubjectList, listBlock.DID)
		lp.p.listFetcher.QueueListURI(listBlock.SubjectList)
	}

	if lp.p.actorFetcher != nil {
		lp.p.actorFetcher.QueueDID(listBlock.DID)
	}

	lp.batchMu.Lock()
	lp.listBlockBatch = append(lp.listBlockBatch, listBlock)
	if len(lp.listBlockBatch) >= lp.batchSize {
		batch := lp.listBlockBatch
		lp.listBlockBatch = make([]*models.ListBlock, 0, lp.batchSize)
		lp.batchMu.Unlock()
		lp.flushListBlocksWithBatch(batch)
	} else {
		lp.batchMu.Unlock()
	}

	return nil
}

func (lp *ListProcessor) handleListBlockDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListBlockByURI(uri)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}

	lp.addDelete("listblock", uri)
	return nil
}

func (lp *ListProcessor) buildListBlock(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) (*models.ListBlock, error) {
	parts := strings.Split(op.Path, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid path: %s", op.Path)
	}
	rkey := parts[1]

	cidStr := ""
	if op.Cid != nil {
		cidStr = op.Cid.String()
	}

	listBlock := &models.ListBlock{
		URI:        fmt.Sprintf("at://%s/%s", commit.Repo, op.Path),
		DID:        commit.Repo,
		RKey:       rkey,
		CID:        cidStr,
		ReceivedAt: time.Now().UTC(),
	}

	if len(commit.Blocks) > 0 && cidStr != "" {
		blockReader, err := car.NewBlockReader(bytes.NewReader(commit.Blocks))
		if err == nil {
			for {
				carBlock, err := blockReader.Next()
				if err != nil {
					break
				}
				if carBlock.Cid().String() == cidStr {
					var record map[string]interface{}
					if err := cbor.Unmarshal(carBlock.RawData(), &record); err == nil {
						if subject, ok := record["subject"].(string); ok {
							listBlock.SubjectList = subject
						}
						if createdAtStr, ok := record["createdAt"].(string); ok {
							if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
								listBlock.CreatedAt = t.UTC()
							}
						}
					}
					break
				}
			}
		}
	}

	if listBlock.SubjectList == "" {
		lp.warnf("WARN: could not extract subject_list from listblock event: %s", op.Path)
		return nil, nil
	}

	return listBlock, nil
}

// ==================== Flush ====================

func (lp *ListProcessor) flushLists() {
	lp.batchMu.Lock()
	if len(lp.listBatch) == 0 {
		lp.batchMu.Unlock()
		return
	}
	batch := lp.listBatch
	lp.listBatch = make([]*models.List, 0, lp.batchSize)
	lp.batchMu.Unlock()
	lp.flushListsWithBatch(batch)
}

func (lp *ListProcessor) flushListsWithBatch(batch []*models.List) {
	if len(batch) == 0 {
		return
	}
	if err := lp.p.db.SaveListsBatch(batch); err != nil {
		lp.warnf("WARN: failed to save batch of %d lists: %v", len(batch), err)
		return
	}
	for _, l := range batch {
		lp.logf("Saved list: did=%s rkey=%s name=%q", l.DID, l.RKey, l.Name)
	}
}

func (lp *ListProcessor) flushListItems() {
	lp.batchMu.Lock()
	if len(lp.listItemBatch) == 0 {
		lp.batchMu.Unlock()
		return
	}
	batch := lp.listItemBatch
	lp.listItemBatch = make([]*models.ListItem, 0, lp.batchSize)
	lp.batchMu.Unlock()
	lp.flushListItemsWithBatch(batch)
}

func (lp *ListProcessor) flushListItemsWithBatch(batch []*models.ListItem) {
	if len(batch) == 0 {
		return
	}
	if err := lp.p.db.SaveListItemsBatch(batch); err != nil {
		lp.warnf("WARN: failed to save batch of %d list items: %v", len(batch), err)
	} else {
		lp.logf("Saved %d list items", len(batch))
	}
}

func (lp *ListProcessor) flushListBlocks() {
	lp.batchMu.Lock()
	if len(lp.listBlockBatch) == 0 {
		lp.batchMu.Unlock()
		return
	}
	batch := lp.listBlockBatch
	lp.listBlockBatch = make([]*models.ListBlock, 0, lp.batchSize)
	lp.batchMu.Unlock()
	lp.flushListBlocksWithBatch(batch)
}

func (lp *ListProcessor) flushListBlocksWithBatch(batch []*models.ListBlock) {
	if len(batch) == 0 {
		return
	}
	if err := lp.p.db.SaveListBlocksBatch(batch); err != nil {
		lp.warnf("WARN: failed to save batch of %d list blocks: %v", len(batch), err)
	} else {
		lp.logf("Saved %d list blocks", len(batch))
	}
}

// ==================== Delete ====================

func (lp *ListProcessor) addDelete(collection, uri string) {
	lp.batchMu.Lock()
	lp.deleteBatch[collection] = append(lp.deleteBatch[collection], uri)
	shouldFlush := len(lp.deleteBatch[collection]) >= lp.batchSize/5
	var batch []string
	if shouldFlush {
		batch = lp.deleteBatch[collection]
		lp.deleteBatch[collection] = make([]string, 0, lp.batchSize/5)
	}
	lp.batchMu.Unlock()

	if shouldFlush {
		lp.flushDeletesWithBatch(collection, batch)
	}
}

func (lp *ListProcessor) flushDeletes() {
	lp.batchMu.Lock()
	deletes := lp.deleteBatch
	lp.deleteBatch = make(map[string][]string)
	lp.batchMu.Unlock()

	for collection, uris := range deletes {
		if len(uris) > 0 {
			lp.flushDeletesWithBatch(collection, uris)
		}
	}
}

func (lp *ListProcessor) flushDeletesWithBatch(collection string, uris []string) {
	if len(uris) == 0 {
		return
	}

	var err error
	switch collection {
	case "list":
		err = lp.p.db.DeleteListsBatch(uris)
	case "listitem":
		err = lp.p.db.DeleteListItemsBatch(uris)
	case "listblock":
		err = lp.p.db.DeleteListBlocksBatch(uris)
	default:
		lp.warnf("WARN: unknown collection for delete: %s", collection)
		return
	}

	if err != nil {
		lp.warnf("WARN: failed to delete batch of %d %s: %v", len(uris), collection, err)
	} else {
		lp.logf("Deleted %d %s", len(uris), collection)
	}
}

// ==================== Logging ====================

func (lp *ListProcessor) logf(format string, args ...interface{}) {
	if lp.verbose {
		lp.p.logger.Printf(format, args...)
	}
}

func (lp *ListProcessor) warnf(format string, args ...interface{}) {
	lp.p.logger.Printf(format, args...)
}

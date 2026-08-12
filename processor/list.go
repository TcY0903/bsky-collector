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

// ListProcessor handles batch processing of list events.
type ListProcessor struct {
	p              *Processor
	listBatch      []*models.List
	listItemBatch  []*models.ListItem
	listBlockBatch []*models.ListBlock
	deleteBatch    map[string][]string // "list", "listitem", "listblock" -> URIs
	batchMu        sync.Mutex
	batchSize      int
	ticker         *time.Ticker
	stopCh         chan struct{}
	closeOnce      sync.Once
}

// NewListProcessor creates a new ListProcessor.
func NewListProcessor(p *Processor) *ListProcessor {
	lp := &ListProcessor{
		p:              p,
		listBatch:      make([]*models.List, 0, 500),
		listItemBatch:  make([]*models.ListItem, 0, 500),
		listBlockBatch: make([]*models.ListBlock, 0, 500),
		deleteBatch:    make(map[string][]string),
		batchSize:      500,
		ticker:         time.NewTicker(3 * time.Second),
		stopCh:         make(chan struct{}),
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

// handleListCreate handles list creation: create record and fetch creator.
func (lp *ListProcessor) handleListCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	list, err := lp.buildList(commit, op)
	if err != nil {
		return err
	}
	list.Status = "active"

	// Queue the list creator for actor fetching
	if lp.p.actorFetcher != nil {
		lp.p.actorFetcher.QueueDID(list.DID)
	}

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

	return nil
}

// handleListUpdate handles list update: update existing or create if unknown.
func (lp *ListProcessor) handleListUpdate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListByURI(uri)
	if err != nil {
		return err
	}

	if existing == nil {
		// List created before startup - create it as active
		// lp.p.logger.Printf("📝 Update for unknown list (created before startup): %s", uri)
		list, err := lp.buildList(commit, op)
		if err != nil {
			return err
		}
		list.Status = "active"

		// Queue the list creator for actor fetching
		if lp.p.actorFetcher != nil {
			lp.p.actorFetcher.QueueDID(list.DID)
		}

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
		return nil
	}

	// Update existing list
	list, err := lp.buildList(commit, op)
	if err != nil {
		return err
	}
	list.Status = "active"

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

	return nil
}

// handleListDelete handles list deletion: mark deleted if exists.
func (lp *ListProcessor) handleListDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListByURI(uri)
	if err != nil {
		return err
	}

	if existing == nil {
		// List was created before we started listening
		return nil
	}

	lp.addDelete("list", uri)
	return nil
}

// buildList builds a List model from Firehose data.
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

	// Parse CAR to extract name, purpose, description, created_at
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

// handleListItemCreate handles list item creation: create record and fetch actor.
func (lp *ListProcessor) handleListItemCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	item, err := lp.buildListItem(commit, op)
	if err != nil {
		return err
	}
	if item == nil {
		return nil
	}
	item.Status = "active"

	// Check if the list exists in our database
	existingList, err := lp.p.db.GetListByURI(item.ListURI)
	if err != nil {
		return err
	}

	if existingList == nil {
		// List doesn't exist yet - skip this list item
		// lp.p.logger.Printf("⚠️ Skipping list item for unknown list: %s (list not yet collected)", item.ListURI)
		return nil
	}

	// Queue the list item subject for actor fetching (the user being added)
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

// handleListItemDelete handles list item deletion: mark deleted if exists.
func (lp *ListProcessor) handleListItemDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListItemByURI(uri)
	if err != nil {
		return err
	}

	if existing == nil {
		// List item was created before we started listening
		return nil
	}

	lp.addDelete("listitem", uri)
	return nil
}

// buildListItem builds a ListItem model from Firehose data.
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

	// Parse CAR to extract list_uri, subject_did, created_at
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
		lp.p.logger.Printf("⚠️ Could not extract list or subject from listitem event: %s", op.Path)
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

// handleListBlockCreate handles list block creation: create record, fetch actor,
// and fetch the target list if it doesn't exist.
func (lp *ListProcessor) handleListBlockCreate(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	listBlock, err := lp.buildListBlock(commit, op)
	if err != nil {
		return err
	}
	if listBlock == nil {
		return nil
	}
	listBlock.Status = "active"

	// Check if the list exists in our database
	existingList, err := lp.p.db.GetListByURI(listBlock.SubjectList)
	if err != nil {
		return err
	}

	if existingList == nil {
		// List doesn't exist - fetch it proactively via API
		// lp.p.logger.Printf("📋 List not found, fetching via API: %s", listBlock.SubjectList)
		if lp.p.listFetcher != nil {
			lp.p.listFetcher.QueueListURI(listBlock.SubjectList)
		}
	}

	// Queue the list block creator for actor fetching
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

// handleListBlockDelete handles list block deletion: mark deleted if exists.
func (lp *ListProcessor) handleListBlockDelete(commit *atproto.SyncSubscribeRepos_Commit, op *atproto.SyncSubscribeRepos_RepoOp) error {
	uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

	existing, err := lp.p.db.GetListBlockByURI(uri)
	if err != nil {
		return err
	}

	if existing == nil {
		// List block was created before we started listening
		return nil
	}

	lp.addDelete("listblock", uri)
	return nil
}

// buildListBlock builds a ListBlock model from Firehose data.
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

	// Parse CAR to extract subject_list, created_at
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
		lp.p.logger.Printf("⚠️ Could not extract subject_list from listblock event: %s", op.Path)
		return nil, nil
	}

	return listBlock, nil
}

// ==================== Flush Methods ====================

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
		lp.p.logger.Printf("⚠️ Failed to save batch of %d lists: %v", len(batch), err)
	} else {
		// lp.p.logger.Printf("📋 Saved %d lists in batch", len(batch))
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
		lp.p.logger.Printf("⚠️ Failed to save batch of %d list items: %v", len(batch), err)
	} else {
		// lp.p.logger.Printf("👥 Saved %d list items in batch", len(batch))
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
		lp.p.logger.Printf("⚠️ Failed to save batch of %d list blocks: %v", len(batch), err)
	} else {
		// lp.p.logger.Printf("🚫 Saved %d list blocks in batch", len(batch))
	}
}

// ==================== Delete Methods ====================

func (lp *ListProcessor) addDelete(collection, uri string) {
	lp.batchMu.Lock()
	defer lp.batchMu.Unlock()
	lp.deleteBatch[collection] = append(lp.deleteBatch[collection], uri)
	if len(lp.deleteBatch[collection]) >= lp.batchSize/5 {
		batch := lp.deleteBatch[collection]
		lp.deleteBatch[collection] = make([]string, 0, lp.batchSize/5)
		lp.batchMu.Unlock()
		lp.flushDeletesWithBatch(collection, batch)
		lp.batchMu.Lock()
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
		if err == nil {
			// lp.p.logger.Printf("🗑️ Deleted %d lists in batch", len(uris))
		}
	case "listitem":
		err = lp.p.db.DeleteListItemsBatch(uris)
		if err == nil {
			// lp.p.logger.Printf("🗑️ Deleted %d list items in batch", len(uris))
		}
	case "listblock":
		err = lp.p.db.DeleteListBlocksBatch(uris)
		if err == nil {
			// lp.p.logger.Printf("🗑️ Deleted %d list blocks in batch", len(uris))
		}
	default:
		// lp.p.logger.Printf("⚠️ Unknown collection for delete: %s", collection)
		return
	}

	if err != nil {
		lp.p.logger.Printf("⚠️ Failed to delete batch of %d %s: %v", len(uris), collection, err)
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

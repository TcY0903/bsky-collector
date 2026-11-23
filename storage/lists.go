package storage

import (
	"database/sql"
	"fmt"

	"bsky-collector/models"
)

// ==================== List ====================

// SaveList inserts or updates a list.
func (db *DB) SaveList(list *models.List) error {
	query := `
	INSERT INTO lists (uri, did, rkey, cid, name, purpose, description, created_at, received_at, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (uri) DO UPDATE SET
		did = EXCLUDED.did,
		rkey = EXCLUDED.rkey,
		cid = EXCLUDED.cid,
		name = EXCLUDED.name,
		purpose = EXCLUDED.purpose,
		description = EXCLUDED.description,
		created_at = EXCLUDED.created_at,
		received_at = EXCLUDED.received_at,
		status = EXCLUDED.status
	`
	_, err := db.conn.Exec(query,
		list.URI,
		list.DID,
		list.RKey,
		list.CID,
		list.Name,
		list.Purpose,
		list.Description,
		list.CreatedAt,
		list.ReceivedAt,
		list.Status,
	)
	return err
}

// SaveListsBatch inserts multiple lists in a single transaction.
func (db *DB) SaveListsBatch(lists []*models.List) error {
	if len(lists) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO lists (uri, did, rkey, cid, name, purpose, description, created_at, received_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (uri) DO UPDATE SET
			did = EXCLUDED.did,
			rkey = EXCLUDED.rkey,
			cid = EXCLUDED.cid,
			name = EXCLUDED.name,
			purpose = EXCLUDED.purpose,
			description = EXCLUDED.description,
			created_at = EXCLUDED.created_at,
			received_at = EXCLUDED.received_at,
			status = EXCLUDED.status
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, list := range lists {
		if list.Status == "" {
			list.Status = "active"
		}
		_, err := stmt.Exec(
			list.URI,
			list.DID,
			list.RKey,
			list.CID,
			list.Name,
			list.Purpose,
			list.Description,
			list.CreatedAt,
			list.ReceivedAt,
			list.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert list: %w", err)
		}
	}

	return tx.Commit()
}

// GetListByURI retrieves a list by its URI.
func (db *DB) GetListByURI(uri string) (*models.List, error) {
	var list models.List
	query := `
	SELECT id, uri, did, rkey, cid, name, purpose, description, created_at, received_at, status
	FROM lists WHERE uri = $1
	`
	err := db.conn.QueryRow(query, uri).Scan(
		&list.ID,
		&list.URI,
		&list.DID,
		&list.RKey,
		&list.CID,
		&list.Name,
		&list.Purpose,
		&list.Description,
		&list.CreatedAt,
		&list.ReceivedAt,
		&list.Status,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get list: %w", err)
	}
	return &list, nil
}

// DeleteList marks a list as deleted.
func (db *DB) DeleteList(uri string) error {
	query := `UPDATE lists SET status = 'deleted' WHERE uri = $1`
	_, err := db.conn.Exec(query, uri)
	return err
}

// DeleteListsBatch marks multiple lists as deleted.
func (db *DB) DeleteListsBatch(uris []string) error {
	if len(uris) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE lists SET status = 'deleted' WHERE uri = $1`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, uri := range uris {
		_, err := stmt.Exec(uri)
		if err != nil {
			return fmt.Errorf("failed to delete list %s: %w", uri, err)
		}
	}

	return tx.Commit()
}

// SearchLists searches lists by exact URI, exact DID, and fuzzy name.
func (db *DB) SearchLists(uri string, did string, keyword string, limit, offset int) ([]*models.List, error) {
	query := `
	SELECT id, uri, did, rkey, cid, name, purpose, description, created_at, received_at, status
	FROM lists 
	WHERE status = 'active'
	`
	args := []interface{}{}
	argCount := 1

	if uri != "" {
		query += fmt.Sprintf(" AND uri = $%d", argCount)
		args = append(args, uri)
		argCount++
	}
	if did != "" {
		query += fmt.Sprintf(" AND did = $%d", argCount)
		args = append(args, did)
		argCount++
	}
	if keyword != "" {
		query += fmt.Sprintf(" AND name ILIKE $%d", argCount)
		args = append(args, "%"+keyword+"%")
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search lists: %w", err)
	}
	defer rows.Close()

	var lists []*models.List
	for rows.Next() {
		var list models.List
		err := rows.Scan(
			&list.ID, &list.URI, &list.DID, &list.RKey, &list.CID,
			&list.Name, &list.Purpose, &list.Description, &list.CreatedAt,
			&list.ReceivedAt, &list.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan list: %w", err)
		}
		lists = append(lists, &list)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating lists: %w", err)
	}
	return lists, nil
}

// ==================== ListItem ====================

// SaveListItem inserts or updates a list item.
func (db *DB) SaveListItem(item *models.ListItem) error {
	query := `
	INSERT INTO list_items (uri, list_uri, did, subject_did, rkey, cid, created_at, received_at, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (uri) DO UPDATE SET
		list_uri = EXCLUDED.list_uri,
		did = EXCLUDED.did,
		subject_did = EXCLUDED.subject_did,
		rkey = EXCLUDED.rkey,
		cid = EXCLUDED.cid,
		created_at = EXCLUDED.created_at,
		received_at = EXCLUDED.received_at,
		status = EXCLUDED.status
	`
	_, err := db.conn.Exec(query,
		item.URI,
		item.ListURI,
		item.DID,
		item.SubjectDID,
		item.RKey,
		item.CID,
		item.CreatedAt,
		item.ReceivedAt,
		item.Status,
	)
	return err
}

// SaveListItemsBatch inserts multiple list items in a single transaction.
func (db *DB) SaveListItemsBatch(items []*models.ListItem) error {
	if len(items) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO list_items (uri, list_uri, did, subject_did, rkey, cid, created_at, received_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (uri) DO UPDATE SET
			list_uri = EXCLUDED.list_uri,
			did = EXCLUDED.did,
			subject_did = EXCLUDED.subject_did,
			rkey = EXCLUDED.rkey,
			cid = EXCLUDED.cid,
			created_at = EXCLUDED.created_at,
			received_at = EXCLUDED.received_at,
			status = EXCLUDED.status
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		if item.Status == "" {
			item.Status = "active"
		}
		_, err := stmt.Exec(
			item.URI,
			item.ListURI,
			item.DID,
			item.SubjectDID,
			item.RKey,
			item.CID,
			item.CreatedAt,
			item.ReceivedAt,
			item.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert list item: %w", err)
		}
	}

	return tx.Commit()
}

// GetListItemByURI retrieves a list item by its URI.
func (db *DB) GetListItemByURI(uri string) (*models.ListItem, error) {
	var item models.ListItem
	query := `
	SELECT id, uri, list_uri, did, subject_did, rkey, cid, created_at, received_at, status
	FROM list_items WHERE uri = $1
	`
	err := db.conn.QueryRow(query, uri).Scan(
		&item.ID,
		&item.URI,
		&item.ListURI,
		&item.DID,
		&item.SubjectDID,
		&item.RKey,
		&item.CID,
		&item.CreatedAt,
		&item.ReceivedAt,
		&item.Status,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}
	return &item, nil
}

// DeleteListItem marks a list item as deleted.
func (db *DB) DeleteListItem(uri string) error {
	query := `UPDATE list_items SET status = 'deleted' WHERE uri = $1`
	_, err := db.conn.Exec(query, uri)
	return err
}

// DeleteListItemsBatch marks multiple list items as deleted.
func (db *DB) DeleteListItemsBatch(uris []string) error {
	if len(uris) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE list_items SET status = 'deleted' WHERE uri = $1`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, uri := range uris {
		_, err := stmt.Exec(uri)
		if err != nil {
			return fmt.Errorf("failed to delete list item %s: %w", uri, err)
		}
	}

	return tx.Commit()
}

// SearchListItems searches list items by exact URI, list URI, adder DID, and subject DID.
func (db *DB) SearchListItems(uri string, listURI string, did string, subjectDID string, limit, offset int) ([]*models.ListItem, error) {
	query := `
	SELECT id, uri, list_uri, did, subject_did, rkey, cid, created_at, received_at, status
	FROM list_items 
	WHERE status = 'active'
	`
	args := []interface{}{}
	argCount := 1

	if uri != "" {
		query += fmt.Sprintf(" AND uri = $%d", argCount)
		args = append(args, uri)
		argCount++
	}
	if listURI != "" {
		query += fmt.Sprintf(" AND list_uri = $%d", argCount)
		args = append(args, listURI)
		argCount++
	}
	if did != "" {
		query += fmt.Sprintf(" AND did = $%d", argCount)
		args = append(args, did)
		argCount++
	}
	if subjectDID != "" {
		query += fmt.Sprintf(" AND subject_did = $%d", argCount)
		args = append(args, subjectDID)
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search list items: %w", err)
	}
	defer rows.Close()

	var items []*models.ListItem
	for rows.Next() {
		var item models.ListItem
		err := rows.Scan(
			&item.ID, &item.URI, &item.ListURI, &item.DID, &item.SubjectDID,
			&item.RKey, &item.CID, &item.CreatedAt, &item.ReceivedAt, &item.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan list item: %w", err)
		}
		items = append(items, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating list items: %w", err)
	}
	return items, nil
}

// ==================== ListBlock ====================

// SaveListBlock inserts or updates a list block.
func (db *DB) SaveListBlock(block *models.ListBlock) error {
	query := `
	INSERT INTO list_blocks (uri, did, subject_list, rkey, cid, created_at, received_at, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT (uri) DO UPDATE SET
		did = EXCLUDED.did,
		subject_list = EXCLUDED.subject_list,
		rkey = EXCLUDED.rkey,
		cid = EXCLUDED.cid,
		created_at = EXCLUDED.created_at,
		received_at = EXCLUDED.received_at,
		status = EXCLUDED.status
	`
	_, err := db.conn.Exec(query,
		block.URI,
		block.DID,
		block.SubjectList,
		block.RKey,
		block.CID,
		block.CreatedAt,
		block.ReceivedAt,
		block.Status,
	)
	return err
}

// SaveListBlocksBatch inserts multiple list blocks in a single transaction.
func (db *DB) SaveListBlocksBatch(blocks []*models.ListBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO list_blocks (uri, did, subject_list, rkey, cid, created_at, received_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (uri) DO UPDATE SET
			did = EXCLUDED.did,
			subject_list = EXCLUDED.subject_list,
			rkey = EXCLUDED.rkey,
			cid = EXCLUDED.cid,
			created_at = EXCLUDED.created_at,
			received_at = EXCLUDED.received_at,
			status = EXCLUDED.status
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, block := range blocks {
		if block.Status == "" {
			block.Status = "active"
		}
		_, err := stmt.Exec(
			block.URI,
			block.DID,
			block.SubjectList,
			block.RKey,
			block.CID,
			block.CreatedAt,
			block.ReceivedAt,
			block.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert list block: %w", err)
		}
	}

	return tx.Commit()
}

// GetListBlockByURI retrieves a list block by its URI.
func (db *DB) GetListBlockByURI(uri string) (*models.ListBlock, error) {
	var block models.ListBlock
	query := `
	SELECT id, uri, did, subject_list, rkey, cid, created_at, received_at, status
	FROM list_blocks WHERE uri = $1
	`
	err := db.conn.QueryRow(query, uri).Scan(
		&block.ID,
		&block.URI,
		&block.DID,
		&block.SubjectList,
		&block.RKey,
		&block.CID,
		&block.CreatedAt,
		&block.ReceivedAt,
		&block.Status,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get list block: %w", err)
	}
	return &block, nil
}

// DeleteListBlock marks a list block as deleted.
func (db *DB) DeleteListBlock(uri string) error {
	query := `UPDATE list_blocks SET status = 'deleted' WHERE uri = $1`
	_, err := db.conn.Exec(query, uri)
	return err
}

// DeleteListBlocksBatch marks multiple list blocks as deleted.
func (db *DB) DeleteListBlocksBatch(uris []string) error {
	if len(uris) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE list_blocks SET status = 'deleted' WHERE uri = $1`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, uri := range uris {
		_, err := stmt.Exec(uri)
		if err != nil {
			return fmt.Errorf("failed to delete list block %s: %w", uri, err)
		}
	}

	return tx.Commit()
}

// SearchListBlocks searches list blocks by exact URI, blocker DID, and subject list.
func (db *DB) SearchListBlocks(uri string, subjectList string, did string, limit, offset int) ([]*models.ListBlock, error) {
	query := `
	SELECT id, uri, did, subject_list, rkey, cid, created_at, received_at, status
	FROM list_blocks 
	WHERE status = 'active'
	`
	args := []interface{}{}
	argCount := 1

	if uri != "" {
		query += fmt.Sprintf(" AND uri = $%d", argCount)
		args = append(args, uri)
		argCount++
	}
	if did != "" {
		query += fmt.Sprintf(" AND did = $%d", argCount)
		args = append(args, did)
		argCount++
	}
	if subjectList != "" {
		query += fmt.Sprintf(" AND subject_list = $%d", argCount)
		args = append(args, subjectList)
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search list blocks: %w", err)
	}
	defer rows.Close()

	var blocks []*models.ListBlock
	for rows.Next() {
		var block models.ListBlock
		err := rows.Scan(
			&block.ID, &block.URI, &block.DID, &block.SubjectList, &block.RKey,
			&block.CID, &block.CreatedAt, &block.ReceivedAt, &block.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan list block: %w", err)
		}
		blocks = append(blocks, &block)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating list blocks: %w", err)
	}
	return blocks, nil
}

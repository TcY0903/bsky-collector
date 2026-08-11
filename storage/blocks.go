package storage

import (
	"database/sql"
	"fmt"

	"bsky-collector/models"
)

// SaveBlock inserts a block record.
func (db *DB) SaveBlock(block *models.Block) error {
	query := `
	INSERT INTO blocks (uri, did, subject_did, rkey, cid, created_at, received_at, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT (uri) DO UPDATE SET
		did = EXCLUDED.did,
		subject_did = EXCLUDED.subject_did,
		rkey = EXCLUDED.rkey,
		cid = EXCLUDED.cid,
		created_at = EXCLUDED.created_at,
		received_at = EXCLUDED.received_at,
		status = EXCLUDED.status
	`
	_, err := db.conn.Exec(query,
		block.URI,
		block.DID,
		block.SubjectDID,
		block.RKey,
		block.CID,
		block.CreatedAt,
		block.ReceivedAt,
		block.Status,
	)
	return err
}

// SaveBlocksBatch inserts multiple blocks in a single transaction.
func (db *DB) SaveBlocksBatch(blocks []*models.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO blocks (uri, did, subject_did, rkey, cid, created_at, received_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (uri) DO UPDATE SET
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

	for _, block := range blocks {
		if block.Status == "" {
			block.Status = "active"
		}
		_, err := stmt.Exec(
			block.URI,
			block.DID,
			block.SubjectDID,
			block.RKey,
			block.CID,
			block.CreatedAt,
			block.ReceivedAt,
			block.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert block: %w", err)
		}
	}

	return tx.Commit()
}

// GetBlockByURI retrieves a block by its URI.
func (db *DB) GetBlockByURI(uri string) (*models.Block, error) {
	var block models.Block
	query := `
	SELECT id, uri, did, subject_did, rkey, cid, created_at, received_at, status
	FROM blocks WHERE uri = $1
	`
	err := db.conn.QueryRow(query, uri).Scan(
		&block.ID,
		&block.URI,
		&block.DID,
		&block.SubjectDID,
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
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	return &block, nil
}

// DeleteBlock marks a block as deleted (soft delete).
func (db *DB) DeleteBlock(uri string) error {
	query := `UPDATE blocks SET status = 'deleted' WHERE uri = $1`
	_, err := db.conn.Exec(query, uri)
	return err
}

// DeleteBlocksBatch marks multiple blocks as deleted in a single transaction.
func (db *DB) DeleteBlocksBatch(uris []string) error {
	if len(uris) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE blocks SET status = 'deleted' WHERE uri = $1`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, uri := range uris {
		_, err := stmt.Exec(uri)
		if err != nil {
			return fmt.Errorf("failed to delete block %s: %w", uri, err)
		}
	}

	return tx.Commit()
}

// GetActiveBlocksByDID returns all active blocks created by a user.
func (db *DB) GetActiveBlocksByDID(did string) ([]*models.Block, error) {
	query := `
	SELECT id, uri, did, subject_did, rkey, cid, created_at, received_at, status
	FROM blocks WHERE did = $1 AND status = 'active'
	`
	rows, err := db.conn.Query(query, did)
	if err != nil {
		return nil, fmt.Errorf("failed to query blocks: %w", err)
	}
	defer rows.Close()

	var blocks []*models.Block
	for rows.Next() {
		var block models.Block
		err := rows.Scan(
			&block.ID,
			&block.URI,
			&block.DID,
			&block.SubjectDID,
			&block.RKey,
			&block.CID,
			&block.CreatedAt,
			&block.ReceivedAt,
			&block.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan block: %w", err)
		}
		blocks = append(blocks, &block)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate blocks: %w", err)
	}
	return blocks, nil
}

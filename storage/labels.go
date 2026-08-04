package storage

import (
	"fmt"

	"bsky-collector/models"
)

// SaveLabel inserts a single label into the database.
func (db *DB) SaveLabel(label *models.Label) error {
	query := `
	INSERT INTO labels (src, uri, cid, val, neg, cts, received_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := db.conn.Exec(query,
		label.Src,
		label.URI,
		label.CID,
		label.Val,
		label.Neg,
		label.CTS,
		label.ReceivedAt,
	)
	return err
}

// SaveLabelsBatch inserts multiple labels in a single transaction.
func (db *DB) SaveLabelsBatch(labels []*models.Label) error {
	if len(labels) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO labels (src, uri, cid, val, neg, cts, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, label := range labels {
		_, err := stmt.Exec(
			label.Src,
			label.URI,
			label.CID,
			label.Val,
			label.Neg,
			label.CTS,
			label.ReceivedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert label: %w", err)
		}
	}

	return tx.Commit()
}

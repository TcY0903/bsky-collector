package storage

import (
	"fmt"

	"bsky-collector/models"
)

// SaveLabel inserts a single label into the database.
func (db *DB) SaveLabel(label *models.Label) error {
	query := `
	INSERT INTO labels (seq, src, uri, val, ver, cts, sig, received_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT (src, uri, val, ver) DO NOTHING
	`
	_, err := db.conn.Exec(query,
		label.Seq,
		label.Src,
		label.URI,
		label.Val,
		label.Ver,
		label.CTS,
		label.Sig,
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
		INSERT INTO labels (seq, src, uri, val, ver, cts, sig, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (src, uri, val, ver) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, label := range labels {
		_, err := stmt.Exec(
			label.Seq,
			label.Src,
			label.URI,
			label.Val,
			label.Ver,
			label.CTS,
			label.Sig,
			label.ReceivedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert label: %w", err)
		}
	}

	return tx.Commit()
}

// GetLabelsByURI returns all labels applied to a specific URI.
func (db *DB) GetLabelsByURI(uri string) ([]*models.Label, error) {
	query := `
	SELECT id, seq, src, uri, val, ver, cts, sig, received_at
	FROM labels WHERE uri = $1
	ORDER BY cts DESC
	`
	rows, err := db.conn.Query(query, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to query labels: %w", err)
	}
	defer rows.Close()

	var labels []*models.Label
	for rows.Next() {
		var label models.Label
		err := rows.Scan(
			&label.ID,
			&label.Seq,
			&label.Src,
			&label.URI,
			&label.Val,
			&label.Ver,
			&label.CTS,
			&label.Sig,
			&label.ReceivedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan label: %w", err)
		}
		labels = append(labels, &label)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating labels: %w", err)
	}

	return labels, nil
}

// GetLabelsBySrc returns all labels issued by a specific labeler.
func (db *DB) GetLabelsBySrc(src string, limit int) ([]*models.Label, error) {
	query := `
	SELECT id, seq, src, uri, val, ver, cts, sig, received_at
	FROM labels WHERE src = $1
	ORDER BY cts DESC
	LIMIT $2
	`
	rows, err := db.conn.Query(query, src, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query labels: %w", err)
	}
	defer rows.Close()

	var labels []*models.Label
	for rows.Next() {
		var label models.Label
		err := rows.Scan(
			&label.ID,
			&label.Seq,
			&label.Src,
			&label.URI,
			&label.Val,
			&label.Ver,
			&label.CTS,
			&label.Sig,
			&label.ReceivedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan label: %w", err)
		}
		labels = append(labels, &label)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating labels: %w", err)
	}

	return labels, nil
}

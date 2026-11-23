package storage

import (
	"database/sql"
	"fmt"

	"bsky-collector/models"
)

// GetLabelByID retrieves a label by its primary key ID.
func (db *DB) GetLabelByID(id int64) (*models.Label, error) {
	var label models.Label
	query := `
	SELECT id, seq, src, uri, val, ver, neg, cts, sig, received_at
	FROM labels WHERE id = $1
	`
	err := db.conn.QueryRow(query, id).Scan(
		&label.ID, &label.Seq, &label.Src, &label.URI, &label.Val,
		&label.Ver, &label.Neg, &label.CTS, &label.Sig, &label.ReceivedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get label: %w", err)
	}
	return &label, nil
}

// SaveLabel inserts a single label into the database.
func (db *DB) SaveLabel(label *models.Label) error {
	query := `
	INSERT INTO labels (seq, src, uri, val, ver, neg, cts, sig, received_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (src, uri, val, ver, neg) DO NOTHING
	`
	_, err := db.conn.Exec(query,
		label.Seq,
		label.Src,
		label.URI,
		label.Val,
		label.Ver,
		label.Neg,
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
		INSERT INTO labels (seq, src, uri, val, ver, neg, cts, sig, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (src, uri, val, ver, neg) DO NOTHING
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
			label.Neg,
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
	SELECT id, seq, src, uri, val, ver, neg, cts, sig, received_at
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
			&label.Neg,
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
	SELECT id, seq, src, uri, val, ver, neg, cts, sig, received_at
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
			&label.Neg,
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

// SearchLabels searches labels by exact src, exact uri, and fuzzy val.
func (db *DB) SearchLabels(val string, src string, uri string, neg *bool, limit, offset int) ([]*models.Label, error) {
	query := `
	SELECT id, seq, src, uri, val, ver, neg, cts, sig, received_at
	FROM labels 
	WHERE 1=1
	`
	args := []interface{}{}
	argCount := 1

	if src != "" {
		query += fmt.Sprintf(" AND src = $%d", argCount)
		args = append(args, src)
		argCount++
	}
	if uri != "" {
		query += fmt.Sprintf(" AND uri = $%d", argCount)
		args = append(args, uri)
		argCount++
	}
	if val != "" {
		query += fmt.Sprintf(" AND val ILIKE $%d", argCount)
		args = append(args, "%"+val+"%")
		argCount++
	}
	if neg != nil {
		query += fmt.Sprintf(" AND neg = $%d", argCount)
		args = append(args, *neg)
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY cts DESC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search labels: %w", err)
	}
	defer rows.Close()

	var labels []*models.Label
	for rows.Next() {
		var label models.Label
		err := rows.Scan(
			&label.ID, &label.Seq, &label.Src, &label.URI, &label.Val,
			&label.Ver, &label.Neg, &label.CTS, &label.Sig, &label.ReceivedAt,
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

package storage

import (
	"database/sql"
	"fmt"
	"time"

	"bsky-collector/models"
)

// SaveLabeler inserts or updates a labeler record.
func (db *DB) SaveLabeler(l *models.Labeler) error {
	query := `
	INSERT INTO labelers (did, endpoint, status, failure_type, last_connected, last_error, fail_count, first_seen, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (did) DO UPDATE SET
		endpoint = EXCLUDED.endpoint,
		status = EXCLUDED.status,
		failure_type = EXCLUDED.failure_type,
		last_connected = EXCLUDED.last_connected,
		last_error = EXCLUDED.last_error,
		fail_count = EXCLUDED.fail_count,
		updated_at = EXCLUDED.updated_at
	`
	_, err := db.conn.Exec(query,
		l.DID, l.Endpoint, l.Status, l.FailureType, l.LastConnected, l.LastError, l.FailCount, l.FirstSeen, l.UpdatedAt)
	return err
}

// LabelerExists checks if a labeler with the given DID exists.
func (db *DB) LabelerExists(did string) (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM labelers WHERE did = $1", did).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to query labeler: %w", err)
	}
	return count > 0, nil
}

// GetAllLabelers returns all labelers ordered by status priority, then first_seen.
// Priority: active > unknown > connect_failed > resolve_failed
func (db *DB) GetAllLabelers() ([]*models.Labeler, error) {
	query := `
	SELECT did, endpoint, status, failure_type, last_connected, last_error, fail_count, first_seen, updated_at
	FROM labelers
	ORDER BY
		CASE status
			WHEN 'active' THEN 0
			WHEN 'unknown' THEN 1
			WHEN 'connect_failed' THEN 2
			WHEN 'resolve_failed' THEN 3
			ELSE 4
		END,
		first_seen ASC
	`
	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query labelers: %w", err)
	}
	defer rows.Close()

	var labelers []*models.Labeler
	for rows.Next() {
		var l models.Labeler
		var endpoint, lastError, failureType sql.NullString
		var lastConnected sql.NullTime
		err := rows.Scan(
			&l.DID, &endpoint, &l.Status, &failureType,
			&lastConnected, &lastError, &l.FailCount, &l.FirstSeen, &l.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan labeler: %w", err)
		}
		if endpoint.Valid {
			l.Endpoint = endpoint.String
		}
		if lastError.Valid {
			l.LastError = lastError.String
		}
		if failureType.Valid {
			l.FailureType = failureType.String
		}
		if lastConnected.Valid {
			l.LastConnected = lastConnected.Time
		}
		labelers = append(labelers, &l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating labelers: %w", err)
	}
	return labelers, nil
}

// UpdateLabelerStatus updates the status of a labeler.
func (db *DB) UpdateLabelerStatus(did, status, failureType, lastError string, failCount int) error {
	query := `
	UPDATE labelers SET
		status = $1,
		failure_type = $2,
		last_error = $3,
		fail_count = $4,
		updated_at = $5
	WHERE did = $6
	`
	_, err := db.conn.Exec(query, status, failureType, lastError, failCount, time.Now().UTC(), did)
	return err
}

// UpdateLabelerConnected marks a labeler as successfully connected.
func (db *DB) UpdateLabelerConnected(did, endpoint string) error {
	query := `
	UPDATE labelers SET
		endpoint = $1,
		status = 'active',
		failure_type = '',
		last_connected = $2,
		last_error = '',
		fail_count = 0,
		updated_at = $2
	WHERE did = $3
	`
	_, err := db.conn.Exec(query, endpoint, time.Now().UTC(), did)
	return err
}

// DeleteLabeler removes a labeler from the database.
func (db *DB) DeleteLabeler(did string) error {
	_, err := db.conn.Exec(`DELETE FROM labelers WHERE did = $1`, did)
	return err
}

package storage

import (
	"database/sql"
	"fmt"

	"bsky-collector/models"
)

// SaveActor inserts or updates an actor profile.
func (db *DB) SaveActor(actor *models.Actor) error {
	query := `
	INSERT INTO actors (
		did, handle, display_name, description, avatar, banner,
		followers_count, follows_count, posts_count, status, received_at, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (did) DO UPDATE SET
		handle = EXCLUDED.handle,
		display_name = EXCLUDED.display_name,
		description = EXCLUDED.description,
		avatar = EXCLUDED.avatar,
		banner = EXCLUDED.banner,
		followers_count = EXCLUDED.followers_count,
		follows_count = EXCLUDED.follows_count,
		posts_count = EXCLUDED.posts_count,
		status = EXCLUDED.status,
		received_at = EXCLUDED.received_at,
		created_at = EXCLUDED.created_at;
	`

	_, err := db.conn.Exec(query,
		actor.DID,
		actor.Handle,
		actor.DisplayName,
		actor.Description,
		actor.Avatar,
		actor.Banner,
		actor.FollowersCount,
		actor.FollowsCount,
		actor.PostsCount,
		actor.Status,
		actor.ReceivedAt,
		actor.CreatedAt,
	)
	return err
}

// SaveActorsBatch inserts or updates multiple actors in a single transaction.
func (db *DB) SaveActorsBatch(actors []*models.Actor) error {
	if len(actors) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO actors (
			did, handle, display_name, description, avatar, banner,
			followers_count, follows_count, posts_count, status, received_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (did) DO UPDATE SET
			handle = EXCLUDED.handle,
			display_name = EXCLUDED.display_name,
			description = EXCLUDED.description,
			avatar = EXCLUDED.avatar,
			banner = EXCLUDED.banner,
			followers_count = EXCLUDED.followers_count,
			follows_count = EXCLUDED.follows_count,
			posts_count = EXCLUDED.posts_count,
			status = EXCLUDED.status,
			received_at = EXCLUDED.received_at,
			created_at = EXCLUDED.created_at;
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, actor := range actors {
		if actor.Status == "" {
			actor.Status = "active"
		}
		_, err := stmt.Exec(
			actor.DID,
			actor.Handle,
			actor.DisplayName,
			actor.Description,
			actor.Avatar,
			actor.Banner,
			actor.FollowersCount,
			actor.FollowsCount,
			actor.PostsCount,
			actor.Status,
			actor.ReceivedAt,
			actor.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert actor: %w", err)
		}
	}

	return tx.Commit()
}

// ActorExists checks if an actor with the given DID exists.
func (db *DB) ActorExists(did string) (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM actors WHERE did = $1", did).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to query actor: %w", err)
	}
	return count > 0, nil
}

// GetActor returns an actor by DID.
func (db *DB) GetActor(did string) (*models.Actor, error) {
	var actor models.Actor
	query := `
	SELECT did, handle, display_name, description, avatar, banner,
	       followers_count, follows_count, posts_count, status, received_at, created_at
	FROM actors WHERE did = $1
	`
	err := db.conn.QueryRow(query, did).Scan(
		&actor.DID,
		&actor.Handle,
		&actor.DisplayName,
		&actor.Description,
		&actor.Avatar,
		&actor.Banner,
		&actor.FollowersCount,
		&actor.FollowsCount,
		&actor.PostsCount,
		&actor.Status,
		&actor.ReceivedAt,
		&actor.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get actor: %w", err)
	}
	return &actor, nil
}

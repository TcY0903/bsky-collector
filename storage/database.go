package storage

import (
	"database/sql"
	"fmt"
	"log"

	"bsky-collector/models"

	_ "github.com/lib/pq"
)

// DB wraps the PostgreSQL database connection.
type DB struct {
	conn *sql.DB
}

// NewDB creates a new database connection and initializes tables.
func NewDB(connStr string) (*DB, error) {
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	db := &DB{conn: conn}

	if err := db.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return db, nil
}

// initSchema creates all necessary tables.
func (db *DB) initSchema() error {
	// Posts table
	postsSQL := `
	CREATE TABLE IF NOT EXISTS posts (
		id BIGSERIAL PRIMARY KEY,
		uri TEXT UNIQUE NOT NULL,
		did TEXT NOT NULL,
		rkey TEXT NOT NULL,
		cid TEXT,
		text TEXT,
		created_at TIMESTAMP,
		received_at TIMESTAMP NOT NULL,
		reply_parent TEXT,
		reply_root TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_posts_uri ON posts(uri);
	CREATE INDEX IF NOT EXISTS idx_posts_did ON posts(did);
	CREATE INDEX IF NOT EXISTS idx_posts_rkey ON posts(rkey);
	CREATE INDEX IF NOT EXISTS idx_posts_cid ON posts(cid);
	CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
	CREATE INDEX IF NOT EXISTS idx_posts_received_at ON posts(received_at);
	CREATE INDEX IF NOT EXISTS idx_posts_reply_parent ON posts(reply_parent);
	CREATE INDEX IF NOT EXISTS idx_posts_reply_root ON posts(reply_root);
	`

	// Actors table with status field reserved
	actorsSQL := `
	CREATE TABLE IF NOT EXISTS actors (
		did TEXT PRIMARY KEY,
		handle TEXT NOT NULL,
		display_name TEXT,
		description TEXT,
		avatar TEXT,
		banner TEXT,
		followers_count INTEGER DEFAULT 0,
		follows_count INTEGER DEFAULT 0,
		posts_count INTEGER DEFAULT 0,
		status TEXT DEFAULT 'active',
		indexed_at TIMESTAMP NOT NULL,
		created_at TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_actors_handle ON actors(handle);
	CREATE INDEX IF NOT EXISTS idx_actors_status ON actors(status);
	`

	queries := []string{postsSQL, actorsSQL}
	for _, q := range queries {
		if _, err := db.conn.Exec(q); err != nil {
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}

	return nil
}

// --- Post related methods ---
// SavePost inserts a single post into the database.
func (db *DB) SavePost(post *models.Post) error {
	query := `
	INSERT INTO posts (uri, did, rkey, cid, text, created_at, received_at, reply_parent, reply_root)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := db.conn.Exec(query,
		post.URI,
		post.DID,
		post.RKey,
		post.Cid,
		post.Text,
		post.CreatedAt,
		post.ReceivedAt,
		post.ReplyParent,
		post.ReplyRoot,
	)
	return err
}

// SavePostsBatch inserts multiple posts in a single transaction.
func (db *DB) SavePostsBatch(posts []*models.Post) error {
	if len(posts) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO posts (uri, did, rkey, cid, text, created_at, received_at, reply_parent, reply_root)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, post := range posts {
		_, err := stmt.Exec(
			post.URI,
			post.DID,
			post.RKey,
			post.Cid,
			post.Text,
			post.CreatedAt,
			post.ReceivedAt,
			post.ReplyParent,
			post.ReplyRoot,
		)
		if err != nil {
			return fmt.Errorf("failed to insert post: %w", err)
		}
	}

	return tx.Commit()
}

// --- Actor related methods ---
// SaveActor inserts or updates an actor profile.
func (db *DB) SaveActor(actor *models.Actor) error {
	query := `
	INSERT INTO actors (
		did, handle, display_name, description, avatar, banner,
		followers_count, follows_count, posts_count, status, indexed_at, created_at
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
		indexed_at = EXCLUDED.indexed_at,
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
		actor.IndexedAt,
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
			followers_count, follows_count, posts_count, status, indexed_at, created_at
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
			indexed_at = EXCLUDED.indexed_at,
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
			actor.IndexedAt,
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
	       followers_count, follows_count, posts_count, status, indexed_at, created_at
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
		&actor.IndexedAt,
		&actor.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // Actor not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get actor: %w", err)
	}
	return &actor, nil
}

// --- Stats ---
// GetStats returns basic statistics about the stored data.
func (db *DB) GetStats() (map[string]int64, error) {
	stats := make(map[string]int64)
	queries := map[string]string{
		"total_posts":  "SELECT COUNT(*) FROM posts;",
		"total_actors": "SELECT COUNT(*) FROM actors;",
	}
	for key, query := range queries {
		var count int64
		err := db.conn.QueryRow(query).Scan(&count)
		if err != nil {
			log.Printf("Warning: failed to get stats for %s: %v", key, err)
			continue
		}
		stats[key] = count
	}
	return stats, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

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

// initSchema creates tables and indexes
func (db *DB) initSchema() error {
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

	if _, err := db.conn.Exec(postsSQL); err != nil {
		return fmt.Errorf("failed to create posts table: %w", err)
	}

	return nil
}

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

// GetStats returns basic statistics about the posts.
func (db *DB) GetStats() (map[string]int64, error) {
	stats := make(map[string]int64)
	queries := map[string]string{
		"total_posts": "SELECT COUNT(*) FROM posts;",
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

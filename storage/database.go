package storage

import (
	"database/sql"
	"fmt"
	"log"
	"time"

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

	// Configure connection pool
	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)

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
	queries := []string{
		// Posts table
		`
		CREATE TABLE IF NOT EXISTS posts (
			id BIGSERIAL PRIMARY KEY,
			uri TEXT UNIQUE NOT NULL,
			did TEXT NOT NULL,
			rkey TEXT NOT NULL,
			cid TEXT,
			text TEXT,
			media JSONB,
			reply_parent TEXT,
			reply_root TEXT,
			created_at TIMESTAMP,
			received_at TIMESTAMP NOT NULL,
			status TEXT DEFAULT 'active'
		);
		CREATE INDEX IF NOT EXISTS idx_posts_uri ON posts(uri);
		CREATE INDEX IF NOT EXISTS idx_posts_did ON posts(did);
		CREATE INDEX IF NOT EXISTS idx_posts_rkey ON posts(rkey);
		CREATE INDEX IF NOT EXISTS idx_posts_cid ON posts(cid);
		CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
		CREATE INDEX IF NOT EXISTS idx_posts_received_at ON posts(received_at);
		CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
		`,

		// Actors table
		`
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
			received_at TIMESTAMP NOT NULL,
			created_at TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_actors_handle ON actors(handle);
		CREATE INDEX IF NOT EXISTS idx_actors_status ON actors(status);
		`,

		// Labels table
		`
		CREATE TABLE IF NOT EXISTS labels (
			id BIGSERIAL PRIMARY KEY,
			src TEXT NOT NULL,
			uri TEXT NOT NULL,
			cid TEXT,
			val TEXT NOT NULL,
			neg BOOLEAN DEFAULT FALSE,
			cts TIMESTAMP,
			received_at TIMESTAMP NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_labels_src ON labels(src);
		CREATE INDEX IF NOT EXISTS idx_labels_uri ON labels(uri);
		CREATE INDEX IF NOT EXISTS idx_labels_val ON labels(val);
		CREATE INDEX IF NOT EXISTS idx_labels_received_at ON labels(received_at);
		`,

		// Post history table
		`
		CREATE TABLE IF NOT EXISTS post_history (
			id BIGSERIAL PRIMARY KEY,
			post_uri TEXT NOT NULL,
			cid TEXT NOT NULL,
			text TEXT,
			media JSONB,
			reply_parent TEXT,
			reply_root TEXT,
			created_at TIMESTAMP,
			received_at TIMESTAMP NOT NULL,
			valid_to TIMESTAMP NOT NULL,
			status TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_post_history_uri ON post_history(post_uri);
		CREATE INDEX IF NOT EXISTS idx_post_history_cid ON post_history(cid);
		CREATE INDEX IF NOT EXISTS idx_post_history_created_at ON post_history(created_at);
		CREATE INDEX IF NOT EXISTS idx_post_history_received_at ON post_history(received_at);
		CREATE INDEX IF NOT EXISTS idx_post_history_valid_to ON post_history(valid_to);
		CREATE INDEX IF NOT EXISTS idx_post_history_status ON post_history(status);
		`,
	}

	for _, q := range queries {
		if _, err := db.conn.Exec(q); err != nil {
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}

	return nil
}

// GetStats returns basic statistics about the stored data.
func (db *DB) GetStats() (map[string]int64, error) {
	stats := make(map[string]int64)
	queries := map[string]string{
		"total_posts":   "SELECT COUNT(*) FROM posts;",
		"total_actors":  "SELECT COUNT(*) FROM actors;",
		"total_labels":  "SELECT COUNT(*) FROM labels;",
		"total_history": "SELECT COUNT(*) FROM post_history;",
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

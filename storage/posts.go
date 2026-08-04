package storage

import (
	"fmt"

	"bsky-collector/models"
)

// SavePost inserts a single post into the database.
func (db *DB) SavePost(post *models.Post) error {
	query := `
	INSERT INTO posts (uri, did, rkey, cid, text, created_at, received_at, reply_parent, reply_root)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (uri) DO NOTHING
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
		ON CONFLICT (uri) DO NOTHING
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

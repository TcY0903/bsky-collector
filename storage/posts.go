package storage

import (
	"database/sql"
	"fmt"

	"bsky-collector/models"
)

// GetPostByURI retrieves a post by its URI.
func (db *DB) GetPostByURI(uri string) (*models.Post, error) {
	var post models.Post
	query := `
	SELECT id, uri, did, rkey, cid, text, media, reply_parent, reply_root, created_at, received_at, status
	FROM posts WHERE uri = $1
	`
	err := db.conn.QueryRow(query, uri).Scan(
		&post.ID,
		&post.URI,
		&post.DID,
		&post.RKey,
		&post.Cid,
		&post.Text,
		&post.Media,
		&post.ReplyParent,
		&post.ReplyRoot,
		&post.CreatedAt,
		&post.ReceivedAt,
		&post.Status,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get post: %w", err)
	}
	return &post, nil
}

// SavePost inserts a single post into the database.
func (db *DB) SavePost(post *models.Post) error {
	query := `
	INSERT INTO posts (uri, did, rkey, cid, text, media, reply_parent, reply_root, created_at, received_at, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	ON CONFLICT (uri) DO UPDATE SET
		did = EXCLUDED.did,
		rkey = EXCLUDED.rkey,
		cid = EXCLUDED.cid,
		text = EXCLUDED.text,
		media = EXCLUDED.media,
		reply_parent = EXCLUDED.reply_parent,
		reply_root = EXCLUDED.reply_root,
		created_at = EXCLUDED.created_at,
		received_at = EXCLUDED.received_at,
		status = EXCLUDED.status
	`
	_, err := db.conn.Exec(query,
		post.URI,
		post.DID,
		post.RKey,
		post.Cid,
		post.Text,
		post.Media,
		post.ReplyParent,
		post.ReplyRoot,
		post.CreatedAt,
		post.ReceivedAt,
		post.Status,
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
		INSERT INTO posts (uri, did, rkey, cid, text, media, reply_parent, reply_root, created_at, received_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (uri) DO UPDATE SET
			did = EXCLUDED.did,
			rkey = EXCLUDED.rkey,
			cid = EXCLUDED.cid,
			text = EXCLUDED.text,
			media = EXCLUDED.media,
			reply_parent = EXCLUDED.reply_parent,
			reply_root = EXCLUDED.reply_root,
			created_at = EXCLUDED.created_at,
			received_at = EXCLUDED.received_at,
			status = EXCLUDED.status
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, post := range posts {
		if post.Status == "" {
			post.Status = "active"
		}
		_, err := stmt.Exec(
			post.URI,
			post.DID,
			post.RKey,
			post.Cid,
			post.Text,
			post.Media,
			post.ReplyParent,
			post.ReplyRoot,
			post.CreatedAt,
			post.ReceivedAt,
			post.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert post: %w", err)
		}
	}

	return tx.Commit()
}

// DeletePost marks a post as deleted (soft delete).
func (db *DB) DeletePost(uri string) error {
	query := `UPDATE posts SET status = 'deleted' WHERE uri = $1`
	_, err := db.conn.Exec(query, uri)
	return err
}

// DeletePostsBatch marks multiple posts as deleted in a single transaction.
func (db *DB) DeletePostsBatch(uris []string) error {
	if len(uris) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE posts SET status = 'deleted' WHERE uri = $1`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, uri := range uris {
		_, err := stmt.Exec(uri)
		if err != nil {
			return fmt.Errorf("failed to delete post %s: %w", uri, err)
		}
	}

	return tx.Commit()
}

// --- Post history management ---
// SavePostHistory inserts a historical version of a post.
func (db *DB) SavePostHistory(history *models.PostHistory) error {
	query := `
	INSERT INTO post_history (post_uri, cid, text, media, reply_parent, reply_root, created_at, received_at, valid_to, status)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := db.conn.Exec(query,
		history.PostURI,
		history.CID,
		history.Text,
		history.Media,
		history.ReplyParent,
		history.ReplyRoot,
		history.CreatedAt,
		history.ReceivedAt,
		history.ValidTo,
		history.Status,
	)
	return err
}

// SavePostHistoryBatch inserts multiple historical versions in a single transaction.
func (db *DB) SavePostHistoryBatch(histories []*models.PostHistory) error {
	if len(histories) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO post_history (post_uri, cid, text, media, reply_parent, reply_root, created_at, received_at, valid_to, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, history := range histories {
		_, err := stmt.Exec(
			history.PostURI,
			history.CID,
			history.Text,
			history.Media,
			history.ReplyParent,
			history.ReplyRoot,
			history.CreatedAt,
			history.ReceivedAt,
			history.ValidTo,
			history.Status,
		)
		if err != nil {
			return fmt.Errorf("failed to insert history: %w", err)
		}
	}

	return tx.Commit()
}

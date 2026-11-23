package storage

import (
	"database/sql"
	"fmt"
	"sort"

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
// GetPostHistoryByID retrieves a historical version of a post by its ID.
func (db *DB) GetPostHistoryByID(id int64) (*models.PostHistory, error) {
	var h models.PostHistory
	query := `
	SELECT id, post_uri, cid, text, media, reply_parent, reply_root, created_at, received_at, valid_to, status
	FROM post_history WHERE id = $1
	`
	err := db.conn.QueryRow(query, id).Scan(
		&h.ID, &h.PostURI, &h.CID, &h.Text, &h.Media, &h.ReplyParent, &h.ReplyRoot,
		&h.CreatedAt, &h.ReceivedAt, &h.ValidTo, &h.Status,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get post history: %w", err)
	}
	return &h, nil
}

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

// --- Search ---
// SearchPosts searches for posts based on keyword, did, and time range
func (db *DB) SearchPosts(uri string, keyword string, did string, startTime, endTime string, limit, offset int) ([]*models.Post, error) {
	query := `
	SELECT id, uri, did, rkey, cid, text, media, reply_parent, reply_root, created_at, received_at, status
	FROM posts 
	WHERE status = 'active'
	`
	args := []interface{}{}
	argCount := 1

	if uri != "" {
		query += fmt.Sprintf(" AND uri = $%d", argCount)
		args = append(args, uri)
		argCount++
	}
	if keyword != "" {
		query += fmt.Sprintf(" AND COALESCE(text, '') ILIKE $%d", argCount)
		args = append(args, "%"+keyword+"%")
		argCount++
	}
	if did != "" {
		query += fmt.Sprintf(" AND did = $%d", argCount)
		args = append(args, did)
		argCount++
	}
	if startTime != "" {
		query += fmt.Sprintf(" AND received_at >= $%d", argCount)
		args = append(args, startTime)
		argCount++
	}
	if endTime != "" {
		query += fmt.Sprintf(" AND received_at <= $%d", argCount)
		args = append(args, endTime)
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY received_at DESC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search posts: %w", err)
	}
	defer rows.Close()

	var posts []*models.Post
	for rows.Next() {
		var post models.Post
		err := rows.Scan(
			&post.ID, &post.URI, &post.DID, &post.RKey, &post.Cid,
			&post.Text, &post.Media, &post.ReplyParent, &post.ReplyRoot,
			&post.CreatedAt, &post.ReceivedAt, &post.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan post: %w", err)
		}
		posts = append(posts, &post)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating posts: %w", err)
	}
	return posts, nil
}

// SearchPostHistory searches for historical versions of a post based on its URI
func (db *DB) SearchPostHistory(postURI string, limit, offset int) ([]*models.PostHistory, error) {
	query := `
	SELECT id, post_uri, cid, text, media, reply_parent, reply_root, created_at, received_at, valid_to, status
	FROM post_history 
	WHERE 1=1
	`
	args := []interface{}{}
	argCount := 1

	if postURI != "" {
		query += fmt.Sprintf(" AND post_uri = $%d", argCount)
		args = append(args, postURI)
		argCount++
	}

	query += fmt.Sprintf(" ORDER BY valid_to DESC, received_at DESC LIMIT $%d OFFSET $%d", argCount, argCount+1)
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search post history: %w", err)
	}
	defer rows.Close()

	var histories []*models.PostHistory
	for rows.Next() {
		var h models.PostHistory
		err := rows.Scan(
			&h.ID, &h.PostURI, &h.CID, &h.Text, &h.Media, &h.ReplyParent, &h.ReplyRoot,
			&h.CreatedAt, &h.ReceivedAt, &h.ValidTo, &h.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan post history: %w", err)
		}
		histories = append(histories, &h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating post history: %w", err)
	}
	return histories, nil
}

// --- Conversation Management ---
// GetConversation fetches the full conversation tree (ancestors + descendants) for a given post URI.
// Returns a flat list of posts ordered by created_at. The template can decide how to render it.
func (db *DB) GetConversation(uri string) ([]*models.Post, error) {
	// 1. Get the target post
	target, err := db.GetPostByURI(uri)
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, nil
	}

	seen := map[string]bool{uri: true}
	var result []*models.Post
	result = append(result, target)

	// 2. Walk up to root using ReplyParent
	current := target
	for current.ReplyParent != "" && !seen[current.ReplyParent] {
		parent, err := db.GetPostByURI(current.ReplyParent)
		if err != nil || parent == nil {
			break
		}
		seen[parent.URI] = true
		result = append(result, parent)
		current = parent
	}

	// 3. Walk down from root using reply_parent = current URI (BFS)
	// Start from the root of the thread
	rootURI := current.URI
	queue := []string{rootURI}
	for len(queue) > 0 {
		parentURI := queue[0]
		queue = queue[1:]

		replies, err := db.GetRepliesByParent(parentURI)
		if err != nil {
			continue
		}
		for _, r := range replies {
			if !seen[r.URI] {
				seen[r.URI] = true
				result = append(result, r)
				queue = append(queue, r.URI)
			}
		}
	}

	// Sort by created_at ascending so the thread reads top-to-bottom
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})

	return result, nil
}

// GetRepliesByParent returns direct replies to a given post URI.
func (db *DB) GetRepliesByParent(parentURI string) ([]*models.Post, error) {
	query := `
	SELECT id, uri, did, rkey, cid, text, media, reply_parent, reply_root, created_at, received_at, status
	FROM posts 
	WHERE reply_parent = $1 AND status = 'active'
	ORDER BY created_at ASC
	`
	rows, err := db.conn.Query(query, parentURI)
	if err != nil {
		return nil, fmt.Errorf("failed to get replies: %w", err)
	}
	defer rows.Close()

	var posts []*models.Post
	for rows.Next() {
		var p models.Post
		err := rows.Scan(
			&p.ID, &p.URI, &p.DID, &p.RKey, &p.Cid,
			&p.Text, &p.Media, &p.ReplyParent, &p.ReplyRoot,
			&p.CreatedAt, &p.ReceivedAt, &p.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan reply: %w", err)
		}
		posts = append(posts, &p)
	}
	return posts, rows.Err()
}

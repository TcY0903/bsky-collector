package models

import "time"

// Post represents a single Bluesky post with its content and metadata.
type Post struct {
	ID          uint      `gorm:"primaryKey"`
	URI         string    `gorm:"uniqueIndex"` // Full AT-URI: at://did/collection/rkey
	DID         string    `gorm:"index"`       // Author's DID
	RKey        string    `gorm:"index"`       // Post record key
	Cid         string    `gorm:"index"`       // Content Identifier (CID) for integrity verification
	Text        string    // Post content
	CreatedAt   time.Time `gorm:"index"` // Post creation time (from Bluesky)
	ReceivedAt  time.Time `gorm:"index"` // Time our system received the post
	ReplyParent string    `gorm:"index"` // URI of the parent post (if this is a reply)
	ReplyRoot   string    `gorm:"index"` // URI of the root post of the thread (if this is a reply)
	Payload     []byte    `gorm:"-"`     // Temporary storage for CAR data, not saved to DB
}

// Like represents a like interaction on a post.
type Like struct {
	ID         uint   `gorm:"primaryKey"`
	DID        string `gorm:"index"` // User who liked
	SubjectURI string `gorm:"index"` // URI of the liked post
	CreatedAt  time.Time
	ReceivedAt time.Time `gorm:"index"`
	RKey       string    `gorm:"index"`
	URI        string    `gorm:"uniqueIndex"`
}

// Repost represents a repost interaction.
type Repost struct {
	ID         uint   `gorm:"primaryKey"`
	DID        string `gorm:"index"` // User who reposted
	SubjectURI string `gorm:"index"` // URI of the reposted post
	CreatedAt  time.Time
	ReceivedAt time.Time `gorm:"index"`
	RKey       string    `gorm:"index"`
	URI        string    `gorm:"uniqueIndex"`
}

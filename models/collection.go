package models

import "time"

// Like represents a like interaction on a post.
type Like struct {
	ID         uint   `gorm:"primaryKey"`
	DID        string `gorm:"index"` // User who liked
	SubjectURI string `gorm:"index"` // URI of the liked post
	RKey       string `gorm:"index"`
	URI        string `gorm:"uniqueIndex"`
	CreatedAt  time.Time
	ReceivedAt time.Time `gorm:"index"`
}

// Repost represents a repost interaction.
type Repost struct {
	ID         uint   `gorm:"primaryKey"`
	DID        string `gorm:"index"` // User who reposted
	SubjectURI string `gorm:"index"` // URI of the reposted post
	RKey       string `gorm:"index"`
	URI        string `gorm:"uniqueIndex"`
	CreatedAt  time.Time
	ReceivedAt time.Time `gorm:"index"`
}

// Follow represents a follow relationship between two Bluesky users.
type Follow struct {
	ID         uint      `gorm:"primaryKey"`
	DID        string    `gorm:"index"`       // The user who initiated the follow
	SubjectDID string    `gorm:"index"`       // The user who was followed
	RKey       string    `gorm:"index"`       // Record key
	URI        string    `gorm:"uniqueIndex"` // AT-URI: at://did/collection/rkey
	CreatedAt  time.Time // When the follow was created (from the record)
	ReceivedAt time.Time `gorm:"index"` // When our system received the event
}

// Block represents a block relationship between two Bluesky users.
type Block struct {
	ID         uint      `gorm:"primaryKey"`
	URI        string    `gorm:"uniqueIndex"` // at://did/app.bsky.graph.block/rkey
	DID        string    `gorm:"index"`       // The user who performed the block
	SubjectDID string    `gorm:"index"`       // The user who was blocked
	RKey       string    `gorm:"index"`       // Record key
	CID        string    `gorm:"index"`       // Content Identifier
	CreatedAt  time.Time // When the block was created (from Bluesky record)
	ReceivedAt time.Time `gorm:"index"` // When our system received the event
	Status     string    `gorm:"index"` // "active" or "deleted"
}

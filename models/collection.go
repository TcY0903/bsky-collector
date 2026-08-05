package models

import "time"

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

// Follow represents a follow relationship between two Bluesky users.
type Follow struct {
	ID         uint      `gorm:"primaryKey"`
	DID        string    `gorm:"index"` // The user who initiated the follow
	SubjectDID string    `gorm:"index"` // The user who was followed
	CreatedAt  time.Time // When the follow was created (from the record)
	ReceivedAt time.Time `gorm:"index"`       // When our system received the event
	RKey       string    `gorm:"index"`       // Record key
	URI        string    `gorm:"uniqueIndex"` // AT-URI: at://did/collection/rkey
}

// Block represents a block relationship between two Bluesky users.
type Block struct {
	ID         uint      `gorm:"primaryKey"`
	DID        string    `gorm:"index"` // The user who performed the block
	SubjectDID string    `gorm:"index"` // The user who was blocked
	CreatedAt  time.Time // When the block was created (from the record)
	ReceivedAt time.Time `gorm:"index"`       // When our system received the event
	RKey       string    `gorm:"index"`       // Record key
	URI        string    `gorm:"uniqueIndex"` // AT-URI: at://did/collection/rkey
}

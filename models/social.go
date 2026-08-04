package models

import "time"

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

package models

import "time"

// Label represents a moderation label applied to a post or account.
type Label struct {
	ID         uint      `gorm:"primaryKey"`
	Src        string    `gorm:"index"` // DID of the labeler that created this label
	URI        string    `gorm:"index"` // AT-URI of the subject (post or account)
	CID        string    // Content identifier of the subject (if available)
	Val        string    `gorm:"index"` // Label value (e.g., "spam", "porn", "misleading")
	Neg        bool      // If true, this is a negation label (overwriting a previous label)
	CTS        time.Time // Timestamp when the label was created (from the label event)
	ReceivedAt time.Time `gorm:"index"` // Time when our system received the label
}

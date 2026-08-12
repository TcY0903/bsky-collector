package models

import "time"

// List represents a Bluesky list (app.bsky.graph.list)
type List struct {
	ID          uint      `gorm:"primaryKey"`
	URI         string    `gorm:"uniqueIndex"` // at://did/app.bsky.graph.list/rkey
	DID         string    `gorm:"index"`       // Creator's DID
	RKey        string    `gorm:"index"`       // Record key
	CID         string    `gorm:"index"`       // Content Identifier
	Name        string    `gorm:"index"`       // List name
	Purpose     string    `gorm:"index"`       // "curatelist", "modlist" or "referencelist"
	Description string    // Optional description
	CreatedAt   time.Time `gorm:"index"`
	ReceivedAt  time.Time `gorm:"index"`
	Status      string    `gorm:"index"` // "active" or "deleted"
}

// ListItem represents a user being added to a list (app.bsky.graph.listitem)
type ListItem struct {
	ID         uint      `gorm:"primaryKey"`
	URI        string    `gorm:"uniqueIndex"` // at://did/app.bsky.graph.listitem/rkey
	ListURI    string    `gorm:"index"`       // The list this item belongs to
	DID        string    `gorm:"index"`       // The user who created this item
	SubjectDID string    `gorm:"index"`       // The user being added to the list
	RKey       string    `gorm:"index"`       // Record key
	CID        string    `gorm:"index"`       // Content Identifier
	CreatedAt  time.Time `gorm:"index"`
	ReceivedAt time.Time `gorm:"index"`
	Status     string    `gorm:"index"` // "active" or "deleted"
}

// ListBlock represents a user blocking an entire list (app.bsky.graph.listblock)
type ListBlock struct {
	ID          uint      `gorm:"primaryKey"`
	URI         string    `gorm:"uniqueIndex"` // at://did/app.bsky.graph.listblock/rkey
	DID         string    `gorm:"index"`       // The user who created this block
	SubjectList string    `gorm:"index"`       // URI of the list being blocked
	RKey        string    `gorm:"index"`       // Record key
	CID         string    `gorm:"index"`       // Content Identifier
	CreatedAt   time.Time `gorm:"index"`
	ReceivedAt  time.Time `gorm:"index"`
	Status      string    `gorm:"index"` // "active" or "deleted"
}

package models

import (
	"time"

	"gorm.io/datatypes"
)

// Post represents a single Bluesky post with its content and metadata.
type Post struct {
	ID          uint           `gorm:"primaryKey"`
	URI         string         `gorm:"uniqueIndex"` // Full AT-URI: at://did/collection/rkey
	DID         string         `gorm:"index"`       // Author's DID
	RKey        string         `gorm:"index"`       // Post record key
	Cid         string         `gorm:"index"`       // Content Identifier (CID) for integrity verification
	Text        string         // Post content
	Media       datatypes.JSON `gorm:"type:jsonb"` // Media metadata in JSON format
	ReplyParent string         `gorm:"index"`      // URI of the parent post (if this is a reply)
	ReplyRoot   string         `gorm:"index"`      // URI of the root post of the thread (if this is a reply)
	CreatedAt   time.Time      `gorm:"index"`      // Post creation time (from Bluesky)
	ReceivedAt  time.Time      `gorm:"index"`      // Time our system received the post
	Status      string         `gorm:"index"`      // "active", "updated" or "deleted"
	NeedActor   bool           `gorm:"-"`          // Indicates if actor info needs to be fetched
	Payload     []byte         `gorm:"-"`          // Temporary storage for CAR data, not saved to DB
}

// PostMedia represents the media content associated with a post
type PostMedia struct {
	Type     string        `json:"type"` // "images", "video", "external", "record"
	Images   []ImageInfo   `json:"images,omitempty"`
	Video    *VideoInfo    `json:"video,omitempty"`
	External *ExternalInfo `json:"external,omitempty"`
	Record   *RecordRef    `json:"record,omitempty"`
}

// ImageInfo represents information about an image associated with a post
type ImageInfo struct {
	URL string `json:"url"`           // CDN URL
	Alt string `json:"alt,omitempty"` // Image alt text
	CID string `json:"cid"`           // Content Identifier (CID)
}

// VideoInfo represents information about a video associated with a post
type VideoInfo struct {
	URL string `json:"url"` // CDN URL
	CID string `json:"cid"` // Content Identifier (CID)
}

// ExternalInfo represents information about an external link card associated with a post
type ExternalInfo struct {
	URI         string `json:"uri"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Thumb       string `json:"thumb,omitempty"` // Thumbnail URL
}

// RecordRef represents a reference to another post
type RecordRef struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
}

// PostHistory represents a historical version of a post (after update or delete).
type PostHistory struct {
	ID          uint           `gorm:"primaryKey"`
	PostURI     string         `gorm:"index"` // Full AT-URI
	CID         string         `gorm:"index"` // Content Identifier
	Text        string         // Post content at the time of archiving
	Media       datatypes.JSON `gorm:"type:jsonb"` // Media metadata
	ReplyParent string         // URI of the parent post
	ReplyRoot   string         // URI of the root post
	CreatedAt   time.Time      `gorm:"index"` // Post creation time (from Bluesky)
	ReceivedAt  time.Time      `gorm:"index"` // When this version was received (valid_from)
	ValidTo     time.Time      `gorm:"index"` // When this version became invalid (was replaced or deleted)
	Status      string         // "updated" or "deleted"
}

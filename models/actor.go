package models

import "time"

// Actor represents a Bluesky user profile.
type Actor struct {
	DID            string    `json:"did" gorm:"primaryKey"` // Decentralized identifier
	Handle         string    `json:"handle" gorm:"index"`   // User handle
	DisplayName    string    `json:"displayName"`
	Description    string    `json:"description"`
	Avatar         string    `json:"avatar"`         // Avatar URL
	Banner         string    `json:"banner"`         // Banner URL
	FollowersCount int       `json:"followersCount"` // Number of followers
	FollowsCount   int       `json:"followsCount"`   // Number of accounts followed
	PostsCount     int       `json:"postsCount"`     // Number of posts
	CreatedAt      time.Time `json:"createdAt"`      // Account creation timestamp (from Bluesky)
	ReceivedAt     time.Time `json:"-"`              // Last time this record was updated
	Status         string    `json:"-" gorm:"index"` // "active", "deactivated", "suspended", ...
}

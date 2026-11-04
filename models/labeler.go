package models

import "time"

// Labeler represents a known labeler service and its connection state.
type Labeler struct {
	DID           string    `gorm:"primaryKey"`
	Endpoint      string    // Resolved WebSocket URL
	Status        string    // "unknown", "active", "resolve_failed", "connect_failed"
	FailureType   string    // "resolve", "connect", or "" (empty when active)
	LastConnected time.Time // Last time we successfully connected
	LastError     string    // Last error message
	FailCount     int       // Consecutive failure count
	FirstSeen     time.Time // When we first discovered this labeler
	UpdatedAt     time.Time // Last time this record was updated
}

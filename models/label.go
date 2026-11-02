package models

import "time"

// Label represents a moderation label applied to a post or account.
type Label struct {
	ID          uint      `gorm:"primaryKey"`
	Seq         int64     `gorm:"index"` // Relay's global sequence number
	UpstreamSeq int64     `gorm:"index"` // Upstream labeler's original sequence
	Src         string    `gorm:"index"` // DID of the labeler that issued this label
	URI         string    `gorm:"index"` // AT-URI of the subject (post or account)
	Val         string    `gorm:"index"` // Label value ("nudity", "spam", "!hide", ...)
	Ver         int       // Label version
	CTS         time.Time `gorm:"index"` // Label creation timestamp from the labeler
	Sig         []byte    // Signature (for verification)
	ReceivedAt  time.Time `gorm:"index"` // When received the label
}

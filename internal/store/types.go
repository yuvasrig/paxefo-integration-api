package store

import (
	"encoding/json"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// Event statuses stored in event_outcomes (provider statuses plus the store-level ones).
const (
	StatusDuplicate = "duplicate"
	StatusConflict  = "conflict"
	StatusFailed    = "failed"
)

// RawEvent is a payload exactly as received, saved before any 2xx (F1-R2).
type RawEvent struct {
	ID         string
	TenantID   string // the API key's tenant
	Provider   string
	Raw        []byte
	Hash       string
	ReceivedAt time.Time
	RequestID  string
}

// CommitInput is the second transaction: normalized rows plus the outcome.
type CommitInput struct {
	RawEventID string
	TenantID   string
	Result     provider.Result
}

// CommitResult is what was decided inside the transaction.
type CommitResult struct {
	Status          string       // accepted, flagged, rejected, duplicate or conflict
	Flags           []rules.Flag // transform flags plus DUPLICATE_CONFLICT when it applies
	FirstRawEventID string       // the event that owns the key, for duplicate and conflict
}

// EventRecord is an event with its outcome, as shown by GET /events/{id}.
type EventRecord struct {
	ID               string
	TenantID         string
	Provider         string
	RequestID        string
	Raw              []byte
	ReceivedAt       time.Time
	HasOutcome       bool
	Status           string
	Reasons          []rules.Problem
	Flags            []rules.Flag
	Sensor           json.RawMessage
	Location         json.RawMessage
	TransformVersion string
	RulesVersion     string
	ProcessedAt      time.Time
	Attempts         int
	Error            string
}

// APIKey is a stored key (never the plaintext).
type APIKey struct {
	ID        int64
	Prefix    string
	TenantID  string
	Name      string
	CreatedAt time.Time
	ExpiresAt *time.Time
	RevokedAt *time.Time
}

// ReadingsQuery selects readings for one device.
type ReadingsQuery struct {
	DeviceID string
	From     *int64 // epoch ms, inclusive
	To       *int64 // epoch ms, exclusive
	Limit    int
	Cursor   string
}

// ReadingItem is one timestamp's sensor and/or location reading.
type ReadingItem struct {
	Timestamp int64                  `json:"timestamp"`
	EventID   string                 `json:"event_id"`
	Sensor    *paxafe.SensorRecord   `json:"sensor"`
	Location  *paxafe.LocationRecord `json:"location"`
}

// ReadingsPage is a page of readings, newest first.
type ReadingsPage struct {
	Items      []ReadingItem `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

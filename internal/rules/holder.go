package rules

import (
	"sync/atomic"
	"time"
)

// Loaded is a ruleset together with where it came from and when it became active.
type Loaded struct {
	Ruleset  *Ruleset
	Source   string // SourceDefault, SourceFile or SourceDatabase
	LoadedAt time.Time
}

// Where a running ruleset came from.
const (
	SourceDefault  = "default"  // the embedded default ruleset
	SourceFile     = "file"     // RULES_PATH
	SourceDatabase = "database" // published with `api rules set`
)

// Holder is the ruleset currently in force. It exists so rules can change while the service runs: the request
// path reads Current() on every payload, and a reloader replaces the value with Set() when an operator publishes
// a new ruleset. Reads and writes are safe from many goroutines, and a request always sees one whole ruleset,
// never a mix of two.
type Holder struct {
	v atomic.Pointer[Loaded]
}

// NewHolder returns a holder that starts with rs.
func NewHolder(rs *Ruleset, source string, now time.Time) *Holder {
	h := &Holder{}
	h.Set(rs, source, now)
	return h
}

// Set makes rs the ruleset in force.
func (h *Holder) Set(rs *Ruleset, source string, now time.Time) {
	h.v.Store(&Loaded{Ruleset: rs, Source: source, LoadedAt: now})
}

// Current returns the ruleset in force. Call it once per payload and use that value throughout, so one payload
// is never processed by two rulesets.
func (h *Holder) Current() *Ruleset { return h.v.Load().Ruleset }

// Loaded returns the ruleset in force with its source and load time.
func (h *Holder) Loaded() Loaded { return *h.v.Load() }

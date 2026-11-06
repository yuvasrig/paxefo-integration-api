// Package alerts decides, from the live figures, which operational problems need attention, and (optionally) tells
// someone. The rules are a pure function of the figures (Evaluate), so each one is easy to read and to test; the
// Notifier only delivers the result: once when an alert starts, once when it clears.
package alerts

import (
	"fmt"
	"sort"
	"time"
)

// Severities.
const (
	Warning  = "warning"
	Critical = "critical"
)

// Alert is one problem. Key identifies it ("dead:acme-pharma") so a problem is reported once, not on every check.
type Alert struct {
	Key      string `json:"key"`
	Severity string `json:"severity"`
	Tenant   string `json:"tenant,omitempty"`
	Title    string `json:"title"`
	Detail   string `json:"detail"`
}

// Tenant is one tenant's live figures.
type Tenant struct {
	ID          string
	Async       bool
	Pending     int
	OldestWait  time.Duration
	Dead        int
	Workers     int
	Rejected15m int // outcomes rejected in the last 15 minutes
	Total15m    int // outcomes of any status in the last 15 minutes

	// The tenant's own thresholds (its settings on top of the global ones).
	LagSLO        time.Duration // a job waiting longer than this is behind
	QueueLimit    int           // jobs it may have waiting before ingest answers 503
	RejectPercent int           // alert when this share of recent events is rejected (0 turns that rule off)
}

// Input is everything the rules look at.
type Input struct {
	Tenants []Tenant
}

// Thresholds that are not settings.
const (
	queueWarnPercent   = 80 // a queue this full is about to refuse payloads
	minEventsForReject = 10 // too few events make a rejection rate meaningless
	criticalLagFactor  = 3  // behind by this many lag targets is critical
)

// Evaluate returns the alerts for the figures, most severe first.
func Evaluate(in Input) []Alert {
	var out []Alert
	add := func(a Alert) { out = append(out, a) }
	for _, t := range in.Tenants {
		if t.Dead > 0 {
			add(Alert{Key: "dead:" + t.ID, Severity: Warning, Tenant: t.ID, Title: "Dead events",
				Detail: fmt.Sprintf("%s has %d events that failed every attempt. Fix the cause, then replay them.", t.ID, t.Dead)})
		}
		if t.Pending > 0 && t.Async && t.Workers == 0 {
			add(Alert{Key: "no-workers:" + t.ID, Severity: Critical, Tenant: t.ID, Title: "No workers",
				Detail: fmt.Sprintf("%s has %d jobs waiting and no worker is running for it.", t.ID, t.Pending)})
		}
		if t.Pending > 0 && t.LagSLO > 0 && t.OldestWait > t.LagSLO {
			sev := Warning
			if t.OldestWait > criticalLagFactor*t.LagSLO {
				sev = Critical
			}
			add(Alert{Key: "lag:" + t.ID, Severity: sev, Tenant: t.ID, Title: "Workers are behind",
				Detail: fmt.Sprintf("%s: the oldest job has waited %s (target %s), %d waiting.", t.ID, t.OldestWait.Round(time.Second), t.LagSLO, t.Pending)})
		}
		if t.QueueLimit > 0 && t.Pending*100 >= t.QueueLimit*queueWarnPercent {
			sev := Warning
			if t.Pending >= t.QueueLimit {
				sev = Critical
			}
			add(Alert{Key: "queue:" + t.ID, Severity: sev, Tenant: t.ID, Title: "Queue nearly full",
				Detail: fmt.Sprintf("%s has %d of %d jobs allowed waiting; at the limit new payloads are refused with 503.", t.ID, t.Pending, t.QueueLimit)})
		}
		if t.RejectPercent > 0 && t.Total15m >= minEventsForReject && t.Rejected15m*100 >= t.Total15m*t.RejectPercent {
			add(Alert{Key: "rejects:" + t.ID, Severity: Warning, Tenant: t.ID, Title: "High reject rate",
				Detail: fmt.Sprintf("%s: %d of %d events in the last 15 minutes were rejected (alert at %d%%). Check the sender or the rules.", t.ID, t.Rejected15m, t.Total15m, t.RejectPercent)})
		}
	}
	rank := map[string]int{Critical: 0, Warning: 1}
	sort.SliceStable(out, func(i, j int) bool {
		if rank[out[i].Severity] != rank[out[j].Severity] {
			return rank[out[i].Severity] < rank[out[j].Severity]
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Package worker runs the async ingest workers: processes that claim queued payloads and run them through the same
// pipeline a synchronous request uses. This file holds the pure decisions (how many workers, how long to wait before
// a retry, whether the workers are keeping up); pool.go runs them.
package worker

import (
	"fmt"
	"math"
	"time"
)

// SizeConfig is what the pool size depends on.
type SizeConfig struct {
	Min, Max      int
	JobsPerWorker int
	LagSLO        time.Duration
}

// Desired is how many workers an instance should run for a backlog of depth jobs whose oldest has waited oldestAge:
// enough that each handles about JobsPerWorker, one more if the oldest job is already behind the target, never fewer
// than Min nor more than Max. It is a pure function, so the sizing rule is easy to test and to change.
func Desired(depth int, oldestAge time.Duration, c SizeConfig) int {
	per := c.JobsPerWorker
	if per < 1 {
		per = 1
	}
	n := int(math.Ceil(float64(depth) / float64(per)))
	if depth > 0 && oldestAge > c.LagSLO {
		n++
	}
	if n < c.Min {
		n = c.Min
	}
	if n > c.Max {
		n = c.Max
	}
	return n
}

// keepingUpRatio: completions at least this fraction of arrivals, with nothing over the lag target, counts as keeping up.
const (
	keepingUpRatio  = 0.9
	inFlightSeconds = 2.0 // a queue this short at the current rate is work in flight, not a backlog
)

const (
	backoffBase = time.Second
	backoffCap  = 5 * time.Minute
)

// Backoff is how long a job waits before attempt number attempt+1: 2s, 4s, 8s... up to five minutes.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 20 {
		return backoffCap
	}
	d := backoffBase << attempt
	if d > backoffCap {
		return backoffCap
	}
	return d
}

// Prediction states.
const (
	StateUpToDate      = "up_to_date"     // nothing is waiting
	StateCatchingUp    = "catching_up"    // a backlog exists and is shrinking
	StateFallingBehind = "falling_behind" // jobs arrive at least as fast as they finish
	StateStalled       = "stalled"        // jobs wait and nothing is finishing
)

// PredictInput is what the prediction is made from.
type PredictInput struct {
	Pending, Processing int
	ArrivalsPerMin      float64 // payloads received in the last minute
	CompletedPerMin     float64 // outcomes written in the last minute
	ActiveWorkers       int
	OldestAge, LagSLO   time.Duration
}

// Prediction says whether the workers are up to date and, if not, how long the current load will take to clear.
type Prediction struct {
	State  string
	ETA    time.Duration // valid when HasETA
	HasETA bool
	Behind bool // the oldest job has waited longer than the lag target
	Text   string
}

// Predict estimates, from the last minute's rates, whether the workers are keeping up and when the backlog clears. It is
// an estimate from recent rates, not a promise: a burst or a slow tenant changes it.
func Predict(in PredictInput) Prediction {
	backlog := in.Pending + in.Processing
	p := Prediction{Behind: in.Pending > 0 && in.OldestAge > in.LagSLO}
	switch {
	case backlog == 0:
		p.State, p.Text = StateUpToDate, "Workers are up to date: nothing is waiting."
	case in.ActiveWorkers == 0:
		p.State, p.Text = StateStalled, fmt.Sprintf("No workers are running, and %d jobs are waiting.", backlog)
	case in.CompletedPerMin <= 0:
		p.State, p.Text = StateStalled, fmt.Sprintf("%d jobs are waiting and none finished in the last minute.", backlog)
	case !p.Behind && in.CompletedPerMin >= keepingUpRatio*in.ArrivalsPerMin && float64(backlog) <= in.CompletedPerMin*inFlightSeconds/60:
		// The whole queue would clear in a couple of seconds and the oldest has waited less than the lag target: that is
		// normal work in progress, not a backlog, even when the last minute's two counts differ by a job or two.
		p.State = StateUpToDate
		p.Text = fmt.Sprintf("Workers are keeping up: %d in the queue, the oldest has waited %s.", backlog, in.OldestAge.Round(time.Millisecond))
	case in.CompletedPerMin <= in.ArrivalsPerMin:
		p.State = StateFallingBehind
		p.Text = fmt.Sprintf("Jobs arrive at %.0f/min but finish at %.0f/min, so the backlog of %d is not shrinking. More workers are needed.",
			in.ArrivalsPerMin, in.CompletedPerMin, backlog)
	default:
		net := in.CompletedPerMin - in.ArrivalsPerMin // jobs per minute the backlog shrinks by
		p.State, p.HasETA = StateCatchingUp, true
		p.ETA = time.Duration(float64(backlog) / net * float64(time.Minute))
		p.Text = fmt.Sprintf("%d jobs are waiting; at the current rates the backlog clears in about %s.", backlog, FormatETA(p.ETA))
	}
	if p.Behind && p.State != StateUpToDate {
		p.Text += " The oldest job has waited longer than the lag target."
	}
	return p
}

// FormatETA renders a duration for people: "45s", "3m 20s", "2h 5m".
func FormatETA(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

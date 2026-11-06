package worker

import (
	"strings"
	"testing"
	"time"
)

func TestDesired(t *testing.T) {
	c := SizeConfig{Min: 2, Max: 8, JobsPerWorker: 50, LagSLO: 10 * time.Second}
	tests := []struct {
		name  string
		depth int
		age   time.Duration
		want  int
	}{
		{"idle stays at the minimum", 0, 0, 2},
		{"small backlog stays at the minimum", 80, time.Second, 2},
		{"one worker per 50 jobs", 250, time.Second, 5},
		{"rounds up", 251, time.Second, 6},
		{"capped at the maximum", 100000, time.Second, 8},
		{"behind the lag target adds one", 250, 30 * time.Second, 6},
		{"lag with an empty queue adds nothing", 0, time.Hour, 2},
		{"lag cannot exceed the maximum", 400, time.Minute, 8},
	}
	for _, tt := range tests {
		if got := Desired(tt.depth, tt.age, c); got != tt.want {
			t.Errorf("%s: Desired(%d, %v) = %d, want %d", tt.name, tt.depth, tt.age, got, tt.want)
		}
	}
	if got := Desired(10, 0, SizeConfig{Min: 0, Max: 4, JobsPerWorker: 0}); got != 4 {
		t.Errorf("a zero JobsPerWorker is treated as 1: %d", got)
	}
	if got := Desired(0, 0, SizeConfig{Min: 0, Max: 4, JobsPerWorker: 10}); got != 0 {
		t.Errorf("an idle pool may scale to zero when Min is 0: %d", got)
	}
}

func TestBackoff(t *testing.T) {
	want := map[int]time.Duration{0: 2 * time.Second, 1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 6: 64 * time.Second, 9: 5 * time.Minute, 50: 5 * time.Minute}
	for attempt, d := range want {
		if got := Backoff(attempt); got != d {
			t.Errorf("Backoff(%d) = %v, want %v", attempt, got, d)
		}
	}
	prev := time.Duration(0)
	for a := 1; a < 30; a++ {
		if Backoff(a) < prev {
			t.Fatalf("backoff must never shrink (attempt %d)", a)
		}
		prev = Backoff(a)
	}
}

func TestPredict(t *testing.T) {
	slo := 10 * time.Second
	tests := []struct {
		name    string
		in      PredictInput
		state   string
		hasETA  bool
		eta     time.Duration
		behind  bool
		textHas string
	}{
		{"a job or two in flight while keeping up is up to date", PredictInput{Pending: 1, ArrivalsPerMin: 250, CompletedPerMin: 249, ActiveWorkers: 2, OldestAge: 20 * time.Millisecond, LagSLO: slo}, StateUpToDate, false, 0, false, "keeping up"},
		{"nothing waiting", PredictInput{ActiveWorkers: 2, LagSLO: slo}, StateUpToDate, false, 0, false, "up to date"},
		{"nothing waiting and no workers is still up to date", PredictInput{LagSLO: slo}, StateUpToDate, false, 0, false, "up to date"},
		{"no workers", PredictInput{Pending: 40, LagSLO: slo}, StateStalled, false, 0, false, "No workers are running"},
		{"workers but nothing finishing", PredictInput{Pending: 40, ActiveWorkers: 2, LagSLO: slo}, StateStalled, false, 0, false, "none finished"},
		{"catching up: 600 waiting, 300/min net", PredictInput{Pending: 580, Processing: 20, ArrivalsPerMin: 100, CompletedPerMin: 400, ActiveWorkers: 4, LagSLO: slo}, StateCatchingUp, true, 2 * time.Minute, false, "about 2m 0s"},
		{"falling behind: arrivals exceed completions", PredictInput{Pending: 500, ArrivalsPerMin: 300, CompletedPerMin: 200, ActiveWorkers: 4, LagSLO: slo}, StateFallingBehind, false, 0, false, "not shrinking"},
		{"equal rates are not catching up", PredictInput{Pending: 10, ArrivalsPerMin: 60, CompletedPerMin: 60, ActiveWorkers: 1, LagSLO: slo}, StateFallingBehind, false, 0, false, "not shrinking"},
		{"behind the lag target", PredictInput{Pending: 100, ArrivalsPerMin: 0, CompletedPerMin: 50, ActiveWorkers: 1, OldestAge: time.Minute, LagSLO: slo}, StateCatchingUp, true, 2 * time.Minute, true, "longer than the lag target"},
		{"only processing counts as backlog", PredictInput{Processing: 5, ArrivalsPerMin: 0, CompletedPerMin: 5, ActiveWorkers: 1, LagSLO: slo}, StateCatchingUp, true, time.Minute, false, "5 jobs"},
	}
	for _, tt := range tests {
		p := Predict(tt.in)
		if p.State != tt.state || p.HasETA != tt.hasETA || p.ETA != tt.eta || p.Behind != tt.behind || !strings.Contains(p.Text, tt.textHas) {
			t.Errorf("%s: %+v, want state %s eta %v behind %v text containing %q", tt.name, p, tt.state, tt.eta, tt.behind, tt.textHas)
		}
	}
}

func TestFormatETA(t *testing.T) {
	for d, want := range map[time.Duration]string{45 * time.Second: "45s", 200 * time.Second: "3m 20s", 125 * time.Minute: "2h 5m", 0: "0s", 59*time.Second + 600*time.Millisecond: "1m 0s"} {
		if got := FormatETA(d); got != want {
			t.Errorf("FormatETA(%v) = %q, want %q", d, got, want)
		}
	}
}

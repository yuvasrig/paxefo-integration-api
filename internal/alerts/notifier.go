package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Notifier checks the alerts on an interval and posts a JSON message to a webhook (a Slack incoming webhook works)
// when an alert starts and when it clears, never on every check. It keeps its state in memory: after a restart a
// still-firing alert is announced once more, which is the safe direction to be wrong in.
type Notifier struct {
	Source   func(ctx context.Context) ([]Alert, error)
	URL      string // ALERT_WEBHOOK_URL; empty disables the notifier
	Interval time.Duration
	HTTP     *http.Client
	Log      *slog.Logger

	firing map[string]Alert
}

// Run checks until ctx ends.
func (n *Notifier) Run(ctx context.Context) {
	if n.URL == "" {
		return
	}
	if n.Interval <= 0 {
		n.Interval = 30 * time.Second
	}
	t := time.NewTicker(n.Interval)
	defer t.Stop()
	for {
		n.Check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Check evaluates once and sends what changed since the last successful check. The new state is remembered only when
// every message was delivered, so a webhook outage is retried at the next check instead of silently losing the alert.
func (n *Notifier) Check(ctx context.Context) {
	current, err := n.Source(ctx)
	if err != nil {
		if ctx.Err() == nil {
			n.log().Error("could not evaluate alerts", "error", err)
		}
		return
	}
	started, cleared, state := n.diff(current)
	if len(started) > 0 && !n.send(ctx, "firing", started) {
		return
	}
	if len(cleared) > 0 && !n.send(ctx, "resolved", cleared) {
		return
	}
	n.firing = state
}

// diff returns the alerts that began and the ones that ended since the last remembered state, and the new state.
func (n *Notifier) diff(current []Alert) (started, cleared []Alert, state map[string]Alert) {
	state = map[string]Alert{}
	for _, a := range current {
		state[a.Key] = a
		if _, was := n.firing[a.Key]; !was {
			started = append(started, a)
		}
	}
	for k, a := range n.firing {
		if _, still := state[k]; !still {
			cleared = append(cleared, a)
		}
	}
	sort.Slice(cleared, func(i, j int) bool { return cleared[i].Key < cleared[j].Key })
	return started, cleared, state
}

func (n *Notifier) send(ctx context.Context, status string, alerts []Alert) bool {
	var lines []string
	for _, a := range alerts {
		lines = append(lines, fmt.Sprintf("[%s] %s: %s", a.Severity, a.Title, a.Detail))
	}
	body, _ := json.Marshal(map[string]any{
		"status": status,
		"text":   "PAXAFE Integration API alerts (" + status + ")\n" + strings.Join(lines, "\n"), // Slack-compatible
		"alerts": alerts,
	})
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URL, bytes.NewReader(body))
	if err != nil {
		n.log().Error("bad alert webhook URL", "error", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	client := n.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		n.log().Error("could not send alert", "error", err)
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode >= 300 {
		n.log().Error("alert webhook refused the message", "status", resp.StatusCode)
		return false
	}
	n.log().Info("alert notification sent", "status", status, "alerts", len(alerts))
	return true
}

func (n *Notifier) log() *slog.Logger {
	if n.Log != nil {
		return n.Log
	}
	return slog.Default()
}

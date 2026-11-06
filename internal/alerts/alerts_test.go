package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func keys(as []Alert) string {
	var k []string
	for _, a := range as {
		k = append(k, a.Severity[:1]+":"+a.Key)
	}
	return strings.Join(k, " ")
}

func TestEvaluate(t *testing.T) {
	in := Input{LagSLO: 10 * time.Second, QueueLimit: 100, RejectPercent: 25}
	cases := []struct {
		name string
		t    Tenant
		want string
	}{
		{"healthy", Tenant{ID: "a", Async: true, Pending: 3, OldestWait: time.Second, Workers: 2, Total15m: 100, Rejected15m: 2}, ""},
		{"dead events", Tenant{ID: "a", Dead: 2}, "w:dead:a"},
		{"jobs waiting and no workers", Tenant{ID: "a", Async: true, Pending: 5, Workers: 0}, "c:no-workers:a"},
		{"a sync tenant needs no workers", Tenant{ID: "a", Async: false, Pending: 5, Workers: 0}, ""},
		{"behind the lag target", Tenant{ID: "a", Async: true, Pending: 5, Workers: 1, OldestWait: 15 * time.Second}, "w:lag:a"},
		{"far behind is critical", Tenant{ID: "a", Async: true, Pending: 5, Workers: 1, OldestWait: 40 * time.Second}, "c:lag:a"},
		{"nothing waiting is never behind", Tenant{ID: "a", Async: true, Pending: 0, OldestWait: time.Minute, Workers: 1}, ""},
		{"queue nearly full", Tenant{ID: "a", Async: true, Pending: 80, Workers: 1}, "w:queue:a"},
		{"queue at the limit", Tenant{ID: "a", Async: true, Pending: 100, Workers: 1}, "c:queue:a"},
		{"just under the queue warning", Tenant{ID: "a", Async: true, Pending: 79, Workers: 1}, ""},
		{"high reject rate", Tenant{ID: "a", Total15m: 40, Rejected15m: 10}, "w:rejects:a"},
		{"too few events to judge", Tenant{ID: "a", Total15m: 4, Rejected15m: 4}, ""},
	}
	for _, c := range cases {
		in.Tenants = []Tenant{c.t}
		if got := keys(Evaluate(in)); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
	in.RejectPercent = 0
	in.Tenants = []Tenant{{ID: "a", Total15m: 40, Rejected15m: 40}}
	if got := Evaluate(in); len(got) != 0 {
		t.Errorf("a reject percent of 0 turns the rule off: %v", got)
	}
}

func TestEvaluateOrdersMostSevereFirst(t *testing.T) {
	got := Evaluate(Input{LagSLO: time.Second, QueueLimit: 10, Tenants: []Tenant{
		{ID: "b", Dead: 1},
		{ID: "a", Async: true, Pending: 2, Workers: 0},
	}})
	if keys(got) != "c:no-workers:a w:dead:b" {
		t.Fatalf("%s", keys(got))
	}
}

type hook struct {
	mu   sync.Mutex
	msgs []map[string]any
	code int
	srv  *httptest.Server
}

func newHook(t *testing.T) *hook {
	h := &hook{code: 200}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.code >= 300 {
			w.WriteHeader(h.code)
			return
		}
		h.msgs = append(h.msgs, m)
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *hook) statuses() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var s []string
	for _, m := range h.msgs {
		s = append(s, m["status"].(string))
	}
	return s
}

func TestNotifierReportsStartsAndClearsOnce(t *testing.T) {
	h := newHook(t)
	cur := []Alert{{Key: "dead:a", Severity: Warning, Title: "Dead events", Detail: "a has 2"}}
	n := &Notifier{URL: h.srv.URL, Source: func(context.Context) ([]Alert, error) { return cur, nil }}
	n.Check(context.Background())
	n.Check(context.Background()) // still firing: not announced again
	n.Check(context.Background())
	if got := h.statuses(); len(got) != 1 || got[0] != "firing" {
		t.Fatalf("an alert is announced once: %v", got)
	}
	text := h.msgs[0]["text"].(string)
	if !strings.Contains(text, "Dead events") || !strings.Contains(text, "a has 2") {
		t.Fatalf("slack-style text: %q", text)
	}
	cur = nil
	n.Check(context.Background())
	n.Check(context.Background())
	if got := h.statuses(); len(got) != 2 || got[1] != "resolved" {
		t.Fatalf("a clear is announced once: %v", got)
	}
	cur = []Alert{{Key: "dead:a"}}
	n.Check(context.Background())
	if got := h.statuses(); len(got) != 3 || got[2] != "firing" {
		t.Fatalf("a recurrence is announced again: %v", got)
	}
}

func TestNotifierRetriesAfterAWebhookFailure(t *testing.T) {
	h := newHook(t)
	h.code = 500
	n := &Notifier{URL: h.srv.URL, Source: func(context.Context) ([]Alert, error) {
		return []Alert{{Key: "k", Severity: Critical, Title: "T"}}, nil
	}}
	n.Check(context.Background())
	h.mu.Lock()
	h.code = 200
	h.mu.Unlock()
	n.Check(context.Background())
	if got := h.statuses(); len(got) != 1 || got[0] != "firing" {
		t.Fatalf("an alert that could not be delivered is sent at the next check: %v", got)
	}
}

func TestNotifierSurvivesEvaluationErrorsAndNoURL(t *testing.T) {
	h := newHook(t)
	n := &Notifier{URL: h.srv.URL, Source: func(context.Context) ([]Alert, error) { return nil, errors.New("db down") }}
	n.Check(context.Background())
	if len(h.statuses()) != 0 {
		t.Fatal("an error is not an alert")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { (&Notifier{}).Run(ctx); close(done) }() // no URL: returns at once
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a notifier with no URL must not run")
	}
}

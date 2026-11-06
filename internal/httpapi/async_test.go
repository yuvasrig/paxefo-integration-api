package httpapi_test

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

func asyncEnv(t *testing.T, maxDepth int) *env {
	e := newEnv(t)
	must(t, e.st.SetTenantIngestMode(ctx, "reviewer", "async"))
	e.ing.DefaultMode = ingest.ModeSync
	e.ing.RetryAfter = 7 * time.Second
	e.ing.Settings = settings.NewHolder(settings.Values{QueueMaxDepth: maxDepth, MaxAttempts: 3})
	return e
}

// An async tenant gets 202 as soon as the raw payload and its job are committed; a sync tenant is unchanged.
func TestAsyncWebhookAcknowledgesWithoutProcessing(t *testing.T) {
	e := asyncEnv(t, 100)
	code, m := e.post("reviewer", sample(t, 2))
	if code != 202 || m["status"] != "queued" || m["event_id"] == "" {
		t.Fatalf("%d %v", code, m)
	}
	id := m["event_id"].(string)
	if ok, _ := e.st.HasJob(ctx, id); !ok {
		t.Fatal("a job must be queued")
	}
	code, _, b := e.do("GET", "/api/v1/events/"+id, e.keys["reviewer"], nil)
	if code != 200 {
		t.Fatalf("%d %s", code, b)
	}
	// Nothing is normalized until a worker runs.
	if n, _ := e.st.PendingDepth(ctx, "reviewer", 10); n != 1 {
		t.Fatalf("depth %d", n)
	}
	if code, m := e.post("acme-pharma", sample(t, 10)); code != 200 || m["status"] == "queued" {
		t.Fatalf("sync tenant must be processed inline: %d %v", code, m)
	}

	// A worker finishing the job produces the same outcome sync mode would have.
	jobs, _ := e.st.ClaimJobs(ctx, "w", "reviewer", 10)
	if len(jobs) != 1 {
		t.Fatalf("%d", len(jobs))
	}
	must(t, e.ing.ProcessJob(ctx, jobs[0]))
	ev, _ := e.st.GetEvent(ctx, "reviewer", id)
	if ev.Status != "accepted" {
		t.Fatalf("%s", ev.Status)
	}
	if ok, _ := e.st.HasJob(ctx, id); ok {
		t.Fatal("job removed with the outcome")
	}
}

// F-async backpressure: a full queue answers 503 + Retry-After and, importantly, stores nothing it did not acknowledge.
func TestAsyncBackpressure(t *testing.T) {
	e := asyncEnv(t, 2)
	for i := 0; i < 2; i++ {
		if code, _ := e.post("reviewer", sample(t, 2)); code != 202 {
			t.Fatalf("%d", code)
		}
	}
	code, hdr, b := e.do("POST", "/api/webhook/tive", e.keys["reviewer"], sample(t, 2))
	if code != 503 || hdr.Get("Retry-After") != "7" {
		t.Fatalf("%d %q %s", code, hdr.Get("Retry-After"), b)
	}
	if !strings.Contains(string(b), "QUEUE_FULL") {
		t.Fatalf("%s", b)
	}
	var raws int
	_ = e.st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error { return tx.QueryRow(ctx, `SELECT count(*) FROM raw_events`).Scan(&raws) })
	if raws != 2 {
		t.Fatalf("a refused payload must not be stored: %d", raws)
	}
	// Another tenant is not affected by this tenant's backlog.
	must(t, e.st.SetTenantIngestMode(ctx, "acme-pharma", "async"))
	if code, _ := e.post("acme-pharma", sample(t, 10)); code != 202 {
		t.Fatalf("%d", code)
	}
}

// Replay must not race a queued job: it refuses events still waiting, and recovers dead ones.
func TestReplayGuardsForAsyncEvents(t *testing.T) {
	e := asyncEnv(t, 100)
	_, m := e.post("reviewer", sample(t, 2))
	id := m["event_id"].(string)
	res, err := e.ing.ReplayWindow(ctx, ingest.ReplayOptions{Filter: store.ReplayFilter{TenantID: "reviewer", From: time.Unix(0, 0), To: time.Now().Add(time.Hour)}, Apply: true})
	must(t, err)
	if res.Reprocessed != 0 || res.Queued != 1 {
		t.Fatalf("a queued event must be left to its worker: %+v", res)
	}
	jobs, _ := e.st.ClaimJobs(ctx, "w", "reviewer", 10)
	must(t, e.st.BuryJob(ctx, jobs[0].TenantID, id, "gave up"))
	res, err = e.ing.ReplayWindow(ctx, ingest.ReplayOptions{Filter: store.ReplayFilter{TenantID: "reviewer", From: time.Unix(0, 0), To: time.Now().Add(time.Hour)}, Apply: true})
	must(t, err)
	if res.Reprocessed != 1 {
		t.Fatalf("dead events are recovered by replay: %+v", res)
	}
	if ev, _ := e.st.GetEvent(ctx, "reviewer", id); ev.Status != "accepted" {
		t.Fatalf("%s", ev.Status)
	}
}

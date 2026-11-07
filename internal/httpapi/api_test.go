package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/observability"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/rulesync"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
)

var ctx = context.Background()

// failingStore fails the next N Commit calls, to exercise "raw first, then fail" (F7-R1).
type failingStore struct {
	*store.PGStore
	failCommits atomic.Int32
}

func (f *failingStore) Commit(c context.Context, in store.CommitInput) (store.CommitResult, error) {
	if f.failCommits.Add(-1) >= 0 {
		return store.CommitResult{}, errors.New("injected commit failure")
	}
	return f.PGStore.Commit(c, in)
}

type env struct {
	t       *testing.T
	st      *failingStore
	srv     *httptest.Server
	keys    map[string]string // tenant -> plaintext key
	now     time.Time
	logs    *syncBuffer
	holder  *rules.Holder
	metrics *observability.Metrics
	ing     *ingest.Service
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pg, err := store.Open(ctx, pgtest.NewDSN(t), store.Options{MaxConns: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, st: &failingStore{PGStore: pg}, keys: map[string]string{}, now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC), logs: &syncBuffer{}}
	for _, id := range []string{"reviewer", "acme-pharma"} {
		must(t, pg.CreateTenant(ctx, id, id, nil))
		k, _ := auth.NewKey()
		must(t, pg.CreateAPIKey(ctx, id, "k-"+id, k.Prefix, k.Hash, nil))
		e.keys[id] = k.Plaintext
	}

	rs, err := rules.Default()
	must(t, err)
	e.holder = rules.NewHolder(rs, rules.SourceDefault, time.Now())
	// The reloader polls fast so tests can change rules "on the fly" without waiting.
	rctx, rcancel := context.WithCancel(ctx)
	t.Cleanup(rcancel)
	go (&rulesync.Reloader{Source: pg, Holder: e.holder, Base: rs, BaseSource: rules.SourceDefault, Interval: 20 * time.Millisecond,
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil))}).Run(rctx)
	log := slog.New(slog.NewJSONHandler(e.logs, nil))
	metrics := observability.NewMetrics(pg.Pool().Stat)
	e.metrics = metrics
	e.ing = &ingest.Service{Store: e.st, Provider: tive.NewDynamic(e.holder, config.Default()), Now: func() time.Time { return e.now }, Log: log, Observer: metrics}
	api := &httpapi.API{
		Ingest:   e.ing,
		Rules:    e.holder,
		Metrics:  metrics.Handler(),
		Observer: metrics,
		Ready: func(c context.Context) (httpapi.ReadyInfo, error) {
			if err := pg.Ping(c); err != nil {
				return httpapi.ReadyInfo{}, err
			}
			v, err := pg.MigrationVersion(c)
			return httpapi.ReadyInfo{MigrationVersion: v, StorageBackend: "postgres", IngestMode: "sync"}, err
		},
		Authenticator: &auth.Authenticator{Keys: pg, Now: func() time.Time { return time.Now() }},
		BodyLimit:     256 << 10,
		Log:           log,
	}
	e.srv = httptest.NewServer(api.Handler())
	t.Cleanup(e.srv.Close)
	return e
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (e *env) do(method, path, key string, body []byte, hdr ...string) (int, http.Header, []byte) {
	e.t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, bytes.NewReader(body))
	must(e.t, err)
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	must(e.t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func (e *env) post(tenant string, body []byte) (int, map[string]any) {
	e.t.Helper()
	code, _, b := e.do("POST", "/api/webhook/tive", e.keys[tenant], body)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return code, m
}

func sample(t *testing.T, n int) []byte {
	b, err := os.ReadFile(fmt.Sprintf("../../testdata/samples/%02d.json", n))
	must(t, err)
	return b
}

// F1..F5, F7: all 13 provided samples through the real endpoint must match the hand-reviewed expected files.
func TestSamplesEndToEnd(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 13; i++ {
		var want struct {
			HTTPStatus int              `json:"http_status"`
			Status     string           `json:"status"`
			Owner      string           `json:"owning_tenant"`
			Flags      []map[string]any `json:"flags"`
			Errors     []map[string]any `json:"errors"`
			Sensor     map[string]any   `json:"sensor"`
			Location   map[string]any   `json:"location"`
		}
		b, err := os.ReadFile(fmt.Sprintf("../../specs/001-integration-api/expected/%02d.json", i))
		must(t, err)
		must(t, json.Unmarshal(b, &want))

		code, got := e.post("reviewer", sample(t, i))
		name := fmt.Sprintf("sample %02d", i)
		if code != want.HTTPStatus {
			t.Errorf("%s: HTTP %d, want %d (%v)", name, code, want.HTTPStatus, got)
			continue
		}
		if got["status"] != want.Status {
			t.Errorf("%s: status %v, want %s", name, got["status"], want.Status)
		}
		if g, w := canon(got["flags"]), canon(want.Flags); !reflect.DeepEqual(g, w) {
			t.Errorf("%s: flags\n got: %v\nwant: %v", name, g, w)
		}
		if g, w := errKeys(got["errors"]), errKeys(want.Errors); !reflect.DeepEqual(g, w) {
			t.Errorf("%s: errors got %v want %v", name, g, w)
		}
		if !reflect.DeepEqual(got["sensor"], anyOrNil(want.Sensor)) {
			t.Errorf("%s: sensor\n got: %v\nwant: %v", name, got["sensor"], want.Sensor)
		}
		if !reflect.DeepEqual(got["location"], anyOrNil(want.Location)) {
			t.Errorf("%s: location\n got: %v\nwant: %v", name, got["location"], want.Location)
		}
		if got["event_id"] == "" || got["request_id"] == "" {
			t.Errorf("%s: ids missing: %v", name, got)
		}

		// Owner tenant: only #9 leaves the reviewer tenant.
		ev := e.getEvent("reviewer", got["event_id"].(string))
		if ev["status"] != want.Status {
			t.Errorf("%s: stored status %v, want %s", name, ev["status"], want.Status)
		}
		raw, _ := json.Marshal(ev["raw_payload"])
		var sent, stored any
		_ = json.Unmarshal(sample(t, i), &sent)
		_ = json.Unmarshal(raw, &stored)
		if !reflect.DeepEqual(sent, stored) {
			t.Errorf("%s: stored raw payload differs from what was sent", name)
		}
	}

	// Readings: the key's tenant sees every device it sent, including sample 9 (AccountId 999 is only metadata).
	code, _, body := e.do("GET", "/api/v1/readings?device_id=B234567", e.keys["reviewer"], nil)
	var page struct {
		Items []struct{ Timestamp int64 }
	}
	_ = json.Unmarshal(body, &page)
	if code != 200 || len(page.Items) != 3 || page.Items[0].Timestamp != 1739647646000 {
		t.Errorf("B234567 readings: %d %s", code, body)
	}
	_, _, body = e.do("GET", "/api/v1/readings?device_id=F678901", e.keys["reviewer"], nil)
	if !strings.Contains(string(body), `"timestamp":1739734046000`) {
		t.Errorf("F678901 (sample 9) must be readable by the tenant that sent it: %s", body)
	}
	_, _, body = e.do("GET", "/api/v1/readings?device_id=F678901", e.keys["acme-pharma"], nil)
	if !strings.Contains(string(body), `"items":[]`) {
		t.Errorf("another tenant must not see it: %s", body)
	}
}

func (e *env) getEvent(tenant, id string) map[string]any {
	e.t.Helper()
	code, _, b := e.do("GET", "/api/v1/events/"+id, e.keys[tenant], nil)
	if code != 200 {
		e.t.Fatalf("GET event %s: %d %s", id, code, b)
	}
	var m map[string]any
	must(e.t, json.Unmarshal(b, &m))
	return m
}

func (e *env) countRaw(tenant string) int {
	var n int
	must(e.t, e.st.WithTenantTx(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM raw_events WHERE tenant_id = $1`, tenant).Scan(&n)
	}))
	return n
}

func anyOrNil(m map[string]any) any {
	if m == nil {
		return nil
	}
	return m
}

func canon(v any) []string {
	b, _ := json.Marshal(v)
	var list []map[string]any
	_ = json.Unmarshal(b, &list)
	out := make([]string, 0, len(list))
	for _, f := range list {
		c, _ := json.Marshal(f)
		out = append(out, string(c))
	}
	sort.Strings(out)
	return out
}

func errKeys(v any) []string {
	b, _ := json.Marshal(v)
	var list []map[string]any
	_ = json.Unmarshal(b, &list)
	out := []string{}
	for _, f := range list {
		out = append(out, fmt.Sprintf("%v@%v", f["code"], f["field"]))
	}
	sort.Strings(out)
	return out
}

// F6-R1: 401 for every bad key, and nothing is stored.
func TestUnauthorized_F6R1(t *testing.T) {
	e := newEnv(t)
	expired, _ := auth.NewKey()
	past := time.Now().Add(-time.Hour)
	must(t, e.st.CreateAPIKey(ctx, "reviewer", "expired", expired.Prefix, expired.Hash, &past))
	revoked, _ := auth.NewKey()
	must(t, e.st.CreateAPIKey(ctx, "reviewer", "revoked", revoked.Prefix, revoked.Hash, nil))
	k, _ := e.st.LookupAPIKey(ctx, revoked.Hash)
	must(t, e.st.RevokeAPIKey(ctx, k.ID))
	unknown, _ := auth.NewKey()

	for name, key := range map[string]string{"missing": "", "garbage": "nope", "unknown": unknown.Plaintext, "expired": expired.Plaintext, "revoked": revoked.Plaintext} {
		code, _, body := e.do("POST", "/api/v1/webhook/tive", key, sample(t, 2))
		if code != 401 || !strings.Contains(string(body), "UNAUTHORIZED") {
			t.Errorf("%s: %d %s", name, code, body)
		}
	}
	for _, path := range []string{"/api/v1/events/evt_x", "/api/v1/readings?device_id=x"} {
		if code, _, _ := e.do("GET", path, "", nil); code != 401 {
			t.Errorf("GET %s without key: %d", path, code)
		}
	}
	if n := e.countRaw("reviewer"); n != 0 {
		t.Fatalf("unauthenticated requests stored %d events", n)
	}
}

// F6-R3, F3-R1: oversized and non-JSON bodies.
func TestBodyLimitAndInvalidJSON(t *testing.T) {
	e := newEnv(t)
	big := append([]byte(`{"x":"`), bytes.Repeat([]byte("a"), 300<<10)...)
	big = append(big, '"', '}')
	if code, _, _ := e.do("POST", "/api/v1/webhook/tive", e.keys["reviewer"], big); code != 413 {
		t.Fatalf("300 KB body: %d, want 413", code)
	}
	if n := e.countRaw("reviewer"); n != 0 {
		t.Fatalf("oversized body stored %d events", n)
	}

	for name, body := range map[string][]byte{"not json": []byte(`{not json`), "empty": {}, "array": []byte(`[1,2]`)} {
		code, got := e.post("reviewer", body)
		if code != 400 || got["status"] != "rejected" || got["event_id"] == "" {
			t.Errorf("%s: %d %v", name, code, got)
		}
		ev := e.getEvent("reviewer", got["event_id"].(string))
		if ev["status"] != "rejected" {
			t.Errorf("%s: stored status %v", name, ev["status"])
		}
	}
	if n := e.countRaw("reviewer"); n != 3 {
		t.Fatalf("invalid bodies are archived: got %d raw events, want 3", n)
	}
}

// The tenant comes from the API key only. The payload's AccountId is stored as metadata and never checked,
// so any value (or none) gives the same answer and the event always lands in the key's tenant.
func TestAccountIDIsMetadataOnly(t *testing.T) {
	e := newEnv(t)
	for _, acct := range []string{`"AccountId": 478`, `"AccountId": 1001`, `"AccountId": 999`, `"AccountId": "abc"`} {
		body := bytes.Replace(sample(t, 0), []byte(`"AccountId": 478`), []byte(acct), 1)
		// Changing AccountId changes the payload, so after the first send the same reading is a conflict, never a rejection.
		code, got := e.post("reviewer", body)
		if code != 200 || (got["status"] != "flagged" && got["status"] != "conflict") {
			t.Fatalf("%s: %d %v", acct, code, got)
		}
		if strings.Contains(fmt.Sprint(got["errors"]), "TENANT") || strings.Contains(fmt.Sprint(got["flags"]), "ACCOUNT") {
			t.Fatalf("%s: AccountId must not produce errors or flags: %v", acct, got)
		}
	}
	if n := e.countRaw("acme-pharma"); n != 0 {
		t.Fatalf("nothing may land in another tenant, got %d", n)
	}
	var acct *float64
	must(t, e.st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT (extras->>'account_id')::float8 FROM sensor_readings LIMIT 1`).Scan(&acct)
	}))
	if acct == nil || *acct != 478 {
		t.Fatalf("AccountId should be kept as metadata, got %v", acct)
	}
}

// F6-R4: per-tenant rate limit with Retry-After.
// Tive does not honour 429 or Retry-After, so the service has no rate limit that could refuse its traffic: a burst far
// beyond anything a tracker fleet sends is all accepted.
func TestNoRateLimitRefusesTheWebhook_F6(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 60; i++ {
		path := "/api/webhook/tive"
		if i%2 == 1 {
			path = "/api/v1/webhook/tive"
		}
		if code, hdr, _ := e.do("POST", path, e.keys["reviewer"], sample(t, 2)); code == 429 || hdr.Get("Retry-After") != "" {
			t.Fatalf("request %d was refused for rate: %d", i, code)
		}
	}
	for i := 0; i < 30; i++ {
		if code, _, _ := e.do("GET", "/api/v1/rules", e.keys["reviewer"], nil); code != 200 {
			t.Fatalf("read %d: %d", i, code)
		}
	}
}

// F7-R1..R4: raw first; a failure after it is recorded and replay recovers idempotently.
func TestFailureAndReplay_F7(t *testing.T) {
	e := newEnv(t)
	e.st.failCommits.Store(1)
	code, got := e.post("reviewer", sample(t, 2))
	if code != 500 || got["status"] != "failed" || got["event_id"] == "" {
		t.Fatalf("%d %v", code, got)
	}
	id := got["event_id"].(string)
	ev := e.getEvent("reviewer", id)
	if ev["status"] != "failed" || ev["raw_payload"] == nil {
		t.Fatalf("raw payload must survive the failure: %v", ev)
	}

	code, _, b := e.do("POST", "/api/v1/events/"+id+"/replay", e.keys["reviewer"], nil)
	var replayed map[string]any
	_ = json.Unmarshal(b, &replayed)
	if code != 200 || replayed["status"] != "accepted" || replayed["sensor"] == nil {
		t.Fatalf("replay: %d %s", code, b)
	}
	if code, _, _ := e.do("POST", "/api/v1/events/"+id+"/replay", e.keys["reviewer"], nil); code != 409 {
		t.Fatalf("replaying a processed event: %d, want 409", code)
	}
	if code, _, _ := e.do("POST", "/api/v1/events/evt_missing/replay", e.keys["reviewer"], nil); code != 404 {
		t.Fatalf("unknown event: %d", code)
	}
	if code, _, _ := e.do("POST", "/api/v1/events/"+id+"/replay", e.keys["acme-pharma"], nil); code != 404 {
		t.Fatalf("another tenant must not replay it: %d", code)
	}
	var n int
	must(t, e.st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM sensor_readings`).Scan(&n)
	}))
	if n != 1 {
		t.Fatalf("readings after replay = %d, want 1", n)
	}
}

// F7-R2: a rejected event can be replayed once the missing context exists (device registered).
func TestReplayRejectedAfterDeviceKnown_F7R2(t *testing.T) {
	e := newEnv(t)
	byName := []byte(`{"DeviceName":"A571992","EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":10}}`)
	code, got := e.post("reviewer", byName)
	if code != 422 {
		t.Fatalf("unknown device by name: %d %v", code, got)
	}
	id := got["event_id"].(string)

	if code, _ := e.post("reviewer", sample(t, 2)); code != 200 { // registers B234567, not A571992
		t.Fatal("setup")
	}
	if code, _, b := e.do("POST", "/api/v1/events/"+id+"/replay", e.keys["reviewer"], nil); code != 200 || !strings.Contains(string(b), "NO_DEVICE_IDENTITY") {
		t.Fatalf("still unknown, replay must reject again: %d %s", code, b)
	}
	if code, _ := e.post("reviewer", sample(t, 0)); code != 200 { // registers A571992
		t.Fatal("setup")
	}
	code, _, b := e.do("POST", "/api/v1/events/"+id+"/replay", e.keys["reviewer"], nil)
	if code != 200 || !strings.Contains(string(b), "DEVICE_RESOLVED_BY_NAME") || strings.Contains(string(b), `"rejected"`) {
		t.Fatalf("replay should now resolve the device: %d %s", code, b)
	}
}

func TestAliasPathAndRequestID(t *testing.T) {
	e := newEnv(t)
	code, hdr, b := e.do("POST", "/api/v1/webhook/tive", e.keys["reviewer"], sample(t, 2), "X-Request-ID", "my-req-1")
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if code != 200 || got["status"] != "accepted" || hdr.Get("X-Request-ID") != "my-req-1" || got["request_id"] != "my-req-1" {
		t.Fatalf("%d %s %v", code, b, hdr)
	}
	_, hdr, _ = e.do("POST", "/api/webhook/tive", e.keys["reviewer"], sample(t, 3), "X-Request-ID", "bad id with spaces")
	if id := hdr.Get("X-Request-ID"); !strings.HasPrefix(id, "req_") {
		t.Fatalf("an unusable request id is replaced, got %q", id)
	}
	code, _, _ = e.do("GET", "/api/v1/events/evt_nope", e.keys["reviewer"], nil)
	if code != 404 {
		t.Fatalf("unknown event: %d", code)
	}
}

func TestReadingsValidation(t *testing.T) {
	e := newEnv(t)
	for path, want := range map[string]int{
		"/api/v1/readings":                            400,
		"/api/v1/readings?device_id=x&from=abc":       400,
		"/api/v1/readings?device_id=x&limit=0":        400,
		"/api/v1/readings?device_id=x&cursor=garbage": 400,
		"/api/v1/readings?device_id=nothing-here":     200,
	} {
		if code, _, b := e.do("GET", path, e.keys["reviewer"], nil); code != want {
			t.Errorf("%s: %d %s, want %d", path, code, b, want)
		}
	}
}

// F4: the same payload again is a duplicate, a changed one a conflict; both are 200.
func TestDuplicateAndConflictOverHTTP_F4(t *testing.T) {
	e := newEnv(t)
	if _, got := e.post("reviewer", sample(t, 2)); got["status"] != "accepted" {
		t.Fatalf("%v", got)
	}
	// Same content, different layout: still a duplicate (F4-R5).
	var m map[string]any
	must(t, json.Unmarshal(sample(t, 2), &m))
	reordered, _ := json.MarshalIndent(m, "", "      ")
	if code, got := e.post("reviewer", reordered); code != 200 || got["status"] != "duplicate" {
		t.Fatalf("%d %v", code, got)
	}
	m["Humidity"].(map[string]any)["Percentage"] = 56.0
	changed, _ := json.Marshal(m)
	if code, got := e.post("reviewer", changed); code != 200 || got["status"] != "conflict" {
		t.Fatalf("%d %v", code, got)
	}
}

// Per-tenant conflict policy over HTTP (F4-R7, F4-R8).
func changedSample(t *testing.T, humidity float64) []byte {
	t.Helper()
	var m map[string]any
	must(t, json.Unmarshal(sample(t, 2), &m))
	m["Humidity"].(map[string]any)["Percentage"] = humidity
	b, _ := json.Marshal(m)
	return b
}

func (e *env) setPolicy(tenant, policy string, status int) {
	e.t.Helper()
	cs, err := store.NewConflictSettings(policy, status)
	must(e.t, err)
	must(e.t, e.st.SetConflictSettings(ctx, tenant, cs))
}

func (e *env) humidity(tenant string) float64 {
	e.t.Helper()
	_, _, b := e.do("GET", "/api/v1/readings?device_id=B234567", e.keys[tenant], nil)
	var page struct {
		Items []struct{ Sensor struct{ Humidity float64 } }
	}
	must(e.t, json.Unmarshal(b, &page))
	if len(page.Items) != 1 {
		e.t.Fatalf("readings: %s", b)
	}
	return page.Items[0].Sensor.Humidity
}

func TestConflictPoliciesOverHTTP_F4R7(t *testing.T) {
	e := newEnv(t)

	// Default (keep_first): 200 conflict, the first reading stays.
	e.post("reviewer", sample(t, 2))
	code, got := e.post("reviewer", changedSample(t, 99.9))
	if code != 200 || got["status"] != "conflict" || e.humidity("reviewer") != 55.3 {
		t.Fatalf("keep_first: %d %v humidity %v", code, got["status"], e.humidity("reviewer"))
	}

	// reject: 409, nothing replaced; the body still says what happened.
	e.setPolicy("reviewer", "reject", 0)
	code, got = e.post("reviewer", changedSample(t, 88.8))
	if code != 409 || got["status"] != "conflict" || got["event_id"] == "" || !strings.Contains(fmt.Sprint(got["flags"]), "rejected") || e.humidity("reviewer") != 55.3 {
		t.Fatalf("reject: %d %v", code, got)
	}
	// ...but a plain duplicate is still a 200.
	if code, got = e.post("reviewer", sample(t, 2)); code != 200 || got["status"] != "duplicate" {
		t.Fatalf("duplicate under reject: %d %v", code, got["status"])
	}

	// The status code is separate from the policy.
	e.setPolicy("reviewer", "reject", 200)
	if code, _ = e.post("reviewer", changedSample(t, 77.7)); code != 200 {
		t.Fatalf("reject with http_status 200: %d", code)
	}

	// keep_last: the new version replaces the stored reading.
	e.setPolicy("reviewer", "keep_last", 0)
	code, got = e.post("reviewer", changedSample(t, 66.6))
	if code != 200 || got["status"] != "conflict" || !strings.Contains(fmt.Sprint(got["flags"]), "replaced") || e.humidity("reviewer") != 66.6 {
		t.Fatalf("keep_last: %d %v humidity %v", code, got["status"], e.humidity("reviewer"))
	}
	e.setPolicy("reviewer", "keep_last", 409)
	if code, _ = e.post("reviewer", changedSample(t, 65.5)); code != 409 || e.humidity("reviewer") != 65.5 {
		t.Fatalf("keep_last answering 409 still replaces: %d humidity %v", code, e.humidity("reviewer"))
	}

	// Another tenant is unaffected by the reviewer's policy.
	e.post("acme-pharma", sample(t, 2))
	if code, got = e.post("acme-pharma", changedSample(t, 99.9)); code != 200 || got["status"] != "conflict" || e.humidity("acme-pharma") != 55.3 {
		t.Fatalf("acme-pharma defaults to keep_first: %d %v", code, got["status"])
	}
}

// Replaying a duplicate or a conflict could put an old version over a newer reading, so only failed, rejected or
// never-finished events can be replayed.
func TestReplayOfConflictIsRefused(t *testing.T) {
	e := newEnv(t)
	e.setPolicy("reviewer", "keep_last", 0)
	e.post("reviewer", sample(t, 2))
	_, conflict := e.post("reviewer", changedSample(t, 99.9))
	_, dup := e.post("reviewer", changedSample(t, 99.9))
	for name, resp := range map[string]map[string]any{"conflict": conflict, "duplicate": dup} {
		if code, _, _ := e.do("POST", "/api/v1/events/"+resp["event_id"].(string)+"/replay", e.keys["reviewer"], nil); code != 409 {
			t.Errorf("replay of a %s: %d, want 409", name, code)
		}
	}
	if e.humidity("reviewer") != 99.9 {
		t.Fatal("the latest reading must not change")
	}
}

// ---- rules are configurable on the fly ----

// publish validates a ruleset like the CLI does and publishes it; running instances adopt it without a restart.
func (e *env) publishRules(version, yaml string) {
	e.t.Helper()
	if _, err := rules.Parse([]byte(yaml)); err != nil {
		e.t.Fatalf("test ruleset is invalid: %v", err)
	}
	if _, err := e.st.PublishRuleset(ctx, version, yaml, "test"); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) waitForRules(version, source string) {
	e.t.Helper()
	for i := 0; i < 300; i++ {
		l := e.holder.Loaded()
		if l.Ruleset.Version == version && l.Source == source {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	l := e.holder.Loaded()
	e.t.Fatalf("rules did not become %s/%s in time; in force: %s/%s", version, source, l.Ruleset.Version, l.Source)
}

// defaultPlus is the default ruleset with extra rules appended, as an operator would write it.
func defaultPlus(version, extra string) string {
	d := string(rules.DefaultYAML())
	d = strings.Replace(d, "version: "+mustDefault().Version, "version: "+version, 1)
	d = strings.Replace(d, "overrides: []", "", 1)
	return d + extra
}

func mustDefault() *rules.Ruleset {
	rs, err := rules.Default()
	if err != nil {
		panic(err)
	}
	return rs
}

const humidityHigh = `  - { id: HUMIDITY-HIGH, code: HUMIDITY_HIGH, field: Humidity.Percentage, when: { outside: [0, 50] }, action: flag, severity: warn }
`

func flagCodesOf(got map[string]any) string {
	var codes []string
	for _, f := range got["flags"].([]any) {
		codes = append(codes, f.(map[string]any)["code"].(string))
	}
	return strings.Join(codes, ",")
}

// A rule added at runtime changes the very next payload, with no restart; a reset puts the defaults back.
func TestRulesChangeOnTheFly(t *testing.T) {
	e := newEnv(t)
	baseVersion := mustDefault().Version

	// Before: sample 3 (humidity 55) carries only SENSOR_FAULT.
	code, got := e.post("reviewer", sample(t, 3))
	if code != 200 || flagCodesOf(got) != "SENSOR_FAULT" {
		t.Fatalf("%d %v", code, got["flags"])
	}
	oldEvent := got["event_id"].(string)

	// An operator publishes the defaults plus one new rule. Nothing is restarted.
	e.publishRules("2026-11-06.live-1", defaultPlus("2026-11-06.live-1", humidityHigh))
	e.waitForRules("2026-11-06.live-1", rules.SourceDatabase)

	code, got = e.post("reviewer", sample(t, 11)) // humidity 54.8
	if code != 200 || got["status"] != "flagged" || flagCodesOf(got) != "HUMIDITY_HIGH" {
		t.Fatalf("the new rule must apply to the next payload: %d %v %v", code, got["status"], got["flags"])
	}
	if ev := e.getEvent("reviewer", got["event_id"].(string)); ev["rules_version"] != "2026-11-06.live-1" {
		t.Fatalf("the event records the rules version that processed it: %v", ev["rules_version"])
	}
	if ev := e.getEvent("reviewer", oldEvent); ev["rules_version"] != baseVersion {
		t.Fatalf("an earlier event keeps its own rules version: %v", ev["rules_version"])
	}

	// GET /rules shows what is in force and where it came from.
	_, _, b := e.do("GET", "/api/v1/rules", e.keys["reviewer"], nil)
	var shown struct {
		Version, Source string
		Rules           []map[string]any
	}
	must(t, json.Unmarshal(b, &shown))
	if shown.Version != "2026-11-06.live-1" || shown.Source != "database" || len(shown.Rules) != 16 {
		t.Fatalf("GET /rules: %s", b)
	}

	// A reset goes back to the defaults, again without a restart.
	if cleared, err := e.st.ClearActiveRuleset(ctx); err != nil || !cleared {
		t.Fatalf("%v %v", cleared, err)
	}
	e.waitForRules(baseVersion, rules.SourceDefault)
	if code, got = e.post("reviewer", sample(t, 5)); code != 200 || flagCodesOf(got) != "TIMESTAMP_SECONDS" {
		t.Fatalf("back to the defaults: %d %v", code, got["flags"])
	}
}

// One ruleset can treat tenants differently through overrides.
func TestRulesPerTenantOverridesOnTheFly(t *testing.T) {
	e := newEnv(t)
	off := `  - { id: HUMIDITY-HIGH, code: HUMIDITY_HIGH, field: Humidity.Percentage, when: { outside: [0, 50] }, action: ignore, severity: warn }
overrides:
  - scope: { tenant: reviewer }
    rules:
      - { id: HUMIDITY-HIGH, action: flag }
`
	e.publishRules("2026-11-06.tenants-1", defaultPlus("2026-11-06.tenants-1", off))
	e.waitForRules("2026-11-06.tenants-1", rules.SourceDatabase)

	// The rule is on for reviewer and off for everyone else.
	if _, got := e.post("reviewer", sample(t, 11)); flagCodesOf(got) != "HUMIDITY_HIGH" {
		t.Fatalf("reviewer: %v", got["flags"])
	}
	if _, got := e.post("acme-pharma", sample(t, 11)); flagCodesOf(got) != "" || got["status"] != "accepted" {
		t.Fatalf("acme-pharma: %v %v", got["status"], got["flags"])
	}
}

// A ruleset that cannot be loaded never takes the service down or replaces the working one.
func TestInvalidPublishedRulesetIsIgnored(t *testing.T) {
	e := newEnv(t)
	e.publishRules("2026-11-06.good", defaultPlus("2026-11-06.good", humidityHigh))
	e.waitForRules("2026-11-06.good", rules.SourceDatabase)

	// Bypass the CLI's validation to simulate a corrupt row. Both statements are one transaction: between two separate
	// ones the reloader can see "no active ruleset" and fall back to the defaults, which is a different (and correct)
	// behaviour than the one under test.
	must(t, pgx.BeginFunc(ctx, e.st.Pool(), func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE rulesets SET active = false WHERE active`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO rulesets (version, yaml, active) VALUES ('2026-11-06.broken', 'version: [', true)`)
		return err
	}))
	time.Sleep(150 * time.Millisecond) // several refresh intervals

	if l := e.holder.Loaded(); l.Ruleset.Version != "2026-11-06.good" {
		t.Fatalf("the working ruleset must stay in force, got %s", l.Ruleset.Version)
	}
	if code, got := e.post("reviewer", sample(t, 11)); code != 200 || flagCodesOf(got) != "HUMIDITY_HIGH" {
		t.Fatalf("ingest keeps working: %d %v", code, got["flags"])
	}
}

package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi"
	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi/middleware"
)

// F13-R3: liveness needs no key and no database.
func TestHealthz_F13R3(t *testing.T) {
	api := &httpapi.API{Log: slog.New(slog.NewJSONHandler(os.Stderr, nil))}
	for _, tt := range []struct {
		method, path string
		want         int
	}{{"GET", "/healthz", 200}, {"POST", "/healthz", 405}, {"GET", "/nope", 404}} {
		rec := httptest.NewRecorder()
		api.Handler().ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
		if rec.Code != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, rec.Code, tt.want)
		}
	}
}

// F13-R4: readiness reports the database, migration version, backend and mode; 503 when the database is gone.
func TestReadyz_F13R4(t *testing.T) {
	e := newEnv(t)
	code, _, b := e.do("GET", "/readyz", "", nil)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if code != 200 || got["status"] != "ok" || got["storage_backend"] != "postgres" || got["ingest_mode"] != "sync" || got["migration_version"].(float64) < 1 {
		t.Fatalf("%d %s", code, b)
	}

	failing := &httpapi.API{Log: slog.New(slog.NewJSONHandler(os.Stderr, nil)), Ready: func(context.Context) (httpapi.ReadyInfo, error) { return httpapi.ReadyInfo{}, errors.New("db down") }}
	rec := httptest.NewRecorder()
	failing.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 503 || strings.Contains(rec.Body.String(), "db down") {
		t.Fatalf("%d %s (the cause must not leak)", rec.Code, rec.Body)
	}
}

// Opening the bare address in a browser must land on the API reference, not a 404.
func TestRootRedirectsToDocs(t *testing.T) {
	api := &httpapi.API{Log: slog.New(slog.NewJSONHandler(os.Stderr, nil))}
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/docs" {
		t.Fatalf("%d %q", rec.Code, rec.Header().Get("Location"))
	}
	rec = httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("other unknown paths stay 404: %d", rec.Code)
	}
}

func TestDocsAndContract(t *testing.T) {
	e := newEnv(t)
	code, hdr, b := e.do("GET", "/docs", "", nil)
	if code != 200 || !strings.Contains(string(b), "<redoc") || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html") {
		t.Fatalf("/docs: %d %s", code, b)
	}
	code, _, b = e.do("GET", "/openapi.yaml", "", nil)
	contract, err := os.ReadFile("../../specs/001-integration-api/contracts/openapi.yaml")
	must(t, err)
	if code != 200 || string(b) != string(contract) {
		t.Fatal("/openapi.yaml must serve exactly the reviewed contract")
	}
}

func TestRulesEndpoint(t *testing.T) {
	e := newEnv(t)
	if code, _, _ := e.do("GET", "/api/v1/rules", "", nil); code != 401 {
		t.Fatalf("rules need a key: %d", code)
	}
	code, _, b := e.do("GET", "/api/v1/rules", e.keys["reviewer"], nil)
	var got struct {
		Version string
		Rules   []map[string]any
	}
	_ = json.Unmarshal(b, &got)
	if code != 200 || got.Version != "2026-11-06.1" || len(got.Rules) != 15 || got.Rules[0]["id"] != "NUMERIC-STRING" {
		t.Fatalf("%d %s", code, b)
	}
}

// M4 exit: /metrics shows request and per-stage histograms and quality counters after traffic.
func TestMetricsAfterTraffic(t *testing.T) {
	e := newEnv(t)
	for _, n := range []int{0, 3, 12} {
		e.post("reviewer", sample(t, n))
	}
	e.do("GET", "/nope", "", nil)
	_, _, b := e.do("GET", "/metrics", "", nil)
	body := string(b)
	for _, want := range []string{
		`http_requests_total{route="POST /api/webhook/tive",status="200"} 2`,
		`http_requests_total{route="POST /api/webhook/tive",status="422"} 1`,
		`http_requests_total{route="unmatched",status="404"} 1`,
		`http_request_duration_seconds_bucket{route="POST /api/webhook/tive"`,
		`ingest_stage_duration_seconds_bucket{stage="raw_save"`,
		`ingest_stage_duration_seconds_bucket{stage="validate"`,
		`ingest_stage_duration_seconds_bucket{stage="normalize_save"`,
		`quality_flags_total{code="SENSOR_FAULT"} 1`,
		`quality_flags_total{code="IDENTITY_MISMATCH"}`,
		`rejects_total{reason="NO_DEVICE_IDENTITY"} 1`,
		`db_pool_acquire_wait_seconds_total`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// M4 exit: each log line carries request_id, tenant_id, event_id, status, latency_ms; never keys or payloads.
func TestAccessLog(t *testing.T) {
	e := newEnv(t)
	key := e.keys["reviewer"]
	_, hdr, _ := e.do("POST", "/api/webhook/tive", key, sample(t, 0), "X-Request-ID", "req-log-1")
	e.do("POST", "/api/webhook/tive", "pxk_test_wrongwrongwrongwrongwrongwrongwrongwrongwrong", sample(t, 0))

	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(e.logs.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["request_id"] == "req-log-1" && m["msg"] == "request" {
			found = m
		}
	}
	if found == nil {
		t.Fatalf("no access log line for req-log-1 in:\n%s", e.logs)
	}
	if found["tenant_id"] != "reviewer" || found["status"] != "flagged" || !strings.HasPrefix(found["event_id"].(string), "evt_") ||
		found["http_status"].(float64) != 200 || found["route"] != "POST /api/webhook/tive" || found["latency_ms"] == nil || found["level"] != "WARN" {
		t.Fatalf("unexpected line %v", found)
	}
	if hdr.Get("X-Request-ID") != "req-log-1" {
		t.Fatal("request id not echoed")
	}
	logs := e.logs.String()
	for _, secret := range []string{key, "Hunts Point", "A571992", "863257063350583", "wrongwrong"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("logs must not contain %q", secret)
		}
	}
}

func TestRecoverMiddleware(t *testing.T) {
	h := middleware.Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
		middleware.RequestID, middleware.Recover(slog.New(slog.NewJSONHandler(os.Stderr, nil))))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "INTERNAL") || strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

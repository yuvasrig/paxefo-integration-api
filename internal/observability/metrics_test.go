package observability

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics(nil)
	m.ObserveRequest("POST /api/v1/webhook/tive", 200, 12*time.Millisecond)
	m.Stage("validate", time.Millisecond)
	m.Outcome("flagged", []rules.Flag{{Code: "SENSOR_FAULT"}, {Code: "SENSOR_FAULT"}}, nil)
	m.Outcome("rejected", nil, []rules.Problem{{Code: "NO_DEVICE_IDENTITY"}})

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`http_requests_total{route="POST /api/v1/webhook/tive",status="200"} 1`,
		`http_request_duration_seconds_bucket{route="POST /api/v1/webhook/tive"`,
		`ingest_stage_duration_seconds_bucket{stage="validate"`,
		`quality_flags_total{code="SENSOR_FAULT"} 2`,
		`rejects_total{reason="NO_DEVICE_IDENTITY"} 1`,
		`events_total{status="flagged"} 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestLoggerLevelAndFormat(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger("warn", &buf)
	log.Info("hidden")
	log.Warn("shown", "request_id", "req_1")
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log must be one JSON line, got %q: %v", buf.String(), err)
	}
	if line["msg"] != "shown" || line["request_id"] != "req_1" || line["level"] != "WARN" || line["time"] == nil {
		t.Fatalf("unexpected line %v", line)
	}
}

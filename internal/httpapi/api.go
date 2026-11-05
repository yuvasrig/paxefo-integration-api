// Package httpapi holds routes, handlers and the response envelope (DESIGN.md section 12).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi/middleware"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// API wires the handlers to the ingest service.
type API struct {
	Ingest        *ingest.Service
	Authenticator *auth.Authenticator
	Limiter       *middleware.RateLimiter
	BodyLimit     int64
	Log           *slog.Logger

	Rules    *rules.Ruleset                           // served by GET /api/v1/rules
	Ready    func(context.Context) (ReadyInfo, error) // /readyz
	Metrics  http.Handler                             // /metrics (nil disables it)
	Observer middleware.RequestObserver               // request metrics (may be nil)
}

// Handler returns the router with the middleware chain applied.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	secured := func(h http.HandlerFunc, extra ...func(http.Handler) http.Handler) http.Handler {
		chain := append([]func(http.Handler) http.Handler{middleware.Auth(a.Authenticator, a.Log), middleware.RateLimit(a.Limiter)}, extra...)
		return middleware.Chain(h, chain...)
	}
	webhook := secured(a.webhook, middleware.BodyLimit(a.BodyLimit))
	mux.Handle("POST /api/v1/webhook/tive", webhook)
	mux.Handle("POST /api/webhook/tive", webhook) // the path named in the exercise
	mux.Handle("GET /api/v1/events/{id}", secured(a.getEvent))
	mux.Handle("POST /api/v1/events/{id}/replay", secured(a.replay))
	mux.Handle("GET /api/v1/readings", secured(a.readings))
	mux.Handle("GET /api/v1/rules", secured(a.getRules))

	// Unauthenticated: probes, metrics and the API contract.
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	if a.Metrics != nil {
		mux.Handle("GET /metrics", a.Metrics)
	}
	mux.HandleFunc("GET /openapi.yaml", a.openapi)
	mux.HandleFunc("GET /docs", a.docs)

	return middleware.Chain(mux, middleware.RequestID, middleware.AccessLog(a.Log, a.Observer), middleware.Recover(a.Log))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// webhook implements POST /api/v1/webhook/tive (F1-R1).
func (a *API) webhook(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			middleware.WriteProblem(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body is too large")
			return
		}
		middleware.WriteProblem(w, r, http.StatusBadRequest, "INVALID_REQUEST", "could not read the request body")
		return
	}
	resp, err := a.Ingest.Ingest(r.Context(), ingest.Request{
		SubmittedBy: p.TenantID, RequestID: middleware.RequestIDFrom(r.Context()), Body: body,
	})
	if err != nil {
		// The raw payload was not saved, so nothing is acknowledged: the sender should retry.
		a.Log.Error("could not save raw event", "request_id", middleware.RequestIDFrom(r.Context()), "tenant_id", p.TenantID, "error", err)
		w.Header().Set("Retry-After", "1")
		middleware.WriteProblem(w, r, http.StatusServiceUnavailable, "UNAVAILABLE", "could not save the payload; retry")
		return
	}
	middleware.AnnotateEvent(r.Context(), resp.EventID, resp.Status)
	writeJSON(w, resp.HTTPStatus, resp)
}

type eventDetail struct {
	EventID          string          `json:"event_id"`
	RequestID        string          `json:"request_id"`
	Status           string          `json:"status"`
	ReceivedAt       time.Time       `json:"received_at"`
	ProcessedAt      *time.Time      `json:"processed_at"`
	TransformVersion string          `json:"transform_version,omitempty"`
	RulesVersion     string          `json:"rules_version,omitempty"`
	RawPayload       json.RawMessage `json:"raw_payload"`
	Sensor           json.RawMessage `json:"sensor"`
	Location         json.RawMessage `json:"location"`
	Flags            []rules.Flag    `json:"flags"`
	Errors           []rules.Problem `json:"errors"`
	Error            string          `json:"error,omitempty"`
}

// getEvent implements GET /api/v1/events/{id} (F7-R6).
func (a *API) getEvent(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	ev, err := a.Ingest.Event(r.Context(), p.TenantID, r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		middleware.WriteProblem(w, r, http.StatusNotFound, "NOT_FOUND", "event not found")
		return
	}
	if err != nil {
		a.internal(w, r, err)
		return
	}
	d := eventDetail{
		EventID: ev.ID, RequestID: ev.RequestID, Status: "pending", ReceivedAt: ev.ReceivedAt,
		RawPayload: rawPayload(ev.Raw), Sensor: ev.Sensor, Location: ev.Location,
		Flags: nonNilFlags(ev.Flags), Errors: nonNilProblems(ev.Reasons),
		TransformVersion: ev.TransformVersion, RulesVersion: ev.RulesVersion, Error: ev.Error,
	}
	if ev.HasOutcome {
		d.Status = ev.Status
		t := ev.ProcessedAt
		d.ProcessedAt = &t
	}
	writeJSON(w, http.StatusOK, d)
}

// rawPayload shows the stored bytes as JSON when they are JSON, otherwise as a string.
func rawPayload(raw []byte) json.RawMessage {
	if json.Valid(raw) {
		return raw
	}
	b, _ := json.Marshal(string(raw))
	return b
}

// replay implements POST /api/v1/events/{id}/replay (F7-R2, F7-R4).
func (a *API) replay(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	resp, err := a.Ingest.Replay(r.Context(), p.TenantID, r.PathValue("id"), middleware.RequestIDFrom(r.Context()))
	switch {
	case errors.Is(err, store.ErrNotFound):
		middleware.WriteProblem(w, r, http.StatusNotFound, "NOT_FOUND", "event not found")
	case errors.Is(err, ingest.ErrNotReplayable):
		middleware.WriteProblem(w, r, http.StatusConflict, "NOT_REPLAYABLE", "event is already processed; nothing to replay")
	case err != nil:
		a.internal(w, r, err)
	default:
		status := http.StatusOK // a replay that re-ran and rejected the record is still a successful replay
		if resp.HTTPStatus == http.StatusInternalServerError {
			status = http.StatusInternalServerError
		}
		writeJSON(w, status, resp)
	}
}

// readings implements GET /api/v1/readings.
func (a *API) readings(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	q := r.URL.Query()
	rq := store.ReadingsQuery{DeviceID: q.Get("device_id"), Cursor: q.Get("cursor")}
	if rq.DeviceID == "" {
		middleware.WriteProblem(w, r, http.StatusBadRequest, "INVALID_REQUEST", "device_id is required")
		return
	}
	for name, dst := range map[string]**int64{"from": &rq.From, "to": &rq.To} {
		if v := q.Get(name); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				middleware.WriteProblem(w, r, http.StatusBadRequest, "INVALID_REQUEST", name+" must be epoch milliseconds")
				return
			}
			*dst = &n
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			middleware.WriteProblem(w, r, http.StatusBadRequest, "INVALID_REQUEST", "limit must be a positive integer")
			return
		}
		rq.Limit = n
	}
	page, err := a.Ingest.Readings(r.Context(), p.TenantID, rq)
	if err != nil {
		if isBadCursor(err) {
			middleware.WriteProblem(w, r, http.StatusBadRequest, "INVALID_REQUEST", "invalid cursor")
			return
		}
		a.internal(w, r, err)
		return
	}
	if page.Items == nil {
		page.Items = []store.ReadingItem{}
	}
	writeJSON(w, http.StatusOK, page)
}

func isBadCursor(err error) bool { return errors.Is(err, store.ErrBadCursor) }

func (a *API) internal(w http.ResponseWriter, r *http.Request, err error) {
	a.Log.Error("request failed", "request_id", middleware.RequestIDFrom(r.Context()), "path", r.URL.Path, "error", err)
	middleware.WriteProblem(w, r, http.StatusInternalServerError, "INTERNAL", "internal error")
}

func nonNilFlags(f []rules.Flag) []rules.Flag {
	if f == nil {
		return []rules.Flag{}
	}
	return f
}

func nonNilProblems(p []rules.Problem) []rules.Problem {
	if p == nil {
		return []rules.Problem{}
	}
	return p
}

package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// RequestObserver receives one observation per request (implemented by observability.Metrics).
type RequestObserver interface {
	ObserveRequest(route string, status int, d time.Duration)
}

// info is filled by later middleware and handlers so the access log can include tenant and event.
type info struct {
	tenant, eventID, eventStatus string
}

type infoKey struct{}

// AnnotateTenant records the authenticated tenant for the access log.
func AnnotateTenant(ctx context.Context, tenantID string) {
	if i, ok := ctx.Value(infoKey{}).(*info); ok {
		i.tenant = tenantID
	}
}

// AnnotateEvent records the event id and its status for the access log.
func AnnotateEvent(ctx context.Context, eventID, status string) {
	if i, ok := ctx.Value(infoKey{}).(*info); ok {
		i.eventID, i.eventStatus = eventID, status
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// AccessLog writes one structured line per request and records RED metrics. It logs ids, never the API key,
// the request body or the payload (DESIGN.md section 10.1).
func AccessLog(log *slog.Logger, obs RequestObserver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			in := &info{}
			r = r.WithContext(context.WithValue(r.Context(), infoKey{}, in))
			sw := &statusWriter{ResponseWriter: w}

			next.ServeHTTP(sw, r)

			if sw.status == 0 {
				sw.status = http.StatusOK
			}
			route := r.Pattern // set by the mux; empty for unmatched paths, which keeps metric cardinality bounded
			if route == "" {
				route = "unmatched"
			}
			d := time.Since(start)
			if obs != nil {
				obs.ObserveRequest(route, sw.status, d)
			}

			level := slog.LevelInfo
			switch {
			case sw.status >= 500:
				level = slog.LevelError
			case sw.status >= 400, in.eventStatus == "flagged", in.eventStatus == "conflict":
				level = slog.LevelWarn
			}
			log.LogAttrs(r.Context(), level, "request",
				slog.String("request_id", RequestIDFrom(r.Context())),
				slog.String("tenant_id", in.tenant),
				slog.String("event_id", in.eventID),
				slog.String("status", in.eventStatus),
				slog.Int("http_status", sw.status),
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.Float64("latency_ms", float64(d.Microseconds())/1000),
			)
		})
	}
}

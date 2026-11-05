// Package middleware holds the HTTP middleware: request ID, panic recovery, authentication, body limit and
// per-tenant rate limit (DESIGN.md sections 4.1, 12, 13).
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
)

type ctxKey int

const requestIDKey ctxKey = 1

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// RequestIDFrom returns the request id set by RequestID.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// RequestID accepts a sane X-Request-ID or generates one, puts it in the context and echoes it (F6-R6).
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !requestIDRe.MatchString(id) {
			b := make([]byte, 12)
			_, _ = rand.Read(b)
			id = "req_" + hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

// WriteProblem writes the standard error envelope {request_id, errors:[{code,message}]}.
func WriteProblem(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"request_id": RequestIDFrom(r.Context()),
		"errors":     []map[string]string{{"code": code, "message": message}},
	})
}

// Recover turns a panic into a 500 instead of dropping the connection.
func Recover(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.Error("panic in handler", "request_id", RequestIDFrom(r.Context()), "panic", v, "path", r.URL.Path)
					WriteProblem(w, r, http.StatusInternalServerError, "INTERNAL", "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Auth checks X-API-Key and stores the principal in the context. Every failure to authenticate is the same
// 401; a failing key store is a 503 (F6-R1). The key is never logged.
func Auth(a *auth.Authenticator, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, err := a.Authenticate(r.Context(), r.Header.Get("X-API-Key"))
			switch {
			case errors.Is(err, auth.ErrUnauthorized):
				WriteProblem(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "missing, invalid, expired or revoked API key")
			case err != nil:
				log.Error("authentication unavailable", "request_id", RequestIDFrom(r.Context()), "error", err)
				w.Header().Set("Retry-After", "1")
				WriteProblem(w, r, http.StatusServiceUnavailable, "UNAVAILABLE", "authentication is temporarily unavailable")
			default:
				AnnotateTenant(r.Context(), p.TenantID)
				next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
			}
		})
	}
}

// BodyLimit rejects bodies larger than max bytes with 413 before anything is saved (F6-R3).
func BodyLimit(max int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > max {
				WriteProblem(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds "+strconv.FormatInt(max, 10)+" bytes")
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, max)
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimiter is an in-memory token bucket per tenant (F6-R4). With several instances each has its own bucket;
// a shared limiter is a documented upgrade.
type RateLimiter struct {
	Rate  float64 // tokens per second
	Burst int
	Now   func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// Allow takes a token for the tenant; when none is left it returns how long to wait.
func (l *RateLimiter) Allow(tenant string) (ok bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.buckets == nil {
		l.buckets = map[string]*bucket{}
	}
	now := l.Now()
	b, found := l.buckets[tenant]
	if !found {
		b = &bucket{tokens: float64(l.Burst), last: now}
		l.buckets[tenant] = b
	}
	b.tokens = math.Min(float64(l.Burst), b.tokens+now.Sub(b.last).Seconds()*l.Rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.Rate * float64(time.Second))
}

// RateLimit applies the limiter per authenticated tenant; it must run after Auth.
func RateLimit(l *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, _ := auth.FromContext(r.Context())
			if ok, wait := l.Allow(p.TenantID); !ok {
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
				WriteProblem(w, r, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded for this tenant")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Chain applies middleware so the first listed runs first.
func Chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

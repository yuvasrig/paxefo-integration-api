package auth

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrUnauthorized covers a missing, malformed, unknown, expired or revoked key. Callers answer 401 and
// must not reveal which case it was (F6-R1).
var ErrUnauthorized = errors.New("unauthorized")

// ErrKeyNotFound is returned by KeyStore for an unknown key hash.
var ErrKeyNotFound = errors.New("api key not found")

// KeyRecord is the stored side of a key.
type KeyRecord struct {
	ID        int64
	TenantID  string
	Name      string
	ExpiresAt *time.Time
	RevokedAt *time.Time
}

// KeyStore looks up a key by its hash.
type KeyStore interface {
	LookupKey(ctx context.Context, hash string) (KeyRecord, error)
}

// Principal is the authenticated caller: the key's tenant is authoritative (DESIGN.md section 7.2).
type Principal struct {
	TenantID string
	KeyID    int64
	KeyName  string
}

type ctxKey struct{}

// WithPrincipal stores p in ctx.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the principal set by the auth middleware.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// Authenticator validates API keys.
type Authenticator struct {
	Keys KeyStore
	Now  func() time.Time
}

// Authenticate checks the plaintext key. A store failure is returned as a plain error (a 5xx), never as ErrUnauthorized.
func (a *Authenticator) Authenticate(ctx context.Context, plaintext string) (Principal, error) {
	if !ValidFormat(plaintext) {
		return Principal{}, ErrUnauthorized
	}
	rec, err := a.Keys.LookupKey(ctx, HashKey(plaintext))
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return Principal{}, ErrUnauthorized
	case err != nil:
		return Principal{}, fmt.Errorf("auth: look up key: %w", err)
	}
	now := a.Now()
	if rec.RevokedAt != nil && !rec.RevokedAt.After(now) {
		return Principal{}, ErrUnauthorized
	}
	if rec.ExpiresAt != nil && !rec.ExpiresAt.After(now) {
		return Principal{}, ErrUnauthorized
	}
	return Principal{TenantID: rec.TenantID, KeyID: rec.ID, KeyName: rec.Name}, nil
}

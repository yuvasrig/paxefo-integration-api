package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeKeys map[string]KeyRecord

func (f fakeKeys) LookupKey(_ context.Context, hash string) (KeyRecord, error) {
	k, ok := f[hash]
	if !ok {
		return KeyRecord{}, ErrKeyNotFound
	}
	return k, nil
}

// F6-R1: missing, malformed, unknown, expired and revoked keys are all unauthorized.
func TestAuthenticate_F6R1(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	good, _ := NewKey()
	expired, _ := NewKey()
	revoked, _ := NewKey()
	future, _ := NewKey()
	unknown, _ := NewKey()
	past, soon := now.Add(-time.Minute), now.Add(time.Hour)

	a := &Authenticator{Keys: fakeKeys{
		good.Hash:    {ID: 1, TenantID: "reviewer", Name: "k"},
		expired.Hash: {ID: 2, TenantID: "reviewer", ExpiresAt: &past},
		revoked.Hash: {ID: 3, TenantID: "reviewer", RevokedAt: &past},
		future.Hash:  {ID: 4, TenantID: "acme-pharma", ExpiresAt: &soon},
	}, Now: func() time.Time { return now }}

	tests := []struct {
		name    string
		key     string
		wantErr bool
		tenant  string
	}{
		{"valid", good.Plaintext, false, "reviewer"},
		{"valid with future expiry", future.Plaintext, false, "acme-pharma"},
		{"missing", "", true, ""},
		{"malformed", "hello", true, ""},
		{"wrong prefix", "abc_test_" + good.Plaintext[len(KeyPrefix):], true, ""},
		{"unknown", unknown.Plaintext, true, ""},
		{"expired", expired.Plaintext, true, ""},
		{"revoked", revoked.Plaintext, true, ""},
		{"huge", string(make([]byte, 10000)), true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := a.Authenticate(context.Background(), tt.key)
			if tt.wantErr {
				if !errors.Is(err, ErrUnauthorized) {
					t.Fatalf("err = %v, want ErrUnauthorized", err)
				}
				return
			}
			if err != nil || p.TenantID != tt.tenant {
				t.Fatalf("principal = %+v, err = %v", p, err)
			}
		})
	}
}

func TestPrincipalContext(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("empty context has no principal")
	}
	ctx := WithPrincipal(context.Background(), Principal{TenantID: "t"})
	if p, ok := FromContext(ctx); !ok || p.TenantID != "t" {
		t.Fatalf("principal = %+v %v", p, ok)
	}
}

func TestStoreErrorsAreNotUnauthorized(t *testing.T) {
	boom := errors.New("db down")
	a := &Authenticator{Keys: errKeys{boom}, Now: time.Now}
	k, _ := NewKey()
	_, err := a.Authenticate(context.Background(), k.Plaintext)
	if err == nil || errors.Is(err, ErrUnauthorized) || !errors.Is(err, boom) {
		t.Fatalf("a database failure must surface as an error, not a 401: %v", err)
	}
}

type errKeys struct{ err error }

func (e errKeys) LookupKey(context.Context, string) (KeyRecord, error) { return KeyRecord{}, e.err }

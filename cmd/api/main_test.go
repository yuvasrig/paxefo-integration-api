package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// F13-R3: /healthz reports liveness without authentication.
func TestHealthz_F13R3(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{"GET returns ok", http.MethodGet, "/healthz", http.StatusOK, `"status":"ok"`},
		{"POST not allowed", http.MethodPost, "/healthz", http.StatusMethodNotAllowed, ""},
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			newMux().ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tt.wantBody) {
				t.Fatalf("body = %q, want it to contain %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestListenAddr(t *testing.T) {
	tests := []struct {
		name string
		port string
		want string
	}{
		{"default", "", ":8080"},
		{"from PORT", "10000", ":10000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PORT", tt.port)
			if got := listenAddr(); got != tt.want {
				t.Fatalf("listenAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}

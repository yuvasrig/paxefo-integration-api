package httpapi

import (
	_ "embed"
	"net/http"

	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi/middleware"
)

//go:embed openapi.yaml
var openapiYAML []byte

// ReadyInfo is what /readyz reports when the service can take traffic.
type ReadyInfo struct {
	Status           string `json:"status"`
	MigrationVersion int64  `json:"migration_version"`
	StorageBackend   string `json:"storage_backend"`
	IngestMode       string `json:"ingest_mode"`
}

// healthz is liveness: the process is up (F13-R3).
func (a *API) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is readiness: the database answers and migrations are applied (F13-R4).
func (a *API) readyz(w http.ResponseWriter, r *http.Request) {
	if a.Ready == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	info, err := a.Ready(r.Context())
	if err != nil {
		a.Log.Warn("not ready", "request_id", middleware.RequestIDFrom(r.Context()), "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	info.Status = "ok"
	writeJSON(w, http.StatusOK, info)
}

func (a *API) openapi(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openapiYAML)
}

const docsPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PAXAFE Tive Integration API</title>
</head>
<body>
<redoc spec-url="/openapi.yaml"></redoc>
<script src="https://cdn.jsdelivr.net/npm/redoc@2.1.5/bundles/redoc.standalone.js"></script>
</body>
</html>
`

// docs serves the Redoc page for the embedded contract.
func (a *API) docs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(docsPage))
}

// getRules implements GET /api/v1/rules: the ruleset currently in force, where it came from and when it was loaded.
// Rules are configurable at runtime (`api rules set`), so this can change between two calls.
func (a *API) getRules(w http.ResponseWriter, r *http.Request) {
	if a.Rules == nil {
		middleware.WriteProblem(w, r, http.StatusNotFound, "NOT_FOUND", "no ruleset loaded")
		return
	}
	l := a.Rules.Loaded()
	writeJSON(w, http.StatusOK, map[string]any{
		"version":   l.Ruleset.Version,
		"source":    l.Source, // default, file or database
		"loaded_at": l.LoadedAt,
		"rules":     l.Ruleset.Rules,
		"overrides": l.Ruleset.Overrides,
	})
}

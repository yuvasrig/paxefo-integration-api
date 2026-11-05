// Package provider defines the Provider interface and registry (DESIGN.md section 3.1).
// A provider turns one vendor's raw payload into PAXAFE records; adding a vendor means
// adding a package that implements Provider, not changing the pipeline.
package provider

import (
	"errors"
	"fmt"
	"sort"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// Event statuses a provider can produce. Duplicate and conflict are decided by the store (M2-M3).
const (
	StatusAccepted = "accepted"
	StatusFlagged  = "flagged"
	StatusRejected = "rejected"
)

// ErrInvalidJSON marks a body that is not a single JSON object (HTTP 400, F3-R1).
var ErrInvalidJSON = errors.New("invalid JSON")

// Result is the outcome of transforming one payload.
type Result struct {
	Status           string
	Sensor           *paxafe.SensorRecord
	Location         *paxafe.LocationRecord
	Flags            []rules.Flag
	Errors           []rules.Problem
	TransformVersion string
	RulesVersion     string
}

// Provider parses and transforms one vendor's payloads. Implementations are pure (no I/O).
type Provider interface {
	Name() string
	// Parse decodes raw bytes; errors wrap ErrInvalidJSON.
	Parse(raw []byte) (doc.Doc, error)
	// Transform applies the provider's rules and maps the cleaned payload to PAXAFE records.
	// A non-nil error is a server bug (for example output failing its schema), not a bad payload.
	Transform(d doc.Doc, sc rules.Scope) (Result, error)
}

// Registry looks providers up by name.
type Registry struct{ byName map[string]Provider }

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{byName: map[string]Provider{}} }

// Register adds p; registering a name twice is an error.
func (r *Registry) Register(p Provider) error {
	if _, dup := r.byName[p.Name()]; dup {
		return fmt.Errorf("provider %q already registered", p.Name())
	}
	r.byName[p.Name()] = p
	return nil
}

// Get returns the provider registered under name.
func (r *Registry) Get(name string) (Provider, error) {
	p, ok := r.byName[name]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q", name)
	}
	return p, nil
}

// Names lists registered providers in sorted order.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.byName))
	for n := range r.byName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

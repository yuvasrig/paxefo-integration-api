// Package tive parses Tive payloads and transforms them to PAXAFE records (DESIGN.md section 6).
// Pure: no I/O, no clock (time arrives in rules.Scope).
package tive

import (
	"fmt"

	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// Name is the provider name stored on events and emitted as PAXAFE "provider".
const Name = "Tive"

// TransformVersion is stored with every outcome so a row traces to the code that produced it (F7-R5).
// Bump it whenever the mapping changes.
const TransformVersion = "tive-transform/1"

// RulesSource supplies the ruleset to apply. Transform asks for it on every payload, so a ruleset swapped in at
// runtime (rules.Holder) takes effect on the next payload without restarting anything.
type RulesSource interface {
	Current() *rules.Ruleset
}

type staticRules struct{ rs *rules.Ruleset }

func (s staticRules) Current() *rules.Ruleset { return s.rs }

// Provider implements provider.Provider for Tive. The Tive-specific checks are configurable rules (see package
// rules); this package only reads the cleaned payload and maps it to PAXAFE records.
type Provider struct {
	rules RulesSource
	cfg   config.Transform
}

var _ provider.Provider = (*Provider)(nil)

// New returns a Tive provider with a fixed ruleset (used by tests and tools).
func New(rs *rules.Ruleset, cfg config.Transform) *Provider {
	return &Provider{rules: staticRules{rs}, cfg: cfg}
}

// NewDynamic returns a Tive provider whose ruleset can change while it runs.
func NewDynamic(src RulesSource, cfg config.Transform) *Provider {
	return &Provider{rules: src, cfg: cfg}
}

// Name implements provider.Provider.
func (*Provider) Name() string { return Name }

// Parse decodes a body into the payload tree; errors wrap provider.ErrInvalidJSON (F3-R1).
func (*Provider) Parse(raw []byte) (doc.Doc, error) {
	d, err := doc.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", provider.ErrInvalidJSON, err)
	}
	return d, nil
}

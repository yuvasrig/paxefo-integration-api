// Package rules is a small rules framework: a mechanism for validating and cleaning a payload with
// configurable rules, plus a set of default rules that ship with the service.
//
// The framework (this package) knows how to load a ruleset, validate it, and apply it. The rules
// themselves are data, not code: each rule says "for this field, when this condition holds, take this action"
// (fix with a named strategy, nullify, drop a section, reject, or just flag). The default ruleset
// (default_rules.yaml, embedded in the binary) implements the standard checks: coercing numeric strings, nulling
// sentinel temperatures, range checks, resolving a device by name, and so on. Anyone can add, change or switch
// off a rule by editing the YAML, without touching Go.
//
// Rules are meant to be configurable on the fly. An operator publishes a new ruleset with `api rules set`; every
// running instance validates and swaps it in within the refresh interval, with no restart (see Holder and the
// rulesync package). An invalid ruleset is rejected when it is published, and one that somehow fails to load
// at runtime never replaces the working one. Each event records the rules version that processed it.
// A ruleset can also carry per-tenant overrides, so tenants can differ.
//
// What is NOT a rule: authentication, size and rate limits, duplicate/conflict handling, and the few
// checks that need context a rule cannot express (the probe fallback, the "no usable data" rejection). Those are
// Go code, with their own settings.
//
// This package is pure: no I/O, no clock (DESIGN.md section 5.1).
package rules

import (
	"bytes"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

//go:embed default_rules.yaml
var defaultRulesYAML []byte

// Actions a rule can take.
const (
	actionFix         = "fix"
	actionNullify     = "nullify"
	actionDropSection = "drop_section"
	actionReject      = "reject"
	actionFlag        = "flag"
	actionIgnore      = "ignore"
)

var validActions = map[string]bool{
	actionFix: true, actionNullify: true, actionDropSection: true,
	actionReject: true, actionFlag: true, actionIgnore: true,
}

// Ruleset is a versioned, ordered list of rules plus per-tenant overrides. It is the unit an operator
// publishes at runtime: the version string identifies it in every event's audit trail, so bump it on every change.
type Ruleset struct {
	Version   string     `yaml:"version"`
	Rules     []Rule     `yaml:"rules"`
	Overrides []Override `yaml:"overrides"`
}

// Rule is one configurable validation or fix: for Field, when When holds, take Action. Rules run in file order,
// so a later rule sees the result of an earlier one (the numeric-string rule runs first for that reason).
// Adding a check to the service usually means adding a Rule to the ruleset, not writing Go.
type Rule struct {
	ID       string         `yaml:"id" json:"id"`                       // unique; recorded on every flag the rule raises
	Code     string         `yaml:"code" json:"code"`                   // the flag (or reject) code reported; any text, so new rules can introduce their own codes
	Field    string         `yaml:"field" json:"field"`                 // dotted path in the payload, e.g. Temperature.Celsius; "*" = every numeric field
	When     When           `yaml:"when" json:"when"`                   // the condition
	Action   string         `yaml:"action" json:"action"`               // fix, nullify, drop_section, reject, flag or ignore (ignore switches a rule off)
	Strategy string         `yaml:"strategy" json:"strategy,omitempty"` // for fix: a named strategy from the registry (strategies.go)
	Params   map[string]any `yaml:"params" json:"params,omitempty"`     // arguments for the strategy
	Section  string         `yaml:"section" json:"section,omitempty"`   // for drop_section: sensor or location
	Severity string         `yaml:"severity" json:"severity,omitempty"` // info, warn or error (descriptive; any flag makes an event flagged)

	afterNow time.Duration
}

// When holds the conditions of a rule; every condition that is set must hold. These are the building blocks a
// ruleset can combine (to add a new kind of condition, add a field here and handle it in Rule.matches in when.go):
//
//	lt / in / outside     numeric comparisons on the field's value
//	is_null / present     whether the field has a value
//	numeric_string        the value is a string that holds a number
//	sibling_present       another field has a value
//	differs_from          the field's text differs from another field's
//	after_now             the (epoch ms) value is more than this far in the future
//	invalid_imei          the value is not a 15-digit device id
//	resolved_by / unresolved_by   the DeviceName is / is not known for the tenant
type When struct {
	Lt             *float64  `yaml:"lt" json:"lt,omitempty"`
	In             []float64 `yaml:"in" json:"in,omitempty"`
	Outside        []float64 `yaml:"outside" json:"outside,omitempty"`
	IsNull         *bool     `yaml:"is_null" json:"is_null,omitempty"`
	SiblingPresent string    `yaml:"sibling_present" json:"sibling_present,omitempty"`
	NumericString  *bool     `yaml:"numeric_string" json:"numeric_string,omitempty"`
	InvalidIMEI    *bool     `yaml:"invalid_imei" json:"invalid_imei,omitempty"`
	UnresolvedBy   string    `yaml:"unresolved_by" json:"unresolved_by,omitempty"`
	ResolvedBy     string    `yaml:"resolved_by" json:"resolved_by,omitempty"`
	AfterNow       string    `yaml:"after_now" json:"after_now,omitempty"`
	DiffersFrom    string    `yaml:"differs_from" json:"differs_from,omitempty"`
	Present        *bool     `yaml:"present" json:"present,omitempty"`
}

func (w When) empty() bool {
	return w.Lt == nil && len(w.In) == 0 && len(w.Outside) == 0 && w.IsNull == nil &&
		w.SiblingPresent == "" && w.NumericString == nil && w.InvalidIMEI == nil &&
		w.UnresolvedBy == "" && w.ResolvedBy == "" && w.AfterNow == "" &&
		w.DiffersFrom == "" && w.Present == nil
}

// Override changes rules for one scope (currently one tenant).
type Override struct {
	Scope struct {
		Tenant string `yaml:"tenant" json:"tenant"`
	} `yaml:"scope" json:"scope"`
	Rules []RuleOverride `yaml:"rules" json:"rules"`
}

// RuleOverride replaces fields of an existing rule.
type RuleOverride struct {
	ID       string `yaml:"id" json:"id"`
	Action   string `yaml:"action" json:"action,omitempty"`
	Strategy string `yaml:"strategy" json:"strategy,omitempty"`
	Severity string `yaml:"severity" json:"severity,omitempty"`
}

// Default returns the embedded default ruleset: the rules that ship with the service. It is what runs until an
// operator publishes a different one.
func Default() (*Ruleset, error) { return Parse(defaultRulesYAML) }

// DefaultYAML returns the source of the embedded default ruleset, a starting point for writing your own
// (`api rules default > my.yaml`).
func DefaultYAML() []byte { return append([]byte(nil), defaultRulesYAML...) }

// Parse decodes and validates a ruleset. Any problem is an error: at startup that stops the service (F13-R2),
// when publishing it refuses the new ruleset, and at runtime it leaves the current ruleset in place.
func Parse(data []byte) (*Ruleset, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var rs Ruleset
	if err := dec.Decode(&rs); err != nil {
		return nil, fmt.Errorf("rules: parse yaml: %w", err)
	}
	if rs.Version == "" {
		return nil, fmt.Errorf("rules: version is required")
	}
	seen := map[string]bool{}
	for i := range rs.Rules {
		r := &rs.Rules[i]
		if r.ID == "" {
			return nil, fmt.Errorf("rules: rule %d: id is required", i+1)
		}
		if seen[r.ID] {
			return nil, fmt.Errorf("rules: duplicate rule id %q", r.ID)
		}
		seen[r.ID] = true
		if err := compile(r); err != nil {
			return nil, fmt.Errorf("rules: rule %s: %w", r.ID, err)
		}
	}
	for _, o := range rs.Overrides {
		for _, ro := range o.Rules {
			base := rs.find(ro.ID)
			if base == nil {
				return nil, fmt.Errorf("rules: override for tenant %q references unknown rule %q", o.Scope.Tenant, ro.ID)
			}
			merged := mergeOverride(*base, ro)
			if err := compile(&merged); err != nil {
				return nil, fmt.Errorf("rules: override %s for tenant %q: %w", ro.ID, o.Scope.Tenant, err)
			}
		}
	}
	return &rs, nil
}

func (rs *Ruleset) find(id string) *Rule {
	for i := range rs.Rules {
		if rs.Rules[i].ID == id {
			return &rs.Rules[i]
		}
	}
	return nil
}

func mergeOverride(r Rule, o RuleOverride) Rule {
	if o.Action != "" {
		r.Action = o.Action
	}
	if o.Strategy != "" {
		r.Strategy = o.Strategy
	}
	if o.Severity != "" {
		r.Severity = o.Severity
	}
	return r
}

// effective returns the rules for a tenant with its overrides applied.
func (rs *Ruleset) effective(tenant string) []Rule {
	out := make([]Rule, len(rs.Rules))
	copy(out, rs.Rules)
	for _, o := range rs.Overrides {
		if o.Scope.Tenant != tenant {
			continue
		}
		for _, ro := range o.Rules {
			for i := range out {
				if out[i].ID == ro.ID {
					merged := mergeOverride(out[i], ro)
					_ = compile(&merged) // validated at load
					out[i] = merged
				}
			}
		}
	}
	return out
}

// compile validates one rule and parses its durations.
func compile(r *Rule) error {
	if r.Code == "" {
		return fmt.Errorf("code is required")
	}
	if r.Field == "" {
		return fmt.Errorf("field is required")
	}
	if r.When.empty() {
		return fmt.Errorf("when must set at least one condition")
	}
	if !validActions[r.Action] {
		return fmt.Errorf("unknown action %q", r.Action)
	}
	switch r.Action {
	case actionFix, actionNullify, actionDropSection, actionFlag:
		if r.Severity != SeverityInfo && r.Severity != SeverityWarn && r.Severity != SeverityError {
			return fmt.Errorf("severity must be info, warn or error (got %q)", r.Severity)
		}
	}
	if n := len(r.When.Outside); n != 0 && n != 2 {
		return fmt.Errorf("when.outside needs exactly two numbers")
	}
	if r.When.UnresolvedBy != "" && r.When.UnresolvedBy != "DeviceName" ||
		r.When.ResolvedBy != "" && r.When.ResolvedBy != "DeviceName" {
		return fmt.Errorf("unresolved_by / resolved_by support only DeviceName")
	}
	var err error
	if r.When.AfterNow != "" {
		if r.afterNow, err = time.ParseDuration(r.When.AfterNow); err != nil {
			return fmt.Errorf("when.after_now: %w", err)
		}
	}

	if err := checkProtected(r); err != nil {
		return err
	}
	if r.Action == actionDropSection && r.Section != "sensor" && r.Section != "location" {
		return fmt.Errorf("drop_section needs section sensor or location (got %q)", r.Section)
	}
	if r.Action == actionFix {
		return validateStrategy(r)
	}
	return nil
}

// Protected fields accept only allow-listed actions and strategies (DESIGN.md section 5.1, guardrail 4).
func protectedKind(field string) string {
	switch {
	case field == "EntryTimeEpoch":
		return "timestamp"
	case strings.HasPrefix(field, "Temperature.") || strings.HasPrefix(field, "ProbeTemperature."):
		return "temperature"
	case field == "DeviceId" || field == "DeviceName":
		return "identity"
	}
	return ""
}

var protectedStrategies = map[string][]string{
	"timestamp":   {"scale"},
	"temperature": {"coerce_number", "fahrenheit_to_celsius"},
	"identity":    {"resolve_device_by_name"},
}

func checkProtected(r *Rule) error {
	if r.Field == "*" {
		if r.When.NumericString == nil || !*r.When.NumericString {
			return fmt.Errorf("wildcard field * requires when.numeric_string: true")
		}
		if r.Action == actionFix && r.Strategy != "coerce_number" {
			return fmt.Errorf("wildcard field * allows only the coerce_number strategy")
		}
		if r.Action == actionNullify || r.Action == actionDropSection {
			return fmt.Errorf("wildcard field * cannot %s", r.Action)
		}
		return nil
	}
	kind := protectedKind(r.Field)
	if kind == "" {
		return nil
	}
	switch r.Action {
	case actionDropSection:
		return fmt.Errorf("protected field %s cannot use drop_section", r.Field)
	case actionNullify:
		if kind != "temperature" {
			return fmt.Errorf("protected field %s cannot be nullified", r.Field)
		}
	case actionFix:
		for _, s := range protectedStrategies[kind] {
			if s == r.Strategy {
				return nil
			}
		}
		return fmt.Errorf("protected field %s does not allow strategy %q", r.Field, r.Strategy)
	}
	return nil
}

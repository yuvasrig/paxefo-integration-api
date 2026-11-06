package rules

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// This file holds the pure operations the admin console uses to change a ruleset from forms (turn a rule off, edit its
// thresholds, add or delete a rule, set a tenant's override), so none of that needs hand-written YAML. Every operation
// changes the ruleset it is given; the caller works on a Clone and checks it with Validate before publishing.

// Marshal writes a ruleset as YAML.
func Marshal(rs *Ruleset) ([]byte, error) {
	b, err := yaml.Marshal(rs)
	if err != nil {
		return nil, fmt.Errorf("rules: marshal: %w", err)
	}
	return b, nil
}

// Validate checks a ruleset exactly as publishing does: it must survive being written and read back.
func Validate(rs *Ruleset) error {
	b, err := Marshal(rs)
	if err != nil {
		return err
	}
	_, err = Parse(b)
	return err
}

// Clone returns an independent copy, safe to edit while the original is in force.
func Clone(rs *Ruleset) (*Ruleset, error) {
	b, err := Marshal(rs)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Find returns the rule with the id, or nil.
func (rs *Ruleset) Find(id string) *Rule { return rs.find(id) }

// SetDisabled switches a rule off or on, keeping its action for when it is switched back on.
func (rs *Ruleset) SetDisabled(id string, off bool) error {
	r := rs.find(id)
	if r == nil {
		return fmt.Errorf("rules: no rule %q", id)
	}
	r.Disabled = off
	return nil
}

// Edit holds the parts of a rule the console lets you change. A nil field is left as it is.
type Edit struct {
	Code     *string
	Severity *string
	Action   *string
	Section  *string
	Lt       *float64
	In       *[]float64
	Outside  *[]float64
	AfterNow *string
	Factor   *float64 // the multiplier of a "scale" fix
}

// Apply changes the rule. Its condition is only changed where the rule already has that kind of condition, so
// editing a threshold can not turn a rule into a different kind of check.
func (rs *Ruleset) Edit(id string, e Edit) error {
	r := rs.find(id)
	if r == nil {
		return fmt.Errorf("rules: no rule %q", id)
	}
	if e.Code != nil {
		r.Code = strings.TrimSpace(*e.Code)
	}
	if e.Severity != nil {
		r.Severity = *e.Severity
	}
	if e.Action != nil && *e.Action != r.Action {
		r.Action = *e.Action
		if r.Action != actionFix { // a strategy only belongs to a fix
			r.Strategy, r.Params = "", nil
		}
	}
	if e.Section != nil {
		r.Section = *e.Section
	}
	if r.Action != actionDropSection {
		r.Section = ""
	}
	if e.Lt != nil && r.When.Lt != nil {
		r.When.Lt = e.Lt
	}
	if e.In != nil && len(r.When.In) > 0 {
		r.When.In = *e.In
	}
	if e.Outside != nil && len(r.When.Outside) > 0 {
		r.When.Outside = *e.Outside
	}
	if e.AfterNow != nil && r.When.AfterNow != "" {
		r.When.AfterNow = *e.AfterNow
	}
	if e.Factor != nil && r.Strategy == "scale" {
		if r.Params == nil {
			r.Params = map[string]any{}
		}
		r.Params["factor"] = *e.Factor
	}
	return nil
}

// Add appends a rule; its id must be new.
func (rs *Ruleset) Add(r Rule) error {
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("rules: a rule needs a name")
	}
	if rs.find(r.ID) != nil {
		return fmt.Errorf("rules: there is already a rule called %q", r.ID)
	}
	rs.Rules = append(rs.Rules, r)
	return nil
}

// Delete removes a rule and any tenant override of it.
func (rs *Ruleset) Delete(id string) error {
	i := rs.indexOf(id)
	if i < 0 {
		return fmt.Errorf("rules: no rule %q", id)
	}
	rs.Rules = append(rs.Rules[:i], rs.Rules[i+1:]...)
	for oi := range rs.Overrides {
		kept := rs.Overrides[oi].Rules[:0]
		for _, ro := range rs.Overrides[oi].Rules {
			if ro.ID != id {
				kept = append(kept, ro)
			}
		}
		rs.Overrides[oi].Rules = kept
	}
	rs.tidyOverrides()
	return nil
}

// Move shifts a rule up (negative) or down (positive) by one place. Rules run in order, so order matters.
func (rs *Ruleset) Move(id string, delta int) error {
	i := rs.indexOf(id)
	if i < 0 {
		return fmt.Errorf("rules: no rule %q", id)
	}
	j := i + delta
	if j < 0 || j >= len(rs.Rules) {
		return nil
	}
	rs.Rules[i], rs.Rules[j] = rs.Rules[j], rs.Rules[i]
	return nil
}

func (rs *Ruleset) indexOf(id string) int {
	for i := range rs.Rules {
		if rs.Rules[i].ID == id {
			return i
		}
	}
	return -1
}

// TenantOverride returns a tenant's override of a rule (empty fields when it has none).
func (rs *Ruleset) TenantOverride(tenant, ruleID string) RuleOverride {
	for _, o := range rs.Overrides {
		if o.Scope.Tenant == tenant {
			for _, ro := range o.Rules {
				if ro.ID == ruleID {
					return ro
				}
			}
		}
	}
	return RuleOverride{}
}

// SetOverride makes a rule behave differently for one tenant. Setting every field empty removes the override.
func (rs *Ruleset) SetOverride(tenant, ruleID string, ro RuleOverride) error {
	if rs.find(ruleID) == nil {
		return fmt.Errorf("rules: no rule %q", ruleID)
	}
	ro.ID = ruleID
	empty := ro.Action == "" && ro.Strategy == "" && ro.Severity == ""
	var scope *Override
	for i := range rs.Overrides {
		if rs.Overrides[i].Scope.Tenant == tenant {
			scope = &rs.Overrides[i]
		}
	}
	if scope == nil {
		if empty {
			return nil
		}
		rs.Overrides = append(rs.Overrides, Override{})
		scope = &rs.Overrides[len(rs.Overrides)-1]
		scope.Scope.Tenant = tenant
	}
	replaced := false
	kept := scope.Rules[:0]
	for _, existing := range scope.Rules {
		switch {
		case existing.ID != ruleID:
			kept = append(kept, existing)
		case !empty:
			kept = append(kept, ro)
			replaced = true
		}
	}
	scope.Rules = kept
	if !empty && !replaced {
		scope.Rules = append(scope.Rules, ro)
	}
	rs.tidyOverrides()
	return nil
}

// tidyOverrides drops tenants that no longer override anything.
func (rs *Ruleset) tidyOverrides() {
	kept := rs.Overrides[:0]
	for _, o := range rs.Overrides {
		if len(o.Rules) > 0 {
			kept = append(kept, o)
		}
	}
	rs.Overrides = kept
}

var versionSuffix = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\.(\d+)$`)

// NextVersion picks the version for a ruleset published today: YYYY-MM-DD.N with the lowest N above every version of
// today already in use (so versions never collide and read in order). A version is recorded on every event, which is
// why the console chooses it, instead of asking a person to remember to change it.
func NextVersion(used []string, now time.Time) string {
	day := now.UTC().Format("2006-01-02")
	max := 0
	for _, v := range used {
		if m := versionSuffix.FindStringSubmatch(v); m != nil && m[1] == day {
			if n, _ := strconv.Atoi(m[2]); n > max {
				max = n
			}
		}
	}
	return fmt.Sprintf("%s.%d", day, max+1)
}

// WithOverride returns the rule as a tenant sees it: the rule with the tenant's override applied.
func (r Rule) WithOverride(o RuleOverride) Rule { return mergeOverride(r, o) }

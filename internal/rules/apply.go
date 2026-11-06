package rules

import (
	"fmt"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
)

// DeviceResolver finds the IMEI for a device name within one tenant. ok is false when the
// name is unknown or maps to more than one IMEI (F5-R3). Ingest loads the answer before
// calling Apply, so Apply itself does no I/O.
type DeviceResolver interface {
	ResolveByName(name string) (imei string, ok bool)
}

// Scope is the context a ruleset is applied in. Time is passed in; Apply never reads a clock.
type Scope struct {
	Tenant        string
	Now           time.Time
	Devices       DeviceResolver
	NumericFields []string // paths that field "*" expands to
}

// Result is the outcome of applying a ruleset.
type Result struct {
	Doc        doc.Doc         // edited copy; the input is never modified
	Flags      []Flag          // every fix, nullification and finding
	Rejections []Problem       // reasons the record is unusable
	Dropped    map[string]bool // sections ("location", "sensor") the record must not emit
}

// Apply is where the configured rules are enforced. It runs the tenant's effective rules (the ruleset plus any
// overrides for that tenant) in file order on a copy of d, and reports what each rule did. The ruleset is whatever
// the caller passes in, so a ruleset swapped in at runtime takes effect on the next call.
// It never stops at a rejection: flags raised alongside a rejection are still returned (F3-R7).
func Apply(d doc.Doc, rs *Ruleset, sc Scope) Result {
	res := Result{Doc: d.Clone(), Dropped: map[string]bool{}}
	rules := rs.effective(sc.Tenant)
	for i := range rules {
		r := &rules[i]
		if r.Action == actionIgnore {
			continue
		}
		paths := []string{r.Field}
		if r.Field == "*" {
			paths = sc.NumericFields
		}
		for _, path := range paths {
			val, present := res.Doc.Get(path)
			t := target{d: res.Doc, path: path, val: val, present: present, sc: sc}
			if r.matches(t) {
				res.act(r, t)
			}
		}
	}
	return res
}

func (res *Result) act(r *Rule, t target) {
	flag := Flag{Code: r.Code, Severity: r.Severity, Field: t.path, Value: t.val, RuleID: r.ID}
	switch r.Action {
	case actionFix:
		newVal, changed := strategies[r.Strategy](strategyInput{t: t, rule: r})
		if !changed || !res.Doc.Set(t.path, newVal) {
			return
		}
		flag.NewValue, flag.Action = newVal, ActionFixed
	case actionNullify:
		if !res.Doc.Set(t.path, nil) {
			return
		}
		flag.Action = ActionNullified
	case actionDropSection:
		res.Dropped[r.Section] = true
		flag.Action = fmt.Sprintf("%s_skipped", r.Section)
	case actionFlag:
		flag.Action = ActionKept
	case actionReject:
		res.Rejections = append(res.Rejections, Problem{
			Code: r.Code, Field: t.path, Message: fmt.Sprintf("rule %s rejected the record", r.ID),
		})
		return
	}
	res.Flags = append(res.Flags, flag)
}

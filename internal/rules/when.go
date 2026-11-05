package rules

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
)

var (
	numericStringRe = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)
	imeiRe          = regexp.MustCompile(`^[0-9]{15}$`)
)

// target is the field value a rule is being evaluated against.
type target struct {
	d       doc.Doc
	path    string
	val     any
	present bool
	sc      Scope
}

// number returns v as a float when it is a JSON number (not a string).
func number(v any) (float64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	f, err := n.Float64()
	return f, err == nil
}

func text(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	}
	return "", false
}

// validIMEI accepts a 15-digit string or an integer number with 15 digits.
func validIMEI(v any) bool {
	s, ok := text(v)
	return ok && imeiRe.MatchString(s)
}

// matches reports whether every condition of the rule holds for t.
func (r *Rule) matches(t target) bool {
	w := r.When
	if w.Lt != nil {
		if f, ok := number(t.val); !ok || !(f < *w.Lt) {
			return false
		}
	}
	if len(w.In) > 0 {
		f, ok := number(t.val)
		if !ok || !contains(w.In, f) {
			return false
		}
	}
	if len(w.Outside) == 2 {
		if f, ok := number(t.val); !ok || (f >= w.Outside[0] && f <= w.Outside[1]) {
			return false
		}
	}
	if w.IsNull != nil && (!t.present || t.val == nil) != *w.IsNull {
		return false
	}
	if w.SiblingPresent != "" {
		if v, ok := t.d.Get(w.SiblingPresent); !ok || v == nil {
			return false
		}
	}
	if w.NumericString != nil {
		s, ok := t.val.(string)
		if (ok && numericStringRe.MatchString(strings.TrimSpace(s))) != *w.NumericString {
			return false
		}
	}
	if w.InvalidIMEI != nil && !validIMEI(t.val) != *w.InvalidIMEI {
		return false
	}
	if w.UnresolvedBy != "" && t.resolved() {
		return false
	}
	if w.ResolvedBy != "" && !t.resolved() {
		return false
	}
	if w.AfterNow != "" {
		f, ok := number(t.val)
		if !ok || int64(f) <= t.sc.Now.Add(r.afterNow).UnixMilli() {
			return false
		}
	}
	if w.DiffersFrom != "" && !r.differs(t) {
		return false
	}
	if w.Present != nil {
		s, isStr := t.val.(string)
		has := t.present && t.val != nil && !(isStr && s == "")
		if has != *w.Present {
			return false
		}
	}
	return true
}

// resolved reports whether DeviceName maps to exactly one known IMEI.
func (t target) resolved() bool {
	_, ok := t.resolveName()
	return ok
}

func (t target) resolveName() (string, bool) {
	name, _ := t.d.Get("DeviceName")
	s, ok := name.(string)
	if !ok || s == "" || t.sc.Devices == nil {
		return "", false
	}
	return t.sc.Devices.ResolveByName(s)
}

// differs compares the target with another field. With a tolerance the values are an epoch
// (ms) and an RFC 3339 time; otherwise they are compared as text. Absent values never differ.
func (r *Rule) differs(t target) bool {
	other, ok := t.d.Get(r.When.DiffersFrom)
	if !ok || other == nil || t.val == nil {
		return false
	}
	if r.When.Tolerance != "" {
		ms, ok1 := number(t.val)
		str, ok2 := other.(string)
		if !ok1 || !ok2 {
			return false
		}
		parsed, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return false
		}
		diff := time.Duration(int64(ms)-parsed.UnixMilli()) * time.Millisecond
		if diff < 0 {
			diff = -diff
		}
		return diff > r.tolerance
	}
	a, ok1 := text(t.val)
	b, ok2 := text(other)
	return ok1 && ok2 && a != b
}

func contains(list []float64, f float64) bool {
	for _, x := range list {
		if x == f {
			return true
		}
	}
	return false
}

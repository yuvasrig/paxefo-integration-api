package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// The friendly rules page. Every rule is shown as a sentence; thresholds are edited in a form; rules can be switched
// off, reordered, added from dropdowns and deleted; and a tenant can override a rule's action or severity. Each change
// is made on a copy of the ruleset in force, checked exactly as publishing checks it, given a new version number
// automatically, and published, so nobody has to write or version YAML. The YAML editor is still there, under Advanced.

// ruleRow is one rule prepared for display and editing.
type ruleRow struct {
	Index                  int
	ID, Field, FieldLabel  string
	Condition, Outcome     string
	Code, Severity, Action string
	Section                string
	Disabled               bool
	First, Last            bool

	// Editable thresholds: only the kinds of condition this rule has.
	HasLt, HasIn, HasOutside, HasAfter, HasFactor bool
	Lt, In, OutMin, OutMax, After, Factor         string

	// For a tenant scope: the tenant's override, and the rule as that tenant sees it.
	Override         rules.RuleOverride
	HasOverride      bool
	EffectiveOutcome string
	EffectiveOff     bool
}

func num(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func ruleRows(rs *rules.Ruleset, scope string) []ruleRow {
	rows := make([]ruleRow, 0, len(rs.Rules))
	for i, r := range rs.Rules {
		row := ruleRow{
			Index: i + 1, ID: r.ID, Field: r.Field, FieldLabel: rules.FieldLabel(r.Field),
			Condition: r.Condition(), Outcome: r.Outcome(), Code: r.Code, Severity: r.Severity, Action: r.Action,
			Section: r.Section, Disabled: r.Disabled, First: i == 0, Last: i == len(rs.Rules)-1,
		}
		if r.When.Lt != nil {
			row.HasLt, row.Lt = true, num(*r.When.Lt)
		}
		if len(r.When.In) > 0 {
			row.HasIn = true
			var parts []string
			for _, n := range r.When.In {
				parts = append(parts, num(n))
			}
			row.In = strings.Join(parts, ", ")
		}
		if len(r.When.Outside) == 2 {
			row.HasOutside, row.OutMin, row.OutMax = true, num(r.When.Outside[0]), num(r.When.Outside[1])
		}
		if r.When.AfterNow != "" {
			row.HasAfter, row.After = true, r.When.AfterNow
		}
		if f, ok := r.Params["factor"]; ok && r.Strategy == "scale" {
			if n, ok := f.(float64); ok {
				row.HasFactor, row.Factor = true, num(n)
			} else if n, ok := f.(int); ok {
				row.HasFactor, row.Factor = true, strconv.Itoa(n)
			}
		}
		if scope != "" {
			row.Override = rs.TenantOverride(scope, r.ID)
			row.HasOverride = row.Override.Action != "" || row.Override.Severity != "" || row.Override.Strategy != ""
			eff := r.WithOverride(row.Override)
			row.EffectiveOutcome = eff.Outcome()
			row.EffectiveOff = r.Disabled || eff.Action == "ignore"
		}
		rows = append(rows, row)
	}
	return rows
}

// rulesView prepares the page: the friendly table for the scope ("" = all tenants, else one tenant), the YAML for the
// advanced editor, the version history and the tenant picker.
func (c *Console) rulesView(ctx context.Context, yaml string, v view) (view, error) {
	hist, err := c.Store.RulesetHistory(ctx, 20)
	if err != nil {
		return v, err
	}
	infos, err := c.Store.ListTenantInfo(ctx)
	if err != nil {
		return v, err
	}
	for _, i := range infos {
		v.Tenants = append(v.Tenants, TenantView{Info: i})
	}
	l := c.Rules.Loaded()
	v.Title, v.Nav, v.History, v.Refresh = "Rules", "rules", hist, c.refresh()
	v.Active = rulesInfo{Version: l.Ruleset.Version, Source: l.Source, LoadedAt: l.LoadedAt, Rules: len(l.Ruleset.Rules)}
	v.RuleRows = ruleRows(l.Ruleset, v.Scope)
	v.Fields = rules.KnownFields()
	for _, r := range v.RuleRows {
		if r.Disabled {
			v.OffRules++
		}
	}
	if yaml == "" {
		if b, err := rules.Marshal(l.Ruleset); err == nil {
			yaml = string(b)
		}
		if row, ok, err := c.Store.ActiveRuleset(ctx); err != nil {
			return v, err
		} else if ok {
			yaml = row.YAML // keeps the comments of a ruleset that was written by hand
		}
	}
	v.YAML = yaml
	return v, nil
}

func (c *Console) rulesPage(w http.ResponseWriter, r *http.Request) {
	yaml := ""
	if from := r.URL.Query().Get("from"); from != "" { // load an older version into the advanced editor
		id, _ := strconv.ParseInt(from, 10, 64)
		hist, err := c.Store.RulesetHistory(r.Context(), 100)
		if err != nil {
			c.fail(w, r, "could not read rule history", err)
			return
		}
		for _, h := range hist {
			if h.ID == id {
				yaml = h.YAML
			}
		}
	}
	c.renderRules(w, r, yaml, view{Scope: r.URL.Query().Get("tenant"), Flash: r.URL.Query().Get("done")})
}

func (c *Console) renderRules(w http.ResponseWriter, r *http.Request, yaml string, v view) {
	v, err := c.rulesView(r.Context(), yaml, v)
	if err != nil {
		c.fail(w, r, "could not read rules", err)
		return
	}
	if v.Scope != "" {
		known := false
		for _, t := range v.Tenants {
			known = known || t.Info.ID == v.Scope
		}
		if !known {
			http.NotFound(w, r)
			return
		}
	}
	c.render(w, r, "rules", v)
}

func rulesURL(scope, done string) string {
	q := "done=" + urlText(done)
	if scope != "" {
		q = "tenant=" + url.QueryEscape(scope) + "&" + q
	}
	return "/admin/rules?" + q
}

// change makes one edit to a copy of the ruleset in force and publishes the result under a new version. The edit
// function returns a sentence saying what was done, shown afterwards. Any problem (including a guardrail on a protected
// field) is shown on the page and nothing is published.
func (c *Console) change(w http.ResponseWriter, r *http.Request, scope string, edit func(rs *rules.Ruleset) (string, error)) {
	ctx := r.Context()
	for attempt := 0; attempt < 3; attempt++ {
		rs, err := rules.Clone(c.Rules.Current())
		if err != nil {
			c.fail(w, r, "could not copy the rules", err)
			return
		}
		done, err := edit(rs)
		if err != nil {
			c.renderRules(w, r, "", view{Scope: scope, Error: "Nothing was changed: " + err.Error()})
			return
		}
		hist, err := c.Store.RulesetHistory(ctx, 500)
		if err != nil {
			c.fail(w, r, "could not read rule history", err)
			return
		}
		used := []string{c.Rules.Current().Version}
		for _, h := range hist {
			used = append(used, h.Version)
		}
		rs.Version = rules.NextVersion(used, c.Now())
		if err := rules.Validate(rs); err != nil {
			c.renderRules(w, r, "", view{Scope: scope, Error: "Nothing was changed: " + err.Error()})
			return
		}
		b, err := rules.Marshal(rs)
		if err != nil {
			c.fail(w, r, "could not write the rules", err)
			return
		}
		if _, err := c.Store.PublishRuleset(ctx, rs.Version, string(b), "admin-console"); err != nil {
			if errors.Is(err, store.ErrVersionExists) { // someone else published at the same moment: pick the next number
				continue
			}
			c.fail(w, r, "could not publish the rules", err)
			return
		}
		c.reloadRules(ctx)
		c.Log.Info("admin: rules changed", "version", rs.Version, "change", done)
		http.Redirect(w, r, rulesURL(scope, fmt.Sprintf("%s Published as version %s; every instance adopts it within %d seconds.", done, rs.Version, c.refresh())), http.StatusSeeOther)
		return
	}
	c.renderRules(w, r, "", view{Scope: scope, Error: "Could not publish: another change was published at the same time. Try again."})
}

func (c *Console) rulesToggle(w http.ResponseWriter, r *http.Request) {
	id, off := r.FormValue("id"), r.FormValue("off") == "1"
	c.change(w, r, "", func(rs *rules.Ruleset) (string, error) {
		if err := rs.SetDisabled(id, off); err != nil {
			return "", err
		}
		if off {
			return "Switched " + id + " off.", nil
		}
		return "Switched " + id + " on.", nil
	})
}

func (c *Console) rulesMove(w http.ResponseWriter, r *http.Request) {
	id, delta := r.FormValue("id"), 1
	if r.FormValue("dir") == "up" {
		delta = -1
	}
	c.change(w, r, "", func(rs *rules.Ruleset) (string, error) {
		return "Moved " + id + ".", rs.Move(id, delta)
	})
}

func (c *Console) rulesDelete(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
	c.change(w, r, "", func(rs *rules.Ruleset) (string, error) {
		return "Deleted " + id + ".", rs.Delete(id)
	})
}

// numList reads "a, b, c" as numbers.
func numList(s string) ([]float64, error) {
	var out []float64
	for _, p := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == ';' }) {
		f, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", p)
		}
		out = append(out, f)
	}
	if len(out) == 0 {
		return nil, errors.New("give at least one number")
	}
	return out, nil
}

func optNum(r *http.Request, name string) (*float64, error) {
	raw := strings.TrimSpace(r.FormValue(name))
	if raw == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("%q is not a number", raw)
	}
	return &f, nil
}

func optStr(r *http.Request, name string) *string {
	if !r.Form.Has(name) {
		return nil
	}
	v := strings.TrimSpace(r.FormValue(name))
	return &v
}

// rulesEdit saves the edit form of one rule: its flag code, severity, action and the thresholds it has.
func (c *Console) rulesEdit(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	id := r.FormValue("id")
	var e rules.Edit
	e.Code, e.Severity, e.Action, e.Section, e.AfterNow = optStr(r, "code"), optStr(r, "severity"), optStr(r, "action"), optStr(r, "section"), optStr(r, "after_now")
	var err error
	bad := func(what string, err error) {
		c.renderRules(w, r, "", view{Error: "Nothing was changed: " + what + ": " + err.Error()})
	}
	if e.Lt, err = optNum(r, "lt"); err != nil {
		bad("below", err)
		return
	}
	if e.Factor, err = optNum(r, "factor"); err != nil {
		bad("multiplier", err)
		return
	}
	if raw := strings.TrimSpace(r.FormValue("in")); raw != "" {
		l, err := numList(raw)
		if err != nil {
			bad("values", err)
			return
		}
		e.In = &l
	}
	lo, err1 := optNum(r, "out_min")
	hi, err2 := optNum(r, "out_max")
	if err1 != nil || err2 != nil {
		bad("range", errors.New("the range needs two numbers"))
		return
	}
	if lo != nil || hi != nil {
		if lo == nil || hi == nil {
			bad("range", errors.New("give both ends of the range"))
			return
		}
		e.Outside = &[]float64{*lo, *hi}
	}
	c.change(w, r, "", func(rs *rules.Ruleset) (string, error) {
		if err := rs.Edit(id, e); err != nil {
			return "", err
		}
		return "Saved " + id + ": " + rs.Find(id).Sentence(), nil
	})
}

// checks are the conditions the add-a-rule form offers.
var checkKinds = map[string]bool{"missing": true, "below": true, "outside": true, "oneof": true, "numtext": true, "future": true, "differs": true}

// ruleFromForm builds a new rule from the add form's dropdowns.
func ruleFromForm(r *http.Request) (rules.Rule, error) {
	field := strings.TrimSpace(r.FormValue("field"))
	if custom := strings.TrimSpace(r.FormValue("field_custom")); custom != "" {
		field = custom
	}
	if field == "" {
		return rules.Rule{}, errors.New("pick the field the rule checks")
	}
	rule := rules.Rule{Field: field, Action: r.FormValue("action"), Severity: r.FormValue("severity")}
	kind := r.FormValue("check")
	if !checkKinds[kind] {
		return rule, errors.New("pick what to check")
	}
	t := func(b bool) *bool { return &b }
	switch kind {
	case "missing":
		rule.When.IsNull = t(true)
	case "numtext":
		rule.When.NumericString = t(true)
	case "below":
		v, err := optNum(r, "v1")
		if err != nil || v == nil {
			return rule, errors.New("\"is below\" needs a number")
		}
		rule.When.Lt = v
	case "outside":
		lo, e1 := optNum(r, "v1")
		hi, e2 := optNum(r, "v2")
		if e1 != nil || e2 != nil || lo == nil || hi == nil {
			return rule, errors.New("\"is outside a range\" needs two numbers")
		}
		rule.When.Outside = []float64{*lo, *hi}
	case "oneof":
		l, err := numList(r.FormValue("list"))
		if err != nil {
			return rule, fmt.Errorf("\"is one of\" needs numbers: %w", err)
		}
		rule.When.In = l
	case "future":
		m, err := optNum(r, "v1")
		if err != nil || m == nil || *m <= 0 {
			return rule, errors.New("\"in the future\" needs a number of minutes")
		}
		rule.When.AfterNow = num(*m) + "m"
	case "differs":
		other := strings.TrimSpace(r.FormValue("other"))
		if other == "" {
			return rule, errors.New("\"differs from\" needs the other field")
		}
		rule.When.Present, rule.When.DiffersFrom = t(true), other
	}
	if rule.Action == "drop_section" {
		rule.Section = r.FormValue("section")
	}
	if rule.Action == "" || rule.Action == "fix" {
		return rule, errors.New("pick what the rule should do")
	}
	if rule.Severity == "" {
		rule.Severity = rules.SeverityWarn
	}
	rule.ID = strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(r.FormValue("id")), " ", "-"))
	rule.Code = strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(r.FormValue("code")), " ", "_"))
	return rule, nil
}

func (c *Console) rulesAdd(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	rule, err := ruleFromForm(r)
	if err != nil {
		c.renderRules(w, r, "", view{Error: "The rule was not added: " + err.Error()})
		return
	}
	c.change(w, r, "", func(rs *rules.Ruleset) (string, error) {
		if rule.ID == "" { // a name is chosen when none is given
			for n := 1; ; n++ {
				if id := fmt.Sprintf("CUSTOM-%d", n); rs.Find(id) == nil {
					rule.ID = id
					break
				}
			}
		}
		if rule.Code == "" {
			rule.Code = strings.ReplaceAll(rule.ID, "-", "_")
		}
		if err := rs.Add(rule); err != nil {
			return "", err
		}
		return "Added " + rule.ID + ": " + rule.Sentence(), nil
	})
}

// rulesOverride sets (or clears) one tenant's override of a rule: its action and severity for that tenant only.
func (c *Console) rulesOverride(w http.ResponseWriter, r *http.Request) {
	tenant, id := r.FormValue("tenant"), r.FormValue("id")
	if ok, err := c.Store.TenantExists(r.Context(), tenant); err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	ro := rules.RuleOverride{Action: r.FormValue("action"), Severity: r.FormValue("severity")}
	c.change(w, r, tenant, func(rs *rules.Ruleset) (string, error) {
		if err := rs.SetOverride(tenant, id, ro); err != nil {
			return "", err
		}
		if ro == (rules.RuleOverride{}) {
			return tenant + " now follows the global rule " + id + ".", nil
		}
		return "Saved " + id + " for " + tenant + ": " + rs.Find(id).WithOverride(ro).Outcome() + ".", nil
	})
}

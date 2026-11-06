package rules

import (
	"strings"
	"testing"
)

func TestMarshalRoundTripsTheDefaultRules(t *testing.T) {
	rs := defaultRules(t)
	b, err := Marshal(rs)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Parse(b)
	if err != nil || len(back.Rules) != len(rs.Rules) || back.Version != rs.Version {
		t.Fatalf("a ruleset must survive being written and read back: %v", err)
	}
	for i := range rs.Rules {
		if rs.Rules[i].Sentence() != back.Rules[i].Sentence() {
			t.Errorf("rule %s changed in the round trip", rs.Rules[i].ID)
		}
	}
}

func TestCloneIsIndependent(t *testing.T) {
	rs := defaultRules(t)
	c, err := Clone(rs)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetDisabled("TEMP-SENTINEL", true); err != nil {
		t.Fatal(err)
	}
	if rs.Find("TEMP-SENTINEL").Disabled {
		t.Fatal("editing the copy must not change the ruleset in force")
	}
}

func TestDisabledRuleDoesNothingAndKeepsItsAction(t *testing.T) {
	rs := defaultRules(t)
	d := parseDoc(t, `{"DeviceId":"863257063350583","EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":-32768},"ProbeTemperature":{"Celsius":5}}`)
	on := Apply(d, rs, scope())
	if v, _ := on.Doc.Get("Temperature.Celsius"); v != nil {
		t.Fatalf("the sentinel is cleared while the rule is on: %v", v)
	}
	c, _ := Clone(rs)
	_ = c.SetDisabled("TEMP-SENTINEL", true)
	off := Apply(d, c, scope())
	if v, _ := off.Doc.Get("Temperature.Celsius"); v == nil {
		t.Fatal("a rule that is off does not run")
	}
	if c.Find("TEMP-SENTINEL").Action != actionNullify {
		t.Fatal("switching a rule off keeps its action")
	}
	_ = c.SetDisabled("TEMP-SENTINEL", false)
	if v, _ := Apply(d, c, scope()).Doc.Get("Temperature.Celsius"); v != nil {
		t.Fatal("switching it back on restores it")
	}
	if err := c.SetDisabled("NOPE", true); err == nil {
		t.Fatal("unknown rule")
	}
}

func TestEditThresholdsAndOutcome(t *testing.T) {
	rs := defaultRules(t)
	c, _ := Clone(rs)
	if err := c.Edit("HUMIDITY-RANGE", Edit{Outside: &[]float64{5, 95}, Severity: ptr("error"), Code: ptr(" HUMIDITY_BAD ")}); err != nil {
		t.Fatal(err)
	}
	r := c.Find("HUMIDITY-RANGE")
	if r.Sentence() != "When Humidity (%) is outside 5 to 95, clear the value and flag it HUMIDITY_BAD (error)." {
		t.Fatal(r.Sentence())
	}
	if err := Validate(c); err != nil {
		t.Fatal(err)
	}
	if err := c.Edit("TS-FUTURE", Edit{AfterNow: ptr("10m")}); err != nil || c.Find("TS-FUTURE").Condition() != "Reading time (epoch) is more than 10 minutes in the future" {
		t.Fatalf("%v %s", err, c.Find("TS-FUTURE").Condition())
	}
	if err := c.Edit("TS-SECONDS", Edit{Factor: ptr(1000.0), Lt: ptr(1e11)}); err != nil || !strings.Contains(c.Find("TS-SECONDS").Sentence(), "below 100,000,000,000") {
		t.Fatalf("%v %s", err, c.Find("TS-SECONDS").Sentence())
	}
	// An edit only changes kinds of condition the rule already has.
	if err := c.Edit("LUX-NEGATIVE", Edit{Outside: &[]float64{0, 1}, In: &[]float64{1}}); err != nil {
		t.Fatal(err)
	}
	w := c.Find("LUX-NEGATIVE").When
	if len(w.Outside) != 0 || len(w.In) != 0 || w.Lt == nil {
		t.Fatalf("a threshold edit must not turn a rule into a different check: %+v", w)
	}
	if err := c.Edit("NOPE", Edit{}); err == nil {
		t.Fatal("unknown rule")
	}
	// Changing the action drops a strategy that only belongs to a fix.
	if err := c.Edit("TS-SECONDS", Edit{Action: ptr("flag")}); err != nil {
		t.Fatal(err)
	}
	if r := c.Find("TS-SECONDS"); r.Strategy != "" || r.Params != nil || r.Action != actionFlag {
		t.Fatalf("%+v", r)
	}
	if rs.Find("HUMIDITY-RANGE").Code != "OUT_OF_RANGE" {
		t.Fatal("the original is untouched")
	}
}

func TestEditCanNotBreakAProtectedField(t *testing.T) {
	c, _ := Clone(defaultRules(t))
	_ = c.Edit("TS-FUTURE", Edit{Action: ptr("nullify"), Severity: ptr("warn")}) // a timestamp must not be cleared
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("the guardrails still apply to edits from the console: %v", err)
	}
}

func TestAddDeleteAndMove(t *testing.T) {
	c, _ := Clone(defaultRules(t))
	n := len(c.Rules)
	r := Rule{ID: "BATTERY-LOW", Code: "BATTERY_LOW", Field: "Battery.Percentage", When: When{Lt: ptr(10.0)}, Action: actionFlag, Severity: SeverityWarn}
	if err := c.Add(r); err != nil || len(c.Rules) != n+1 || Validate(c) != nil {
		t.Fatal("a new rule is appended and valid")
	}
	if err := c.Add(r); err == nil {
		t.Fatal("a duplicate name is refused")
	}
	if err := c.Add(Rule{}); err == nil {
		t.Fatal("a rule needs a name")
	}
	if c.Find("BATTERY-LOW").Sentence() != "When Battery (%) is below 10, keep the value and flag it BATTERY_LOW (warning)." {
		t.Fatal(c.Find("BATTERY-LOW").Sentence())
	}
	if err := c.Move("BATTERY-LOW", -1); err != nil || c.Rules[n-1].ID != "BATTERY-LOW" {
		t.Fatal("moved up")
	}
	if err := c.Move("BATTERY-LOW", 1); err != nil || c.Rules[n].ID != "BATTERY-LOW" {
		t.Fatal("moved down")
	}
	if err := c.Move(c.Rules[0].ID, -1); err != nil || c.Rules[0].ID != "NUMERIC-STRING" {
		t.Fatal("the first rule can not move up, and that is not an error")
	}
	if err := c.SetOverride("acme", "BATTERY-LOW", RuleOverride{Action: actionReject}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("BATTERY-LOW"); err != nil || c.Find("BATTERY-LOW") != nil || len(c.Rules) != n || len(c.Overrides) != 0 {
		t.Fatalf("deleting a rule also removes tenant overrides of it: %d overrides", len(c.Overrides))
	}
	if Validate(c) != nil {
		t.Fatal("still valid")
	}
	if err := c.Delete("BATTERY-LOW"); err == nil {
		t.Fatal("already gone")
	}
}

func TestTenantOverrides(t *testing.T) {
	c, _ := Clone(defaultRules(t))
	if got := c.TenantOverride("acme", "HUMIDITY-RANGE"); got.Action != "" {
		t.Fatal("no override yet")
	}
	_ = c.SetOverride("acme", "HUMIDITY-RANGE", RuleOverride{Action: actionIgnore})
	_ = c.SetOverride("acme", "LUX-NEGATIVE", RuleOverride{Severity: SeverityError})
	_ = c.SetOverride("bioco", "HUMIDITY-RANGE", RuleOverride{Action: actionReject})
	if Validate(c) != nil || c.TenantOverride("acme", "HUMIDITY-RANGE").Action != actionIgnore || c.TenantOverride("bioco", "HUMIDITY-RANGE").Action != actionReject {
		t.Fatal("overrides are per tenant")
	}
	// Setting an override again replaces it.
	_ = c.SetOverride("acme", "HUMIDITY-RANGE", RuleOverride{Action: actionFlag})
	if c.TenantOverride("acme", "HUMIDITY-RANGE").Action != actionFlag || len(c.Overrides[0].Rules) != 2 {
		t.Fatalf("%+v", c.Overrides)
	}
	// It takes effect for that tenant only.
	d := parseDoc(t, `{"DeviceId":"863257063350583","EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":5},"Humidity":{"Percentage":150}}`)
	sc := scope()
	sc.Tenant = "acme"
	if v, _ := Apply(d, c, sc).Doc.Get("Humidity.Percentage"); v == nil {
		t.Fatal("acme's override only flags, so the value is kept")
	}
	sc.Tenant = "reviewer"
	if v, _ := Apply(d, c, sc).Doc.Get("Humidity.Percentage"); v != nil {
		t.Fatal("other tenants still clear it")
	}
	// Clearing removes the override, and an emptied tenant disappears.
	_ = c.SetOverride("acme", "HUMIDITY-RANGE", RuleOverride{})
	_ = c.SetOverride("acme", "LUX-NEGATIVE", RuleOverride{})
	if len(c.Overrides) != 1 || c.Overrides[0].Scope.Tenant != "bioco" {
		t.Fatalf("%+v", c.Overrides)
	}
	_ = c.SetOverride("nobody", "HUMIDITY-RANGE", RuleOverride{}) // clearing what is not there is fine
	if err := c.SetOverride("acme", "NOPE", RuleOverride{Action: actionFlag}); err == nil {
		t.Fatal("unknown rule")
	}
}

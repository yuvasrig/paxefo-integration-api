package rules

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestDefaultRuleset(t *testing.T) {
	rs := defaultRules(t)
	if rs.Version == "" || len(rs.Rules) != 16 {
		t.Fatalf("version %q with %d rules, want a version and 16 rules", rs.Version, len(rs.Rules))
	}
}

// The embedded ruleset must be byte-identical to the reviewed contract file.
func TestEmbeddedRulesMatchContract(t *testing.T) {
	want, err := os.ReadFile("../../specs/001-integration-api/contracts/rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(defaultRulesYAML) != string(want) {
		t.Fatal("internal/rules/default_rules.yaml differs from specs/001-integration-api/contracts/rules.yaml")
	}
}

// F13-R2: invalid rulesets fail at load time.
func TestParseValidation_F13R2(t *testing.T) {
	const base = "version: t\nrules:\n"
	tests := []struct {
		name    string
		yaml    string
		wantErr string // empty = valid
	}{
		{"valid minimal", base + `  - { id: A, code: SENSOR_FAULT, field: Humidity.Percentage, when: { lt: 0 }, action: nullify, severity: warn }`, ""},
		{"duplicate id", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }
  - { id: A, code: X, field: Light.Lux, when: { lt: 1 }, action: nullify, severity: warn }`, "duplicate"},
		{"unknown strategy", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: fix, strategy: teleport, severity: warn }`, "unknown strategy"},
		{"fix without strategy", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: fix, severity: warn }`, "strategy"},
		{"unknown action", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: explode, severity: warn }`, "unknown action"},
		{"unknown when key", base + `  - { id: A, code: X, field: Light.Lux, when: { bigger_than: 0 }, action: nullify, severity: warn }`, "bigger_than"},
		{"empty when", base + `  - { id: A, code: X, field: Light.Lux, when: {}, action: nullify, severity: warn }`, "when"},
		{"missing id", base + `  - { code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }`, "id"},
		{"missing code", base + `  - { id: A, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }`, "code"},
		{"missing severity on nullify", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify }`, "severity"},
		{"bad severity", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: loud }`, "severity"},
		{"drop_section needs section", base + `  - { id: A, code: X, field: Location.Latitude, when: { outside: [-90, 90] }, action: drop_section, severity: error }`, "section"},
		{"drop_section bad section", base + `  - { id: A, code: X, field: Location.Latitude, when: { outside: [-90, 90] }, action: drop_section, section: moon, severity: error }`, "section"},
		{"bad duration", base + `  - { id: A, code: X, field: EntryTimeEpoch, when: { after_now: soon }, action: reject }`, "after_now"},
		{"outside needs two numbers", base + `  - { id: A, code: X, field: Light.Lux, when: { outside: [1] }, action: nullify, severity: warn }`, "outside"},
		{"no version", "rules:\n  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }", "version"},
		{"not yaml", "version: [", "yaml"},
		// Protected fields (temperature, timestamp, device identity) accept only allow-listed strategies.
		{"protected: drop_section on temperature", base + `  - { id: A, code: X, field: Temperature.Celsius, when: { lt: 0 }, action: drop_section, section: sensor, severity: warn }`, "protected"},
		{"protected: scale on temperature", base + `  - { id: A, code: X, field: Temperature.Celsius, when: { lt: 0 }, action: fix, strategy: scale, params: { factor: 2 }, severity: warn }`, "protected"},
		{"protected: nullify on timestamp", base + `  - { id: A, code: X, field: EntryTimeEpoch, when: { lt: 0 }, action: nullify, severity: warn }`, "protected"},
		{"protected: scale on DeviceId", base + `  - { id: A, code: X, field: DeviceId, when: { present: true }, action: fix, strategy: scale, params: { factor: 2 }, severity: warn }`, "protected"},
		{"protected: allowed fahrenheit_to_celsius", base + `  - { id: A, code: X, field: Temperature.Celsius, when: { is_null: true, sibling_present: Temperature.Fahrenheit }, action: fix, strategy: fahrenheit_to_celsius, severity: info }`, ""},
		{"protected: allowed scale on timestamp", base + `  - { id: A, code: X, field: EntryTimeEpoch, when: { lt: 1.0e12 }, action: fix, strategy: scale, params: { factor: 1000 }, severity: warn }`, ""},
		{"wildcard only with coerce_number", base + `  - { id: A, code: X, field: "*", when: { numeric_string: true }, action: fix, strategy: scale, params: { factor: 2 }, severity: info }`, "wildcard"},
		{"override unknown rule", base + `  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }
overrides:
  - scope: { tenant: t1 }
    rules:
      - { id: NOPE, action: reject }`, "override"},
		{"override unsafe action", base + `  - { id: A, code: X, field: Temperature.Celsius, when: { lt: 0 }, action: nullify, severity: warn }
overrides:
  - scope: { tenant: t1 }
    rules:
      - { id: A, action: drop_section }`, "protected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)):
				t.Fatalf("error %q does not mention %q", err, tt.wantErr)
			}
		})
	}
}

// Overrides change a rule's action for one tenant only.
func TestOverrides(t *testing.T) {
	rs, err := Parse([]byte(`version: t
rules:
  - { id: NUMERIC-STRING, code: COERCED_TYPE, field: "*", when: { numeric_string: true }, action: fix, strategy: coerce_number, severity: info }
overrides:
  - scope: { tenant: strict }
    rules:
      - { id: NUMERIC-STRING, action: reject }
`))
	if err != nil {
		t.Fatal(err)
	}
	in := `{"DeviceId":"863257063350583","Temperature":{"Celsius":"10.5"}}`

	loose := scope()
	res := Apply(parseDoc(t, in), rs, loose)
	if len(res.Rejections) != 0 || len(res.Flags) != 1 {
		t.Fatalf("default tenant: flags %v rejections %v", res.Flags, res.Rejections)
	}
	if v, _ := res.Doc.Get("Temperature.Celsius"); v != json.Number("10.5") {
		t.Fatalf("coerced value = %#v", v)
	}

	strict := scope()
	strict.Tenant = "strict"
	res = Apply(parseDoc(t, in), rs, strict)
	if len(res.Rejections) != 1 || res.Rejections[0].Code != "COERCED_TYPE" {
		t.Fatalf("strict tenant rejections = %v", res.Rejections)
	}
}

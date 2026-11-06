package rules

import (
	"strings"
	"testing"
	"time"
)

func TestSentencesForTheDefaultRules(t *testing.T) {
	rs := defaultRules(t)
	want := map[string]string{
		"TEMP-SENTINEL":        "When Temperature (°C) is exactly one of -32,768, 327.67, 32,767, clear the value and flag it SENSOR_FAULT (warning).",
		"HUMIDITY-RANGE":       "When Humidity (%) is outside 0 to 100, clear the value and flag it OUT_OF_RANGE (warning).",
		"LUX-NEGATIVE":         "When Light (lux) is below 0, clear the value and flag it OUT_OF_RANGE (warning).",
		"TS-SECONDS":           "When Reading time (epoch) is below 1,000,000,000,000, multiply it by 1,000 and flag it TIMESTAMP_SECONDS (warning).",
		"TS-FUTURE":            "When Reading time (epoch) is more than 5 minutes in the future, reject the whole reading (TIMESTAMP_FUTURE).",
		"NUMERIC-STRING":       "When every numeric field is text that holds a number, convert it to a number and flag it COERCED_TYPE (info).",
		"LAT-RANGE":            "When Latitude is outside -90 to 90, drop the location record and keep the rest and flag it INVALID_COORDINATES (error).",
		"NO-IDENTITY":          "When Device ID (IMEI) is not a valid 15-digit device ID and the device name is not a known device, reject the whole reading (NO_DEVICE_IDENTITY).",
		"TEMP-F-ONLY":          "When Temperature (°C) is missing and Temperature (°F) is present, calculate it from the Fahrenheit value and flag it TEMPERATURE_CONVERTED (info).",
		"ENTITY-NAME-MISMATCH": "When Entity name has a value and differs from Device name, keep the value and flag it IDENTITY_MISMATCH (warning).",
	}
	for _, r := range rs.Rules {
		got := r.Sentence()
		if strings.TrimSpace(got) == "" || strings.Contains(got, "%!") || strings.Contains(got, "<nil>") {
			t.Errorf("%s: a rule must read as a sentence: %q", r.ID, got)
		}
		if w, ok := want[r.ID]; ok && got != w {
			t.Errorf("%s:\n got  %s\n want %s", r.ID, got, w)
		}
	}
}

func TestSentenceOfADisabledRuleSaysSo(t *testing.T) {
	r := defaultRules(t).Rules[6]
	r.Disabled = true
	if got := r.Sentence(); !strings.HasPrefix(got, "(off) When ") {
		t.Fatal(got)
	}
	r = Rule{Field: "Humidity.Percentage", Code: "X", Action: actionIgnore, When: When{Lt: ptr(5.0)}}
	if got := r.Outcome(); got != "do nothing" {
		t.Fatal(got)
	}
}

func ptr[T any](v T) *T { return &v }

func TestNumberAndDuration(t *testing.T) {
	for in, want := range map[float64]string{0: "0", 5: "5", 327.67: "327.67", -32768: "-32,768", 1e12: "1,000,000,000,000", 999: "999", 1000: "1,000", 12.5: "12.5", -0.5: "-0.5"} {
		if got := Number(in); got != want {
			t.Errorf("Number(%v) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"5m": "5 minutes", "1m": "1 minute", "2h": "2 hours", "30s": "30 seconds", "90m": "90 minutes", "1h30m": "90 minutes", "nonsense": "nonsense"} {
		if got := Duration(in); got != want {
			t.Errorf("Duration(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFieldLabels(t *testing.T) {
	if FieldLabel("Humidity.Percentage") != "Humidity (%)" || FieldLabel("Custom.Field") != "Custom.Field" {
		t.Fatal("known fields get a label, unknown ones show their path")
	}
	fields := KnownFields()
	if len(fields) < 10 {
		t.Fatal("the console offers the common fields")
	}
	for i := 1; i < len(fields); i++ {
		if fields[i-1].Label > fields[i].Label {
			t.Fatal("sorted by label")
		}
	}
	for _, f := range fields {
		if f.Path == "*" {
			t.Fatal("the wildcard is not a field to pick")
		}
	}
}

func TestEveryDefaultRuleActionHasAFriendlyLabel(t *testing.T) {
	labelled := map[string]bool{}
	for _, a := range ActionLabels {
		labelled[a.Value] = true
	}
	for a := range validActions {
		if !labelled[a] {
			t.Errorf("action %q has no label for the console", a)
		}
	}
	if SeverityLabel("warn") != "warning" || SeverityLabel("info") != "info" || SeverityLabel("error") != "error" {
		t.Fatal("severity words")
	}
}

func TestNextVersion(t *testing.T) {
	now := time.Date(2026, 11, 6, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		used []string
		want string
	}{
		{nil, "2026-11-06.1"},
		{[]string{"2026-11-06.1"}, "2026-11-06.2"},
		{[]string{"2026-11-06.1", "2026-11-06.7", "2026-11-06.3"}, "2026-11-06.8"},
		{[]string{"2026-11-05.9", "custom", "console-test.1"}, "2026-11-06.1"},
		{[]string{"2026-11-06.2", "2026-11-05.40"}, "2026-11-06.3"},
	}
	for _, c := range cases {
		if got := NextVersion(c.used, now); got != c.want {
			t.Errorf("%v: got %s, want %s", c.used, got, c.want)
		}
	}
}

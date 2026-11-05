package rules

import (
	"encoding/json"
	"math"
	"testing"
)

func applyYAML(t *testing.T, yaml, in string) Result {
	t.Helper()
	rs, err := Parse([]byte("version: t\nrules:\n" + yaml))
	if err != nil {
		t.Fatal(err)
	}
	return Apply(parseDoc(t, in), rs, scope())
}

func TestStrategyScale(t *testing.T) {
	const r = `  - { id: S, code: TIMESTAMP_SECONDS, field: EntryTimeEpoch, when: { lt: 1.0e12 }, action: fix, strategy: scale, params: { factor: 1000 }, severity: warn }`
	res := applyYAML(t, r, `{"EntryTimeEpoch":1739561246}`)
	if v, _ := res.Doc.Get("EntryTimeEpoch"); v != json.Number("1739561246000") {
		t.Fatalf("scaled = %#v", v)
	}
}

func TestStrategyCoerceNumber(t *testing.T) {
	const r = `  - { id: C, code: COERCED_TYPE, field: "*", when: { numeric_string: true }, action: fix, strategy: coerce_number, severity: info }`
	tests := []struct {
		in   string
		want any
	}{
		{`"-100"`, json.Number("-100")},
		{`"10.5"`, json.Number("10.5")},
		{`" 10.5 "`, json.Number("10.5")},
		{`"1e3"`, json.Number("1e3")},
		{`".5"`, json.Number(".5")},
		{`"abc"`, "abc"},
		{`""`, ""},
		{`"0x10"`, "0x10"},
		{`"NaN"`, "NaN"},
		{`"Inf"`, "Inf"},
		{`"1,5"`, "1,5"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			res := applyYAML(t, r, `{"Temperature":{"Celsius":`+tt.in+`}}`)
			if v, _ := res.Doc.Get("Temperature.Celsius"); v != tt.want {
				t.Fatalf("got %#v, want %#v", v, tt.want)
			}
		})
	}
}

func TestStrategyFahrenheitToCelsius(t *testing.T) {
	const r = `  - { id: F, code: TEMPERATURE_CONVERTED, field: Temperature.Celsius, when: { is_null: true, sibling_present: Temperature.Fahrenheit }, action: fix, strategy: fahrenheit_to_celsius, severity: info }`
	tests := []struct {
		f    string
		want float64
	}{{"32", 0}, {"212", 100}, {"-40", -40}, {"50", 10}, {"39.2", 4}}
	for _, tt := range tests {
		t.Run(tt.f, func(t *testing.T) {
			res := applyYAML(t, r, `{"Temperature":{"Celsius":null,"Fahrenheit":`+tt.f+`}}`)
			v, _ := res.Doc.Get("Temperature.Celsius")
			n, ok := v.(json.Number)
			if !ok {
				t.Fatalf("got %#v", v)
			}
			if f, _ := n.Float64(); math.Abs(f-tt.want) > 1e-9 {
				t.Fatalf("got %v, want %v", f, tt.want)
			}
		})
	}
}

func TestStrategyNormalizeEnum(t *testing.T) {
	const r = `  - { id: E, code: COERCED_TYPE, field: Location.LocationMethod, when: { present: true }, action: fix, strategy: normalize_enum, params: { map: { GPS: gps, "WI-FI": wifi, cellular: cell } }, severity: info }`
	tests := []struct{ in, want string }{{"GPS", "gps"}, {" wi-fi ", "wifi"}, {"Cellular", "cell"}, {"gps", "gps"}, {"satellite", "satellite"}}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			res := applyYAML(t, r, `{"Location":{"LocationMethod":"`+tt.in+`"}}`)
			if v, _ := res.Doc.Get("Location.LocationMethod"); v != tt.want {
				t.Fatalf("got %#v, want %q", v, tt.want)
			}
		})
	}
}

// F2-R10: magnitude is derived from x, y, z only when G is missing.
func TestStrategyDeriveMagnitude_F2R10(t *testing.T) {
	const r = `  - { id: M, code: COERCED_TYPE, field: Accelerometer.G, when: { is_null: true }, action: fix, strategy: derive_magnitude, severity: info }`
	res := applyYAML(t, r, `{"Accelerometer":{"G":null,"X":3,"Y":4,"Z":0}}`)
	v, _ := res.Doc.Get("Accelerometer.G")
	if f, _ := v.(json.Number).Float64(); f != 5 {
		t.Fatalf("magnitude = %#v, want 5", v)
	}
	res = applyYAML(t, r, `{"Accelerometer":{"G":null,"X":3,"Y":null,"Z":0}}`)
	if v, _ := res.Doc.Get("Accelerometer.G"); v != nil {
		t.Fatalf("missing axis must leave G null, got %#v", v)
	}
	if len(res.Flags) != 0 {
		t.Fatalf("no change means no flag, got %v", res.Flags)
	}
	if got := Magnitude(3, 4, 12); got != 13 {
		t.Fatalf("Magnitude = %v", got)
	}
}

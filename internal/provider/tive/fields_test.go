package tive

import (
	"testing"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

func mustDoc(t *testing.T, s string) doc.Doc {
	t.Helper()
	d, err := doc.Parse([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// F2-R15: a wrong-type value becomes null and is flagged INVALID_TYPE.
func TestReaderNum_F2R15(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		wantNil  bool
		want     float64
		wantFlag bool
	}{
		{"number", `{"T":{"C":10.5}}`, false, 10.5, false},
		{"integer", `{"T":{"C":10}}`, false, 10, false},
		{"negative", `{"T":{"C":-18.5}}`, false, -18.5, false},
		{"null is absent, no flag", `{"T":{"C":null}}`, true, 0, false},
		{"missing is absent, no flag", `{"T":{}}`, true, 0, false},
		{"parent missing, no flag", `{}`, true, 0, false},
		{"parent null, no flag", `{"T":null}`, true, 0, false},
		{"non numeric string", `{"T":{"C":"abc"}}`, true, 0, true},
		{"numeric string left uncoerced", `{"T":{"C":"10.5"}}`, true, 0, true},
		{"object", `{"T":{"C":{"x":1}}}`, true, 0, true},
		{"array", `{"T":{"C":[1]}}`, true, 0, true},
		{"bool", `{"T":{"C":true}}`, true, 0, true},
		{"huge exponent", `{"T":{"C":1e999}}`, true, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newReader(mustDoc(t, tt.json))
			got := r.num("T.C")
			if (got == nil) != tt.wantNil {
				t.Fatalf("got %v, wantNil %v", got, tt.wantNil)
			}
			if got != nil && *got != tt.want {
				t.Fatalf("got %v, want %v", *got, tt.want)
			}
			if tt.wantFlag != (len(r.flags) == 1) {
				t.Fatalf("flags = %+v, wantFlag %v", r.flags, tt.wantFlag)
			}
			if tt.wantFlag {
				f := r.flags[0]
				if f.Code != rules.CodeInvalidType || f.Severity != rules.SeverityWarn || f.Field != "T.C" || f.Action != rules.ActionNullified {
					t.Fatalf("unexpected flag %+v", f)
				}
			}
		})
	}
}

func TestReaderInt(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		want    int
		wantNil bool
	}{
		{"integer", `{"B":{"P":65}}`, 65, false},
		{"float rounds half away", `{"B":{"P":64.5}}`, 65, false},
		{"float below", `{"B":{"P":64.4}}`, 64, false},
		{"negative half away", `{"B":{"P":-0.5}}`, -1, false},
		{"null", `{"B":{"P":null}}`, 0, true},
		{"string", `{"B":{"P":"x"}}`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newReader(mustDoc(t, tt.json))
			got := r.integer("B.P")
			if (got == nil) != tt.wantNil || (got != nil && *got != tt.want) {
				t.Fatalf("got %v, want %v (nil=%v)", got, tt.want, tt.wantNil)
			}
		})
	}
}

func TestReaderStr(t *testing.T) {
	r := newReader(mustDoc(t, `{"N":"A571992","E":"","Z":null,"X":12,"O":{"a":1}}`))
	if s, ok := r.str("N"); !ok || s != "A571992" {
		t.Fatalf("N = %q %v", s, ok)
	}
	if s, ok := r.str("E"); ok || s != "" {
		t.Fatalf("empty string must count as absent: %q %v", s, ok)
	}
	if _, ok := r.str("Z"); ok {
		t.Fatal("null must be absent")
	}
	if _, ok := r.str("Missing"); ok {
		t.Fatal("missing must be absent")
	}
	if _, ok := r.str("X"); ok {
		t.Fatal("number is not a string")
	}
	if len(r.flags) != 0 {
		t.Fatalf("string reads raise no flags: %+v", r.flags)
	}
}

func TestNumericFieldsExcludeIdentifiers(t *testing.T) {
	for _, id := range []string{"DeviceId", "DeviceName", "EntityName", "ShipmentId", "PublicShipmentId", "Shipment.Id", "Shipment.DeviceId"} {
		for _, f := range NumericFields {
			if f == id {
				t.Fatalf("%s is an identifier and must never be coerced", id)
			}
		}
	}
	want := map[string]bool{"Temperature.Celsius": false, "ProbeTemperature.Fahrenheit": false, "Cellular.Dbm": false, "EntryTimeEpoch": false, "AccountId": false, "Location.Latitude": false}
	for _, f := range NumericFields {
		if _, ok := want[f]; ok {
			want[f] = true
		}
	}
	for f, seen := range want {
		if !seen {
			t.Fatalf("NumericFields missing %s", f)
		}
	}
}

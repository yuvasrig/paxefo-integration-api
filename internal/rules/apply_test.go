package rules

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
)

type fakeDevices map[string]string

func (f fakeDevices) ResolveByName(name string) (string, bool) { v, ok := f[name]; return v, ok }

var testNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

var testNumeric = []string{
	"EntryTimeEpoch", "AccountId", "Cellular.Dbm", "Temperature.Celsius", "Temperature.Fahrenheit",
	"ProbeTemperature.Celsius", "ProbeTemperature.Fahrenheit", "Humidity.Percentage",
	"Accelerometer.G", "Accelerometer.X", "Accelerometer.Y", "Accelerometer.Z", "Light.Lux",
	"Battery.Percentage", "Location.Latitude", "Location.Longitude",
}

func defaultRules(t *testing.T) *Ruleset {
	t.Helper()
	rs, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	return rs
}

func scope() Scope {
	return Scope{Tenant: "reviewer", Now: testNow, Devices: fakeDevices{"A571992": "863257063350583"}, NumericFields: testNumeric}
}

func parseDoc(t *testing.T, s string) doc.Doc {
	t.Helper()
	d, err := doc.Parse([]byte(s))
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return d
}

// flagKey is "CODE@Field" for order-independent comparison.
func flagKeys(fs []Flag) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Code + "@" + f.Field
	}
	sort.Strings(out)
	return out
}

// One table-driven case per rule ID (constitution #9). Requirement IDs are in the case names.
func TestApplyDefaultRules(t *testing.T) {
	const imei = `"DeviceId":"863257063350583","DeviceName":"A571992"`
	nowMs := testNow.UnixMilli()

	tests := []struct {
		name        string
		in          string
		sc          func(Scope) Scope
		wantFlags   []string
		wantReject  []string
		wantDropped []string
		wantVals    map[string]any // path -> expected value (json.Number as string, string, nil)
		wantFlag    func(t *testing.T, fs []Flag)
	}{
		{name: "NUMERIC-STRING F2-R4 coerces string numbers on numeric fields", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Cellular":{"Dbm":"-100"},"Temperature":{"Celsius":"10.5","Fahrenheit":"50.9"}}`,
			wantFlags: []string{"COERCED_TYPE@Cellular.Dbm", "COERCED_TYPE@Temperature.Celsius", "COERCED_TYPE@Temperature.Fahrenheit"},
			wantVals:  map[string]any{"Cellular.Dbm": json.Number("-100"), "Temperature.Celsius": json.Number("10.5")},
			wantFlag: func(t *testing.T, fs []Flag) {
				for _, f := range fs {
					if f.Code == CodeCoercedType && f.Field == "Temperature.Celsius" {
						if f.Value != "10.5" || f.NewValue != json.Number("10.5") || f.RuleID != "NUMERIC-STRING" || f.Severity != SeverityInfo || f.Action != ActionFixed {
							t.Fatalf("unexpected flag %+v", f)
						}
					}
				}
			}},
		{name: "NUMERIC-STRING leaves identifiers alone", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"AccountId":"478","ShipmentId":"12345"}`,
			wantFlags: []string{"COERCED_TYPE@AccountId"},
			wantVals:  map[string]any{"DeviceId": "863257063350583", "ShipmentId": "12345", "AccountId": json.Number("478")}},
		{name: "NUMERIC-STRING ignores non-numeric strings", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":"abc"}}`,
			wantVals: map[string]any{"Temperature.Celsius": "abc"}},
		{name: "NO-IDENTITY F3-R2 rejects unknown name without IMEI", in: `{"DeviceName":"Ship33CABOL","EntryTimeEpoch":1739215646000}`,
			wantReject: []string{"NO_DEVICE_IDENTITY"}},
		{name: "NO-IDENTITY F3-R2 rejects invalid IMEI with unknown name", in: `{"DeviceId":"123","DeviceName":"Unknown","EntryTimeEpoch":1739215646000}`,
			wantReject: []string{"NO_DEVICE_IDENTITY"}},
		{name: "NO-IDENTITY F3-R2 rejects when no name at all", in: `{"EntryTimeEpoch":1739215646000}`,
			wantReject: []string{"NO_DEVICE_IDENTITY"}},
		{name: "NO-IDENTITY F3-R2 valid IMEI needs no resolution", in: `{"DeviceId":"863257063350583","DeviceName":"Unknown","EntryTimeEpoch":1739215646000}`},
		{name: "NAME-RESOLVED F5-R2 fills IMEI from known name", in: `{"DeviceName":"A571992","EntryTimeEpoch":1739215646000}`,
			wantFlags: []string{"DEVICE_RESOLVED_BY_NAME@DeviceId"},
			wantVals:  map[string]any{"DeviceId": "863257063350583"}},
		{name: "NAME-RESOLVED F5-R2 invalid IMEI replaced", in: `{"DeviceId":"abc","DeviceName":"A571992","EntryTimeEpoch":1739215646000}`,
			wantFlags: []string{"DEVICE_RESOLVED_BY_NAME@DeviceId"},
			wantVals:  map[string]any{"DeviceId": "863257063350583"}},
		{name: "TS-SECONDS F2-R5 scales seconds", in: `{` + imei + `,"EntryTimeEpoch":1739561246}`,
			wantFlags: []string{"TIMESTAMP_SECONDS@EntryTimeEpoch"},
			wantVals:  map[string]any{"EntryTimeEpoch": json.Number("1739561246000")},
			wantFlag: func(t *testing.T, fs []Flag) {
				f := fs[0]
				if f.Value != json.Number("1739561246") || f.NewValue != json.Number("1739561246000") || f.RuleID != "TS-SECONDS" || f.Severity != SeverityWarn {
					t.Fatalf("unexpected flag %+v", f)
				}
			}},
		{name: "TS-SECONDS F2-R5 leaves milliseconds", in: `{` + imei + `,"EntryTimeEpoch":1739215646000}`},
		{name: "TS-FUTURE F3-R4 rejects more than 5 min ahead", in: `{` + imei + `,"EntryTimeEpoch":` + itoa(nowMs+6*60*1000) + `}`,
			wantReject: []string{"TIMESTAMP_FUTURE"}},
		{name: "TS-FUTURE F3-R4 allows 4 min ahead", in: `{` + imei + `,"EntryTimeEpoch":` + itoa(nowMs+4*60*1000) + `}`},
		{name: "TS-UTC-MISMATCH F2-R6 flags 5h difference, keeps epoch", in: `{` + imei + `,"EntryTimeEpoch":1739647646000,"EntryTimeUtc":"2025-02-15T14:27:26Z"}`,
			wantFlags: []string{"TIMESTAMP_MISMATCH@EntryTimeEpoch"},
			wantVals:  map[string]any{"EntryTimeEpoch": json.Number("1739647646000")}},
		{name: "TS-UTC-MISMATCH F2-R6 within 1s tolerance", in: `{` + imei + `,"EntryTimeEpoch":1739215646500,"EntryTimeUtc":"2025-02-10T19:27:26Z"}`},
		{name: "TS-UTC-MISMATCH F2-R6 absent UTC is fine", in: `{` + imei + `,"EntryTimeEpoch":1739215646000}`},
		{name: "TS-UTC-MISMATCH F2-R6 unparsable UTC is ignored", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"EntryTimeUtc":"yesterday"}`},
		{name: "TS-SECONDS then UTC match F2-R5/R6 no mismatch after scaling", in: `{` + imei + `,"EntryTimeEpoch":1739561246,"EntryTimeUtc":"2025-02-14T19:27:26Z"}`,
			wantFlags: []string{"TIMESTAMP_SECONDS@EntryTimeEpoch"}},
		{name: "TEMP-F-ONLY F2-R9 converts Fahrenheit", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":null,"Fahrenheit":50}}`,
			wantFlags: []string{"TEMPERATURE_CONVERTED@Temperature.Celsius"},
			wantVals:  map[string]any{"Temperature.Celsius": json.Number("10")}},
		{name: "TEMP-F-ONLY F2-R9 missing Celsius key", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Fahrenheit":212}}`,
			wantFlags: []string{"TEMPERATURE_CONVERTED@Temperature.Celsius"},
			wantVals:  map[string]any{"Temperature.Celsius": json.Number("100")}},
		{name: "TEMP-F-ONLY F2-R9 not applied when Celsius present", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":10,"Fahrenheit":99}}`,
			wantVals: map[string]any{"Temperature.Celsius": json.Number("10")}},
		{name: "TEMP-F-ONLY F2-R9 not applied without Fahrenheit", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":null}}`},
		{name: "TEMP-SENTINEL F2-R2 nulls device temperature -32768", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":-32768}}`,
			wantFlags: []string{"SENSOR_FAULT@Temperature.Celsius"}, wantVals: map[string]any{"Temperature.Celsius": nil}},
		{name: "TEMP-SENTINEL F2-R2 nulls 32767", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":32767}}`,
			wantFlags: []string{"SENSOR_FAULT@Temperature.Celsius"}},
		{name: "PROBE-SENTINEL F2-R2 nulls 327.67", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"ProbeTemperature":{"Celsius":327.67}}`,
			wantFlags: []string{"SENSOR_FAULT@ProbeTemperature.Celsius"}, wantVals: map[string]any{"ProbeTemperature.Celsius": nil}},
		{name: "PROBE-SENTINEL F2-R2 nulls -32768", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"ProbeTemperature":{"Celsius":-32768}}`,
			wantFlags: []string{"SENSOR_FAULT@ProbeTemperature.Celsius"}},
		{name: "PROBE-SENTINEL F2-R2 normal value untouched", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"ProbeTemperature":{"Celsius":9.9375}}`,
			wantVals: map[string]any{"ProbeTemperature.Celsius": json.Number("9.9375")}},
		{name: "PROBE-SENTINEL F2-R2 null probe untouched", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"ProbeTemperature":{"Celsius":null}}`},
		{name: "HUMIDITY-RANGE F2-R3 over 100", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Humidity":{"Percentage":101}}`,
			wantFlags: []string{"OUT_OF_RANGE@Humidity.Percentage"}, wantVals: map[string]any{"Humidity.Percentage": nil}},
		{name: "HUMIDITY-RANGE F2-R3 negative", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Humidity":{"Percentage":-1}}`,
			wantFlags: []string{"OUT_OF_RANGE@Humidity.Percentage"}},
		{name: "HUMIDITY-RANGE F2-R3 boundaries ok", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Humidity":{"Percentage":100}}`},
		{name: "LUX-NEGATIVE F2-R3", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Light":{"Lux":-0.1}}`,
			wantFlags: []string{"OUT_OF_RANGE@Light.Lux"}, wantVals: map[string]any{"Light.Lux": nil}},
		{name: "LUX-NEGATIVE F2-R3 zero ok", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Light":{"Lux":0}}`},
		{name: "BATTERY-RANGE F2-R3 over 100", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Battery":{"Percentage":120}}`,
			wantFlags: []string{"OUT_OF_RANGE@Battery.Percentage"}, wantVals: map[string]any{"Battery.Percentage": nil}},
		{name: "LAT-RANGE F2-R1 drops location", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Location":{"Latitude":95.0,"Longitude":-73.0}}`,
			wantFlags: []string{"INVALID_COORDINATES@Location.Latitude"}, wantDropped: []string{"location"},
			wantFlag: func(t *testing.T, fs []Flag) {
				if fs[0].Severity != SeverityError || fs[0].Action != ActionLocationSkipped || fs[0].RuleID != "LAT-RANGE" {
					t.Fatalf("unexpected flag %+v", fs[0])
				}
			}},
		{name: "LAT-RANGE F2-R1 -90.0001", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Location":{"Latitude":-90.0001,"Longitude":0}}`,
			wantFlags: []string{"INVALID_COORDINATES@Location.Latitude"}, wantDropped: []string{"location"}},
		{name: "LAT-RANGE F2-R1 boundary 90 ok", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Location":{"Latitude":90,"Longitude":180}}`},
		{name: "LON-RANGE F2-R1 drops location", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Location":{"Latitude":40.0,"Longitude":-200.0}}`,
			wantFlags: []string{"INVALID_COORDINATES@Location.Longitude"}, wantDropped: []string{"location"}},
		{name: "LAT+LON F2-R1 both bad give two flags, one dropped section", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Location":{"Latitude":95,"Longitude":-200}}`,
			wantFlags: []string{"INVALID_COORDINATES@Location.Latitude", "INVALID_COORDINATES@Location.Longitude"}, wantDropped: []string{"location"}},
		{name: "ENTITY-NAME-MISMATCH F2-R7 differs from DeviceName", in: `{` + imei + `,"EntityName":"Other","EntryTimeEpoch":1739215646000}`,
			wantFlags: []string{"IDENTITY_MISMATCH@EntityName"}, wantVals: map[string]any{"EntityName": "Other"}},
		{name: "ENTITY-NAME-MISMATCH F2-R7 equal is fine", in: `{` + imei + `,"EntityName":"A571992","EntryTimeEpoch":1739215646000}`},
		{name: "ENTITY-NAME-MISMATCH F2-R7 absent is fine", in: `{` + imei + `,"EntryTimeEpoch":1739215646000}`},
		{name: "SHIPMENT-DEVICE-MISMATCH F2-R7", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Shipment":{"DeviceId":"866088073468439"}}`,
			wantFlags: []string{"IDENTITY_MISMATCH@Shipment.DeviceId"}},
		{name: "SHIPMENT-DEVICE-MISMATCH F2-R7 same is fine", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Shipment":{"DeviceId":"863257063350583"}}`},
		{name: "SHIPMENT-DEVICE-MISMATCH F2-R7 null shipment is fine", in: `{` + imei + `,"EntryTimeEpoch":1739215646000,"Shipment":null}`},
		{name: "F3-R7 flags raised alongside a rejection are kept (sample 12)", in: `{"EntityName":"A571992","DeviceName":"Ship33CABOL","EntryTimeEpoch":1739215646000}`,
			wantFlags: []string{"IDENTITY_MISMATCH@EntityName"}, wantReject: []string{"NO_DEVICE_IDENTITY"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := scope()
			if tt.sc != nil {
				sc = tt.sc(sc)
			}
			in := parseDoc(t, tt.in)
			orig := in.Clone()
			res := Apply(in, defaultRules(t), sc)

			if got := flagKeys(res.Flags); !equalStrings(got, tt.wantFlags) {
				t.Fatalf("flags = %v, want %v", got, tt.wantFlags)
			}
			var gotRej []string
			for _, p := range res.Rejections {
				gotRej = append(gotRej, p.Code)
			}
			sort.Strings(gotRej)
			if !equalStrings(gotRej, tt.wantReject) {
				t.Fatalf("rejections = %v, want %v", gotRej, tt.wantReject)
			}
			var gotDropped []string
			for s := range res.Dropped {
				gotDropped = append(gotDropped, s)
			}
			sort.Strings(gotDropped)
			if !equalStrings(gotDropped, tt.wantDropped) {
				t.Fatalf("dropped = %v, want %v", gotDropped, tt.wantDropped)
			}
			for path, want := range tt.wantVals {
				got, _ := res.Doc.Get(path)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s = %#v, want %#v", path, got, want)
				}
			}
			if tt.wantFlag != nil {
				tt.wantFlag(t, res.Flags)
			}
			if !reflect.DeepEqual(in, orig) {
				t.Fatal("Apply must not modify its input (rules run on a copy)")
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

// F2-R11: every change records rule ID, original and new value.
func TestApplyRecordsOriginalAndNewValue_F2R11(t *testing.T) {
	res := Apply(parseDoc(t, `{"DeviceId":"863257063350583","DeviceName":"A","EntryTimeEpoch":1739561246,"ProbeTemperature":{"Celsius":327.67}}`), defaultRules(t), scope())
	byCode := map[string]Flag{}
	for _, f := range res.Flags {
		byCode[f.Code] = f
	}
	if f := byCode[CodeTimestampSeconds]; f.Value != json.Number("1739561246") || f.NewValue != json.Number("1739561246000") || f.RuleID == "" {
		t.Fatalf("TIMESTAMP_SECONDS flag = %+v", f)
	}
	if f := byCode[CodeSensorFault]; f.Value != json.Number("327.67") || f.Action != ActionNullified || f.RuleID != "PROBE-SENTINEL" {
		t.Fatalf("SENSOR_FAULT flag = %+v", f)
	}
}

// Rules run in file order: seconds are scaled before the future check and the UTC comparison.
func TestApplyRuleOrder(t *testing.T) {
	res := Apply(parseDoc(t, `{"DeviceId":"863257063350583","DeviceName":"A","EntryTimeEpoch":1739561246,"EntryTimeUtc":"2025-02-14T19:27:26Z"}`), defaultRules(t), scope())
	if len(res.Rejections) != 0 {
		t.Fatalf("unexpected rejections %v", res.Rejections)
	}
	for _, f := range res.Flags {
		if f.Code == CodeTimestampMismatch {
			t.Fatal("UTC comparison must use the scaled epoch")
		}
	}
}

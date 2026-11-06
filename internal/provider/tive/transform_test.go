package tive

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

type devices map[string]string

func (d devices) ResolveByName(n string) (string, bool) { v, ok := d[n]; return v, ok }

var fixedNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func newProvider(t *testing.T) *Provider {
	t.Helper()
	rs, err := rules.Default()
	if err != nil {
		t.Fatal(err)
	}
	return New(rs, config.Default())
}

func transform(t *testing.T, in string) provider.Result {
	t.Helper()
	p := newProvider(t)
	d, err := p.Parse([]byte(in))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := p.Transform(d, rules.Scope{Tenant: "reviewer", Now: fixedNow, Devices: devices{"A571992": "863257063350583"}})
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	return res
}

func marshal(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

const head = `"DeviceId":"863257063350583","DeviceName":"A571992","EntryTimeEpoch":1739215646000,`

func flagCodes(r provider.Result) string {
	var cs []string
	for _, f := range r.Flags {
		cs = append(cs, f.Code)
	}
	return strings.Join(cs, ",")
}

// F1-R3 / mapping.md: sensor mapping and temperature selection.
func TestTransformTemperature(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		want       any // nil = JSON null
		wantSource string
		wantFlags  string
		wantStatus string
	}{
		{"device temp preferred over probe", `{` + head + `"Temperature":{"Celsius":10.078125},"ProbeTemperature":{"Celsius":9.9}}`, 10.08, "device", "", provider.StatusAccepted},
		{"F2-R8 probe fallback when device temp null", `{` + head + `"Temperature":{"Celsius":null},"ProbeTemperature":{"Celsius":9.9375}}`, 9.94, "probe", "TEMPERATURE_FALLBACK", provider.StatusFlagged},
		{"F2-R8 probe fallback when device temp faulty", `{` + head + `"Temperature":{"Celsius":-32768},"ProbeTemperature":{"Celsius":9.5}}`, 9.5, "probe", "SENSOR_FAULT,TEMPERATURE_FALLBACK", provider.StatusFlagged},
		{"F2-R2 faulty probe unused when device ok", `{` + head + `"Temperature":{"Celsius":4.625},"ProbeTemperature":{"Celsius":327.67}}`, 4.63, "device", "SENSOR_FAULT", provider.StatusFlagged},
		{"F2-R9 Fahrenheit only", `{` + head + `"Temperature":{"Fahrenheit":50}}`, 10.0, "device", "TEMPERATURE_CONVERTED", provider.StatusFlagged},
		{"both temps absent but humidity ok and coordinates ok", `{` + head + `"Humidity":{"Percentage":40},"Location":{"Latitude":1,"Longitude":2}}`, nil, "", "", provider.StatusAccepted},
		{"F2-R15 wrong type temp", `{` + head + `"Temperature":{"Celsius":{"x":1}},"Location":{"Latitude":1,"Longitude":2}}`, nil, "", "INVALID_TYPE", provider.StatusFlagged},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := transform(t, tt.in)
			if res.Status != tt.wantStatus {
				t.Fatalf("status = %s, want %s (errors %v)", res.Status, tt.wantStatus, res.Errors)
			}
			if got := flagCodes(res); got != tt.wantFlags {
				t.Fatalf("flags = %q, want %q", got, tt.wantFlags)
			}
			if tt.want == nil {
				if res.Sensor != nil && res.Sensor.Temperature != nil {
					t.Fatalf("temperature = %v, want null", *res.Sensor.Temperature)
				}
				return
			}
			if res.Sensor == nil {
				t.Fatal("no sensor record")
			}
			if got := marshal(t, res.Sensor)["temperature"]; got != tt.want {
				t.Fatalf("temperature = %v, want %v", got, tt.want)
			}
			if res.Sensor.TemperatureSource != tt.wantSource {
				t.Fatalf("source = %q, want %q", res.Sensor.TemperatureSource, tt.wantSource)
			}
		})
	}
}

func TestTransformSensorFields(t *testing.T) {
	res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Humidity":{"Percentage":38.70000076293945},"Light":{"Lux":125.55},
	  "Accelerometer":{"G":0.9901862198596787,"X":-0.5625,"Y":-0.4375,"Z":0.6875}}`)
	m := marshal(t, res.Sensor)
	acc := m["accelerometer"].(map[string]any)
	if m["humidity"] != 38.7 || m["light_level"] != 125.6 || acc["x"] != -0.563 || acc["y"] != -0.438 || acc["z"] != 0.688 || acc["magnitude"] != 0.990 {
		t.Fatalf("unexpected sensor output: %v", m)
	}
	if m["device_id"] != "A571992" || m["device_imei"] != "863257063350583" || m["timestamp"] != float64(1739215646000) || m["provider"] != "Tive" || m["type"] != "Active" {
		t.Fatalf("shared fields wrong: %v", m)
	}
}

// F2-R10: magnitude derived from the axes when G is missing.
func TestTransformMagnitude_F2R10(t *testing.T) {
	tests := []struct {
		name string
		acc  string
		want any
	}{
		{"G given", `{"G":1.0,"X":3,"Y":4,"Z":0}`, 1.0},
		{"G missing derived", `{"X":3,"Y":4,"Z":0}`, 5.0},
		{"G null derived", `{"G":null,"X":0,"Y":0,"Z":2}`, 2.0},
		{"axis missing, no magnitude", `{"X":3,"Y":4}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Accelerometer":`+tt.acc+`}`)
			acc := marshal(t, res.Sensor)["accelerometer"].(map[string]any)
			if acc["magnitude"] != tt.want {
				t.Fatalf("magnitude = %v, want %v", acc["magnitude"], tt.want)
			}
		})
	}
	res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Accelerometer":null}`)
	if v := marshal(t, res.Sensor)["accelerometer"]; v != nil {
		t.Fatalf("null accelerometer must stay null, got %v", v)
	}
}

func TestTransformNoSensorRecord(t *testing.T) {
	res := transform(t, `{`+head+`"Location":{"Latitude":40,"Longitude":-73}}`)
	if res.Sensor != nil {
		t.Fatalf("no sensor values means no sensor record, got %+v", res.Sensor)
	}
	if res.Location == nil || res.Status != provider.StatusAccepted {
		t.Fatalf("location-only payload must be accepted with a location: %+v", res)
	}
}

// F2-R14 and mapping.md location rules.
func TestTransformAccuracyCategory_F2R14(t *testing.T) {
	tests := []struct {
		meters string
		want   any
	}{
		{"0", "High"}, {"23", "High"}, {"49.9", "High"}, {"50", "Medium"}, {"499", "Medium"},
		{"500", "Low"}, {"501", "Low"}, {"null", nil},
	}
	for _, tt := range tests {
		t.Run(tt.meters, func(t *testing.T) {
			res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Location":{"Latitude":1,"Longitude":2,"Accuracy":{"Meters":`+tt.meters+`}}}`)
			if got := marshal(t, res.Location)["location_accuracy_category"]; got != tt.want {
				t.Fatalf("category = %v, want %v", got, tt.want)
			}
		})
	}
	res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Location":{"Latitude":1,"Longitude":2,"Accuracy":{"Meters":49.6}}}`)
	m := marshal(t, res.Location)
	if m["location_accuracy"] != float64(50) || m["location_accuracy_category"] != "High" {
		t.Fatalf("accuracy 49.6 = %v / %v, want 50 / High (category from the unrounded value)", m["location_accuracy"], m["location_accuracy_category"])
	}
}

func TestTransformLocationSource(t *testing.T) {
	tests := []struct {
		method string
		want   any
	}{{`"gps"`, "GPS"}, {`"wifi"`, "WiFi"}, {`"cell"`, "Cellular"}, {`"GPS"`, "GPS"}, {`null`, nil}, {`"satellite"`, nil}}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Location":{"Latitude":1,"Longitude":2,"LocationMethod":`+tt.method+`}}`)
			if got := marshal(t, res.Location)["location_source"]; got != tt.want {
				t.Fatalf("source = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTransformLocationFields(t *testing.T) {
	res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Cellular":{"Dbm":-100.004},"Battery":{"Percentage":65},
	  "Location":{"Latitude":40.810562,"Longitude":-73.879285,"FormattedAddress":"114 Hunts Point Market, Bronx, NY 10474, USA","WifiAccessPointUsedCount":5}}`)
	m := marshal(t, res.Location)
	addr := m["address"].(map[string]any)
	if m["latitude"] != 40.810562 || m["longitude"] != -73.879285 || m["battery_level"] != float64(65) || m["cellular_dbm"] != -100.0 || m["wifi_access_points"] != float64(5) {
		t.Fatalf("unexpected location: %v", m)
	}
	if addr["locality"] != "Bronx" || addr["state"] != "NY" || addr["postal_code"] != "10474" || addr["country"] != "USA" || addr["street"] != nil {
		t.Fatalf("unexpected address: %v", addr)
	}
	for _, k := range []string{"altitude", "cellular_network_type", "cellular_operator", "location_accuracy"} {
		if m[k] != nil {
			t.Fatalf("%s must be null, got %v", k, m[k])
		}
	}
	if res2 := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Location":{"Latitude":1,"Longitude":2}}`); marshal(t, res2.Location)["address"] != nil {
		t.Fatal("no FormattedAddress means address null")
	}
}

// F2-R1: bad coordinates drop only the location.
func TestTransformCoordinates_F2R1(t *testing.T) {
	tests := []struct {
		name     string
		loc      string
		wantLoc  bool
		wantFlag string
	}{
		{"valid", `{"Latitude":40,"Longitude":-73}`, true, ""},
		{"lat 95", `{"Latitude":95,"Longitude":-73}`, false, "INVALID_COORDINATES"},
		{"lon -200", `{"Latitude":40,"Longitude":-200}`, false, "INVALID_COORDINATES"},
		{"lon missing", `{"Latitude":40}`, false, "INVALID_COORDINATES"},
		{"both missing, quiet", `{}`, false, ""},
		{"location null, quiet", `null`, false, ""},
		{"string coordinates coerced", `{"Latitude":"40.5","Longitude":"-73.5"}`, true, "COERCED_TYPE,COERCED_TYPE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Location":`+tt.loc+`}`)
			if (res.Location != nil) != tt.wantLoc {
				t.Fatalf("location present = %v, want %v", res.Location != nil, tt.wantLoc)
			}
			if res.Sensor == nil {
				t.Fatal("the valid half must still be emitted")
			}
			if got := flagCodes(res); got != tt.wantFlag {
				t.Fatalf("flags = %q, want %q", got, tt.wantFlag)
			}
		})
	}
}

// F3-R3, F3-R5, F3-R2, F3-R4: rejections.
func TestTransformRejections(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"F3-R3 timestamp missing", `{"DeviceId":"863257063350583","DeviceName":"A571992","Temperature":{"Celsius":10}}`, "NO_TIMESTAMP"},
		{"F3-R3 timestamp zero", `{"DeviceId":"863257063350583","DeviceName":"A571992","EntryTimeEpoch":0,"Temperature":{"Celsius":10}}`, "NO_TIMESTAMP"},
		{"F3-R3 timestamp wrong type", `{"DeviceId":"863257063350583","DeviceName":"A571992","EntryTimeEpoch":"soon","Temperature":{"Celsius":10}}`, "NO_TIMESTAMP"},
		{"F3-R4 timestamp in the future", `{"DeviceId":"863257063350583","DeviceName":"A571992","EntryTimeEpoch":1893456000000,"Temperature":{"Celsius":10}}`, "TIMESTAMP_FUTURE"},
		{"F3-R5 no temperature and no coordinates", `{` + head + `"Humidity":{"Percentage":40}}`, "NO_USABLE_DATA"},
		{"F3-R5 faulty temperature and bad coordinates", `{` + head + `"Temperature":{"Celsius":-32768},"Location":{"Latitude":95,"Longitude":0}}`, "NO_USABLE_DATA"},
		{"F3-R2 unknown device no IMEI", `{"DeviceName":"Ship33CABOL","EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":10}}`, "NO_DEVICE_IDENTITY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := transform(t, tt.in)
			if res.Status != provider.StatusRejected {
				t.Fatalf("status = %s, want rejected", res.Status)
			}
			found := false
			for _, e := range res.Errors {
				found = found || e.Code == tt.want
			}
			if !found {
				t.Fatalf("errors = %v, want %s", res.Errors, tt.want)
			}
			if res.Sensor != nil || res.Location != nil {
				t.Fatal("a rejected record emits no PAXAFE records")
			}
		})
	}
}

// F3-R7: flags raised before the rejection are still returned (sample 12 shape).
func TestTransformRejectionKeepsFlags_F3R7(t *testing.T) {
	res := transform(t, `{"EntityName":"A571992","DeviceName":"Ship33CABOL","EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":10}}`)
	if res.Status != provider.StatusRejected || flagCodes(res) != "IDENTITY_MISMATCH" {
		t.Fatalf("status %s flags %q", res.Status, flagCodes(res))
	}
}

// F5-R2: name-only payload is resolved.
func TestTransformResolvedByName_F5R2(t *testing.T) {
	res := transform(t, `{"DeviceName":"A571992","EntryTimeEpoch":1739215646000,"Temperature":{"Celsius":10}}`)
	if res.Status != provider.StatusFlagged || flagCodes(res) != "DEVICE_RESOLVED_BY_NAME" {
		t.Fatalf("status %s flags %q errors %v", res.Status, flagCodes(res), res.Errors)
	}
	if res.Sensor.DeviceIMEI != "863257063350583" || res.Sensor.DeviceID != "A571992" {
		t.Fatalf("sensor = %+v", res.Sensor)
	}
}

// An event is flagged if and only if it carries any flag, whatever the severity.
func TestTransformStatus(t *testing.T) {
	if got := transform(t, `{`+head+`"Temperature":{"Celsius":10}}`); got.Status != provider.StatusAccepted || len(got.Flags) != 0 {
		t.Fatalf("no flags means accepted: %s %v", got.Status, got.Flags)
	}
	if got := transform(t, `{`+head+`"Temperature":{"Celsius":"10.5"}}`).Status; got != provider.StatusFlagged {
		t.Fatalf("an info flag (COERCED_TYPE) still means flagged, got %s", got)
	}
	if got := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Humidity":{"Percentage":150}}`).Status; got != provider.StatusFlagged {
		t.Fatalf("OUT_OF_RANGE: status = %s", got)
	}
}

// EntryTimeUtc is ignored: EntryTimeEpoch is the event time, and a disagreement is not an event.
func TestEntryTimeUtcIsIgnored(t *testing.T) {
	res := transform(t, `{`+head+`"EntryTimeUtc":"2025-02-10T14:27:26Z","Temperature":{"Celsius":10}}`) // 5 h off
	if res.Status != provider.StatusAccepted || len(res.Flags) != 0 || res.Sensor.Timestamp != 1739215646000 {
		t.Fatalf("%s %v %+v", res.Status, res.Flags, res.Sensor)
	}
	res = transform(t, `{`+head+`"EntryTimeUtc":"garbage","Temperature":{"Celsius":10}}`)
	if res.Status != provider.StatusAccepted {
		t.Fatalf("an unparsable EntryTimeUtc is ignored too: %s", res.Status)
	}
}

// TEMP_EXCURSION fires only when the payload carries a shipment (mapping.md); the band 2-8 C is inclusive.
func TestTempExcursion(t *testing.T) {
	ship := `"Shipment":{"Id":"S-1"},`
	tests := []struct {
		name    string
		temp    string
		extra   string
		want    string // flag codes
		field   string
		wantVal float64
	}{
		{"in band", `{"Celsius":5}`, ship, "", "", 0},
		{"lower bound is in band", `{"Celsius":2}`, ship, "", "", 0},
		{"upper bound is in band", `{"Celsius":8}`, ship, "", "", 0},
		{"just below", `{"Celsius":1.99}`, ship, "TEMP_EXCURSION", "Temperature.Celsius", 1.99},
		{"just above", `{"Celsius":8.01}`, ship, "TEMP_EXCURSION", "Temperature.Celsius", 8.01},
		{"warm with shipment", `{"Celsius":10.078125}`, ship, "TEMP_EXCURSION", "Temperature.Celsius", 10.078125},
		{"frozen with shipment", `{"Celsius":-18.5}`, ship, "TEMP_EXCURSION", "Temperature.Celsius", -18.5},
		{"warm but no shipment", `{"Celsius":22}`, ``, "", "", 0},
		{"warm, shipment null", `{"Celsius":22}`, `"Shipment":null,`, "", "", 0},
		{"shipment id only in ShipmentId", `{"Celsius":22}`, `"ShipmentId":"S-2",`, "TEMP_EXCURSION", "Temperature.Celsius", 22},
		{"shipment without an id is not a shipment", `{"Celsius":22}`, `"Shipment":{"Carrier":"X"},`, "", "", 0},
		{"probe value used on fallback", `{"Celsius":null},"ProbeTemperature":{"Celsius":12}`, ship, "TEMPERATURE_FALLBACK,TEMP_EXCURSION", "ProbeTemperature.Celsius", 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := tt.temp
			if !strings.Contains(temp, "}") || strings.Count(temp, "{") > strings.Count(temp, "}") {
				temp += "}"
			}
			res := transform(t, `{`+head+tt.extra+`"Temperature":`+temp+`}`)
			if got := flagCodes(res); got != tt.want {
				t.Fatalf("flags = %q, want %q", got, tt.want)
			}
			if tt.want == "" {
				if res.Status != provider.StatusAccepted {
					t.Fatalf("status %s", res.Status)
				}
				return
			}
			for _, f := range res.Flags {
				if f.Code == "TEMP_EXCURSION" && (f.Field != tt.field || f.Value != tt.wantVal || f.Severity != "warn") {
					t.Fatalf("flag %+v", f)
				}
			}
			if res.Status != provider.StatusFlagged || res.Sensor == nil {
				t.Fatalf("an excursion is flagged but the reading is still stored: %s %+v", res.Status, res.Sensor)
			}
		})
	}
}

func TestTransformVersions(t *testing.T) {
	res := transform(t, `{`+head+`"Temperature":{"Celsius":10}}`)
	if res.TransformVersion == "" || res.RulesVersion == "" {
		t.Fatalf("versions must be set for audit: %+v", res)
	}
}

// F1-R6 / F3-R1: parse errors.
func TestParse_F3R1(t *testing.T) {
	p := newProvider(t)
	for _, in := range []string{`{not json`, ``, `[1]`, `"x"`} {
		if _, err := p.Parse([]byte(in)); err == nil || !strings.Contains(err.Error(), provider.ErrInvalidJSON.Error()) {
			t.Fatalf("Parse(%q) = %v, want ErrInvalidJSON", in, err)
		}
	}
	if p.Name() != "Tive" {
		t.Fatalf("Name = %q", p.Name())
	}
}

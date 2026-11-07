package compare

import "testing"

func row(rows []Row, field string) (Row, bool) {
	for _, r := range rows {
		if r.Field == field {
			return r, true
		}
	}
	return Row{}, false
}

func TestFields(t *testing.T) {
	tests := []struct {
		name                       string
		raw, sensor, location      string
		flags                      []Flag
		field, received, processed string
		changed                    bool
		note                       string
	}{
		{name: "rounding is not a change", raw: `{"Temperature":{"Celsius":4.625}}`, sensor: `{"temperature":4.63}`,
			field: "Temperature °C", received: "4.625", processed: "4.63", changed: false},
		{name: "sentinel nulled", raw: `{"Temperature":{"Celsius":-32768}}`, sensor: `{"temperature":null}`,
			flags: []Flag{{Code: "SENSOR_FAULT", Field: "Temperature.Celsius", Action: "set_null", RuleID: "TEMP-SENTINEL"}},
			field: "Temperature °C", received: "-32768", processed: "null", changed: true, note: "SENSOR_FAULT (rule TEMP-SENTINEL, set_null)"},
		{name: "fahrenheit only is converted", raw: `{"Temperature":{"Celsius":null,"Fahrenheit":40.1}}`, sensor: `{"temperature":4.5}`,
			field: "Temperature °C", received: "null (Fahrenheit 40.1)", processed: "4.5", changed: true},
		{name: "numeric string is coerced", raw: `{"Humidity":{"Percentage":"55.3"}}`, sensor: `{"humidity":55}`,
			field: "Humidity %", received: `"55.3"`, processed: "55", changed: false},
		{name: "seconds become milliseconds", raw: `{"EntryTimeEpoch":1739302046}`, sensor: `{"timestamp":1739302046000}`,
			field: "Timestamp", received: "1739302046", processed: "1739302046000", changed: true},
		{name: "invalid latitude skipped", raw: `{"Location":{"Latitude":95.0,"Longitude":-71.0}}`, location: ``,
			flags: []Flag{{Code: "INVALID_COORDINATES", Field: "Location.Latitude", Action: "location_skipped", RuleID: "LAT-RANGE"}},
			field: "Latitude", received: "95.0", processed: "—", changed: true, note: "INVALID_COORDINATES (rule LAT-RANGE, location_skipped)"},
		{name: "device identifiers unchanged", raw: `{"DeviceId":"863257063350583"}`, sensor: `{"device_imei":"863257063350583"}`,
			field: "Device IMEI", received: `"863257063350583"`, processed: `"863257063350583"`, changed: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := Fields([]byte(tt.raw), []byte(tt.sensor), []byte(tt.location), tt.flags)
			got, ok := row(rows, tt.field)
			if !ok {
				t.Fatalf("no row %q in %+v", tt.field, rows)
			}
			if got.Received != tt.received || got.Processed != tt.processed || got.Changed != tt.changed || got.Note != tt.note {
				t.Errorf("got %+v, want received=%q processed=%q changed=%v note=%q", got, tt.received, tt.processed, tt.changed, tt.note)
			}
		})
	}
}

func TestFieldsSkipsFieldsAbsentOnBothSides(t *testing.T) {
	rows := Fields([]byte(`{"Temperature":{"Celsius":4.5}}`), []byte(`{"temperature":4.5}`), nil, nil)
	if _, ok := row(rows, "Latitude"); ok {
		t.Errorf("Latitude should be left out: %+v", rows)
	}
}

func TestFieldsToleratesGarbage(t *testing.T) {
	for _, in := range []string{``, `not json`, `[]`, `null`} {
		if rows := Fields([]byte(in), []byte(in), []byte(in), nil); len(rows) != 0 {
			t.Errorf("input %q gave rows %+v", in, rows)
		}
	}
}

func TestPointOf(t *testing.T) {
	p := PointOf(
		[]byte(`{"EntryTimeEpoch":1739302046,"Temperature":{"Celsius":-32768},"Location":{"Latitude":95,"Longitude":10}}`),
		[]byte(`{"temperature":null}`), nil)
	if p.At != 1739302046000 {
		t.Errorf("At = %d, want milliseconds", p.At)
	}
	if p.RecTemp == nil || *p.RecTemp != -32768 || p.ProcTemp != nil {
		t.Errorf("temperatures = %v / %v", p.RecTemp, p.ProcTemp)
	}
	if p.RecLat == nil || p.ProcLat != nil {
		t.Errorf("latitudes = %v / %v", p.RecLat, p.ProcLat)
	}
}

func TestPointOfFallsBackToProcessedTimestamp(t *testing.T) {
	p := PointOf([]byte(`{}`), []byte(`{"timestamp":1739302046000}`), nil)
	if p.At != 1739302046000 {
		t.Errorf("At = %d", p.At)
	}
}

func TestSummarize(t *testing.T) {
	events := []Event{
		{Raw: []byte(`{"Temperature":{"Celsius":4.625}}`), Sensor: []byte(`{"temperature":4.63}`), Flags: nil},
		{Raw: []byte(`{"Temperature":{"Celsius":-32768}}`), Sensor: []byte(`{"temperature":null}`), Flags: []string{"SENSOR_FAULT"}},
		{Raw: []byte(`{}`), Flags: []string{"SENSOR_FAULT", "NO_USABLE_DATA"}},
	}
	s := Summarize(events)
	if s.Events != 3 || s.Changed != 1 || s.NoRecord != 1 {
		t.Errorf("summary = %+v", s)
	}
	if len(s.Flags) != 2 || s.Flags[0].Code != "SENSOR_FAULT" || s.Flags[0].Count != 2 {
		t.Errorf("flags = %+v", s.Flags)
	}
}

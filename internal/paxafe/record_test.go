package paxafe

import (
	"encoding/json"
	"testing"
)

func ptr[T any](v T) *T { return &v }

// F1-R5: records keep full precision in memory and round only in JSON.
func TestSensorRecordMarshal_F1R5(t *testing.T) {
	r := SensorRecord{
		DeviceID: "A571992", DeviceIMEI: "863257063350583", Timestamp: 1739215646000,
		Temperature: ptr(10.078125), Humidity: ptr(38.70000076293945), LightLevel: ptr(0.0),
		Accelerometer: &Accelerometer{X: ptr(-0.5625), Y: ptr(-0.4375), Z: ptr(0.6875), Magnitude: ptr(0.9901862198596787)},
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["temperature"] != 10.08 || got["humidity"] != 38.7 || got["light_level"] != 0.0 {
		t.Fatalf("unexpected scalars: %s", b)
	}
	acc := got["accelerometer"].(map[string]any)
	if acc["x"] != -0.563 || acc["y"] != -0.438 || acc["z"] != 0.688 || acc["magnitude"] != 0.990 {
		t.Fatalf("unexpected accelerometer: %s", b)
	}
	if got["provider"] != "Tive" || got["type"] != "Active" {
		t.Fatalf("constants missing: %s", b)
	}
	if v, ok := got["tilt"]; !ok || v != nil {
		t.Fatalf("tilt must be present and null: %s", b)
	}
	if v, ok := got["box_open"]; !ok || v != nil {
		t.Fatalf("box_open must be present and null: %s", b)
	}
	if *r.Temperature != 10.078125 {
		t.Fatal("marshal must not mutate the record")
	}
}

func TestSensorRecordMarshalNulls(t *testing.T) {
	b, err := json.Marshal(SensorRecord{DeviceID: "D", DeviceIMEI: "1", Timestamp: 1})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	for _, k := range []string{"temperature", "humidity", "light_level", "accelerometer", "tilt", "box_open"} {
		if v, ok := got[k]; !ok || v != nil {
			t.Fatalf("%s must be present and null: %s", k, b)
		}
	}
}

func TestLocationRecordMarshal_F1R5(t *testing.T) {
	r := LocationRecord{
		DeviceID: "A571992", DeviceIMEI: "863257063350583", Timestamp: 1739215646000,
		Latitude: 40.810562, Longitude: -73.879285,
		LocationAccuracy: ptr(23), AccuracyCategory: ptr("High"), LocationSource: ptr("WiFi"),
		Address:      &Address{Locality: ptr("Bronx"), State: ptr("NY"), Country: ptr("USA"), PostalCode: ptr("10474"), FullAddress: ptr("114 Hunts Point Market, Bronx, NY 10474, USA")},
		BatteryLevel: ptr(65), CellularDbm: ptr(-100.004), WifiAccessPoints: ptr(5),
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if got["latitude"] != 40.810562 || got["longitude"] != -73.879285 {
		t.Fatalf("coordinates must keep full precision: %s", b)
	}
	if got["cellular_dbm"] != -100.0 {
		t.Fatalf("dbm must round to 2dp: %s", b)
	}
	for _, k := range []string{"altitude", "cellular_network_type", "cellular_operator"} {
		if v, ok := got[k]; !ok || v != nil {
			t.Fatalf("%s must be present and null: %s", k, b)
		}
	}
	addr := got["address"].(map[string]any)
	if v, ok := addr["street"]; !ok || v != nil {
		t.Fatalf("street must be present and null: %s", b)
	}
}

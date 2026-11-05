package paxafe

import (
	"os"
	"testing"
)

// F1-R6: emitted records validate against the PAXAFE JSON Schemas.
func TestValidate_F1R6(t *testing.T) {
	good := SensorRecord{DeviceID: "A571992", DeviceIMEI: "863257063350583", Timestamp: 1739215646000, Temperature: ptr(10.08)}
	if err := ValidateSensor(good); err != nil {
		t.Fatalf("valid sensor rejected: %v", err)
	}
	bad := good
	bad.Timestamp = -1
	if err := ValidateSensor(bad); err == nil {
		t.Fatal("negative timestamp must fail the schema")
	}

	loc := LocationRecord{DeviceID: "A571992", DeviceIMEI: "863257063350583", Timestamp: 1739215646000, Latitude: 40.8, Longitude: -73.9}
	if err := ValidateLocation(loc); err != nil {
		t.Fatalf("valid location rejected: %v", err)
	}
	loc.Latitude = 95
	if err := ValidateLocation(loc); err == nil {
		t.Fatal("latitude 95 must fail the schema")
	}
	loc.Latitude = 40
	loc.BatteryLevel = ptr(101)
	if err := ValidateLocation(loc); err == nil {
		t.Fatal("battery 101 must fail the schema")
	}
	loc.BatteryLevel = nil
	loc.AccuracyCategory = ptr("Huge")
	if err := ValidateLocation(loc); err == nil {
		t.Fatal("unknown accuracy category must fail the schema")
	}
}

// The embedded schemas must be byte-identical to the provided assignment files.
func TestEmbeddedSchemasMatchAssignment(t *testing.T) {
	for name, src := range map[string]string{
		"schemas/px-sensor-schema.json":   "../../assignment/px-sensor-schema.json",
		"schemas/px-location-schema.json": "../../assignment/px-location-schema.json",
	} {
		embedded, err := schemaFS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		orig, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if string(embedded) != string(orig) {
			t.Fatalf("%s differs from %s", name, src)
		}
	}
}

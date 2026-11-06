package tive

import "testing"

// Data PAXAFE output omits is still captured for storage.
func TestAux(t *testing.T) {
	raw := `{` + head + `"EntityName":"A571992","Temperature":{"Celsius":10},"ProbeTemperature":{"Celsius":9.9375,"Fahrenheit":49.8875},
	 "Cellular":{"SignalStrength":"Poor","Dbm":-100},"Battery":{"Percentage":65,"Estimation":"Days","IsCharging":false},
	 "Shipment":{"Id":"CL-1","Carrier":"EXCALIBUR","Description":null,"ShipFrom":{"Latitude":26.1,"Longitude":-98.2,"FormattedAddress":"Pharr, TX"},"ShipTo":{"Latitude":"x","Longitude":-73.9}},
	 "PublicShipmentId":"40X614N4WC","AccountId":478,
	 "Location":{"Latitude":1,"Longitude":2,"GeolocationSourceName":"skyhook","CellTowerUsedCount":1,"Accuracy":{"Meters":23,"Kilometers":0.023,"Miles":0.014}}}`
	res := transform(t, raw)
	aux := res.Aux
	if aux.ProbeTemperature == nil || *aux.ProbeTemperature != 9.9375 {
		t.Fatalf("probe = %v", aux.ProbeTemperature)
	}
	if aux.LocationExtras["signal_strength"] != "Poor" || aux.LocationExtras["battery_estimation"] != "Days" ||
		aux.LocationExtras["battery_is_charging"] != false || aux.LocationExtras["geolocation_source"] != "skyhook" ||
		aux.LocationExtras["cell_towers"] != 1.0 || aux.LocationExtras["account_id"] != 478.0 {
		t.Fatalf("location extras = %v", aux.LocationExtras)
	}
	sh := aux.Shipment
	if sh == nil || sh.ID != "CL-1" || sh.PublicID != "40X614N4WC" || sh.Carrier != "EXCALIBUR" || sh.FromAddress != "Pharr, TX" {
		t.Fatalf("shipment = %+v", sh)
	}
	if sh.FromLat == nil || *sh.FromLat != 26.1 || sh.ToLat != nil || sh.ToLon == nil {
		t.Fatalf("shipment coordinates = %+v", sh)
	}
	for _, f := range res.Flags {
		if f.Code == "INVALID_TYPE" {
			t.Fatalf("stored-only data must not raise INVALID_TYPE: %+v", f)
		}
	}
}

func TestAuxNoShipment(t *testing.T) {
	if res := transform(t, `{`+head+`"Temperature":{"Celsius":10},"Shipment":null,"ShipmentId":null}`); res.Aux.Shipment != nil {
		t.Fatalf("no shipment id means no shipment: %+v", res.Aux.Shipment)
	}
}

// AccountId is stored as metadata and never routes, validates or flags anything.
func TestAccountIDIsMetadataOnly(t *testing.T) {
	for _, acct := range []string{`"AccountId":478,`, `"AccountId":999,`, `"AccountId":"not a number",`, ``} {
		res := transform(t, `{`+head+acct+`"Temperature":{"Celsius":5}}`)
		if res.Status != "accepted" || len(res.Flags) != 0 || len(res.Errors) != 0 {
			t.Fatalf("%q: %s %v %v", acct, res.Status, res.Flags, res.Errors)
		}
	}
	res := transform(t, `{`+head+`"AccountId":999,"Temperature":{"Celsius":5},"Location":{"Latitude":1,"Longitude":2}}`)
	if res.Aux.SensorExtras["account_id"] != 999.0 || res.Aux.LocationExtras["account_id"] != 999.0 {
		t.Fatalf("extras %v %v", res.Aux.SensorExtras, res.Aux.LocationExtras)
	}
}

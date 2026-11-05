package tive

import "github.com/yuvasrig/paxefo-integration-api/internal/provider"

// buildAux collects the Tive data PAXAFE output leaves out (contracts/mapping.md "Kept outside PAXAFE output").
// Reads here are quiet: a wrong type in stored-only data does not raise a flag.
func buildAux(rd *reader, probe *float64) provider.Aux {
	aux := provider.Aux{ProbeTemperature: probe, SensorExtras: map[string]any{}, LocationExtras: map[string]any{}}

	putStr := func(m map[string]any, key, path string) {
		if s, ok := rd.str(path); ok {
			m[key] = s
		}
	}
	putNum := func(m map[string]any, key, path string) {
		if f := rd.quietNum(path); f != nil {
			m[key] = *f
		}
	}
	putStr(aux.SensorExtras, "entity_name", "EntityName")
	if f := rd.quietNum("ProbeTemperature.Fahrenheit"); f != nil {
		aux.SensorExtras["probe_fahrenheit"] = *f
	}

	putStr(aux.LocationExtras, "signal_strength", "Cellular.SignalStrength")
	putStr(aux.LocationExtras, "battery_estimation", "Battery.Estimation")
	if v, _ := rd.doc.Get("Battery.IsCharging"); v != nil {
		if b, ok := v.(bool); ok {
			aux.LocationExtras["battery_is_charging"] = b
		}
	}
	putStr(aux.LocationExtras, "geolocation_source", "Location.GeolocationSourceName")
	putNum(aux.LocationExtras, "cell_towers", "Location.CellTowerUsedCount")
	putNum(aux.LocationExtras, "accuracy_km", "Location.Accuracy.Kilometers")
	putNum(aux.LocationExtras, "accuracy_miles", "Location.Accuracy.Miles")
	putNum(aux.LocationExtras, "account_id", "AccountId")

	id, _ := rd.str("Shipment.Id")
	if id == "" {
		id, _ = rd.str("ShipmentId")
	}
	if id != "" {
		sh := &provider.Shipment{ID: id}
		sh.PublicID, _ = rd.str("PublicShipmentId")
		sh.Carrier, _ = rd.str("Shipment.Carrier")
		sh.Description, _ = rd.str("Shipment.Description")
		sh.FromLat, sh.FromLon = rd.quietNum("Shipment.ShipFrom.Latitude"), rd.quietNum("Shipment.ShipFrom.Longitude")
		sh.FromAddress, _ = rd.str("Shipment.ShipFrom.FormattedAddress")
		sh.ToLat, sh.ToLon = rd.quietNum("Shipment.ShipTo.Latitude"), rd.quietNum("Shipment.ShipTo.Longitude")
		sh.ToAddress, _ = rd.str("Shipment.ShipTo.FormattedAddress")
		aux.Shipment = sh
	}
	return aux
}

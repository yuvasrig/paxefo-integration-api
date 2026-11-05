// Package paxafe defines the PAXAFE output records, rounding at marshal time and
// JSON Schema validation. Records hold full precision; rounding happens only in
// MarshalJSON (F1-R5). Pure: no I/O.
package paxafe

import "encoding/json"

const (
	ProviderTive = "Tive"
	TypeActive   = "Active"

	TemperatureSourceDevice = "device"
	TemperatureSourceProbe  = "probe"
)

// Accelerometer readings in G.
type Accelerometer struct {
	X, Y, Z, Magnitude *float64
}

// SensorRecord is the PAXAFE sensor payload (px-sensor-schema.json).
type SensorRecord struct {
	DeviceID    string
	DeviceIMEI  string
	Timestamp   int64
	Temperature *float64
	// TemperatureSource is stored with the reading but is not part of PAXAFE output.
	TemperatureSource string
	Humidity          *float64
	LightLevel        *float64
	Accelerometer     *Accelerometer
}

// Address is the parsed address; FullAddress is always the verbatim input.
type Address struct {
	Street, Locality, State, Country, PostalCode, FullAddress *string
}

// LocationRecord is the PAXAFE location payload (px-location-schema.json).
type LocationRecord struct {
	DeviceID         string
	DeviceIMEI       string
	Timestamp        int64
	Latitude         float64
	Longitude        float64
	LocationAccuracy *int
	AccuracyCategory *string
	LocationSource   *string
	Address          *Address
	BatteryLevel     *int
	CellularDbm      *float64
	WifiAccessPoints *int
}

type accelerometerJSON struct {
	X         *float64 `json:"x"`
	Y         *float64 `json:"y"`
	Z         *float64 `json:"z"`
	Magnitude *float64 `json:"magnitude"`
}

// MarshalJSON emits the PAXAFE sensor shape with the documented decimal places:
// temperature 2, humidity 1, light 1, accelerometer 3.
func (r SensorRecord) MarshalJSON() ([]byte, error) {
	var acc *accelerometerJSON
	if r.Accelerometer != nil {
		acc = &accelerometerJSON{
			X:         roundPtr(r.Accelerometer.X, 3),
			Y:         roundPtr(r.Accelerometer.Y, 3),
			Z:         roundPtr(r.Accelerometer.Z, 3),
			Magnitude: roundPtr(r.Accelerometer.Magnitude, 3),
		}
	}
	return json.Marshal(struct {
		DeviceID      string             `json:"device_id"`
		DeviceIMEI    string             `json:"device_imei"`
		Timestamp     int64              `json:"timestamp"`
		Provider      string             `json:"provider"`
		Type          string             `json:"type"`
		Temperature   *float64           `json:"temperature"`
		Humidity      *float64           `json:"humidity"`
		LightLevel    *float64           `json:"light_level"`
		Accelerometer *accelerometerJSON `json:"accelerometer"`
		Tilt          *struct{}          `json:"tilt"`
		BoxOpen       *bool              `json:"box_open"`
	}{
		DeviceID: r.DeviceID, DeviceIMEI: r.DeviceIMEI, Timestamp: r.Timestamp,
		Provider: ProviderTive, Type: TypeActive,
		Temperature:   roundPtr(r.Temperature, 2),
		Humidity:      roundPtr(r.Humidity, 1),
		LightLevel:    roundPtr(r.LightLevel, 1),
		Accelerometer: acc,
	})
}

type addressJSON struct {
	Street      *string `json:"street"`
	Locality    *string `json:"locality"`
	State       *string `json:"state"`
	Country     *string `json:"country"`
	PostalCode  *string `json:"postal_code"`
	FullAddress *string `json:"full_address"`
}

// MarshalJSON emits the PAXAFE location shape; coordinates keep full precision,
// cellular_dbm is rounded to 2 decimals.
func (r LocationRecord) MarshalJSON() ([]byte, error) {
	var addr *addressJSON
	if r.Address != nil {
		a := addressJSON(*r.Address)
		addr = &a
	}
	return json.Marshal(struct {
		DeviceID         string       `json:"device_id"`
		DeviceIMEI       string       `json:"device_imei"`
		Timestamp        int64        `json:"timestamp"`
		Provider         string       `json:"provider"`
		Type             string       `json:"type"`
		Latitude         float64      `json:"latitude"`
		Longitude        float64      `json:"longitude"`
		Altitude         *float64     `json:"altitude"`
		LocationAccuracy *int         `json:"location_accuracy"`
		AccuracyCategory *string      `json:"location_accuracy_category"`
		LocationSource   *string      `json:"location_source"`
		Address          *addressJSON `json:"address"`
		BatteryLevel     *int         `json:"battery_level"`
		CellularDbm      *float64     `json:"cellular_dbm"`
		NetworkType      *string      `json:"cellular_network_type"`
		Operator         *string      `json:"cellular_operator"`
		WifiAccessPoints *int         `json:"wifi_access_points"`
	}{
		DeviceID: r.DeviceID, DeviceIMEI: r.DeviceIMEI, Timestamp: r.Timestamp,
		Provider: ProviderTive, Type: TypeActive,
		Latitude: r.Latitude, Longitude: r.Longitude,
		LocationAccuracy: r.LocationAccuracy, AccuracyCategory: r.AccuracyCategory,
		LocationSource: r.LocationSource, Address: addr,
		BatteryLevel: r.BatteryLevel, CellularDbm: roundPtr(r.CellularDbm, 2),
		WifiAccessPoints: r.WifiAccessPoints,
	})
}

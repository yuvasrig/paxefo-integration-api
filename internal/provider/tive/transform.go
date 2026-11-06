package tive

import (
	"fmt"
	"math"
	"strings"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// Transform applies the ruleset, then maps the cleaned payload to PAXAFE sensor and location
// records per contracts/mapping.md. Invalid optional fields are nulled and flagged; the record
// is rejected only when it is unusable (constitution #3 and #4).
func (p *Provider) Transform(d doc.Doc, sc rules.Scope) (provider.Result, error) {
	sc.NumericFields = NumericFields
	ruled := rules.Apply(d, p.rules, sc)
	rd := newReader(ruled.Doc)

	res := provider.Result{TransformVersion: TransformVersion, RulesVersion: p.rules.Version}
	errs := ruled.Rejections
	var extra []rules.Flag

	// Identity (F3-R2, F5): rules resolve by name; this is the backstop if a ruleset omits that.
	imei := imeiOf(ruled.Doc)
	if imei == "" && !hasCode(errs, rules.CodeNoDeviceIdentity) {
		errs = append(errs, rules.Problem{Code: rules.CodeNoDeviceIdentity, Field: "DeviceId", Message: "no valid device IMEI and DeviceName is not known"})
	}
	deviceID, _ := rd.str("DeviceName")
	if deviceID == "" {
		deviceID = imei
	}

	// Timestamp (F3-R3).
	var ts int64
	if f := rd.num("EntryTimeEpoch"); f != nil && *f > 0 {
		ts = int64(math.Round(*f))
	}
	if ts == 0 {
		errs = append(errs, rules.Problem{Code: rules.CodeNoTimestamp, Field: "EntryTimeEpoch", Message: "EntryTimeEpoch is missing or not a positive number"})
	}

	// Temperature: device value, else probe (F2-R8).
	temp, source := rd.num("Temperature.Celsius"), paxafe.TemperatureSourceDevice
	probe := rd.num("ProbeTemperature.Celsius")
	if temp == nil && probe != nil {
		temp, source = probe, paxafe.TemperatureSourceProbe
		extra = append(extra, rules.Flag{
			Code: rules.CodeTemperatureFallback, Severity: rules.SeverityInfo, Field: "Temperature.Celsius",
			NewValue: *probe, Action: rules.ActionFixed,
		})
	}
	if temp == nil {
		source = ""
	}

	humidity, lux := rd.num("Humidity.Percentage"), rd.num("Light.Lux")
	acc := readAccelerometer(rd)

	var sensor *paxafe.SensorRecord
	if !ruled.Dropped["sensor"] && (temp != nil || humidity != nil || lux != nil || acc != nil) {
		sensor = &paxafe.SensorRecord{
			DeviceID: deviceID, DeviceIMEI: imei, Timestamp: ts,
			Temperature: temp, TemperatureSource: source,
			Humidity: humidity, LightLevel: lux, Accelerometer: acc,
		}
	}

	location, locFlags := p.readLocation(rd, ruled, deviceID, imei, ts)
	extra = append(extra, locFlags...)

	// Unusable: neither half (F3-R5).
	if temp == nil && location == nil {
		errs = append(errs, rules.Problem{Code: rules.CodeNoUsableData, Message: "no usable temperature and no valid coordinates"})
	}

	res.Aux = buildAux(rd, probe)
	res.Flags = append(append(append(res.Flags, ruled.Flags...), rd.flags...), extra...)
	res.Errors = errs
	if len(errs) > 0 {
		res.Status = provider.StatusRejected
		return res, nil // a rejected record emits no PAXAFE records (F3-R7)
	}

	if sensor != nil {
		if err := paxafe.ValidateSensor(*sensor); err != nil {
			return res, fmt.Errorf("tive: sensor record: %w", err)
		}
	}
	if location != nil {
		if err := paxafe.ValidateLocation(*location); err != nil {
			return res, fmt.Errorf("tive: location record: %w", err)
		}
	}
	res.Sensor, res.Location, res.Status = sensor, location, statusOf(res.Flags)
	return res, nil
}

// statusOf: an event is flagged if and only if it carries any flag.
func statusOf(flags []rules.Flag) string {
	if len(flags) > 0 {
		return provider.StatusFlagged
	}
	return provider.StatusAccepted
}

func hasCode(ps []rules.Problem, code string) bool {
	for _, p := range ps {
		if p.Code == code {
			return true
		}
	}
	return false
}

// imeiOf returns DeviceId when it is a 15-digit string (or an integer with 15 digits).
func imeiOf(d doc.Doc) string {
	v, _ := d.Get("DeviceId")
	var s string
	switch t := v.(type) {
	case string:
		s = t
	case interface{ String() string }:
		s = t.String()
	}
	if len(s) != 15 {
		return ""
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return ""
		}
	}
	return s
}

// readAccelerometer builds the accelerometer, deriving magnitude from the axes when G is missing (F2-R10).
func readAccelerometer(rd *reader) *paxafe.Accelerometer {
	x, y, z, g := rd.num("Accelerometer.X"), rd.num("Accelerometer.Y"), rd.num("Accelerometer.Z"), rd.num("Accelerometer.G")
	if x == nil && y == nil && z == nil && g == nil {
		return nil
	}
	if g == nil && x != nil && y != nil && z != nil {
		m := rules.Magnitude(*x, *y, *z)
		g = &m
	}
	return &paxafe.Accelerometer{X: x, Y: y, Z: z, Magnitude: g}
}

// readLocation builds the location record. It returns nil when the rules dropped the section or the
// coordinates are unusable; a half-missing or out-of-range pair is flagged INVALID_COORDINATES.
func (p *Provider) readLocation(rd *reader, ruled rules.Result, deviceID, imei string, ts int64) (*paxafe.LocationRecord, []rules.Flag) {
	lat, lon := rd.num("Location.Latitude"), rd.num("Location.Longitude")
	if ruled.Dropped["location"] || (lat == nil && lon == nil) {
		return nil, nil
	}
	if lat == nil || lon == nil || *lat < -90 || *lat > 90 || *lon < -180 || *lon > 180 {
		return nil, []rules.Flag{{
			Code: rules.CodeInvalidCoordinates, Severity: rules.SeverityError, Field: "Location",
			Action: rules.ActionLocationSkipped,
		}}
	}

	loc := &paxafe.LocationRecord{DeviceID: deviceID, DeviceIMEI: imei, Timestamp: ts, Latitude: *lat, Longitude: *lon}

	if meters := rd.num("Location.Accuracy.Meters"); meters != nil {
		rounded := int(math.Round(*meters))
		loc.LocationAccuracy = &rounded
		cat := p.accuracyCategory(*meters)
		loc.AccuracyCategory = &cat
	}
	loc.LocationSource = locationSource(rd)
	if full, ok := rd.str("Location.FormattedAddress"); ok {
		loc.Address = parseAddress(full)
	}
	if b := rd.integer("Battery.Percentage"); b != nil && *b >= 0 && *b <= 100 {
		loc.BatteryLevel = b
	}
	loc.CellularDbm = rd.num("Cellular.Dbm")
	loc.WifiAccessPoints = rd.integer("Location.WifiAccessPointUsedCount")
	return loc, nil
}

// accuracyCategory: High below the first threshold, Medium below the second, else Low (bounds exclusive).
func (p *Provider) accuracyCategory(meters float64) string {
	switch {
	case meters < p.cfg.AccuracyHighBelowM:
		return "High"
	case meters < p.cfg.AccuracyMediumBelowM:
		return "Medium"
	default:
		return "Low"
	}
}

func locationSource(rd *reader) *string {
	m, ok := rd.str("Location.LocationMethod")
	if !ok {
		return nil
	}
	var s string
	switch strings.ToLower(strings.TrimSpace(m)) {
	case "gps":
		s = "GPS"
	case "wifi":
		s = "WiFi"
	case "cell":
		s = "Cellular"
	default:
		return nil
	}
	return &s
}

package tive

import (
	"encoding/json"
	"math"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// NumericFields lists every Tive field that should hold a number. The NUMERIC-STRING rule
// (field "*") coerces numeric strings only on these paths, so identifiers such as
// DeviceId "863257063350583" are never turned into numbers.
var NumericFields = []string{
	"EntryTimeEpoch",
	"AccountId",
	"Cellular.Dbm",
	"Temperature.Celsius",
	"Temperature.Fahrenheit",
	"ProbeTemperature.Celsius",
	"ProbeTemperature.Fahrenheit",
	"Humidity.Percentage",
	"Accelerometer.G",
	"Accelerometer.X",
	"Accelerometer.Y",
	"Accelerometer.Z",
	"Light.Lux",
	"Battery.Percentage",
	"Location.Latitude",
	"Location.Longitude",
	"Location.Accuracy.Meters",
	"Location.Accuracy.Kilometers",
	"Location.Accuracy.Miles",
	"Location.CellTowerUsedCount",
	"Location.WifiAccessPointUsedCount",
}

const maxInt = 1 << 31

// reader pulls typed values out of a rule-cleaned doc and records INVALID_TYPE flags
// for values of the wrong JSON type (F2-R15). null and missing both mean "absent" and raise no flag.
type reader struct {
	doc   doc.Doc
	flags []rules.Flag
}

func newReader(d doc.Doc) *reader { return &reader{doc: d} }

// num returns the number at path, or nil when absent or of the wrong type.
func (r *reader) num(path string) *float64 {
	v, present := r.doc.Get(path)
	if !present || v == nil {
		return nil
	}
	if n, ok := v.(json.Number); ok {
		f, err := n.Float64()
		if err == nil && !math.IsInf(f, 0) {
			return &f
		}
	}
	r.flags = append(r.flags, rules.Flag{
		Code: rules.CodeInvalidType, Severity: rules.SeverityWarn,
		Field: path, Value: v, Action: rules.ActionNullified,
	})
	return nil
}

// integer is num rounded half away from zero, for fields PAXAFE defines as integers.
func (r *reader) integer(path string) *int {
	f := r.num(path)
	if f == nil {
		return nil
	}
	if math.Abs(*f) >= maxInt {
		v, _ := r.doc.Get(path)
		r.flags = append(r.flags, rules.Flag{
			Code: rules.CodeInvalidType, Severity: rules.SeverityWarn,
			Field: path, Value: v, Action: rules.ActionNullified,
		})
		return nil
	}
	i := int(math.Round(*f))
	return &i
}

// str returns a non-empty string at path; anything else counts as absent (no flag).
func (r *reader) str(path string) (string, bool) {
	v, _ := r.doc.Get(path)
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

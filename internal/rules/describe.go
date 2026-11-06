package rules

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file turns a rule into plain English for the admin console, so a rule can be read (and checked) without
// knowing the YAML. It is pure and has no effect on how rules run.

// Field is a payload field the console offers when adding a rule.
type Field struct{ Path, Label string }

var fieldLabels = map[string]string{
	"*":                        "every numeric field",
	"DeviceId":                 "Device ID (IMEI)",
	"DeviceName":               "Device name",
	"EntityName":               "Entity name",
	"EntryTimeEpoch":           "Reading time (epoch)",
	"Temperature.Celsius":      "Temperature (°C)",
	"Temperature.Fahrenheit":   "Temperature (°F)",
	"ProbeTemperature.Celsius": "Probe temperature (°C)",
	"Humidity.Percentage":      "Humidity (%)",
	"Light.Lux":                "Light (lux)",
	"Battery.Percentage":       "Battery (%)",
	"Location.Latitude":        "Latitude",
	"Location.Longitude":       "Longitude",
	"Location.Accuracy.Meters": "Location accuracy (m)",
	"Cellular.Dbm":             "Cellular signal (dBm)",
	"Accelerometer.X":          "Accelerometer X",
	"Accelerometer.Y":          "Accelerometer Y",
	"Accelerometer.Z":          "Accelerometer Z",
	"Accelerometer.G":          "Accelerometer magnitude (G)",
	"Shipment.DeviceId":        "Shipment device ID",
}

// FieldLabel is a friendly name for a payload field; an unknown field is shown as its path.
func FieldLabel(path string) string {
	if l, ok := fieldLabels[path]; ok {
		return l
	}
	return path
}

// KnownFields lists the fields the console offers, by label.
func KnownFields() []Field {
	var out []Field
	for p, l := range fieldLabels {
		if p != "*" {
			out = append(out, Field{p, l})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label < out[j].Label })
	return out
}

// Actions the console offers, with the wording used next to them.
var ActionLabels = []struct{ Value, Label string }{
	{actionFlag, "Keep the value and flag it"},
	{actionNullify, "Clear the value and flag it"},
	{actionDropSection, "Drop the sensor or location record and flag it"},
	{actionReject, "Reject the whole reading"},
	{actionFix, "Fix it with a built-in fix"},
	{actionIgnore, "Do nothing"},
}

// SeverityLabel is the word shown for a severity.
func SeverityLabel(s string) string {
	switch s {
	case SeverityWarn:
		return "warning"
	case SeverityError:
		return "error"
	case SeverityInfo:
		return "info"
	}
	return s
}

// Condition says in words when the rule applies, for example "is outside 0 to 100".
func (r Rule) Condition() string {
	w, f := r.When, FieldLabel(r.Field)
	var parts []string
	if w.NumericString != nil && *w.NumericString {
		parts = append(parts, "is text that holds a number")
	}
	if w.InvalidIMEI != nil && *w.InvalidIMEI {
		parts = append(parts, "is not a valid 15-digit device ID")
	}
	if w.UnresolvedBy != "" {
		parts = append(parts, "the device name is not a known device")
	}
	if w.ResolvedBy != "" {
		parts = append(parts, "the device name is a known device")
	}
	if w.IsNull != nil && *w.IsNull {
		parts = append(parts, "is missing")
	}
	if w.Present != nil && *w.Present {
		parts = append(parts, "has a value")
	}
	if w.SiblingPresent != "" {
		parts = append(parts, FieldLabel(w.SiblingPresent)+" is present")
	}
	if w.Lt != nil {
		parts = append(parts, "is below "+Number(*w.Lt))
	}
	if len(w.In) > 0 {
		var ns []string
		for _, n := range w.In {
			ns = append(ns, Number(n))
		}
		parts = append(parts, "is exactly one of "+strings.Join(ns, ", "))
	}
	if len(w.Outside) == 2 {
		parts = append(parts, fmt.Sprintf("is outside %s to %s", Number(w.Outside[0]), Number(w.Outside[1])))
	}
	if w.AfterNow != "" {
		parts = append(parts, "is more than "+Duration(w.AfterNow)+" in the future")
	}
	if w.DiffersFrom != "" {
		parts = append(parts, "differs from "+FieldLabel(w.DiffersFrom))
	}
	if len(parts) == 0 {
		return "always"
	}
	// "Humidity (%) is outside 0 to 100", "Device ID is not valid and the device name is not known".
	out := make([]string, len(parts))
	for i, p := range parts {
		switch {
		case strings.HasPrefix(p, "is "), strings.HasPrefix(p, "has "):
			out[i] = f + " " + p
		default:
			out[i] = p
		}
	}
	return strings.Join(out, " and ")
}

// Outcome says in words what the rule does, for example "clear the value and flag it OUT_OF_RANGE (warning)".
func (r Rule) Outcome() string {
	var what string
	switch r.Action {
	case actionFix:
		what = strategyWords(r)
	case actionNullify:
		what = "clear the value"
	case actionDropSection:
		what = "drop the " + r.Section + " record and keep the rest"
	case actionReject:
		return "reject the whole reading (" + r.Code + ")"
	case actionFlag:
		what = "keep the value"
	case actionIgnore:
		return "do nothing"
	default:
		what = r.Action
	}
	return what + " and flag it " + r.Code + " (" + SeverityLabel(r.Severity) + ")"
}

func strategyWords(r Rule) string {
	switch r.Strategy {
	case "coerce_number":
		return "convert it to a number"
	case "scale":
		if f, ok := toFloat(r.Params["factor"]); ok {
			return "multiply it by " + Number(f)
		}
		return "rescale it"
	case "fahrenheit_to_celsius":
		return "calculate it from the Fahrenheit value"
	case "resolve_device_by_name":
		return "look the device up by its name"
	case "normalize_enum":
		return "tidy it to a standard value"
	case "derive_magnitude":
		return "calculate it from the X, Y and Z axes"
	}
	return "fix it (" + r.Strategy + ")"
}

// Sentence is the whole rule as one sentence.
func (r Rule) Sentence() string {
	c := r.Condition()
	if r.Disabled {
		return "(off) When " + c + ", " + r.Outcome() + "."
	}
	return "When " + c + ", " + r.Outcome() + "."
}

// Number formats a number for people: no trailing zeros, thousands separated.
func Number(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	intPart, frac, _ := strings.Cut(s, ".")
	if len(intPart) > 3 {
		var b strings.Builder
		for i, c := range intPart {
			if i > 0 && (len(intPart)-i)%3 == 0 {
				b.WriteByte(',')
			}
			b.WriteRune(c)
		}
		intPart = b.String()
	}
	if frac != "" {
		intPart += "." + frac
	}
	if neg {
		return "-" + intPart
	}
	return intPart
}

// Duration formats a Go duration string ("5m") in words ("5 minutes").
func Duration(s string) string {
	d, err := time.ParseDuration(s)
	if err != nil {
		return s
	}
	unit := func(n float64, one, many string) string {
		if n == 1 {
			return "1 " + one
		}
		return Number(n) + " " + many
	}
	switch {
	case d >= time.Hour && d%time.Hour == 0:
		return unit(d.Hours(), "hour", "hours")
	case d >= time.Minute && d%time.Minute == 0:
		return unit(d.Minutes(), "minute", "minutes")
	case d >= time.Second && d%time.Second == 0:
		return unit(d.Seconds(), "second", "seconds")
	}
	return s
}

package rules

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// strategyInput is what a fix strategy sees.
type strategyInput struct {
	t    target
	rule *Rule
}

// strategy returns the replacement value, or changed=false to leave the field alone.
type strategy func(in strategyInput) (newVal any, changed bool)

var strategies = map[string]strategy{
	"scale":                  stratScale,
	"coerce_number":          stratCoerceNumber,
	"fahrenheit_to_celsius":  stratFahrenheitToCelsius,
	"normalize_enum":         stratNormalizeEnum,
	"derive_magnitude":       stratDeriveMagnitude,
	"resolve_device_by_name": stratResolveDevice,
}

// validateStrategy checks a fix rule's strategy name and params at load time.
func validateStrategy(r *Rule) error {
	if r.Strategy == "" {
		return fmt.Errorf("action fix needs a strategy")
	}
	if _, ok := strategies[r.Strategy]; !ok {
		return fmt.Errorf("unknown strategy %q", r.Strategy)
	}
	switch r.Strategy {
	case "scale":
		if _, ok := toFloat(r.Params["factor"]); !ok {
			return fmt.Errorf("strategy scale needs params.factor")
		}
	case "fahrenheit_to_celsius":
		if r.When.SiblingPresent == "" {
			return fmt.Errorf("strategy fahrenheit_to_celsius needs when.sibling_present (the Fahrenheit field)")
		}
	case "normalize_enum":
		if m, ok := r.Params["map"].(map[string]any); !ok || len(m) == 0 {
			return fmt.Errorf("strategy normalize_enum needs params.map")
		}
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case float64:
		return t, true
	}
	return 0, false
}

func formatNumber(f float64) json.Number {
	return json.Number(strconv.FormatFloat(f, 'f', -1, 64))
}

// Magnitude is the acceleration magnitude sqrt(x^2 + y^2 + z^2), in G.
func Magnitude(x, y, z float64) float64 { return math.Sqrt(x*x + y*y + z*z) }

// scale multiplies a number by params.factor (e.g. epoch seconds to milliseconds).
func stratScale(in strategyInput) (any, bool) {
	f, ok := number(in.t.val)
	factor, _ := toFloat(in.rule.Params["factor"])
	if !ok {
		return nil, false
	}
	return formatNumber(f * factor), true
}

// coerce_number turns a numeric string into a number, keeping its text.
func stratCoerceNumber(in strategyInput) (any, bool) {
	s, ok := in.t.val.(string)
	if !ok {
		return nil, false
	}
	s = strings.TrimSpace(s)
	if !numericStringRe.MatchString(s) {
		return nil, false
	}
	return json.Number(s), true
}

// fahrenheit_to_celsius converts the sibling Fahrenheit field: (F - 32) * 5/9.
func stratFahrenheitToCelsius(in strategyInput) (any, bool) {
	v, _ := in.t.d.Get(in.rule.When.SiblingPresent)
	f, ok := number(v)
	if !ok {
		return nil, false
	}
	return formatNumber((f - 32) * 5 / 9), true
}

// normalize_enum maps a string through params.map, case-insensitively after trimming.
func stratNormalizeEnum(in strategyInput) (any, bool) {
	s, ok := in.t.val.(string)
	if !ok {
		return nil, false
	}
	key := strings.ToUpper(strings.TrimSpace(s))
	m, _ := in.rule.Params["map"].(map[string]any)
	for k, v := range m {
		if strings.ToUpper(k) == key {
			if to, ok := v.(string); ok && to != s {
				return to, true
			}
			return nil, false
		}
	}
	return nil, false
}

// derive_magnitude fills Accelerometer.G from X, Y, Z when all three are numbers.
func stratDeriveMagnitude(in strategyInput) (any, bool) {
	var xyz [3]float64
	for i, axis := range []string{"X", "Y", "Z"} {
		v, _ := in.t.d.Get("Accelerometer." + axis)
		f, ok := number(v)
		if !ok {
			return nil, false
		}
		xyz[i] = f
	}
	return formatNumber(Magnitude(xyz[0], xyz[1], xyz[2])), true
}

// resolve_device_by_name replaces DeviceId with the IMEI found for DeviceName.
func stratResolveDevice(in strategyInput) (any, bool) {
	imei, ok := in.t.resolveName()
	if !ok {
		return nil, false
	}
	return imei, true
}

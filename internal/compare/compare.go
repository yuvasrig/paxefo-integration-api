// Package compare sets what a tenant sent (the Tive payload as received) beside what the API produced from it (the PAXAFE
// sensor and location records), field by field. Everything here is a pure function of those documents, so the admin
// console only has to fetch them and draw the result.
package compare

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
)

// roundingTolerance: the records are rounded on output (2 to 4 decimals), so a difference this small is the rounding
// step, not a change the rules made. The values are still shown; they are just not marked as changed.
const roundingTolerance = 0.0051

// wholeNumbers are rounded to an integer on output, so they may move by up to half a unit without being changed.
var wholeNumbers = map[string]bool{"Humidity %": true, "Location accuracy (m)": true, "Battery %": true, "Cellular dBm": true}

// msThreshold: an epoch below this is in seconds (the same boundary the ENTRY-TIME rule uses).
const msThreshold = 1e11

// Flag is a quality flag of the event, reduced to what the table needs.
type Flag struct{ Code, Field, Action, RuleID string }

// Row is one field: the value received, the value produced, and which rules touched it.
type Row struct {
	Field     string
	Received  string
	Processed string
	Changed   bool   // the processed value differs from the received one by more than rounding
	Note      string // the flags raised on this field, e.g. "SENSOR_FAULT (rule TEMP-SENTINEL, set_null)"
}

// spec says where a field lives in the Tive payload and in the PAXAFE record that carries it.
type spec struct {
	label    string
	received []string
	record   string // "sensor" or "location"
	produced []string
}

var specs = []spec{
	{"Device name", []string{"DeviceName"}, "sensor", []string{"device_id"}},
	{"Device IMEI", []string{"DeviceId"}, "sensor", []string{"device_imei"}},
	{"Timestamp", []string{"EntryTimeEpoch"}, "sensor", []string{"timestamp"}},
	{"Temperature °C", []string{"Temperature", "Celsius"}, "sensor", []string{"temperature"}},
	{"Humidity %", []string{"Humidity", "Percentage"}, "sensor", []string{"humidity"}},
	{"Light (lux)", []string{"Light", "Lux"}, "sensor", []string{"light_level"}},
	{"Latitude", []string{"Location", "Latitude"}, "location", []string{"latitude"}},
	{"Longitude", []string{"Location", "Longitude"}, "location", []string{"longitude"}},
	{"Location accuracy (m)", []string{"Location", "Accuracy", "Meters"}, "location", []string{"location_accuracy"}},
	{"Battery %", []string{"Battery", "Percentage"}, "location", []string{"battery_level"}},
	{"Cellular dBm", []string{"Cellular", "Dbm"}, "location", []string{"cellular_dbm"}},
}

// Fields compares the three documents. A field that is absent on both sides and has no flag is left out.
func Fields(raw, sensor, location []byte, flags []Flag) []Row {
	rawDoc, sensorDoc, locationDoc := decode(raw), decode(sensor), decode(location)
	var rows []Row
	for _, s := range specs {
		rv, rok := lookup(rawDoc, s.received)
		doc := sensorDoc
		if s.record == "location" {
			doc = locationDoc
		}
		pv, pok := lookup(doc, s.produced)

		row := Row{Field: s.label, Received: show(rv, rok), Processed: show(pv, pok), Note: noteFor(flags, strings.Join(s.received, "."))}
		if s.label == "Temperature °C" && (!rok || rv == nil) { // a Fahrenheit-only reading is converted
			if f, ok := lookup(rawDoc, []string{"Temperature", "Fahrenheit"}); ok && f != nil {
				row.Received = show(rv, rok) + " (Fahrenheit " + show(f, true) + ")"
			}
		}
		tol := roundingTolerance
		if wholeNumbers[s.label] {
			tol = 0.5
		}
		row.Changed = changed(rv, rok, pv, pok, tol)
		if !rok && !pok && row.Note == "" {
			continue
		}
		if rv == nil && pv == nil && row.Note == "" {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

// Point is one event reduced to what the charts draw: the temperature and position received and produced.
type Point struct {
	At                int64 // epoch milliseconds: the reading's own time
	RecTemp, ProcTemp *float64
	RecLat, RecLon    *float64
	ProcLat, ProcLon  *float64
}

// PointOf reads one event's chart values. A value that is missing, null or not a number is nil.
func PointOf(raw, sensor, location []byte) Point {
	rawDoc, sensorDoc, locationDoc := decode(raw), decode(sensor), decode(location)
	p := Point{
		RecTemp:  number(rawDoc, "Temperature", "Celsius"),
		RecLat:   number(rawDoc, "Location", "Latitude"),
		RecLon:   number(rawDoc, "Location", "Longitude"),
		ProcTemp: number(sensorDoc, "temperature"),
		ProcLat:  number(locationDoc, "latitude"),
		ProcLon:  number(locationDoc, "longitude"),
	}
	if at := number(rawDoc, "EntryTimeEpoch"); at != nil && *at > 0 {
		p.At = toMillis(*at)
	} else if at := number(sensorDoc, "timestamp"); at != nil {
		p.At = int64(*at)
	}
	return p
}

// Summary counts what happened across a set of events.
type Summary struct {
	Events   int
	Changed  int // events where at least one field changed by more than rounding
	NoRecord int // events that produced no PAXAFE record
	Flags    []FlagCount
}

// FlagCount is how often one flag code was raised.
type FlagCount struct {
	Code  string
	Count int
}

// Event is what Summarize needs of one event.
type Event struct {
	Raw, Sensor, Location []byte
	Flags                 []string
}

// Summarize counts changed events, events without a record, and flags (most frequent first).
func Summarize(events []Event) Summary {
	s := Summary{Events: len(events)}
	counts := map[string]int{}
	for _, e := range events {
		if len(bytes.TrimSpace(e.Sensor)) == 0 && len(bytes.TrimSpace(e.Location)) == 0 {
			s.NoRecord++
		}
		for _, r := range Fields(e.Raw, e.Sensor, e.Location, nil) {
			if r.Changed {
				s.Changed++
				break
			}
		}
		for _, code := range e.Flags {
			counts[code]++
		}
	}
	for code, n := range counts {
		s.Flags = append(s.Flags, FlagCount{code, n})
	}
	sort.Slice(s.Flags, func(i, j int) bool {
		if s.Flags[i].Count != s.Flags[j].Count {
			return s.Flags[i].Count > s.Flags[j].Count
		}
		return s.Flags[i].Code < s.Flags[j].Code
	})
	return s
}

// --- helpers ---

func decode(b []byte) map[string]any {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber() // numbers keep their text, so 4.625 is shown as 4.625
	var m map[string]any
	if dec.Decode(&m) != nil {
		return nil
	}
	return m
}

// lookup walks a path through nested objects; ok is false when any step is absent, v is nil for a JSON null.
func lookup(doc map[string]any, path []string) (v any, ok bool) {
	var cur any = doc
	for _, key := range path {
		m, isMap := cur.(map[string]any)
		if !isMap {
			return nil, false
		}
		if cur, ok = m[key]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func show(v any, ok bool) string {
	switch x := v.(type) {
	case nil:
		if !ok {
			return "—"
		}
		return "null"
	case json.Number:
		return x.String()
	case string:
		return strconv.Quote(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// asFloat reads a number, or a numeric string, as a float.
func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

func number(doc map[string]any, path ...string) *float64 {
	v, ok := lookup(doc, path)
	if !ok {
		return nil
	}
	f, ok := asFloat(v)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return &f
}

func toMillis(epoch float64) int64 {
	if epoch < msThreshold {
		return int64(epoch * 1000)
	}
	return int64(epoch)
}

// changed reports whether the produced value differs from the received one by more than rounding. A received value
// that was dropped or nulled counts as changed; so does a value that appeared with no counterpart (a fallback).
func changed(rv any, rok bool, pv any, pok bool, tol float64) bool {
	rf, rnum := asFloat(rv)
	pf, pnum := asFloat(pv)
	switch {
	case rnum && pnum:
		return math.Abs(rf-pf) > tol
	case rnum != pnum:
		return true
	default:
		return show(rv, rok) != show(pv, pok) && (rv != nil || pv != nil)
	}
}

func noteFor(flags []Flag, field string) string {
	var parts []string
	for _, f := range flags {
		if f.Field != field {
			continue
		}
		p := f.Code
		var extra []string
		if f.RuleID != "" {
			extra = append(extra, "rule "+f.RuleID)
		}
		if f.Action != "" {
			extra = append(extra, f.Action)
		}
		if len(extra) > 0 {
			p += " (" + strings.Join(extra, ", ") + ")"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "; ")
}

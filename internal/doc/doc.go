// Package doc is the parsed-payload tree that the rules engine edits and providers read.
// Numbers are kept as json.Number so their text and original JSON type survive parsing,
// which lets rules tell the string "10.5" from the number 10.5 (F2-R4). Pure: no I/O.
package doc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Doc is a decoded JSON object. Values are map[string]any, []any, string, json.Number, bool or nil.
type Doc map[string]any

// Parse decodes raw as a single JSON object.
func Parse(raw []byte) (Doc, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid JSON: unexpected data after the first value")
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("invalid JSON: payload must be a JSON object")
	}
	return Doc(m), nil
}

// Get returns the value at a dotted path such as "Temperature.Celsius".
// present is false when any step is missing or a parent is not an object;
// a JSON null yields (nil, true).
func (d Doc) Get(path string) (v any, present bool) {
	var cur any = map[string]any(d)
	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[key]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// Set stores v at path. It never creates intermediate objects: it returns false
// when the parent is missing, null or not an object.
func (d Doc) Set(path string, v any) bool {
	keys := strings.Split(path, ".")
	var cur any = map[string]any(d)
	for _, key := range keys[:len(keys)-1] {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		cur, ok = m[key]
		if !ok {
			return false
		}
	}
	m, ok := cur.(map[string]any)
	if !ok {
		return false
	}
	m[keys[len(keys)-1]] = v
	return true
}

// Clone returns a deep copy, so rules can edit a copy and leave the parsed original alone.
func (d Doc) Clone() Doc { return Doc(cloneValue(map[string]any(d)).(map[string]any)) }

func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = cloneValue(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValue(e)
		}
		return out
	default:
		return v
	}
}

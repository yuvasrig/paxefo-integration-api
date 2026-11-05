package paxafe

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schemas/*.json
var schemaFS embed.FS

var (
	sensorSchema   = mustCompile("schemas/px-sensor-schema.json")
	locationSchema = mustCompile("schemas/px-location-schema.json")
)

func mustCompile(name string) *jsonschema.Schema {
	raw, err := schemaFS.ReadFile(name)
	if err != nil {
		panic(fmt.Sprintf("paxafe: read embedded schema %s: %v", name, err)) // startup only, never in a request path
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		panic(fmt.Sprintf("paxafe: parse schema %s: %v", name, err))
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(name, doc); err != nil {
		panic(fmt.Sprintf("paxafe: add schema %s: %v", name, err))
	}
	s, err := c.Compile(name)
	if err != nil {
		panic(fmt.Sprintf("paxafe: compile schema %s: %v", name, err))
	}
	return s
}

// ValidateSensor checks a record against px-sensor-schema.json (F1-R6).
func ValidateSensor(r SensorRecord) error { return validate(sensorSchema, r) }

// ValidateLocation checks a record against px-location-schema.json (F1-R6).
func ValidateLocation(r LocationRecord) error { return validate(locationSchema, r) }

func validate(s *jsonschema.Schema, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("decode record: %w", err)
	}
	if err := s.Validate(inst); err != nil {
		return fmt.Errorf("schema validation: %w", err)
	}
	return nil
}

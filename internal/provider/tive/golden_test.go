package tive

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// The store adds this flag when a key already exists; the transform cannot produce it.
var ingestLevelCodes = map[string]bool{rules.CodeDuplicateConflict: true}

type expectedFile struct {
	SampleName string           `json:"sample_name"`
	HTTPStatus int              `json:"http_status"`
	Flags      []map[string]any `json:"flags"`
	Errors     []map[string]any `json:"errors"`
	Sensor     map[string]any   `json:"sensor"`
	Location   map[string]any   `json:"location"`
}

// F1-R3, F1-R4, F2-*, F3-*: all 13 provided samples must reproduce the hand-reviewed expected files.
func TestGoldenSamples(t *testing.T) {
	for i := 0; i < 13; i++ {
		i := i
		t.Run(fmt.Sprintf("sample_%02d", i), func(t *testing.T) {
			raw, err := os.ReadFile(fmt.Sprintf("../../../testdata/samples/%02d.json", i))
			if err != nil {
				t.Fatal(err)
			}
			var want expectedFile
			b, err := os.ReadFile(fmt.Sprintf("../../../specs/001-integration-api/expected/%02d.json", i))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &want); err != nil {
				t.Fatal(err)
			}

			p := newProvider(t)
			d, err := p.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.Transform(d, rules.Scope{Tenant: "reviewer", Now: fixedNow, Devices: devices{"A571992": "863257063350583"}})
			if err != nil {
				t.Fatal(err)
			}

			// Status: the transform decides rejected / flagged / accepted from the flags it can produce.
			wantFlags := withoutIngestCodes(want.Flags)
			wantStatus := provider.StatusAccepted
			if want.HTTPStatus == 422 {
				wantStatus = provider.StatusRejected
			} else if len(wantFlags) > 0 {
				wantStatus = provider.StatusFlagged // flagged if and only if there is any flag
			}
			if got.Status != wantStatus {
				t.Errorf("status = %s, want %s", got.Status, wantStatus)
			}

			if g, w := canonFlags(t, got.Flags), canonFlags(t, wantFlags); !reflect.DeepEqual(g, w) {
				t.Errorf("flags differ\n got: %v\nwant: %v", g, w)
			}
			if g, w := canonProblems(got.Errors), canonProblems(want.Errors); !reflect.DeepEqual(g, w) {
				t.Errorf("errors differ\n got: %v\nwant: %v", g, w)
			}
			if g := asMap(t, got.Sensor); !reflect.DeepEqual(g, want.Sensor) {
				t.Errorf("sensor differs\n got: %v\nwant: %v", g, want.Sensor)
			}
			if g := asMap(t, got.Location); !reflect.DeepEqual(g, want.Location) {
				t.Errorf("location differs\n got: %v\nwant: %v", g, want.Location)
			}
		})
	}
}

func withoutIngestCodes(fs []map[string]any) []map[string]any {
	var out []map[string]any
	for _, f := range fs {
		if !ingestLevelCodes[f["code"].(string)] {
			out = append(out, f)
		}
	}
	return out
}

// asMap converts a record (or nil pointer) to generic JSON for comparison.
func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) == "null" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// canonFlags renders flags as sorted canonical JSON strings, so order does not matter.
func canonFlags[T any](t *testing.T, fs []T) []string {
	t.Helper()
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		b, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		c, _ := json.Marshal(m) // map keys are sorted
		out = append(out, string(c))
	}
	sort.Strings(out)
	return out
}

func canonProblems[T any](ps []T) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		b, _ := json.Marshal(p)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		out = append(out, fmt.Sprintf("%v@%v", m["code"], m["field"]))
	}
	sort.Strings(out)
	return out
}

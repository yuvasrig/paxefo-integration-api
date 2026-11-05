package tive

import (
	"fmt"
	"os"
	"testing"

	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// FuzzParseTransform: arbitrary bytes never panic, and anything Transform accepts is schema-valid
// and internally consistent (a rejected record emits no PAXAFE records).
func FuzzParseTransform(f *testing.F) {
	for i := 0; i < 13; i++ {
		b, err := os.ReadFile(fmt.Sprintf("../../../testdata/samples/%02d.json", i))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	for _, s := range []string{
		``, `{}`, `null`, `[]`, `{"DeviceId":1}`, `{"Temperature":"x","Location":[1],"EntryTimeEpoch":{}}`,
		`{"DeviceName":"A571992","EntryTimeEpoch":1e999,"Temperature":{"Celsius":1e400}}`,
		`{"DeviceName":"A571992","EntryTimeEpoch":-5,"Location":{"Latitude":"NaN","Longitude":"Inf"}}`,
		`{"DeviceId":"863257063350583","EntryTimeEpoch":"1739215646000","Temperature":{"Celsius":"  10.5 "}}`,
	} {
		f.Add([]byte(s))
	}

	rs, err := rules.Default()
	if err != nil {
		f.Fatal(err)
	}
	p := New(rs, config.Default())
	f.Fuzz(func(t *testing.T, raw []byte) {
		d, err := p.Parse(raw)
		if err != nil {
			return
		}
		res, err := p.Transform(d, rules.Scope{Tenant: "reviewer", Now: fixedNow, Devices: devices{"A571992": "863257063350583"}})
		if err != nil {
			t.Fatalf("transform error on input %q: %v", raw, err)
		}
		switch res.Status {
		case provider.StatusRejected:
			if res.Sensor != nil || res.Location != nil || len(res.Errors) == 0 {
				t.Fatalf("inconsistent rejected result for %q: %+v", raw, res)
			}
		case provider.StatusAccepted, provider.StatusFlagged:
			if len(res.Errors) != 0 || (res.Sensor == nil && res.Location == nil) {
				t.Fatalf("inconsistent result for %q: %+v", raw, res)
			}
		default:
			t.Fatalf("unknown status %q", res.Status)
		}
	})
}

package rules

import (
	"sync"
	"testing"
	"time"
)

func TestHolder(t *testing.T) {
	def, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	h := NewHolder(def, SourceDefault, t0)
	if h.Current() != def {
		t.Fatal("Current must return the ruleset it was created with")
	}
	if l := h.Loaded(); l.Source != SourceDefault || !l.LoadedAt.Equal(t0) {
		t.Fatalf("loaded = %+v", l)
	}

	next, err := Parse([]byte("version: v2\nrules:\n  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }\n"))
	if err != nil {
		t.Fatal(err)
	}
	t1 := t0.Add(time.Minute)
	h.Set(next, SourceDatabase, t1)
	if h.Current() != next || h.Loaded().Source != SourceDatabase || !h.Loaded().LoadedAt.Equal(t1) {
		t.Fatalf("Set did not take effect: %+v", h.Loaded())
	}
}

// Rules can change while requests are being processed: readers always see one whole ruleset.
func TestHolderIsSafeUnderConcurrentChange(t *testing.T) {
	a, _ := Parse([]byte("version: a\nrules:\n  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }\n"))
	b, _ := Parse([]byte("version: b\nrules:\n  - { id: B, code: Y, field: Light.Lux, when: { lt: 1 }, action: nullify, severity: warn }\n"))
	h := NewHolder(a, SourceDefault, time.Now())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rs := h.Current()
				if (rs.Version == "a") != (rs.Rules[0].ID == "A") {
					t.Error("saw a mix of two rulesets")
					return
				}
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		if i%2 == 0 {
			h.Set(b, SourceDatabase, time.Now())
		} else {
			h.Set(a, SourceDatabase, time.Now())
		}
	}
	close(stop)
	wg.Wait()
}

func TestDefaultYAMLIsACopy(t *testing.T) {
	a := DefaultYAML()
	if len(a) == 0 {
		t.Fatal("empty")
	}
	a[0] = '!'
	if DefaultYAML()[0] == '!' {
		t.Fatal("DefaultYAML must return a copy so callers cannot change the embedded default")
	}
	if _, err := Parse(DefaultYAML()); err != nil {
		t.Fatalf("the default must parse: %v", err)
	}
}

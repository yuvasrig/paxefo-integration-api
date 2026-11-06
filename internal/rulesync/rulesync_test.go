package rulesync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

type fakeSource struct {
	mu  sync.Mutex
	row store.RulesetRow
	ok  bool
	err error
}

func (f *fakeSource) ActiveRuleset(context.Context) (store.RulesetRow, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.row, f.ok, f.err
}

func (f *fakeSource) publish(version, yaml string) {
	f.mu.Lock()
	f.row, f.ok, f.err = store.RulesetRow{Version: version, YAML: yaml, Active: true}, true, nil
	f.mu.Unlock()
}

func (f *fakeSource) reset()         { f.mu.Lock(); f.ok = false; f.mu.Unlock() }
func (f *fakeSource) fail(err error) { f.mu.Lock(); f.err = err; f.mu.Unlock() }

type obs struct {
	mu       sync.Mutex
	results  []string
	versions []string
}

func (o *obs) RulesReload(r string) { o.mu.Lock(); o.results = append(o.results, r); o.mu.Unlock() }
func (o *obs) RulesActive(v, s string) {
	o.mu.Lock()
	o.versions = append(o.versions, v+"/"+s)
	o.mu.Unlock()
}

func ruleYAML(version, id string) string {
	return "version: " + version + "\nrules:\n  - { id: " + id + ", code: X, field: Light.Lux, when: { lt: 0 }, action: nullify, severity: warn }\n"
}

func setup(t *testing.T) (*Reloader, *fakeSource, *obs, *rules.Ruleset) {
	t.Helper()
	base, err := rules.Default()
	if err != nil {
		t.Fatal(err)
	}
	src, o := &fakeSource{}, &obs{}
	h := rules.NewHolder(base, rules.SourceDefault, time.Now())
	return &Reloader{Source: src, Holder: h, Base: base, BaseSource: rules.SourceDefault, Observer: o}, src, o, base
}

func TestRefreshSwapsInAPublishedRuleset(t *testing.T) {
	r, src, o, base := setup(t)
	ctx := context.Background()

	if err := r.Refresh(ctx); err != nil || r.Holder.Current() != base {
		t.Fatalf("nothing published: the base stays in force (%v)", err)
	}
	src.publish("v1", ruleYAML("v1", "A"))
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if l := r.Holder.Loaded(); l.Ruleset.Version != "v1" || l.Source != rules.SourceDatabase {
		t.Fatalf("loaded %+v", l)
	}
	cur := r.Holder.Current()
	if err := r.Refresh(ctx); err != nil || r.Holder.Current() != cur {
		t.Fatal("the same version again must not reload")
	}

	src.publish("v2", ruleYAML("v2", "B"))
	if err := r.Refresh(ctx); err != nil || r.Holder.Current().Version != "v2" || r.Holder.Current().Rules[0].ID != "B" {
		t.Fatalf("a new version replaces the old: %v", err)
	}
	if len(o.results) != 2 || o.results[0] != "changed" || len(o.versions) != 2 || o.versions[1] != "v2/database" {
		t.Fatalf("observer %v %v", o.results, o.versions)
	}
}

// A bad ruleset, or an unreachable database, never replaces a working ruleset.
func TestRefreshKeepsTheWorkingRulesetOnTrouble(t *testing.T) {
	r, src, o, _ := setup(t)
	ctx := context.Background()
	src.publish("v1", ruleYAML("v1", "A"))
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	good := r.Holder.Current()

	for name, yaml := range map[string]string{
		"not yaml":            "version: [",
		"unknown strategy":    "version: bad\nrules:\n  - { id: A, code: X, field: Light.Lux, when: { lt: 0 }, action: fix, strategy: teleport, severity: warn }\n",
		"unsafe on protected": "version: bad2\nrules:\n  - { id: A, code: X, field: Temperature.Celsius, when: { lt: 0 }, action: drop_section, section: sensor, severity: warn }\n",
		"no version":          "rules: []\n",
	} {
		src.publish("bad-"+name, yaml)
		if err := r.Refresh(ctx); err == nil {
			t.Errorf("%s: want an error", name)
		}
		if r.Holder.Current() != good {
			t.Fatalf("%s: the working ruleset must stay in force", name)
		}
	}

	src.publish("v1", ruleYAML("v1", "A"))
	src.fail(errors.New("db down"))
	if err := r.Refresh(ctx); err == nil || r.Holder.Current() != good {
		t.Fatalf("a database error must leave the ruleset alone: %v", err)
	}
	invalid, errs := 0, 0
	for _, res := range o.results {
		switch res {
		case "invalid":
			invalid++
		case "error":
			errs++
		}
	}
	if invalid != 4 || errs != 1 {
		t.Fatalf("observer results %v", o.results)
	}
}

func TestRefreshFallsBackToTheBaseAfterAReset(t *testing.T) {
	r, src, o, base := setup(t)
	r.BaseSource = rules.SourceFile
	ctx := context.Background()
	src.publish("v1", ruleYAML("v1", "A"))
	_ = r.Refresh(ctx)
	src.reset()
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if l := r.Holder.Loaded(); l.Ruleset != base || l.Source != rules.SourceFile {
		t.Fatalf("after a reset the base ruleset is back: %+v", l)
	}
	if got := o.versions[len(o.versions)-1]; got != base.Version+"/file" {
		t.Fatalf("observer %v", o.versions)
	}
	if err := r.Refresh(ctx); err != nil || len(o.results) != 2 {
		t.Fatalf("a second refresh with nothing published is a no-op: %v %v", err, o.results)
	}
}

// Run picks up a change without anyone calling Refresh, and survives errors.
func TestRunPollsUntilCancelled(t *testing.T) {
	r, src, _, _ := setup(t)
	r.Interval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	src.fail(errors.New("db down"))
	time.Sleep(40 * time.Millisecond)
	src.fail(nil)
	src.publish("v9", ruleYAML("v9", "Z"))
	waitFor(t, func() bool { return r.Holder.Current().Version == "v9" })

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must stop when its context ends")
	}
}

func TestRunDisabledReturnsImmediately(t *testing.T) {
	r, _, _, _ := setup(t)
	r.Interval = 0
	done := make(chan struct{})
	go func() { r.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("an interval of 0 turns polling off")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

package doc

import (
	"encoding/json"
	"reflect"
	"testing"
)

func mustParse(t *testing.T, s string) Doc {
	t.Helper()
	d, err := Parse([]byte(s))
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	return d
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"object", `{"a":1}`, false},
		{"empty object", `{}`, false},
		{"whitespace around", " \n{\"a\":1}\n ", false},
		{"not json", `{not json`, true},
		{"empty body", ``, true},
		{"array", `[1,2]`, true},
		{"string", `"x"`, true},
		{"null", `null`, true},
		{"two objects", `{"a":1}{"b":2}`, true},
		{"trailing garbage", `{"a":1} xyz`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.in))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseKeepsNumberText(t *testing.T) {
	d := mustParse(t, `{"a":10.078125,"b":"10.5","c":1739215646000,"d":327.67}`)
	for k, want := range map[string]any{"a": json.Number("10.078125"), "b": "10.5", "c": json.Number("1739215646000"), "d": json.Number("327.67")} {
		if got, _ := d.Get(k); got != want {
			t.Fatalf("%s = %#v, want %#v", k, got, want)
		}
	}
}

func TestGet(t *testing.T) {
	d := mustParse(t, `{"T":{"C":1,"N":null},"S":null,"A":{"B":{"C":2}}}`)
	tests := []struct {
		path        string
		wantPresent bool
		wantNil     bool
	}{
		{"T.C", true, false},
		{"T.N", true, true},
		{"T.Missing", false, true},
		{"S", true, true},
		{"S.X", false, true},
		{"A.B.C", true, false},
		{"Nope.X", false, true},
		{"T.C.D", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			v, ok := d.Get(tt.path)
			if ok != tt.wantPresent || (v == nil) != tt.wantNil {
				t.Fatalf("Get(%q) = (%v, %v), want present=%v nil=%v", tt.path, v, ok, tt.wantPresent, tt.wantNil)
			}
		})
	}
}

func TestSet(t *testing.T) {
	d := mustParse(t, `{"T":{"C":1},"S":null}`)
	if !d.Set("T.C", json.Number("2")) {
		t.Fatal("Set on existing field must succeed")
	}
	if v, _ := d.Get("T.C"); v != json.Number("2") {
		t.Fatalf("T.C = %v", v)
	}
	if !d.Set("T.New", "x") {
		t.Fatal("Set of new field under existing object must succeed")
	}
	if !d.Set("Top", "y") {
		t.Fatal("Set of new top-level field must succeed")
	}
	if d.Set("S.X", "z") {
		t.Fatal("Set under a null parent must fail")
	}
	if d.Set("Nope.X", "z") {
		t.Fatal("Set under a missing parent must fail")
	}
}

func TestClone(t *testing.T) {
	d := mustParse(t, `{"T":{"C":1},"L":[1,{"x":2}]}`)
	c := d.Clone()
	c.Set("T.C", json.Number("9"))
	if v, _ := d.Get("T.C"); v != json.Number("1") {
		t.Fatalf("original mutated: %v", v)
	}
	if !reflect.DeepEqual(d["L"], c["L"]) {
		t.Fatal("arrays must be copied equal")
	}
}

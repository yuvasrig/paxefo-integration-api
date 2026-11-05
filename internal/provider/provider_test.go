package provider

import (
	"testing"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

type stub struct{ name string }

func (s stub) Name() string                                 { return s.name }
func (stub) Parse([]byte) (doc.Doc, error)                  { return nil, nil }
func (stub) Transform(doc.Doc, rules.Scope) (Result, error) { return Result{}, nil }

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(stub{"Tive"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(stub{"Tive"}); err == nil {
		t.Fatal("registering the same name twice must fail")
	}
	if err := r.Register(stub{"Elpro"}); err != nil {
		t.Fatal(err)
	}
	p, err := r.Get("Tive")
	if err != nil || p.Name() != "Tive" {
		t.Fatalf("Get(Tive) = %v, %v", p, err)
	}
	if _, err := r.Get("Nope"); err == nil {
		t.Fatal("unknown provider must error")
	}
	if got := r.Names(); len(got) != 2 || got[0] != "Elpro" || got[1] != "Tive" {
		t.Fatalf("Names() = %v", got)
	}
}

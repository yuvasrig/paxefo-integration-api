package auth

import (
	"regexp"
	"strings"
	"testing"
)

// F6-R2: keys are random, prefixed, and only their hash is stored.
func TestNewKey_F6R2(t *testing.T) {
	k1, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, _ := NewKey()
	if k1.Plaintext == k2.Plaintext {
		t.Fatal("keys must be random")
	}
	if !regexp.MustCompile(`^pxk_test_[0-9A-Za-z]{40,}$`).MatchString(k1.Plaintext) {
		t.Fatalf("unexpected key format %q", k1.Plaintext)
	}
	if k1.Hash != HashKey(k1.Plaintext) || len(k1.Hash) != 64 {
		t.Fatalf("hash mismatch: %q", k1.Hash)
	}
	if strings.Contains(k1.Hash, k1.Plaintext) || !strings.HasPrefix(k1.Plaintext, k1.Prefix) || len(k1.Prefix) != len("pxk_test_")+4 {
		t.Fatalf("prefix %q", k1.Prefix)
	}
	if HashKey("a") == HashKey("b") {
		t.Fatal("different keys must hash differently")
	}
}

func TestValidFormat(t *testing.T) {
	k, _ := NewKey()
	for in, want := range map[string]bool{k.Plaintext: true, "": false, "pxk_test_": false, "nope": false, "pxk_test_ab cd": false, strings.Repeat("x", 500): false} {
		if ValidFormat(in) != want {
			t.Fatalf("ValidFormat(%q) != %v", in, want)
		}
	}
}

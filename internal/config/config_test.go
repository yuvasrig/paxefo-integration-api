package config

import "testing"

func TestDefaults(t *testing.T) {
	c := Default()
	if c.AccuracyHighBelowM != 50 || c.AccuracyMediumBelowM != 500 {
		t.Fatalf("accuracy thresholds = %v/%v, want 50/500", c.AccuracyHighBelowM, c.AccuracyMediumBelowM)
	}
	if c.TempMinC != 2 || c.TempMaxC != 8 {
		t.Fatalf("temperature band = %v-%v, want 2-8", c.TempMinC, c.TempMaxC)
	}
}

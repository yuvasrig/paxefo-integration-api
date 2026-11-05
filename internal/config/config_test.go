package config

import "testing"

func TestDefaults(t *testing.T) {
	c := Default()
	if c.AccuracyHighBelowM != 50 || c.AccuracyMediumBelowM != 500 {
		t.Fatalf("accuracy thresholds = %v/%v, want 50/500", c.AccuracyHighBelowM, c.AccuracyMediumBelowM)
	}
}

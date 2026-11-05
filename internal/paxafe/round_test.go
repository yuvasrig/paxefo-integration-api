package paxafe

import "testing"

// F1-R5: output rounding is half away from zero, applied only when emitting.
func TestRound_F1R5(t *testing.T) {
	tests := []struct {
		name   string
		in     float64
		places int
		want   float64
	}{
		{"neg half away", -0.5625, 3, -0.563},
		{"pos half away", 0.6875, 3, 0.688},
		{"4.625 to 2dp", 4.625, 2, 4.63},
		{"10.078125 to 2dp", 10.078125, 2, 10.08},
		{"6.125 to 2dp", 6.125, 2, 6.13},
		{"decimal literal 1.005", 1.005, 2, 1.01},
		{"humidity float noise", 38.70000076293945, 1, 38.7},
		{"already rounded", 10.0, 2, 10.0},
		{"negative temp", -18.5, 2, -18.5},
		{"zero", 0, 3, 0},
		{"G value", 0.9901862198596787, 3, 0.990},
		{"integer places", 22.5, 0, 23},
		{"neg integer places", -22.5, 0, -23},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Round(tt.in, tt.places); got != tt.want {
				t.Fatalf("Round(%v, %d) = %v, want %v", tt.in, tt.places, got, tt.want)
			}
		})
	}
}

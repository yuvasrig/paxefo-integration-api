// Package config holds typed settings with defaults for every threshold (constitution #8).
// Rule thresholds (sentinels, ranges, timestamp limits) live in the ruleset, not here.
package config

// Transform holds the settings the Tive transform needs.
type Transform struct {
	// AccuracyHighBelowM: accuracy under this many meters is category High.
	AccuracyHighBelowM float64
	// AccuracyMediumBelowM: accuracy under this many meters (and not High) is Medium; otherwise Low.
	AccuracyMediumBelowM float64
	// TempMinC and TempMaxC are the cold-chain band (inclusive). A reading outside it is flagged
	// TEMP_EXCURSION when the payload carries a shipment.
	TempMinC float64
	TempMaxC float64
}

// Default returns the documented defaults (DESIGN.md section 6).
func Default() Transform {
	return Transform{AccuracyHighBelowM: 50, AccuracyMediumBelowM: 500, TempMinC: 2, TempMaxC: 8}
}

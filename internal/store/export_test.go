package store

// SetTotalEventsCap lets a test lower the cap on the total event count.
func SetTotalEventsCap(n int) { totalEventsCap = n }

package paxafe

import (
	"math"
	"math/big"
	"strconv"
)

// Round rounds x to the given decimal places, half away from zero (F1-R5).
// It rounds the shortest decimal form of x, so 1.005 gives 1.01 rather than
// the 1.00 that float multiplication would produce.
func Round(x float64, places int) float64 {
	if x == 0 || math.IsNaN(x) || math.IsInf(x, 0) {
		return x
	}
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(x, 'f', -1, 64))
	if !ok {
		return x
	}
	scale := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(places)), nil))
	r.Mul(r, scale)

	neg := r.Sign() < 0
	r.Abs(r)
	r.Add(r, big.NewRat(1, 2))
	n := new(big.Int).Quo(r.Num(), r.Denom()) // floor of (|x|*10^places + 1/2)
	out := new(big.Rat).SetInt(n)
	out.Quo(out, scale)
	f, _ := out.Float64()
	if neg {
		return -f
	}
	return f
}

func roundPtr(p *float64, places int) *float64 {
	if p == nil {
		return nil
	}
	v := Round(*p, places)
	return &v
}

package store

import (
	"crypto/rand"
	"time"
)

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewID returns prefix + 26 characters: 10 of millisecond time then 16 random, so ids sort by creation time.
func NewID(prefix string) string {
	var out [26]byte
	ms := uint64(time.Now().UnixMilli())
	for i := 9; i >= 0; i-- {
		out[i] = crockford[ms&31]
		ms >>= 5
	}
	var r [16]byte
	_, _ = rand.Read(r[:])
	for i := 0; i < 16; i++ {
		out[10+i] = crockford[r[i]&31]
	}
	return prefix + string(out[:])
}

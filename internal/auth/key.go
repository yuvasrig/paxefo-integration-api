// Package auth creates and hashes API keys. Only the SHA-256 hash is stored; the plaintext is shown
// once at creation (DESIGN.md section 13).
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// KeyPrefix marks our keys for secret scanning and environment identification.
const KeyPrefix = "pxk_test_"

const (
	base62    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	keyBytes  = 32
	prefixLen = 4
	maxKeyLen = 128
)

// Key is a newly created API key. Plaintext must be shown to the user once and never stored.
type Key struct {
	Plaintext string
	Prefix    string // display prefix, stored to help identify a key
	Hash      string // hex SHA-256, stored
}

// NewKey returns a key of the form pxk_test_<32 random bytes in base62>.
func NewKey() (Key, error) {
	b := make([]byte, keyBytes)
	if _, err := rand.Read(b); err != nil {
		return Key{}, fmt.Errorf("auth: read random bytes: %w", err)
	}
	plain := KeyPrefix + encodeBase62(b)
	return Key{Plaintext: plain, Prefix: plain[:len(KeyPrefix)+prefixLen], Hash: HashKey(plain)}, nil
}

// HashKey returns the hex SHA-256 of a key. The key is high-entropy, so a plain hash is enough.
func HashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// ValidFormat is a cheap check before hitting the database.
func ValidFormat(s string) bool {
	if len(s) <= len(KeyPrefix) || len(s) > maxKeyLen || !strings.HasPrefix(s, KeyPrefix) {
		return false
	}
	for _, c := range s[len(KeyPrefix):] {
		if !strings.ContainsRune(base62, c) {
			return false
		}
	}
	return true
}

func encodeBase62(b []byte) string {
	n := new(big.Int).SetBytes(b)
	base := big.NewInt(62)
	mod := new(big.Int)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, base62[mod.Int64()])
	}
	for len(out) < 43 { // 32 bytes need at most 43 base62 digits; pad so keys have a fixed shape
		out = append(out, base62[0])
	}
	return string(out)
}

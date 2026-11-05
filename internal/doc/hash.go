package doc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CanonicalHash is the SHA-256 of the payload in canonical JSON form: object keys sorted and no
// insignificant whitespace, number text preserved. Key order or formatting therefore never makes
// two copies of the same payload look different (F4-R5). Bytes that are not valid JSON hash as-is.
func CanonicalHash(raw []byte) string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err == nil {
		if b, err := json.Marshal(v); err == nil { // map keys are marshalled in sorted order
			raw = b
		}
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

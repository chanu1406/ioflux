package trace

import (
	"crypto/sha256"
	"encoding/hex"
)

// DigestPrefix labels the hash algorithm in a digest string, so a later change
// of algorithm produces values that cannot be mistaken for the current ones.
const DigestPrefix = "sha256:"

// Digest returns the content digest of a trace file's bytes, in the form
// "sha256:<hex>". It is the trace's identity: two runs carrying the same digest
// replayed the same workload, and two runs carrying different digests did not.
//
// The digest covers the raw file bytes rather than a canonical form of the
// parsed ops, keeping identity to one definition. `ioflux gen` omits a header
// timestamp for the same reason, so regenerating from identical flags
// reproduces the digest.
//
// A semantically identical but reserialized trace therefore digests differently
// and compares as a different workload — the safe direction to fail.
func Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return DigestPrefix + hex.EncodeToString(sum[:])
}

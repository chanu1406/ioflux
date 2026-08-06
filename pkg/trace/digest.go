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
// parsed operations. That keeps identity to one definition — the artifact on
// disk — instead of two that can disagree, and it costs nothing in stability
// because a trace file is written once and never rewritten in place. `ioflux
// gen` deliberately omits a timestamp from its header for the same reason, so
// regenerating a synthetic trace from identical flags reproduces the digest.
//
// A consequence worth stating: a trace that is semantically identical but
// reserialized (different key order, different whitespace) digests differently
// and compares as a different workload. That is the safe direction to fail —
// it reports a difference that is not there rather than hiding one that is.
func Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return DigestPrefix + hex.EncodeToString(sum[:])
}

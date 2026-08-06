package trace_test

import (
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/trace"
)

func TestDigestIsStableAndPrefixed(t *testing.T) {
	b := []byte(`{"ioflux_trace_version":1}` + "\n")

	got := trace.Digest(b)

	if !strings.HasPrefix(got, trace.DigestPrefix) {
		t.Errorf("Digest(%q) = %q, want a %q prefix", b, got, trace.DigestPrefix)
	}
	if second := trace.Digest(b); second != got {
		t.Errorf("Digest is not deterministic: %q then %q", got, second)
	}
	// sha256 hex is 64 characters after the prefix.
	if want := len(trace.DigestPrefix) + 64; len(got) != want {
		t.Errorf("len(Digest) = %d, want %d (%q)", len(got), want, got)
	}
}

func TestDigestDistinguishesContent(t *testing.T) {
	a := trace.Digest([]byte("one"))
	b := trace.Digest([]byte("two"))

	if a == b {
		t.Errorf("different bytes produced the same digest %q", a)
	}
}

// A one-byte change anywhere must change the digest; a comparison relies on it
// to tell two traces apart.
func TestDigestDetectsSingleByteChange(t *testing.T) {
	base := strings.Repeat("a", 4096)

	if trace.Digest([]byte(base)) == trace.Digest([]byte(base[:4095]+"b")) {
		t.Error("digest did not change for a one-byte difference")
	}
}

func TestDigestOfEmptyInput(t *testing.T) {
	// An empty trace is not valid, but Digest is a pure function over bytes and
	// must not panic on one.
	if got := trace.Digest(nil); !strings.HasPrefix(got, trace.DigestPrefix) {
		t.Errorf("Digest(nil) = %q, want a well-formed digest", got)
	}
}

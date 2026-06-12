package payload_test

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"testing"

	"github.com/chanuollala/ioflux/pkg/payload"
)

// referenceFill is the definitional per-byte form of the seeded fill. It pins
// the byte mapping so the optimized word-at-a-time Fill can never silently
// change the payload content across versions (parallel per-worker
// materialization relies on every worker producing identical bytes).
func referenceFill(dst []byte, seed, opID int64, target string, off int64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(target))
	targetHash := h.Sum64()
	rotl := func(x uint64, k uint) uint64 { return (x << k) | (x >> (64 - k)) }
	splitmix := func(x uint64) uint64 {
		x += 0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		return x ^ (x >> 31)
	}
	base := uint64(seed) ^ rotl(uint64(opID)*0x9e3779b97f4a7c15, 17) ^ rotl(targetHash, 31)
	for i := range dst {
		abs := off + int64(i)
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], splitmix(base+uint64(abs/8)*0x9e3779b97f4a7c15))
		dst[i] = tmp[abs&7]
	}
}

func TestFillMatchesReferenceMapping(t *testing.T) {
	cfg := payload.Config{Mode: payload.ModeSeeded, Seed: 7}
	for _, tc := range []struct {
		off  int64
		size int
	}{
		{0, 0}, {0, 1}, {0, 7}, {0, 8}, {0, 9}, {0, 4096},
		{1, 1}, {3, 5}, {5, 3}, {7, 16}, {4093, 8192}, {1<<40 + 3, 257},
	} {
		got := make([]byte, tc.size)
		want := make([]byte, tc.size)
		payload.Fill(got, cfg, 42, "target-a", tc.off)
		referenceFill(want, 7, 42, "target-a", tc.off)
		if !bytes.Equal(got, want) {
			t.Fatalf("Fill(off=%d, size=%d) diverges from reference mapping", tc.off, tc.size)
		}
	}
}

func BenchmarkFillSeeded(b *testing.B) {
	cfg := payload.Config{Mode: payload.ModeSeeded, Seed: 7}
	buf := make([]byte, 1<<20)
	b.SetBytes(int64(len(buf)))
	for i := 0; i < b.N; i++ {
		payload.Fill(buf, cfg, 42, "target", 0)
	}
}

func TestFillSeededDeterministicAcrossChunks(t *testing.T) {
	cfg := payload.Config{Mode: payload.ModeSeeded, Seed: 123}
	whole := make([]byte, 8192)
	payload.Fill(whole, cfg, 42, "target-a", 4093)

	chunked := make([]byte, len(whole))
	payload.Fill(chunked[:17], cfg, 42, "target-a", 4093)
	payload.Fill(chunked[17:4096], cfg, 42, "target-a", 4093+17)
	payload.Fill(chunked[4096:], cfg, 42, "target-a", 4093+4096)

	if !bytes.Equal(chunked, whole) {
		t.Fatal("chunked seeded fill differs from single-shot fill")
	}
}

func TestFillSeededVariesByIdentity(t *testing.T) {
	cfg := payload.Config{Mode: payload.ModeSeeded, Seed: 123}
	a := make([]byte, 4096)
	b := make([]byte, 4096)
	payload.Fill(a, cfg, 42, "target-a", 0)
	payload.Fill(b, cfg, 43, "target-a", 0)
	if bytes.Equal(a, b) {
		t.Fatal("different op IDs produced identical payload")
	}
	payload.Fill(b, cfg, 42, "target-b", 0)
	if bytes.Equal(a, b) {
		t.Fatal("different targets produced identical payload")
	}
}

func TestFillSeededNonZeroAndVaried(t *testing.T) {
	buf := make([]byte, 64<<10)
	payload.Fill(buf, payload.Config{Mode: payload.ModeSeeded, Seed: 99}, 7, "target", 0)
	seen := make(map[byte]struct{})
	var nonzero bool
	for _, b := range buf {
		if b != 0 {
			nonzero = true
		}
		seen[b] = struct{}{}
	}
	if !nonzero {
		t.Fatal("seeded fill produced all zeros")
	}
	if len(seen) < 200 {
		t.Fatalf("seeded fill used only %d byte values, want broad variation", len(seen))
	}
}

func TestFillZeroMode(t *testing.T) {
	buf := bytes.Repeat([]byte{0xff}, 1024)
	payload.Fill(buf, payload.Config{Mode: payload.ModeZero, Seed: 99}, 7, "target", 0)
	if !bytes.Equal(buf, make([]byte, len(buf))) {
		t.Fatal("zero fill left non-zero bytes")
	}
}

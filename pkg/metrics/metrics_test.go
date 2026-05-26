package metrics_test

import (
	"math/rand/v2"
	"testing"

	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// TestMergeMatchesUnion verifies the lossless-merge property: percentiles of
// merge(hist(A), hist(B)) must equal percentiles of hist(A ∪ B).
// This is the §8.5 / §9 correctness requirement for distributed aggregation.
func TestMergeMatchesUnion(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 0))

	const n = 10_000
	setA := make([]int64, n)
	setB := make([]int64, n)
	for i := range setA {
		// 1 µs to ~1 s range
		setA[i] = 1_000 + int64(rng.IntN(999_999_000))
		setB[i] = 1_000 + int64(rng.IntN(999_999_000))
	}

	ha, hb := metrics.New(), metrics.New()
	for _, v := range setA {
		ha.RecordValue(v)
	}
	for _, v := range setB {
		hb.RecordValue(v)
	}

	merged := metrics.New()
	merged.Merge(ha)
	merged.Merge(hb)

	union := metrics.New()
	for _, v := range setA {
		union.RecordValue(v)
	}
	for _, v := range setB {
		union.RecordValue(v)
	}

	if merged.TotalCount() != union.TotalCount() {
		t.Fatalf("TotalCount: merged=%d union=%d", merged.TotalCount(), union.TotalCount())
	}

	for _, q := range []float64{50, 90, 99, 99.9} {
		got := merged.Percentile(q)
		want := union.Percentile(q)
		if got != want {
			t.Errorf("q=%.1f: merged=%d union=%d", q, got, want)
		}
	}
}

// TestMergeMonotonic verifies that merged percentiles are non-decreasing.
func TestMergeMonotonic(t *testing.T) {
	rng := rand.New(rand.NewPCG(99, 0))
	h := metrics.New()
	for range 5_000 {
		h.RecordValue(1_000 + int64(rng.IntN(500_000_000)))
	}
	p50 := h.Percentile(50)
	p90 := h.Percentile(90)
	p99 := h.Percentile(99)
	max := h.Max()
	if !(p50 <= p90 && p90 <= p99 && p99 <= max) {
		t.Errorf("percentiles not monotonic: p50=%d p90=%d p99=%d max=%d", p50, p90, p99, max)
	}
}

// TestClampBelowMin verifies that values below 1 µs are clamped and recorded.
func TestClampBelowMin(t *testing.T) {
	h := metrics.New()
	h.RecordValue(0)
	h.RecordValue(-100)
	if h.TotalCount() != 2 {
		t.Errorf("TotalCount=%d, want 2 after clamped records", h.TotalCount())
	}
}

// TestRecorderMerge verifies that Recorder.Merge aggregates correctly.
func TestRecorderMerge(t *testing.T) {
	ra := metrics.NewRecorder()
	ra.Record(trace.OpRead, 500_000, 4096, false)
	ra.Record(trace.OpRead, 1_000_000, 4096, false)
	ra.Record(trace.OpOpen, 200_000, 0, false)

	rb := metrics.NewRecorder()
	rb.Record(trace.OpRead, 750_000, 8192, false)
	rb.Record(trace.OpClose, 100_000, 0, false)

	merged := metrics.NewRecorder()
	merged.Merge(ra)
	merged.Merge(rb)

	if merged.Count(trace.OpRead) != 3 {
		t.Errorf("READ count=%d, want 3", merged.Count(trace.OpRead))
	}
	if merged.Count(trace.OpOpen) != 1 {
		t.Errorf("OPEN count=%d, want 1", merged.Count(trace.OpOpen))
	}
	if merged.Count(trace.OpClose) != 1 {
		t.Errorf("CLOSE count=%d, want 1", merged.Count(trace.OpClose))
	}
	if merged.TotalOps() != 5 {
		t.Errorf("TotalOps=%d, want 5", merged.TotalOps())
	}
	if merged.Bytes != 4096+4096+8192 {
		t.Errorf("Bytes=%d, want %d", merged.Bytes, 4096+4096+8192)
	}
	if h := merged.Histogram(trace.OpRead); h == nil {
		t.Error("READ histogram is nil after merge")
	} else if h.TotalCount() != 3 {
		t.Errorf("READ histogram TotalCount=%d, want 3", h.TotalCount())
	}
}

// TestRecorderErrors verifies that errored ops are counted separately.
func TestRecorderErrors(t *testing.T) {
	r := metrics.NewRecorder()
	r.Record(trace.OpRead, 1_000_000, 0, true)
	r.Record(trace.OpRead, 2_000_000, 1024, false)
	if r.Errors != 1 {
		t.Errorf("Errors=%d, want 1", r.Errors)
	}
	if r.TotalOps() != 2 {
		t.Errorf("TotalOps=%d, want 2", r.TotalOps())
	}
}

// TestOpKindsSorted verifies that OpKinds returns a sorted slice.
func TestOpKindsSorted(t *testing.T) {
	r := metrics.NewRecorder()
	r.Record(trace.OpRead, 1_000_000, 0, false)
	r.Record(trace.OpOpen, 1_000_000, 0, false)
	r.Record(trace.OpClose, 1_000_000, 0, false)
	kinds := r.OpKinds()
	for i := 1; i < len(kinds); i++ {
		if kinds[i] < kinds[i-1] {
			t.Errorf("OpKinds not sorted: %v[%d]=%q < %v[%d]=%q", kinds, i, kinds[i], kinds, i-1, kinds[i-1])
		}
	}
}

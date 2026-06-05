package metrics_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/trace"
)

func TestRecorderSnapshotRoundTrip(t *testing.T) {
	rec := snapshotRecorder(11)

	var snap metrics.RecorderSnapshot
	b, err := json.Marshal(rec.Export())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	imported := metrics.ImportRecorder(snap)

	assertRecorderCounters(t, imported, rec)
	assertRecorderHistograms(t, imported, rec)
}

func TestRecorderSnapshotMergeMatchesOriginalMerge(t *testing.T) {
	a := snapshotRecorder(3)
	b := snapshotRecorder(29)

	got := metrics.NewRecorder()
	got.Merge(metrics.ImportRecorder(a.Export()))
	got.Merge(metrics.ImportRecorder(b.Export()))

	want := metrics.NewRecorder()
	want.Merge(a)
	want.Merge(b)

	assertRecorderCounters(t, got, want)
	assertRecorderHistograms(t, got, want)
}

func snapshotRecorder(seed int64) *metrics.Recorder {
	r := metrics.NewRecorder()
	kinds := []trace.OpKind{
		trace.OpOpen,
		trace.OpRead,
		trace.OpWrite,
		trace.OpStat,
		trace.OpFsync,
		trace.OpClose,
		trace.OpPut,
		trace.OpGet,
		trace.OpHead,
		trace.OpDelete,
	}
	for i, kind := range kinds {
		for j := 0; j <= i+2; j++ {
			latency := 1_000 + seed*97 + int64(i*i+1)*40_000 + int64(j*j+1)*7_000
			bytesN := int64(0)
			switch kind {
			case trace.OpRead, trace.OpWrite, trace.OpPut, trace.OpGet:
				bytesN = int64(j+1) * 4096
			}
			r.Record(kind, latency, bytesN, (int(seed)+i+j)%11 == 0)
		}
	}
	for i := 0; i < 31; i++ {
		r.RecordDrift(seed + int64(i*i)*1_000)
		r.RecordCompletionLag(seed + int64(i*i+3)*2_000)
	}
	r.BacklogEvents = 7 + seed
	r.BacklogBlockedNS = 123_456 + seed
	r.MaxInflightDepth = 5
	r.PeakInflight = 1
	return r
}

func assertRecorderCounters(t *testing.T, got, want *metrics.Recorder) {
	t.Helper()
	if got.Bytes != want.Bytes {
		t.Fatalf("Bytes=%d, want %d", got.Bytes, want.Bytes)
	}
	if got.Errors != want.Errors {
		t.Fatalf("Errors=%d, want %d", got.Errors, want.Errors)
	}
	if got.BacklogEvents != want.BacklogEvents {
		t.Fatalf("BacklogEvents=%d, want %d", got.BacklogEvents, want.BacklogEvents)
	}
	if got.BacklogBlockedNS != want.BacklogBlockedNS {
		t.Fatalf("BacklogBlockedNS=%d, want %d", got.BacklogBlockedNS, want.BacklogBlockedNS)
	}
	if got.MaxInflightDepth != want.MaxInflightDepth {
		t.Fatalf("MaxInflightDepth=%d, want %d", got.MaxInflightDepth, want.MaxInflightDepth)
	}
	if got.PeakInflight != want.PeakInflight {
		t.Fatalf("PeakInflight=%d, want %d", got.PeakInflight, want.PeakInflight)
	}
	if got.TotalOps() != want.TotalOps() {
		t.Fatalf("TotalOps=%d, want %d", got.TotalOps(), want.TotalOps())
	}
	for _, kind := range want.OpKinds() {
		if got.Count(kind) != want.Count(kind) {
			t.Fatalf("%s count=%d, want %d", kind, got.Count(kind), want.Count(kind))
		}
	}
}

func assertRecorderHistograms(t *testing.T, got, want *metrics.Recorder) {
	t.Helper()
	for _, kind := range want.OpKinds() {
		assertHistogramEqual(t, string(kind), got.Histogram(kind), want.Histogram(kind))
	}
	assertHistogramEqual(t, "drift", got.DriftHist, want.DriftHist)
	assertHistogramEqual(t, "completion lag", got.CompletionLagHist, want.CompletionLagHist)
}

func assertHistogramEqual(t *testing.T, name string, got, want *metrics.Histogram) {
	t.Helper()
	if got == nil || want == nil {
		if got != want {
			t.Fatalf("%s histogram nil mismatch: got nil=%v want nil=%v", name, got == nil, want == nil)
		}
		return
	}
	if got.TotalCount() != want.TotalCount() {
		t.Fatalf("%s TotalCount=%d, want %d", name, got.TotalCount(), want.TotalCount())
	}
	for _, q := range []float64{50, 99, 99.9} {
		if got.Percentile(q) != want.Percentile(q) {
			t.Fatalf("%s p%.1f=%d, want %d", name, q, got.Percentile(q), want.Percentile(q))
		}
	}
	if got.Max() != want.Max() {
		t.Fatalf("%s Max=%d, want %d", name, got.Max(), want.Max())
	}
	if math.Abs(got.Mean()-want.Mean()) > 0.000001 {
		t.Fatalf("%s Mean=%f, want %f", name, got.Mean(), want.Mean())
	}
}

package fidelity_test

import (
	"testing"

	"github.com/chanuollala/ioflux/pkg/fidelity"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// makeHdr returns a minimal trace.Header for fidelity tests.
func makeHdr(numOps int64, durationNS int64) trace.Header {
	return trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Summary: trace.Summary{
			NumOps:     numOps,
			NumStreams: 1,
			DurationNS: durationNS,
		},
	}
}

// makeRecorder builds a recorder with manually injected drift, backlog, and
// completion-lag samples. It does not go through the replay engine.
func makeRecorder(driftSamplesNS []int64, completionLagSamplesNS []int64, backlogEvents int64, opsCompleted int64) *metrics.Recorder {
	rec := metrics.NewRecorder()
	for _, ns := range driftSamplesNS {
		rec.RecordDrift(ns)
	}
	for _, ns := range completionLagSamplesNS {
		rec.RecordCompletionLag(ns)
	}
	rec.BacklogEvents = backlogEvents
	// Inject opsCompleted via Record (kind doesn't matter for coverage check).
	for i := int64(0); i < opsCompleted; i++ {
		rec.Record(trace.OpRead, 1_000_000, 0, false)
	}
	return rec
}

// TestConcurrencyCheckCatchesViolation verifies that a stream with PeakInflight>1
// is listed in Violations and MaxPerStreamInflight reflects it.
func TestConcurrencyCheckCatchesViolation(t *testing.T) {
	rec := makeRecorder(nil, nil, 0, 10)
	hdr := makeHdr(10, 0)
	peakByStream := map[int64]int64{
		0: 1, // fine
		1: 2, // violation
		2: 1, // fine
	}
	rep := fidelity.Build(rec, hdr, "asap", 0, peakByStream)

	if rep.ConcurrencyCheck.MaxPerStreamInflight != 2 {
		t.Errorf("MaxPerStreamInflight=%d, want 2", rep.ConcurrencyCheck.MaxPerStreamInflight)
	}
	if len(rep.ConcurrencyCheck.Violations) != 1 || rep.ConcurrencyCheck.Violations[0] != 1 {
		t.Errorf("Violations=%v, want [1]", rep.ConcurrencyCheck.Violations)
	}
}

// TestConcurrencyCheckNoViolations verifies clean runs report zero violations.
func TestConcurrencyCheckNoViolations(t *testing.T) {
	rec := makeRecorder(nil, nil, 0, 5)
	hdr := makeHdr(5, 0)
	peakByStream := map[int64]int64{0: 1, 1: 1}
	rep := fidelity.Build(rec, hdr, "asap", 0, peakByStream)
	if len(rep.ConcurrencyCheck.Violations) != 0 {
		t.Errorf("Violations=%v, want empty", rep.ConcurrencyCheck.Violations)
	}
	if rep.ConcurrencyCheck.MaxPerStreamInflight != 1 {
		t.Errorf("MaxPerStreamInflight=%d, want 1", rep.ConcurrencyCheck.MaxPerStreamInflight)
	}
}

// TestBuild_HighFidelity verifies a healthy fast run is not flagged.
func TestBuild_HighFidelity(t *testing.T) {
	// Zero drift, no backlog, all ops issued.
	drifts := make([]int64, 100) // all zero
	rec := makeRecorder(drifts, drifts, 0, 100)
	hdr := makeHdr(100, int64(100*10_000_000)) // 100 ops × 10ms = 1s trace
	meanInterArrival := int64(10_000_000)      // 10 ms

	rep := fidelity.Build(rec, hdr, "timeline", meanInterArrival, map[int64]int64{0: 1})

	if rep.LowFidelity {
		t.Errorf("LowFidelity=true, want false; reason=%q", rep.LowFidelityReason)
	}
	if rep.Coverage.OpsSkipped != 0 {
		t.Errorf("OpsSkipped=%d, want 0", rep.Coverage.OpsSkipped)
	}
}

// TestBuild_LowFidelityDriftPath verifies that high schedule drift triggers
// the low-fidelity flag even when backlog and coverage are fine.
func TestBuild_LowFidelityDriftPath(t *testing.T) {
	// 100 ops, each with 5-second drift → p99 drift >> 10% of 10ms inter-arrival.
	const bigDrift = int64(5_000_000_000) // 5 s
	drifts := make([]int64, 100)
	for i := range drifts {
		drifts[i] = bigDrift
	}
	rec := makeRecorder(drifts, drifts, 0, 100)
	hdr := makeHdr(100, int64(100*10_000_000))
	meanInterArrival := int64(10_000_000) // 10 ms; threshold = 1 ms

	rep := fidelity.Build(rec, hdr, "timeline", meanInterArrival, map[int64]int64{0: 1})

	if !rep.LowFidelity {
		t.Errorf("LowFidelity=false, want true")
	}
	if rep.LowFidelityReason == "" {
		t.Errorf("LowFidelityReason empty, want non-empty")
	}
	if rep.ScheduleDrift.P99NS < bigDrift/2 {
		t.Errorf("ScheduleDrift.P99NS=%d, want ≥%d", rep.ScheduleDrift.P99NS, bigDrift/2)
	}
}

// TestBuild_LowFidelityBacklogPath verifies that >5% of ops being backlog-
// blocked triggers the low-fidelity flag independent of drift.
func TestBuild_LowFidelityBacklogPath(t *testing.T) {
	const total = 100
	const backlogged = 10 // 10% > 5% threshold

	rec := makeRecorder(nil, nil, backlogged, total)
	hdr := makeHdr(total, 0)

	rep := fidelity.Build(rec, hdr, "asap", 0, map[int64]int64{0: 1})

	if !rep.LowFidelity {
		t.Errorf("LowFidelity=false, want true (10%% backlogged)")
	}
	if rep.Backlog.FractionOpsBacklogged < 0.05 {
		t.Errorf("FractionOpsBacklogged=%.3f, want > 0.05", rep.Backlog.FractionOpsBacklogged)
	}
}

// TestBuild_LowFidelityCoveragePath verifies that skipped ops trigger
// the low-fidelity flag.
func TestBuild_LowFidelityCoveragePath(t *testing.T) {
	// Trace has 100 ops but only 90 were issued (10 skipped).
	rec := makeRecorder(nil, nil, 0, 90)
	hdr := makeHdr(100, 0)

	rep := fidelity.Build(rec, hdr, "asap", 0, map[int64]int64{0: 1})

	if !rep.LowFidelity {
		t.Errorf("LowFidelity=false, want true (10 ops skipped)")
	}
	if rep.Coverage.OpsSkipped != 10 {
		t.Errorf("OpsSkipped=%d, want 10", rep.Coverage.OpsSkipped)
	}
}

// TestBuild_DriftFallback verifies the 10ms fallback when mean inter-arrival
// is unknown (0). A 1ns drift must not trigger low-fidelity; a 20ms drift must.
func TestBuild_DriftFallback(t *testing.T) {
	smallDrift := []int64{1_000_000} // 1 ms — below 10ms fallback
	rec := makeRecorder(smallDrift, smallDrift, 0, 1)
	hdr := makeHdr(1, 0) // DurationNS=0 → meanInterArrival=0 → fallback
	rep := fidelity.Build(rec, hdr, "timeline", 0, nil)
	if rep.LowFidelity {
		t.Errorf("LowFidelity=true for 1ms drift with 10ms fallback threshold; reason=%q", rep.LowFidelityReason)
	}

	bigDrift := []int64{20_000_000} // 20 ms — above 10ms fallback
	rec2 := makeRecorder(bigDrift, bigDrift, 0, 1)
	hdr2 := makeHdr(1, 0)
	rep2 := fidelity.Build(rec2, hdr2, "timeline", 0, nil)
	if !rep2.LowFidelity {
		t.Errorf("LowFidelity=false for 20ms drift with 10ms fallback threshold")
	}
}

// TestBuild_ScaledModeLowersMeanInterArrival verifies that the caller passing a
// speedup-divided meanInterArrivalNS (as the scheduler does for scaled mode)
// correctly tightens the low-fidelity drift threshold relative to unscaled.
func TestBuild_ScaledModeLowersMeanInterArrival(t *testing.T) {
	// 600µs drift: below the 1ms threshold for 10ms mean (1x), above the 500µs
	// threshold for 5ms mean (2x speedup). Both use mode "scaled".
	const drift = int64(600_000) // 600 µs

	recHF := makeRecorder([]int64{drift}, []int64{drift}, 0, 1)
	hdr := makeHdr(1, 0)
	repHF := fidelity.Build(recHF, hdr, "scaled", 10_000_000, nil) // 10ms mean → 1ms threshold
	if repHF.LowFidelity {
		t.Errorf("10ms mean: LowFidelity=true for 600µs drift, want false; reason=%q", repHF.LowFidelityReason)
	}

	recLF := makeRecorder([]int64{drift}, []int64{drift}, 0, 1)
	repLF := fidelity.Build(recLF, hdr, "scaled", 5_000_000, nil) // 5ms mean (2x) → 500µs threshold
	if !repLF.LowFidelity {
		t.Errorf("5ms mean (2x scaled): LowFidelity=false for 600µs drift, want true")
	}
}

// TestBuild_ScaledSlowdownRaisesThreshold verifies that a speedup < 1 (slowdown)
// widens the drift threshold. With 0.5× speedup a 10ms mean becomes 20ms in
// real time, so the threshold doubles and a 1.5ms drift stays high-fidelity.
func TestBuild_ScaledSlowdownRaisesThreshold(t *testing.T) {
	// 1.5ms drift:
	//   - unscaled (10ms mean → 1ms threshold): low fidelity
	//   - 0.5x speedup (20ms real mean → 2ms threshold): high fidelity
	const drift = int64(1_500_000) // 1.5 ms

	recLF := makeRecorder([]int64{drift}, []int64{drift}, 0, 1)
	hdr := makeHdr(1, 0)
	repLF := fidelity.Build(recLF, hdr, "scaled", 10_000_000, nil) // 10ms mean → 1ms threshold
	if !repLF.LowFidelity {
		t.Errorf("10ms mean: LowFidelity=false for 1.5ms drift, want true")
	}

	recHF := makeRecorder([]int64{drift}, []int64{drift}, 0, 1)
	// 0.5x slowdown: scheduler passes 10ms / 0.5 = 20ms mean → 2ms threshold
	repHF := fidelity.Build(recHF, hdr, "scaled", 20_000_000, nil)
	if repHF.LowFidelity {
		t.Errorf("20ms mean (0.5x slowdown): LowFidelity=true for 1.5ms drift, want false; reason=%q", repHF.LowFidelityReason)
	}
}

// TestBuild_CoverageReflectsPassedHdrNumOps documents that fidelity.Build uses
// hdr.Summary.NumOps exactly as given. The scheduler must pass the actual loaded
// count — not the advisory trace header value — to get correct coverage.
func TestBuild_CoverageReflectsPassedHdrNumOps(t *testing.T) {
	rec := makeRecorder(nil, nil, 0, 8)
	hdr := makeHdr(12, 0) // header claims 12; scheduler would correct this before calling Build
	rep := fidelity.Build(rec, hdr, "asap", 0, nil)
	if rep.Coverage.OpsInTrace != 12 || rep.Coverage.OpsIssued != 8 || rep.Coverage.OpsSkipped != 4 {
		t.Errorf("Coverage=%+v, want {OpsInTrace:12 OpsIssued:8 OpsSkipped:4}", rep.Coverage)
	}
}

// TestBuild_AsapNoDrift verifies asap mode never flags low-fidelity on drift
// even if a DriftHist somehow has data (drift is not measured in asap mode, but
// guard the code path regardless).
func TestBuild_AsapNoDrift(t *testing.T) {
	bigDrift := make([]int64, 100)
	for i := range bigDrift {
		bigDrift[i] = 5_000_000_000 // 5 s
	}
	rec := makeRecorder(bigDrift, bigDrift, 0, 100)
	hdr := makeHdr(100, int64(100*10_000_000))
	rep := fidelity.Build(rec, hdr, "asap", 10_000_000, map[int64]int64{0: 1})
	// In asap mode the drift gate is skipped; only backlog/coverage trigger LF.
	if rep.LowFidelity {
		t.Errorf("asap mode: LowFidelity=true, want false; reason=%q", rep.LowFidelityReason)
	}
}

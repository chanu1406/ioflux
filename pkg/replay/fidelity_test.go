package replay_test

// Integration tests for the fidelity report. These run full replay paths so
// they can't live in pkg/fidelity (which doesn't import replay). The three
// tests cover the three low-fidelity trigger paths: drift-driven,
// backlog-driven, and fast-backend (high fidelity).

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/fidelity"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// buildFidelityTrace creates a trace with one stream of nREADs spaced at
// intervalNS apart. Sets DurationNS so the mean-inter-arrival threshold works.
func buildFidelityTrace(t *testing.T, nREADs int, intervalNS int64) (*bytes.Buffer, trace.Header) {
	t.Helper()
	const fileSize = 4 << 20    // 4 MiB
	const readLen = int64(4096) // 4 KiB per read

	const (
		hRef = int64(1)
		tgt  = 0
	)

	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets: []trace.TargetInfo{
			{ID: 0, Name: "shard0.bin", Kind: trace.TargetFile, Size: fileSize},
		},
	}

	// ptr64 allocates a stable int64 so multiple pointers don't alias.
	ptr64 := func(v int64) *int64 { return &v }
	ptrInt := func(v int) *int { return &v }

	var ops []trace.Op
	opSeq := int64(0)
	nextID := func() *int64 { id := opSeq; opSeq++; return ptr64(id) }

	// OPEN at t=0
	ops = append(ops, trace.Op{
		T:    0,
		OpID: nextID(),
		S:    0,
		Op:   trace.OpOpen,
		Tgt:  ptrInt(tgt),
		H:    ptr64(hRef),
		Mode: trace.ModeRead,
	})

	for i := 0; i < nREADs; i++ {
		off := int64(i) * readLen
		ops = append(ops, trace.Op{
			T:    int64(i) * intervalNS,
			OpID: nextID(),
			S:    0,
			Op:   trace.OpRead,
			H:    ptr64(hRef),
			Off:  ptr64(off),
			Len:  ptr64(readLen),
		})
	}

	// CLOSE
	ops = append(ops, trace.Op{
		T:    int64(nREADs) * intervalNS,
		OpID: nextID(),
		S:    0,
		Op:   trace.OpClose,
		H:    ptr64(hRef),
	})

	totalOps := int64(len(ops))
	durationNS := int64(nREADs) * intervalNS
	hdr.Summary = trace.Summary{
		NumOps:     totalOps,
		NumStreams: 1,
		TotalBytes: int64(nREADs) * readLen,
		DurationNS: durationNS,
	}

	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	for _, op := range ops {
		if err := tw.WriteOp(op); err != nil {
			t.Fatalf("WriteOp: %v", err)
		}
	}
	return &buf, hdr
}

// TestSlowBackendIsFlaggedLowFidelity_DriftDriven verifies coordinated-omission
// fidelity detection. 1 stream × 100 READs at 10ms cadence; engine adds
// 100ms per READ. Because the stream is strictly sequential, each op falls
// 90ms further behind its intended arrival. By op 100 the cumulative drift is
// ~9 s. maxInflight=1000 so the semaphore is never the bottleneck.
func TestSlowBackendIsFlaggedLowFidelity_DriftDriven(t *testing.T) {
	const nREADs = 100
	const intervalNS = int64(10 * time.Millisecond)
	const readDelay = 100 * time.Millisecond // each READ takes 100ms; 90ms overrun

	buf, hdr := buildFidelityTrace(t, nREADs, intervalNS)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	eng := mem.New(
		mem.WithSizeFunc(func(_ string) int64 { return hdr.Targets[0].Size }),
		mem.WithInjectedDelayFunc(func(k trace.OpKind) time.Duration {
			if k == trace.OpRead {
				return readDelay
			}
			return 0
		}),
	)

	plan := replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "timeline",
		MaxInflight: 1000,
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := res.Fidelity

	if !f.LowFidelity {
		t.Errorf("LowFidelity=false, want true (90ms overrun × 100 ops)")
	}
	// p99 drift ≥ 5 s: by op 56 the cumulative overrun is 56×90ms ≈ 5.04 s.
	const wantDriftP99 = int64(5 * time.Second)
	if f.ScheduleDrift.P99NS < wantDriftP99 {
		t.Errorf("ScheduleDrift.P99NS=%dns, want ≥%dns", f.ScheduleDrift.P99NS, wantDriftP99)
	}
	// The semaphore is never the bottleneck (1 in-flight stream << 1000 cap).
	if res.BacklogEvents != 0 {
		t.Errorf("BacklogEvents=%d, want 0 (drift is pure timeline overrun, not semaphore backlog)", res.BacklogEvents)
	}
	// All ops must have been issued.
	if f.Coverage.OpsSkipped != 0 {
		t.Errorf("OpsSkipped=%d, want 0", f.Coverage.OpsSkipped)
	}
}

// TestSlowBackendIsFlaggedLowFidelity_BacklogDriven exercises the backlog path:
// many streams, slow engine, tight maxInflight cap → >5% backlog events.
func TestSlowBackendIsFlaggedLowFidelity_BacklogDriven(t *testing.T) {
	const workers = 8
	const shardsPerWorker = 4
	buf := smallTrace(t, workers, workers*shardsPerWorker)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()

	eng := mem.New(
		mem.WithSizeFunc(func(target string) int64 {
			for _, tgt := range hdr.Targets {
				if tgt.Name == target {
					return tgt.Size
				}
			}
			return 64 << 20
		}),
		mem.WithInjectedDelay(5*time.Millisecond),
	)

	plan := replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		MaxInflight: 2, // extremely tight: forces backlog on 8 concurrent streams
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := res.Fidelity

	if res.BacklogEvents == 0 {
		t.Errorf("BacklogEvents=0, want > 0 with maxInflight=2 and 8 streams")
	}
	if f.Backlog.FractionOpsBacklogged <= fidelity.LowFidelityBacklogFraction {
		t.Errorf("FractionOpsBacklogged=%.3f, want > %.2f",
			f.Backlog.FractionOpsBacklogged, fidelity.LowFidelityBacklogFraction)
	}
	if !f.LowFidelity {
		t.Errorf("LowFidelity=false, want true (backlog-driven)")
	}
}

// TestFastBackendIsHighFidelity verifies a zero-delay engine on a timeline
// trace produces LowFidelity=false. Uses 5 READs at 500ms cadence so the
// 10%-of-mean-inter-arrival threshold (~35ms) stays well above macOS/Linux
// timer granularity (~10ms), preventing platform-dependent flakes.
// Total test time: ~2.5 s.
func TestFastBackendIsHighFidelity(t *testing.T) {
	const nREADs = 5
	const intervalNS = int64(500 * time.Millisecond)

	buf, hdr := buildFidelityTrace(t, nREADs, intervalNS)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Zero-delay engine: service time ≈ 0, so drift ≈ 0.
	eng := mem.New(mem.WithSizeFunc(func(_ string) int64 { return hdr.Targets[0].Size }))

	plan := replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "timeline",
		MaxInflight: 1000,
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Fidelity.LowFidelity {
		t.Errorf("LowFidelity=true for zero-delay engine; reason=%q drift_p99=%dns",
			res.Fidelity.LowFidelityReason, res.Fidelity.ScheduleDrift.P99NS)
	}
	if res.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("OpsSkipped=%d, want 0", res.Fidelity.Coverage.OpsSkipped)
	}
}

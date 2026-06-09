package cluster_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/gen/trainingread"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// genTrace builds a small deterministic training-read trace and returns its
// bytes and parsed header.
func genTrace(t *testing.T, workers, shards int) ([]byte, trace.Header) {
	t.Helper()
	p := trainingread.DefaultParams()
	p.Shards = shards
	p.ShardSize = 128 << 10
	p.RecordSize = 16 << 10
	p.DataloaderWorkers = workers
	p.Epochs = 1
	p.Shuffle = false
	p.Seed = 1
	var buf bytes.Buffer
	if err := trainingread.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	b := buf.Bytes()
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return b, r.Header()
}

func memPlan(traceBytes []byte, assigned []int64) cluster.Plan {
	return cluster.Plan{
		TracePath:       "test.ioflux",
		TraceBytes:      traceBytes,
		AssignedStreams: assigned,
		Engine:          cluster.EngineSpec{Name: "mem"},
		Mode:            "asap",
		MaxInflight:     512,
	}
}

// runSession drives one Session through PREPARE → RUN → COLLECT and returns its
// raw output.
func runSession(t *testing.T, traceBytes []byte, assigned []int64) *replay.WorkerOutput {
	t.Helper()
	s := cluster.NewSession()
	if _, err := s.Prepare(context.Background(), memPlan(traceBytes, assigned)); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := s.Run(context.Background(), time.Now(), nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out, err := s.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return out
}

// TestSession_WholeTrace runs every stream through one Session and asserts full,
// honest coverage — the single-node path expressed via the worker primitive.
func TestSession_WholeTrace(t *testing.T) {
	traceBytes, hdr := genTrace(t, 4, 8)
	out := runSession(t, traceBytes, []int64{0, 1, 2, 3})

	if out.ActualNumOps != hdr.Summary.NumOps {
		t.Errorf("ActualNumOps=%d, want %d", out.ActualNumOps, hdr.Summary.NumOps)
	}
	if got := out.Recorder.TotalOps(); got != hdr.Summary.NumOps {
		t.Errorf("recorded ops=%d, want %d", got, hdr.Summary.NumOps)
	}
	if out.Recorder.Errors != 0 {
		t.Errorf("errors=%d, want 0", out.Recorder.Errors)
	}
	if out.Recorder.Bytes <= 0 {
		t.Errorf("bytes=%d, want > 0", out.Recorder.Bytes)
	}
	if out.Hostname == "" {
		t.Error("Collect did not stamp a hostname")
	}
}

// TestSession_SplitEquivalentToWhole proves the distribution seam at the Session
// level: splitting a trace's streams across two Sessions and merging with
// BuildResults yields the same deterministic totals as one Session running all
// streams. This is the in-process precursor to the cross-process equivalence test.
func TestSession_SplitEquivalentToWhole(t *testing.T) {
	traceBytes, hdr := genTrace(t, 4, 8)

	opts := replay.SchedulerOpts{
		Mode:        "asap",
		MaxInflight: 512,
		PlanInfo:    results.PlanInfo{Engine: "mem", Mode: "asap", TraceKind: string(hdr.Kind)},
	}

	whole := runSession(t, traceBytes, []int64{0, 1, 2, 3})
	single := replay.BuildResults([]*replay.WorkerOutput{whole}, opts, hdr, 0)

	outA := runSession(t, traceBytes, []int64{0, 1})
	outB := runSession(t, traceBytes, []int64{2, 3})
	dist := replay.BuildResults([]*replay.WorkerOutput{outA, outB}, opts, hdr, 7)

	if dist.OpsCompleted != single.OpsCompleted {
		t.Errorf("OpsCompleted: split=%d whole=%d", dist.OpsCompleted, single.OpsCompleted)
	}
	if dist.OpsCompleted != hdr.Summary.NumOps {
		t.Errorf("OpsCompleted=%d, want %d (trace.num_ops)", dist.OpsCompleted, hdr.Summary.NumOps)
	}
	if dist.BytesMoved != single.BytesMoved {
		t.Errorf("BytesMoved: split=%d whole=%d", dist.BytesMoved, single.BytesMoved)
	}
	if dist.Errors != 0 {
		t.Errorf("Errors=%d, want 0", dist.Errors)
	}
	if dist.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("Coverage.OpsSkipped=%d, want 0", dist.Fidelity.Coverage.OpsSkipped)
	}
	if dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		t.Errorf("max-per-stream in-flight=%d, want ≤1", dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight)
	}
	if len(dist.Hosts) != 2 {
		t.Errorf("dist.Hosts=%d, want 2", len(dist.Hosts))
	}
}

// TestSession_IdleWorker confirms an empty stream assignment runs nothing: a
// worker the coordinator over-provisioned (workers > streams) must produce an
// empty, error-free output rather than silently replaying all streams.
func TestSession_IdleWorker(t *testing.T) {
	traceBytes, _ := genTrace(t, 4, 8)
	out := runSession(t, traceBytes, nil)

	if out.ActualNumOps != 0 {
		t.Errorf("idle worker ActualNumOps=%d, want 0", out.ActualNumOps)
	}
	if out.Recorder.TotalOps() != 0 {
		t.Errorf("idle worker recorded ops=%d, want 0", out.Recorder.TotalOps())
	}
}

// TestSession_RunBeforePrepare guards the phase ordering the Server relies on.
func TestSession_RunBeforePrepare(t *testing.T) {
	s := cluster.NewSession()
	if err := s.Run(context.Background(), time.Now(), nil); err == nil {
		t.Fatal("Run before Prepare succeeded, want error")
	}
	if _, err := s.Collect(); err == nil {
		t.Fatal("Collect before Run succeeded, want error")
	}
}

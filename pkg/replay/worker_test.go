package replay_test

import (
	"context"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// TestWorkerMerge_SplitEquivalentToWhole proves the M2 aggregation seam: a trace
// whose streams are split across two workers and merged with BuildResults yields
// the same deterministic totals (ops, bytes, coverage, error count, strict
// concurrency) as replaying every stream in one worker. It exercises
// StreamIDs/WithStreams/RunWorker/BuildResults without any gRPC — the in-process
// precursor to the cross-process distributed-equivalence test.
func TestWorkerMerge_SplitEquivalentToWhole(t *testing.T) {
	const workers, shards = 4, 8
	buf := smallTrace(t, workers, shards)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()
	eng := memEngineForTrace(hdr)

	exec, err := replay.Prepare(replay.Plan{
		TracePath: "test.ioflux", Engine: eng, EngineName: "mem", Mode: "asap",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	opts := replay.SchedulerOpts{
		Mode:        "asap",
		MaxInflight: 512,
		PlanInfo:    results.PlanInfo{Engine: "mem", Mode: "asap", TraceKind: string(hdr.Kind)},
	}

	// Whole-trace run through the worker primitive, then aggregate the single
	// WorkerOutput — this is exactly the single-node path.
	whole, err := exec.RunWorker(context.Background(), time.Now(), nil)
	if err != nil {
		t.Fatalf("RunWorker (whole): %v", err)
	}
	single := replay.BuildResults([]*replay.WorkerOutput{whole}, opts, hdr, 0)

	// Partition the streams across two workers and run each subset.
	ids := exec.StreamIDs()
	if len(ids) < 2 {
		t.Fatalf("need ≥2 streams to split, got %d", len(ids))
	}
	half := len(ids) / 2
	outA, err := exec.WithStreams(ids[:half]).RunWorker(context.Background(), time.Now(), nil)
	if err != nil {
		t.Fatalf("RunWorker (A): %v", err)
	}
	outA.Hostname = "hostA"
	outB, err := exec.WithStreams(ids[half:]).RunWorker(context.Background(), time.Now(), nil)
	if err != nil {
		t.Fatalf("RunWorker (B): %v", err)
	}
	outB.Hostname = "hostB"
	dist := replay.BuildResults([]*replay.WorkerOutput{outA, outB}, opts, hdr, 1234)

	// Deterministic quantities must match the whole-trace run exactly.
	if dist.OpsCompleted != single.OpsCompleted {
		t.Errorf("OpsCompleted: split=%d whole=%d", dist.OpsCompleted, single.OpsCompleted)
	}
	if dist.BytesMoved != single.BytesMoved {
		t.Errorf("BytesMoved: split=%d whole=%d", dist.BytesMoved, single.BytesMoved)
	}
	if dist.OpsCompleted != hdr.Summary.NumOps {
		t.Errorf("OpsCompleted=%d, want %d (trace.num_ops)", dist.OpsCompleted, hdr.Summary.NumOps)
	}
	if dist.Errors != 0 {
		t.Errorf("Errors=%d, want 0", dist.Errors)
	}
	if got, want := dist.Fidelity.Coverage.OpsIssued, single.Fidelity.Coverage.OpsIssued; got != want {
		t.Errorf("Coverage.OpsIssued: split=%d whole=%d", got, want)
	}
	if dist.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("Coverage.OpsSkipped=%d, want 0", dist.Fidelity.Coverage.OpsSkipped)
	}
	if dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		t.Errorf("max-per-stream in-flight=%d, want ≤1 (replay added parallelism)",
			dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight)
	}

	// Multi-host metadata is populated for the split run and omitted for single.
	if len(dist.Hosts) != 2 {
		t.Errorf("dist.Hosts=%d, want 2", len(dist.Hosts))
	}
	if dist.Straggler == nil {
		t.Fatal("dist.Straggler is nil, want a straggler window for a multi-host run")
	}
	if dist.GoDeliverySkewNS != 1234 {
		t.Errorf("GoDeliverySkewNS=%d, want 1234", dist.GoDeliverySkewNS)
	}
	if single.Hosts != nil || single.Straggler != nil {
		t.Errorf("single-node result must omit hosts/straggler, got hosts=%v straggler=%v",
			single.Hosts, single.Straggler)
	}

	// Per-host ops must sum to the global total.
	var sum int64
	for _, h := range dist.Hosts {
		sum += h.OpsCompleted
	}
	if sum != dist.OpsCompleted {
		t.Errorf("sum of per-host ops=%d, want %d", sum, dist.OpsCompleted)
	}
}

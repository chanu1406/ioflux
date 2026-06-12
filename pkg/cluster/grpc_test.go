package cluster_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// bufWorker starts an in-process gRPC worker over a bufconn listener and returns
// a remoteWorker dialed to it. It exercises the real serialization/wire path while
// staying hermetic and fast (no TCP, runs on darwin).
func bufWorker(t *testing.T) cluster.Worker {
	t.Helper()
	lis := bufconn.Listen(8 << 20)
	gs := grpc.NewServer()
	cluster.NewServer().RegisterTo(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	w, err := cluster.DialWorker("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("DialWorker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func largeReadTrace(t *testing.T, readOps int) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "large.dat", Kind: trace.TargetFile, Size: 4096}},
		Summary: trace.Summary{
			NumOps:     int64(readOps + 2),
			NumStreams: 1,
			TotalBytes: int64(readOps),
			DurationNS: int64(readOps + 1),
		},
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	tgt, h := 0, int64(1)
	if err := tw.WriteOp(trace.Op{T: 0, OpID: trace.Ptr(int64(0)), S: 0, Op: trace.OpOpen, Tgt: &tgt, H: &h, Mode: trace.ModeRead}); err != nil {
		t.Fatalf("WriteOp open: %v", err)
	}
	for i := 0; i < readOps; i++ {
		id := int64(i + 1)
		off := int64(i % 4096)
		length := int64(1)
		if err := tw.WriteOp(trace.Op{T: id, OpID: &id, S: 0, Op: trace.OpRead, H: &h, Off: &off, Len: &length}); err != nil {
			t.Fatalf("WriteOp read: %v", err)
		}
	}
	closeID := int64(readOps + 1)
	if err := tw.WriteOp(trace.Op{T: closeID, OpID: &closeID, S: 0, Op: trace.OpClose, H: &h}); err != nil {
		t.Fatalf("WriteOp close: %v", err)
	}
	return buf.Bytes()
}

// TestGRPC_DistributedEquivalence is the M2 headline correctness anchor over the
// real wire: a fixed seeded trace replayed (a) single-node in-process and (b)
// across two gRPC workers via bufconn must agree on every deterministic quantity
// — ops, bytes, coverage, errors — and never add per-stream parallelism. The
// recorder snapshot crosses the wire losslessly (TestRecorderSnapshotProtoRoundTrip),
// so merged totals match exactly.
func TestGRPC_DistributedEquivalence(t *testing.T) {
	traceBytes, hdr := genTrace(t, 6, 12)

	single := coordRun(t, traceBytes, 1) // in-process localWorker baseline

	workers := []cluster.Worker{bufWorker(t), bufWorker(t)}
	c := &cluster.Coordinator{}
	dist, err := c.Run(context.Background(), memPlan(traceBytes, nil), workers)
	if err != nil {
		t.Fatalf("distributed Run: %v", err)
	}

	if dist.OpsCompleted != single.OpsCompleted {
		t.Errorf("OpsCompleted: gRPC=%d single=%d", dist.OpsCompleted, single.OpsCompleted)
	}
	if dist.OpsCompleted != hdr.Summary.NumOps {
		t.Errorf("OpsCompleted=%d, want %d (trace.num_ops)", dist.OpsCompleted, hdr.Summary.NumOps)
	}
	if dist.BytesMoved != single.BytesMoved {
		t.Errorf("BytesMoved: gRPC=%d single=%d", dist.BytesMoved, single.BytesMoved)
	}
	if dist.Errors != 0 {
		t.Errorf("Errors=%d, want 0", dist.Errors)
	}
	if dist.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("coverage.ops_skipped=%d, want 0", dist.Fidelity.Coverage.OpsSkipped)
	}
	if dist.Fidelity.Coverage.OpsIssued != single.Fidelity.Coverage.OpsIssued {
		t.Errorf("coverage.ops_issued: gRPC=%d single=%d", dist.Fidelity.Coverage.OpsIssued, single.Fidelity.Coverage.OpsIssued)
	}
	if dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		t.Errorf("max-per-stream in-flight=%d, want ≤1", dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight)
	}
	if len(dist.Hosts) != 2 {
		t.Fatalf("dist.Hosts=%d, want 2", len(dist.Hosts))
	}
	var sum int64
	for _, h := range dist.Hosts {
		sum += h.OpsCompleted
	}
	if sum != dist.OpsCompleted {
		t.Errorf("sum of per-host ops=%d, want %d", sum, dist.OpsCompleted)
	}
}

func TestGRPC_PrepareStreamLargeTrace(t *testing.T) {
	traceBytes := largeReadTrace(t, 70_000)
	if len(traceBytes) <= 4<<20 {
		t.Fatalf("generated trace size=%d, want >4MiB", len(traceBytes))
	}
	w := bufWorker(t)
	if _, err := w.Register(context.Background()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	plan := memPlan(traceBytes, []int64{0})
	if _, err := w.Prepare(context.Background(), plan); err != nil {
		t.Fatalf("streamed Prepare for %d-byte trace: %v", len(traceBytes), err)
	}
}

// TestGRPC_WorkerReusableAfterCollect confirms a worker serves sequential runs:
// after a full Prepare→Run→Collect cycle releases it, a second run prepares
// cleanly on the same worker.
func TestGRPC_WorkerReusableAfterCollect(t *testing.T) {
	w := bufWorker(t)
	traceBytes, _ := genTrace(t, 2, 4)
	plan := memPlan(traceBytes, []int64{0, 1})
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := w.Register(ctx); err != nil {
			t.Fatalf("Register (run %d): %v", i, err)
		}
		if _, err := w.Prepare(ctx, plan); err != nil {
			t.Fatalf("Prepare (run %d): %v", i, err)
		}
		if err := w.Run(ctx, time.Now(), nil); err != nil {
			t.Fatalf("Run (run %d): %v", i, err)
		}
		if _, err := w.Collect(ctx); err != nil {
			t.Fatalf("Collect (run %d): %v", i, err)
		}
	}
}

// TestGRPC_WorkerRejectsOverlappingRun pins result-safety: while a run is active
// and uncollected, a second Prepare is rejected, so it cannot clobber the first
// run's results before they are collected.
func TestGRPC_WorkerRejectsOverlappingRun(t *testing.T) {
	w := bufWorker(t)
	traceBytes, _ := genTrace(t, 2, 4)
	plan := memPlan(traceBytes, []int64{0, 1})
	ctx := context.Background()

	if _, err := w.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := w.Prepare(ctx, plan); err != nil {
		t.Fatalf("Prepare (1): %v", err)
	}
	if err := w.Run(ctx, time.Now(), nil); err != nil {
		t.Fatalf("Run (1): %v", err)
	}
	// Run finished but results not yet collected: the worker is still owned, so a
	// second Prepare must be rejected rather than overwriting the pending results.
	if _, err := w.Prepare(ctx, plan); err == nil {
		t.Fatal("Prepare during an uncollected run succeeded, want busy rejection")
	}
	if _, err := w.Collect(ctx); err != nil {
		t.Fatalf("Collect: %v", err)
	}
}

// TestGRPC_PrepareFailureAborts confirms a worker-side PREPARE error propagates
// over the wire and aborts the run with no results (no partial output).
func TestGRPC_PrepareFailureAborts(t *testing.T) {
	traceBytes, _ := genTrace(t, 2, 4)

	plan := memPlan(traceBytes, nil)
	plan.Engine = cluster.EngineSpec{Name: "bogus-engine"} // BuildEngine rejects this

	c := &cluster.Coordinator{}
	res, err := c.Run(context.Background(), plan, []cluster.Worker{bufWorker(t)})
	if err == nil {
		t.Fatal("Run with an unbuildable engine succeeded, want error")
	}
	if res != nil {
		t.Errorf("failed run returned results=%v, want nil", res)
	}
}

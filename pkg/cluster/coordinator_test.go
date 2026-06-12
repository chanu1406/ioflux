package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// localWorkers returns n in-process workers, each wrapping a fresh Session.
func localWorkers(n int) []cluster.Worker {
	ws := make([]cluster.Worker, n)
	for i := range ws {
		ws[i] = cluster.NewLocalWorker(cluster.NewSession())
	}
	return ws
}

func coordRun(t *testing.T, traceBytes []byte, nWorkers int) *results.Results {
	t.Helper()
	c := &cluster.Coordinator{}
	res, err := c.Run(context.Background(), memPlan(traceBytes, nil), localWorkers(nWorkers))
	if err != nil {
		t.Fatalf("Coordinator.Run(%d workers): %v", nWorkers, err)
	}
	return res
}

// TestCoordinator_DistributedEquivalence is the headline M2 correctness anchor
// (in-process form): the same trace replayed single-node and across three workers
// must yield identical deterministic totals — ops, bytes, coverage, error count —
// and never add per-stream parallelism. Latency percentiles are timing-dependent
// across separate runs and are not asserted here; merge losslessness is covered
// by the histogram/snapshot tests.
func TestCoordinator_DistributedEquivalence(t *testing.T) {
	traceBytes, hdr := genTrace(t, 6, 12)

	single := coordRun(t, traceBytes, 1)
	dist := coordRun(t, traceBytes, 3)

	if single.OpsCompleted != hdr.Summary.NumOps {
		t.Errorf("single OpsCompleted=%d, want %d", single.OpsCompleted, hdr.Summary.NumOps)
	}
	if dist.OpsCompleted != single.OpsCompleted {
		t.Errorf("OpsCompleted: dist=%d single=%d", dist.OpsCompleted, single.OpsCompleted)
	}
	if dist.BytesMoved != single.BytesMoved {
		t.Errorf("BytesMoved: dist=%d single=%d", dist.BytesMoved, single.BytesMoved)
	}
	if dist.Errors != 0 || single.Errors != 0 {
		t.Errorf("errors: dist=%d single=%d, want 0", dist.Errors, single.Errors)
	}
	if dist.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("dist coverage.ops_skipped=%d, want 0", dist.Fidelity.Coverage.OpsSkipped)
	}
	if dist.Fidelity.Coverage.OpsIssued != single.Fidelity.Coverage.OpsIssued {
		t.Errorf("coverage.ops_issued: dist=%d single=%d", dist.Fidelity.Coverage.OpsIssued, single.Fidelity.Coverage.OpsIssued)
	}
	if dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		t.Errorf("dist max-per-stream in-flight=%d, want ≤1", dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight)
	}

	// Single-node output omits the distributed-only fields; multi-host populates them.
	if single.Hosts != nil || single.Straggler != nil {
		t.Errorf("single-node result must omit hosts/straggler, got hosts=%v straggler=%v", single.Hosts, single.Straggler)
	}
	if len(dist.Hosts) != 3 {
		t.Fatalf("dist.Hosts=%d, want 3", len(dist.Hosts))
	}
	if dist.Straggler == nil {
		t.Fatal("dist.Straggler is nil, want a straggler window")
	}
	var sum int64
	for _, h := range dist.Hosts {
		sum += h.OpsCompleted
	}
	if sum != dist.OpsCompleted {
		t.Errorf("sum of per-host ops=%d, want %d", sum, dist.OpsCompleted)
	}
}

// TestCoordinator_NoWorkers rejects an empty worker set rather than producing a
// silently-empty result.
func TestCoordinator_NoWorkers(t *testing.T) {
	traceBytes, _ := genTrace(t, 2, 4)
	c := &cluster.Coordinator{}
	if _, err := c.Run(context.Background(), memPlan(traceBytes, nil), nil); err == nil {
		t.Fatal("Run with no workers succeeded, want error")
	}
}

// stubWorker is a programmable Worker for failure/version tests. Run blocks until
// ctx is cancelled unless runErr is set, in which case it fails immediately.
type stubWorker struct {
	info      cluster.WorkerInfo
	runErr    error
	cancelled chan struct{} // closed if Run observed ctx cancellation
	once      sync.Once
}

func newStubWorker() *stubWorker {
	return &stubWorker{
		info:      cluster.WorkerInfo{Hostname: "stub", CPUs: 1, Version: cluster.Version},
		cancelled: make(chan struct{}),
	}
}

func (w *stubWorker) Register(context.Context) (cluster.WorkerInfo, error) { return w.info, nil }
func (w *stubWorker) Prepare(context.Context, cluster.Plan) (cluster.PrepareResult, error) {
	return cluster.PrepareResult{}, nil
}
func (w *stubWorker) Run(ctx context.Context, _ time.Time, _ func(ops, bytes int64)) error {
	if w.runErr != nil {
		return w.runErr
	}
	<-ctx.Done()
	w.once.Do(func() { close(w.cancelled) })
	return ctx.Err()
}
func (w *stubWorker) Collect(context.Context) (*replay.WorkerOutput, error) {
	return &replay.WorkerOutput{Recorder: nil}, nil
}
func (w *stubWorker) Close() error { return nil }

// TestCoordinator_AbortOnWorkerFailure pins PRD §8.9: one worker failing mid-RUN
// cancels the rest and aborts with no results.
func TestCoordinator_AbortOnWorkerFailure(t *testing.T) {
	traceBytes, _ := genTrace(t, 2, 4)

	failing := newStubWorker()
	failing.runErr = errors.New("backend exploded")
	bystander := newStubWorker()

	c := &cluster.Coordinator{}
	res, err := c.Run(context.Background(), memPlan(traceBytes, nil), []cluster.Worker{failing, bystander})
	if err == nil {
		t.Fatal("Run with a failing worker succeeded, want error")
	}
	if res != nil {
		t.Errorf("aborted run returned results=%v, want nil", res)
	}
	if !strings.Contains(err.Error(), "backend exploded") {
		t.Errorf("error=%q, want it to surface the root cause (not context.Canceled)", err)
	}

	// The bystander must have observed cancellation rather than running to completion.
	select {
	case <-bystander.cancelled:
	case <-time.After(2 * time.Second):
		t.Error("bystander worker was not cancelled after a peer failed")
	}
}

// progressWorker is a worker that fires its first progress tick after a fixed
// delay, used to verify Go-delivery skew measurement.
type progressWorker struct{ startDelay time.Duration }

func (w *progressWorker) Register(context.Context) (cluster.WorkerInfo, error) {
	return cluster.WorkerInfo{Hostname: "prog", CPUs: 1, Version: cluster.Version}, nil
}
func (w *progressWorker) Prepare(context.Context, cluster.Plan) (cluster.PrepareResult, error) {
	return cluster.PrepareResult{}, nil
}
func (w *progressWorker) Run(ctx context.Context, _ time.Time, progress func(ops, bytes int64)) error {
	select {
	case <-time.After(w.startDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if progress != nil {
		progress(0, 0) // the "started" tick the coordinator times
	}
	return nil
}
func (w *progressWorker) Collect(context.Context) (*replay.WorkerOutput, error) {
	return &replay.WorkerOutput{Recorder: metrics.NewRecorder(), PeakByStream: map[int64]int64{}}, nil
}
func (w *progressWorker) Close() error { return nil }

// TestCoordinator_GoDeliverySkew proves the skew diagnostic tracks when each
// worker actually starts (its first progress tick), not coordinator-side goroutine
// dispatch: a worker that starts 50ms later than its peer yields a skew of at
// least that gap.
func TestCoordinator_GoDeliverySkew(t *testing.T) {
	traceBytes, _ := genTrace(t, 2, 4)
	workers := []cluster.Worker{
		&progressWorker{startDelay: 0},
		&progressWorker{startDelay: 50 * time.Millisecond},
	}
	c := &cluster.Coordinator{}
	res, err := c.Run(context.Background(), memPlan(traceBytes, nil), workers)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GoDeliverySkewNS < (30 * time.Millisecond).Nanoseconds() {
		t.Errorf("GoDeliverySkewNS=%d, want ≳50ms (skew should reflect first-progress timing)", res.GoDeliverySkewNS)
	}
}

// TestCoordinator_VersionMismatch rejects a worker running a different protocol.
func TestCoordinator_VersionMismatch(t *testing.T) {
	traceBytes, _ := genTrace(t, 2, 4)
	stale := newStubWorker()
	stale.info.Version = "0.0.0-stale"

	c := &cluster.Coordinator{}
	if _, err := c.Run(context.Background(), memPlan(traceBytes, nil), []cluster.Worker{stale}); err == nil {
		t.Fatal("Run with a version-mismatched worker succeeded, want error")
	}
}

type prepareModeWorker struct {
	mu          sync.Mutex
	prepareMode string
}

func (w *prepareModeWorker) Register(context.Context) (cluster.WorkerInfo, error) {
	return cluster.WorkerInfo{Hostname: "prep", CPUs: 1, Version: cluster.Version}, nil
}
func (w *prepareModeWorker) Prepare(_ context.Context, p cluster.Plan) (cluster.PrepareResult, error) {
	w.mu.Lock()
	w.prepareMode = p.PrepareMode
	w.mu.Unlock()
	return cluster.PrepareResult{}, nil
}
func (w *prepareModeWorker) Run(context.Context, time.Time, func(ops, bytes int64)) error { return nil }
func (w *prepareModeWorker) Collect(context.Context) (*replay.WorkerOutput, error) {
	return &replay.WorkerOutput{Recorder: metrics.NewRecorder(), PeakByStream: map[int64]int64{}}, nil
}
func (w *prepareModeWorker) Close() error { return nil }
func (w *prepareModeWorker) mode() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.prepareMode
}

func emptyTrace(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "obj", Kind: trace.TargetObject, Size: 1}},
		Summary:       trace.Summary{NumOps: 0, NumStreams: 0},
	}); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	return buf.Bytes()
}

func TestCoordinator_SharedPrepareVerifiesAfterWorkerZero(t *testing.T) {
	workers := []*prepareModeWorker{{}, {}}
	cworkers := []cluster.Worker{workers[0], workers[1]}
	plan := memPlan(emptyTrace(t), nil)
	plan.Engine.Name = "s3"
	plan.PrepareMode = "materialize-synthetic"

	res, err := (&cluster.Coordinator{}).Run(context.Background(), plan, cworkers)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if workers[0].mode() != "materialize-synthetic" {
		t.Fatalf("worker0 PrepareMode=%q, want materialize-synthetic", workers[0].mode())
	}
	if workers[1].mode() != "assume-existing" {
		t.Fatalf("worker1 PrepareMode=%q, want assume-existing verification", workers[1].mode())
	}
	if res.Plan.PrepareScope != cluster.PrepareScopeShared {
		t.Fatalf("PrepareScope=%q, want shared", res.Plan.PrepareScope)
	}
}

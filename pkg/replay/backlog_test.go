package replay_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// TestBacklog_TotalInflightCapped verifies that maxInflight caps global
// in-flight operations and records semaphore backlog.
func TestBacklog_TotalInflightCapped(t *testing.T) {
	// Use a larger smallTrace so there are more concurrent streams than maxInflight.
	const workers, shards = 16, 32
	buf := smallTrace(t, workers, shards)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()

	// Rebuild the trace buffer from hdr for the engine.
	buf = smallTrace(t, workers, shards)
	r, err = trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader2: %v", err)
	}

	var globalInflight atomic.Int64
	var peakGlobalInflight atomic.Int64

	baseEng := mem.New(
		mem.WithSizeFunc(func(name string) int64 {
			for _, tgt := range hdr.Targets {
				if tgt.Name == name && tgt.Size > 0 {
					return tgt.Size
				}
			}
			return 64 << 20
		}),
		mem.WithInjectedDelay(5*time.Millisecond),
	)
	cntEng := &countingEngine{
		inner: baseEng,
		onEnterRead: func() {
			cur := globalInflight.Add(1)
			for {
				old := peakGlobalInflight.Load()
				if cur <= old {
					break
				}
				if peakGlobalInflight.CompareAndSwap(old, cur) {
					break
				}
			}
		},
		onLeaveRead: func() { globalInflight.Add(-1) },
	}

	const maxInflight = 8
	plan := replay.Plan{
		Engine:      cntEng,
		EngineName:  "counting",
		Mode:        "asap",
		MaxInflight: maxInflight,
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Engine-level peak must not exceed maxInflight.
	if peak := peakGlobalInflight.Load(); peak > maxInflight {
		t.Errorf("peakGlobalInflight=%d, want <= maxInflight=%d (semaphore not enforcing cap)",
			peak, maxInflight)
	}

	// Backlog counters must be positive — the semaphore was saturated.
	if res.BacklogEvents == 0 {
		t.Error("BacklogEvents=0, want > 0 (many slow streams with tight cap should create backlog)")
	}
	if res.BacklogBlockedNS == 0 {
		t.Error("BacklogBlockedNS=0, want > 0")
	}

	// In asap mode, semaphore-wait time must not be credited to op latency
	// (PRD §8.5: "Latency = service time"). Injected delay is 5ms; even with
	// significant queue waits, p99 must stay close to service time.
	if readStats, ok := res.PerOpMap()["READ"]; ok {
		const maxAsapLatencyNS = int64(50 * time.Millisecond)
		if readStats.P99NS > maxAsapLatencyNS {
			t.Errorf("asap READ.p99=%dms, want <= 50ms (semaphore-wait time must not be credited as latency in asap mode)",
				readStats.P99NS/1_000_000)
		}
	}
}

// TestBacklog_NoEventsWhenUncapped verifies BacklogEvents=0 when MaxInflight
// far exceeds the number of concurrent in-flight ops.
func TestBacklog_NoEventsWhenUncapped(t *testing.T) {
	const workers, shards = 4, 8
	buf := smallTrace(t, workers, shards)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()
	eng := memEngineForTrace(hdr)

	// Re-read: NewReader consumed the buffer header, but Prepare needs a fresh reader.
	buf2 := smallTrace(t, workers, shards)
	r2, err := trace.NewReader(buf2)
	if err != nil {
		t.Fatalf("NewReader2: %v", err)
	}

	plan := replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		MaxInflight: 10000, // never saturates
	}
	exec, err := replay.Prepare(plan, r2)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.BacklogEvents != 0 {
		t.Errorf("BacklogEvents=%d, want 0 (uncapped semaphore should never block)", res.BacklogEvents)
	}
}

// TestMemEngineYields_1000Streams verifies that many MemEngine streams complete
// without scheduler starvation.
func TestMemEngineYields_1000Streams(t *testing.T) {
	buf := build1000StreamTrace(t)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	eng := mem.New(mem.WithFixedSize(4096))

	plan := replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		MaxInflight: 10000,
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	start := time.Now()
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)

	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}
	// 1000 streams × 2 ops (OPEN+CLOSE) = 2000 ops total.
	if res.OpsCompleted != 2000 {
		t.Errorf("OpsCompleted=%d, want 2000", res.OpsCompleted)
	}
	// MemEngine is instant; 1000 goroutines should all complete well under 30s.
	if elapsed > 30*time.Second {
		t.Errorf("Run took %v, want < 30s (possible scheduler starvation)", elapsed)
	}
}

// build1000StreamTrace builds a minimal trace: 1000 streams × (OPEN + CLOSE).
func build1000StreamTrace(t *testing.T) *bytes.Buffer {
	t.Helper()
	const nStreams = 1000

	targets := make([]trace.TargetInfo, nStreams)
	for i := range targets {
		targets[i] = trace.TargetInfo{
			ID:   i,
			Name: fmt.Sprintf("shard_%04d", i),
			Kind: trace.TargetFile,
			Size: 4096,
		}
	}

	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       targets,
		Summary: trace.Summary{
			NumOps:     int64(nStreams * 2),
			NumStreams: nStreams,
		},
	}

	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	for s := 0; s < nStreams; s++ {
		tgt := s
		h := int64(s + 1)
		id0 := int64(s * 2)
		id1 := id0 + 1
		tw.WriteOp(trace.Op{T: 0, OpID: &id0, S: int64(s), Op: trace.OpOpen, Tgt: &tgt, H: &h, Mode: trace.ModeRead}) //nolint:errcheck
		tw.WriteOp(trace.Op{T: 0, OpID: &id1, S: int64(s), Op: trace.OpClose, H: &h})                                 //nolint:errcheck
	}
	return &buf
}

// countingEngine wraps a MemEngine and calls onEnterRead/onLeaveRead around
// each Read call to measure global concurrent in-flight ops.
type countingEngine struct {
	inner       *mem.MemEngine
	onEnterRead func()
	onLeaveRead func()
}

func (e *countingEngine) Caps() engine.Capabilities { return e.inner.Caps() }

func (e *countingEngine) Open(ctx context.Context, target string, mode engine.Mode, flags engine.OpenFlags) (engine.Handle, error) {
	return e.inner.Open(ctx, target, mode, flags)
}

func (e *countingEngine) Read(ctx context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	if e.onEnterRead != nil {
		e.onEnterRead()
	}
	n, err := e.inner.Read(ctx, h, off, length, buf)
	if e.onLeaveRead != nil {
		e.onLeaveRead()
	}
	return n, err
}

func (e *countingEngine) Write(ctx context.Context, h engine.Handle, off int64, data []byte) (int, error) {
	return e.inner.Write(ctx, h, off, data)
}

func (e *countingEngine) Fsync(ctx context.Context, h engine.Handle) error {
	return e.inner.Fsync(ctx, h)
}

func (e *countingEngine) Close(ctx context.Context, h engine.Handle) error {
	return e.inner.Close(ctx, h)
}

func (e *countingEngine) Stat(ctx context.Context, target string) (engine.ObjectInfo, error) {
	return e.inner.Stat(ctx, target)
}

func (e *countingEngine) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}

func (e *countingEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}

func (e *countingEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}

func (e *countingEngine) Delete(_ context.Context, _ string) error {
	return engine.ErrUnsupported
}

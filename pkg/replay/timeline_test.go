package replay_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// buildTimelineTrace builds a single-stream trace with:
//   - 1 OPEN at t=0
//   - nReads READ ops spaced cadenceNS apart (op i at t = i * cadenceNS)
//   - 1 CLOSE after the last READ
func buildTimelineTrace(t *testing.T, nReads int, cadenceNS int64) *bytes.Buffer {
	t.Helper()
	tgt := 0
	h := int64(1)
	off := int64(0)
	readLen := int64(4096)
	numOps := int64(nReads + 2)

	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets: []trace.TargetInfo{
			{ID: 0, Name: "target", Kind: trace.TargetFile, Size: 1 << 20},
		},
		Summary: trace.Summary{NumOps: numOps, NumStreams: 1},
	}

	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	opID := int64(0)
	writeOp := func(op trace.Op) {
		if err := tw.WriteOp(op); err != nil {
			t.Fatalf("WriteOp: %v", err)
		}
	}

	writeOp(trace.Op{T: 0, OpID: &opID, S: 0, Op: trace.OpOpen, Tgt: &tgt, H: &h, Mode: trace.ModeRead})
	opID++

	for i := 0; i < nReads; i++ {
		ts := int64(i) * cadenceNS
		writeOp(trace.Op{T: ts, OpID: trace.Ptr(opID), S: 0, Op: trace.OpRead, H: &h, Off: &off, Len: &readLen})
		opID++
	}

	lastT := int64(0)
	if nReads > 0 {
		lastT = int64(nReads-1) * cadenceNS
	}
	writeOp(trace.Op{T: lastT, OpID: &opID, S: 0, Op: trace.OpClose, H: &h})

	return &buf
}

// memEngineSlowRead returns a MemEngine that sleeps readDelay before each READ.
// Sizes are taken from hdr.Targets; any target not in the header defaults to 64MiB.
func memEngineSlowRead(hdr trace.Header, readDelay time.Duration) *mem.MemEngine {
	sizeMap := make(map[string]int64, len(hdr.Targets))
	for _, tgt := range hdr.Targets {
		sizeMap[tgt.Name] = tgt.Size
	}
	return mem.New(
		mem.WithSizeFunc(func(name string) int64 {
			if sz := sizeMap[name]; sz > 0 {
				return sz
			}
			return 64 << 20
		}),
		mem.WithInjectedDelayFunc(func(op trace.OpKind) time.Duration {
			if op == trace.OpRead {
				return readDelay
			}
			return 0
		}),
	)
}

// TestTimeline_CoordinatedOmission verifies that timeline latency is measured
// from intendedArrival, not actualIssue.
//
// Setup: 1 stream × 100 READs at 10ms cadence; engine sleeps 100ms per READ.
// Service time (100ms) >> cadence (10ms) → each op falls 90ms further behind.
// At op K, drift = K×90ms. p99 latency accumulates to ~9s.
// Service-time accounting would record only ~100ms; CO-correct must exceed 5s.
//
// This test takes ~10 seconds intentionally — skip with -short.
func TestTimeline_CoordinatedOmission(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long CO correctness test in short mode")
	}

	const nReads = 100
	const cadenceNS = int64(10 * time.Millisecond)

	singleTargetHdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "target", Kind: trace.TargetFile, Size: 1 << 20}},
		Summary:       trace.Summary{NumOps: nReads + 2, NumStreams: 1},
	}
	buf := buildTimelineTrace(t, nReads, cadenceNS)
	eng := memEngineSlowRead(singleTargetHdr, 100*time.Millisecond)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	plan := replay.Plan{
		Engine:      eng,
		EngineName:  "mem-slow",
		Mode:        "timeline",
		MaxInflight: 1000, // never saturates with 1 sequential stream
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	pm := res.PerOpMap()
	readStats, ok := pm["READ"]
	if !ok {
		t.Fatal("no READ stats in results")
	}

	const fiveSeconds = int64(5 * time.Second)

	// CO-correct p99 accumulates to ~9s; service-time-only p99 = ~100ms.
	if readStats.P99NS < fiveSeconds {
		t.Errorf("READ.p99=%dms, want >= 5000ms (CO-correct latency required); "+
			"if < 200ms, latency is measuring service time instead of completion−intendedArrival",
			readStats.P99NS/1_000_000)
	}

	// Schedule drift (actualIssue − intendedArrival) also accumulates.
	if res.ScheduleDrift.P99NS < fiveSeconds {
		t.Errorf("ScheduleDrift.P99NS=%dms, want >= 5000ms",
			res.ScheduleDrift.P99NS/1_000_000)
	}

	// With 1 stream and maxInflight=1000, the semaphore is never full.
	// CO drift is stream-internal (causal wait), not a semaphore backlog.
	if res.BacklogEvents != 0 {
		t.Errorf("BacklogEvents=%d, want 0 (large maxInflight should prevent backlog with 1 stream)",
			res.BacklogEvents)
	}
}

// TestTimeline_LatencyEqualsCompletionMinusIntended verifies the basic CO
// formula when ops arrive on time: both READs record latency ≈ service time,
// not 0 (issue-relative) and not cumulative.
func TestTimeline_LatencyEqualsCompletionMinusIntended(t *testing.T) {
	// 2 READs: t=0 and t=10ms. Engine sleeps 5ms per READ.
	// Cadence (10ms) > service (5ms) → both arrive on time.
	// Expected latency for each: ~5ms (completion − intendedArrival = service time).
	const cadenceNS = int64(10 * time.Millisecond)
	buf := buildTimelineTrace(t, 2, cadenceNS)

	singleTargetHdr := trace.Header{
		Targets: []trace.TargetInfo{{ID: 0, Name: "target", Kind: trace.TargetFile, Size: 1 << 20}},
	}
	eng := memEngineSlowRead(singleTargetHdr, 5*time.Millisecond)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	plan := replay.Plan{Engine: eng, EngineName: "mem", Mode: "timeline", MaxInflight: 1000}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	pm := res.PerOpMap()
	readStats, ok := pm["READ"]
	if !ok {
		t.Fatal("no READ stats")
	}

	const oneMSNS = int64(time.Millisecond)
	const fiftyMSNS = int64(50 * time.Millisecond)
	// Latency must be close to 5ms service time.
	// < 1ms → measuring issue−issue (not CO). > 50ms → accumulating across ops.
	if readStats.P50NS < oneMSNS {
		t.Errorf("READ.p50=%dns, want >= 1ms (latency must not be near-zero)", readStats.P50NS)
	}
	if readStats.P99NS > fiftyMSNS {
		t.Errorf("READ.p99=%dms, want <= 50ms (latency must not accumulate)", readStats.P99NS/1_000_000)
	}
}

// TestTimeline_StrictSequentialityHolds verifies that replay does not add
// parallelism within a stream.
func TestTimeline_StrictSequentialityHolds(t *testing.T) {
	const workers, shards = 4, 8
	buf := smallTrace(t, workers, shards)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()

	baseEng := mem.New(
		mem.WithSizeFunc(func(name string) int64 {
			for _, tgt := range hdr.Targets {
				if tgt.Name == name && tgt.Size > 0 {
					return tgt.Size
				}
			}
			return 64 << 20
		}),
		mem.WithInjectedDelay(5*time.Millisecond), // slow enough to reveal races
	)
	tracker := newInflightTracker(baseEng)

	plan := replay.Plan{
		Engine:      tracker,
		EngineName:  "tracking",
		Mode:        "asap",
		MaxInflight: 1000, // semaphore never saturates; tests structural sequentiality
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := exec.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	for target, peak := range tracker.peak {
		if peak > 1 {
			t.Errorf("target %q: peak concurrent in-flight = %d, want <= 1 (stream sequentiality violated)",
				target, peak)
		}
	}
}

// TestPrepareRejectsGroupedTrace verifies that grouped traces are rejected.
func TestPrepareRejectsGroupedTrace(t *testing.T) {
	tgt := 0
	h := int64(1)
	id0 := int64(0)
	id1 := int64(1)
	group1 := int64(1)
	off0 := int64(0)
	readLen := int64(4096)

	ops := []trace.Op{
		{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt, H: &h, Mode: trace.ModeRead},
		{T: 1, OpID: &id1, S: 0, Op: trace.OpRead, H: &h, Off: &off0, Len: &readLen, Group: &group1},
	}

	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(basicHeader(int64(len(ops)))); err != nil {
		t.Fatal(err)
	}
	for _, op := range ops {
		if err := tw.WriteOp(op); err != nil {
			t.Fatal(err)
		}
	}

	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	eng := mem.New(mem.WithFixedSize(4096))
	_, err = replay.Prepare(replay.Plan{Engine: eng, EngineName: "mem", Mode: "asap"}, r)
	if err == nil {
		t.Fatal("Prepare should have rejected trace with non-default group")
	}
	if !strings.Contains(err.Error(), "non-default group") {
		t.Fatalf("Prepare error = %v, want substring 'non-default group'", err)
	}
}

// --- inflightTracker: per-target concurrent-call tracking engine wrapper ---

// inflightTracker wraps an engine and tracks, per target, the peak number of
// concurrent in-flight operations. Used to verify stream strict sequentiality:
// since in the generated traces each target belongs to at most one stream at a
// time, peak-per-target > 1 implies a stream issued concurrent ops.
type inflightTracker struct {
	inner   engine.Engine
	mu      sync.Mutex
	handles map[engine.Handle]string // h → target name
	cur     map[string]int           // current concurrent in-flight per target
	peak    map[string]int           // peak concurrent in-flight per target
}

func newInflightTracker(inner engine.Engine) *inflightTracker {
	return &inflightTracker{
		inner:   inner,
		handles: make(map[engine.Handle]string),
		cur:     make(map[string]int),
		peak:    make(map[string]int),
	}
}

func (e *inflightTracker) Caps() engine.Capabilities { return e.inner.Caps() }

func (e *inflightTracker) Open(ctx context.Context, target string, mode engine.Mode, flags engine.OpenFlags) (engine.Handle, error) {
	h, err := e.inner.Open(ctx, target, mode, flags)
	if err == nil {
		e.mu.Lock()
		e.handles[h] = target
		e.mu.Unlock()
	}
	return h, err
}

func (e *inflightTracker) enterHandle(h engine.Handle) {
	e.mu.Lock()
	defer e.mu.Unlock()
	target := e.handles[h]
	e.cur[target]++
	if e.cur[target] > e.peak[target] {
		e.peak[target] = e.cur[target]
	}
}

func (e *inflightTracker) leaveHandle(h engine.Handle) {
	e.mu.Lock()
	e.cur[e.handles[h]]--
	e.mu.Unlock()
}

func (e *inflightTracker) Read(ctx context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	e.enterHandle(h)
	n, err := e.inner.Read(ctx, h, off, length, buf)
	e.leaveHandle(h)
	return n, err
}

func (e *inflightTracker) Write(ctx context.Context, h engine.Handle, off int64, data []byte) (int, error) {
	e.enterHandle(h)
	n, err := e.inner.Write(ctx, h, off, data)
	e.leaveHandle(h)
	return n, err
}

func (e *inflightTracker) Fsync(ctx context.Context, h engine.Handle) error {
	e.enterHandle(h)
	err := e.inner.Fsync(ctx, h)
	e.leaveHandle(h)
	return err
}

func (e *inflightTracker) Close(ctx context.Context, h engine.Handle) error {
	err := e.inner.Close(ctx, h)
	e.mu.Lock()
	delete(e.handles, h)
	e.mu.Unlock()
	return err
}

func (e *inflightTracker) Stat(ctx context.Context, target string) (engine.ObjectInfo, error) {
	return e.inner.Stat(ctx, target)
}

func (e *inflightTracker) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return engine.ErrUnsupported
}

func (e *inflightTracker) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}

func (e *inflightTracker) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}

func (e *inflightTracker) Delete(_ context.Context, _ string) error {
	return engine.ErrUnsupported
}

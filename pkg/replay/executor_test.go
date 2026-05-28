package replay_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/gen/trainingread"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// smallTrace generates a small training-read trace into a buffer.
func smallTrace(t *testing.T, workers, shards int) *bytes.Buffer {
	t.Helper()
	p := trainingread.DefaultParams()
	p.Shards = shards
	p.ShardSize = 128 << 10 // 128 KiB
	p.RecordSize = 16 << 10 // 16 KiB
	p.DataloaderWorkers = workers
	p.Epochs = 1
	p.Shuffle = false
	p.Seed = 1
	var buf bytes.Buffer
	if err := trainingread.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return &buf
}

func memEngineForTrace(hdr trace.Header) *mem.MemEngine {
	sizeMap := make(map[string]int64, len(hdr.Targets))
	for _, tgt := range hdr.Targets {
		sizeMap[tgt.Name] = tgt.Size
	}
	return mem.New(mem.WithSizeFunc(func(name string) int64 {
		if sz, ok := sizeMap[name]; ok && sz > 0 {
			return sz
		}
		return 64 << 20
	}))
}

func prepareOps(t *testing.T, hdr trace.Header, ops []trace.Op, eng engine.Engine) error {
	t.Helper()
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
	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = replay.Prepare(replay.Plan{Engine: eng, EngineName: "test", Mode: "asap"}, r)
	return err
}

func basicHeader(numOps int64) trace.Header {
	return trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "f0", Kind: trace.TargetFile, Size: 4096}},
		Summary:       trace.Summary{NumOps: numOps, NumStreams: 1},
	}
}

// TestEndToEnd replays a small training-read trace against MemEngine and
// verifies results match trace metadata.
func TestEndToEnd(t *testing.T) {
	const workers, shards = 2, 4
	buf := smallTrace(t, workers, shards)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()
	eng := memEngineForTrace(hdr)

	plan := replay.Plan{
		TracePath:  "test.ioflux",
		Engine:     eng,
		EngineName: "mem",
		Mode:       "asap",
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}
	if res.OpsCompleted != hdr.Summary.NumOps {
		t.Errorf("OpsCompleted=%d, want %d (trace.num_ops)", res.OpsCompleted, hdr.Summary.NumOps)
	}
	if res.BytesMoved != hdr.Summary.TotalBytes {
		t.Errorf("BytesMoved=%d, want %d (trace.total_bytes)", res.BytesMoved, hdr.Summary.TotalBytes)
	}

	pm := res.PerOpMap()
	for _, opType := range []string{"READ", "OPEN", "CLOSE"} {
		s, ok := pm[opType]
		if !ok {
			t.Errorf("per_op_stats missing %s", opType)
			continue
		}
		if s.Count == 0 {
			t.Errorf("%s count=0, want > 0", opType)
		}
	}

	// Percentiles must be monotonically non-decreasing.
	for _, s := range res.PerOpStats {
		if !(s.P50NS <= s.P90NS && s.P90NS <= s.P99NS && s.P99NS <= s.MaxNS) {
			t.Errorf("%s: percentiles not monotonic p50=%d p90=%d p99=%d max=%d",
				s.OpType, s.P50NS, s.P90NS, s.P99NS, s.MaxNS)
		}
	}
}

// TestDeterministicReplay verifies that replaying the same trace twice produces
// equal ops-completed counts.
func TestDeterministicReplay(t *testing.T) {
	buf := smallTrace(t, 2, 4)
	buf2 := bytes.NewBuffer(buf.Bytes())

	runOnce := func(data *bytes.Buffer) int64 {
		r, err := trace.NewReader(data)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		eng := memEngineForTrace(r.Header())
		plan := replay.Plan{Engine: eng, EngineName: "mem", Mode: "asap"}
		exec, err := replay.Prepare(plan, r)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		res, err := exec.Run(context.Background())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return res.OpsCompleted
	}

	if c1, c2 := runOnce(buf), runOnce(buf2); c1 != c2 {
		t.Errorf("OpsCompleted differs across runs: %d vs %d", c1, c2)
	}
}

func TestPrepareValidatesMalformedTrace(t *testing.T) {
	tgt0 := 0
	tgt99 := 99
	h0 := int64(1)
	off0 := int64(0)
	len1024 := int64(1024)
	id0 := int64(0)
	id1 := int64(1)

	cases := []struct {
		name string
		ops  []trace.Op
		want string
	}{
		{
			name: "target-out-of-range",
			ops: []trace.Op{
				{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt99, H: &h0, Mode: trace.ModeRead},
			},
			want: "out of range",
		},
		{
			name: "read-missing-offset",
			ops: []trace.Op{
				{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeRead},
				{T: 1, OpID: &id1, S: 0, Op: trace.OpRead, H: &h0, Len: &len1024},
			},
			want: "READ missing required off",
		},
		{
			name: "read-before-open",
			ops: []trace.Op{
				{T: 0, OpID: &id0, S: 0, Op: trace.OpRead, H: &h0, Off: &off0, Len: &len1024},
			},
			want: "unknown handle",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := prepareOps(t, basicHeader(int64(len(tc.ops))), tc.ops, newCapsEngine(engine.Capabilities{
				Seekable:     true,
				PartialWrite: true,
				Durable:      true,
				ObjectAPI:    true,
			}))
			if err == nil {
				t.Fatal("Prepare should reject malformed trace")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Prepare error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

// TestCapsRejectionPartialWrite verifies that Prepare rejects a trace with an
// offset WRITE against an engine that reports PartialWrite=false.
func TestCapsRejectionPartialWrite(t *testing.T) {
	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	tgt0 := 0
	h0 := int64(1)
	off1024 := int64(1024)
	id0 := int64(0)
	id1 := int64(1)
	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "f0", Kind: trace.TargetFile, Size: 4096}},
		Summary:       trace.Summary{NumOps: 2, NumStreams: 1},
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	for _, op := range []trace.Op{
		{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeReadWrite},
		{T: 1, OpID: &id1, S: 0, Op: trace.OpWrite, H: &h0, Off: &off1024, Len: trace.Ptr(int64(1024))},
	} {
		if err := tw.WriteOp(op); err != nil {
			t.Fatal(err)
		}
	}

	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	plan := replay.Plan{
		Engine:     newCapsEngine(engine.Capabilities{Seekable: true, PartialWrite: false}),
		EngineName: "nopw",
		Mode:       "asap",
	}
	_, err = replay.Prepare(plan, r)
	if err == nil {
		t.Fatal("Prepare should have failed for offset WRITE against PartialWrite=false engine")
	}
	if !strings.Contains(err.Error(), "PartialWrite=false") {
		t.Fatalf("Prepare error = %v, want PartialWrite=false rejection", err)
	}
}

func TestCapsRejectionReadRequiresSeekable(t *testing.T) {
	tgt0 := 0
	h0 := int64(1)
	off0 := int64(0)
	id0 := int64(0)
	id1 := int64(1)
	len1024 := int64(1024)
	ops := []trace.Op{
		{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeRead},
		{T: 1, OpID: &id1, S: 0, Op: trace.OpRead, H: &h0, Off: &off0, Len: &len1024},
	}

	err := prepareOps(t, basicHeader(int64(len(ops))), ops, newCapsEngine(engine.Capabilities{
		Seekable:     false,
		PartialWrite: true,
	}))
	if err == nil {
		t.Fatal("Prepare should reject READ against Seekable=false engine")
	}
	if !strings.Contains(err.Error(), "READ op") || !strings.Contains(err.Error(), "Seekable=false") {
		t.Fatalf("Prepare error = %v, want READ Seekable=false rejection", err)
	}
}

func TestCapsRejectionStatRequiresSeekable(t *testing.T) {
	tgt0 := 0
	id0 := int64(0)
	ops := []trace.Op{
		{T: 0, OpID: &id0, S: 0, Op: trace.OpStat, Tgt: &tgt0},
	}

	err := prepareOps(t, basicHeader(int64(len(ops))), ops, newCapsEngine(engine.Capabilities{
		Seekable: false,
	}))
	if err == nil {
		t.Fatal("Prepare should reject STAT against Seekable=false engine")
	}
	if !strings.Contains(err.Error(), "STAT op") || !strings.Contains(err.Error(), "Seekable=false") {
		t.Fatalf("Prepare error = %v, want STAT Seekable=false rejection", err)
	}
}

// TestCapsRejectionFsync verifies that Prepare rejects FSYNC against an engine
// that reports Durable=false.
func TestCapsRejectionFsync(t *testing.T) {
	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	tgt0 := 0
	h0 := int64(1)
	off0 := int64(0)
	id0 := int64(0)
	id1 := int64(1)
	id2 := int64(2)
	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "f0", Kind: trace.TargetFile, Size: 4096}},
		Summary:       trace.Summary{NumOps: 3, NumStreams: 1},
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	for _, op := range []trace.Op{
		{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeWrite},
		{T: 1, OpID: &id1, S: 0, Op: trace.OpWrite, H: &h0, Off: &off0, Len: trace.Ptr(int64(512))},
		{T: 2, OpID: &id2, S: 0, Op: trace.OpFsync, H: &h0},
	} {
		if err := tw.WriteOp(op); err != nil {
			t.Fatal(err)
		}
	}

	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	// PartialWrite=true so the WRITE passes; Durable=false so FSYNC is rejected.
	plan := replay.Plan{
		Engine:     newCapsEngine(engine.Capabilities{Seekable: true, PartialWrite: true, Durable: false}),
		EngineName: "nodur",
		Mode:       "asap",
	}
	_, err = replay.Prepare(plan, r)
	if err == nil {
		t.Fatal("Prepare should have failed for FSYNC against Durable=false engine")
	}
	if !strings.Contains(err.Error(), "Durable=false") {
		t.Fatalf("Prepare error = %v, want Durable=false rejection", err)
	}
}

// TestStreamOrderPreservation verifies OPEN → READ* → CLOSE ordering within a
// single stream via a recording engine wrapper.
func TestStreamOrderPreservation(t *testing.T) {
	p := trainingread.DefaultParams()
	p.Shards = 1
	p.ShardSize = 64 << 10
	p.RecordSize = 16 << 10
	p.DataloaderWorkers = 1
	p.Epochs = 1
	p.Shuffle = false
	p.Seed = 1

	var traceBuf bytes.Buffer
	if err := trainingread.Generate(p, &traceBuf); err != nil {
		t.Fatal(err)
	}
	r, err := trace.NewReader(&traceBuf)
	if err != nil {
		t.Fatal(err)
	}
	hdr := r.Header()

	rec := &orderRecorder{}
	recEng := &recordingEngine{inner: memEngineForTrace(hdr), rec: rec}

	plan := replay.Plan{Engine: recEng, EngineName: "recording", Mode: "asap"}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := exec.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	calls := rec.snapshot()
	if len(calls) == 0 {
		t.Fatal("no engine calls recorded")
	}
	if calls[0] != "OPEN" {
		t.Errorf("first call=%q, want OPEN", calls[0])
	}
	if calls[len(calls)-1] != "CLOSE" {
		t.Errorf("last call=%q, want CLOSE", calls[len(calls)-1])
	}
	for i := 1; i < len(calls)-1; i++ {
		if calls[i] != "READ" {
			t.Errorf("calls[%d]=%q, want READ", i, calls[i])
		}
	}
}

// TestStrictSequentialityRace runs a multi-stream replay under -race to detect
// data races in the executor.
func TestStrictSequentialityRace(t *testing.T) {
	buf := smallTrace(t, 4, 8)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatal(err)
	}
	eng := memEngineForTrace(r.Header())
	plan := replay.Plan{Engine: eng, EngineName: "mem", Mode: "asap"}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}
}

func TestRunRecordsEngineErrorsWithoutReturningFatalError(t *testing.T) {
	tgt0 := 0
	h0 := int64(1)
	off0 := int64(0)
	len1024 := int64(1024)
	id0 := int64(0)
	id1 := int64(1)
	id2 := int64(2)
	ops := []trace.Op{
		{T: 0, OpID: &id0, S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeRead},
		{T: 1, OpID: &id1, S: 0, Op: trace.OpRead, H: &h0, Off: &off0, Len: &len1024},
		{T: 2, OpID: &id2, S: 0, Op: trace.OpClose, H: &h0},
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
		t.Fatal(err)
	}
	exec, err := replay.Prepare(replay.Plan{
		Engine:     &readFailEngine{},
		EngineName: "read-fail",
		Mode:       "asap",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned fatal error %v; op errors should be counted in results", err)
	}
	if res.Errors != 1 {
		t.Fatalf("Errors=%d, want 1", res.Errors)
	}
}

// TestEndToEnd_LocalFile replays a small read trace against LocalFileEngine over a
// pre-created temp file and verifies that all ops complete without error.
func TestEndToEnd_LocalFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "shard.tar")

	const fileSize = 128 * 1024 // 128 KiB
	if err := os.WriteFile(target, make([]byte, fileSize), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	const readLen = int64(32 * 1024) // 4 reads x 32 KiB = 128 KiB
	tgt0 := 0
	h0 := int64(42)
	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets: []trace.TargetInfo{
			{ID: 0, Name: target, Kind: trace.TargetFile, Size: fileSize},
		},
		Summary: trace.Summary{NumOps: 6, NumStreams: 1, TotalBytes: 4 * readLen},
	}
	ops := []trace.Op{
		{T: 0, OpID: trace.Ptr(int64(0)), S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeRead},
		{T: 1000, OpID: trace.Ptr(int64(1)), S: 0, Op: trace.OpRead, H: &h0, Off: trace.Ptr(int64(0)), Len: trace.Ptr(readLen)},
		{T: 2000, OpID: trace.Ptr(int64(2)), S: 0, Op: trace.OpRead, H: &h0, Off: trace.Ptr(readLen), Len: trace.Ptr(readLen)},
		{T: 3000, OpID: trace.Ptr(int64(3)), S: 0, Op: trace.OpRead, H: &h0, Off: trace.Ptr(2 * readLen), Len: trace.Ptr(readLen)},
		{T: 4000, OpID: trace.Ptr(int64(4)), S: 0, Op: trace.OpRead, H: &h0, Off: trace.Ptr(3 * readLen), Len: trace.Ptr(readLen)},
		{T: 5000, OpID: trace.Ptr(int64(5)), S: 0, Op: trace.OpClose, H: &h0},
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

	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	eng := localfile.New()
	plan := replay.Plan{
		TracePath:   filepath.Join(dir, "test.ioflux"),
		Engine:      eng,
		EngineName:  "local",
		Mode:        "asap",
		MaxInflight: 64,
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}
	if res.OpsCompleted != hdr.Summary.NumOps {
		t.Errorf("OpsCompleted=%d, want %d", res.OpsCompleted, hdr.Summary.NumOps)
	}
	if res.BytesMoved != 4*readLen {
		t.Errorf("BytesMoved=%d, want %d", res.BytesMoved, 4*readLen)
	}
}

// --- test engine helpers ---

// capsEngine is a minimal engine stub that only reports custom Capabilities
// and returns ErrUnsupported for all operations.
type capsEngine struct {
	caps engine.Capabilities
}

func newCapsEngine(caps engine.Capabilities) *capsEngine { return &capsEngine{caps: caps} }

func (e *capsEngine) Caps() engine.Capabilities { return e.caps }
func (e *capsEngine) Open(_ context.Context, _ string, _ engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	return 0, engine.ErrUnsupported
}
func (e *capsEngine) Read(_ context.Context, _ engine.Handle, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *capsEngine) Write(_ context.Context, _ engine.Handle, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *capsEngine) Fsync(_ context.Context, _ engine.Handle) error { return engine.ErrUnsupported }
func (e *capsEngine) Close(_ context.Context, _ engine.Handle) error { return engine.ErrUnsupported }
func (e *capsEngine) Stat(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *capsEngine) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}
func (e *capsEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *capsEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *capsEngine) Delete(_ context.Context, _ string) error { return engine.ErrUnsupported }

// orderRecorder records the sequence of engine op names.
type orderRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *orderRecorder) record(s string) {
	r.mu.Lock()
	r.calls = append(r.calls, s)
	r.mu.Unlock()
}

func (r *orderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// recordingEngine wraps a MemEngine and records OPEN/READ/WRITE/CLOSE calls.
type recordingEngine struct {
	inner *mem.MemEngine
	rec   *orderRecorder
}

func (e *recordingEngine) Caps() engine.Capabilities { return e.inner.Caps() }

func (e *recordingEngine) Open(ctx context.Context, target string, mode engine.Mode, flags engine.OpenFlags) (engine.Handle, error) {
	e.rec.record("OPEN")
	return e.inner.Open(ctx, target, mode, flags)
}
func (e *recordingEngine) Read(ctx context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	e.rec.record("READ")
	return e.inner.Read(ctx, h, off, length, buf)
}
func (e *recordingEngine) Write(ctx context.Context, h engine.Handle, off int64, data []byte) (int, error) {
	e.rec.record("WRITE")
	return e.inner.Write(ctx, h, off, data)
}
func (e *recordingEngine) Fsync(ctx context.Context, h engine.Handle) error {
	e.rec.record("FSYNC")
	return e.inner.Fsync(ctx, h)
}
func (e *recordingEngine) Close(ctx context.Context, h engine.Handle) error {
	e.rec.record("CLOSE")
	return e.inner.Close(ctx, h)
}
func (e *recordingEngine) Stat(ctx context.Context, t string) (engine.ObjectInfo, error) {
	return e.inner.Stat(ctx, t)
}
func (e *recordingEngine) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return engine.ErrUnsupported
}
func (e *recordingEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *recordingEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *recordingEngine) Delete(_ context.Context, _ string) error { return engine.ErrUnsupported }

type readFailEngine struct{}

var errInjectedRead = errors.New("injected read failure")

func (e *readFailEngine) Caps() engine.Capabilities {
	return engine.Capabilities{
		Seekable:     true,
		PartialWrite: true,
	}
}

func (e *readFailEngine) Open(_ context.Context, _ string, _ engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	return 1, nil
}
func (e *readFailEngine) Read(_ context.Context, _ engine.Handle, _, _ int64, _ []byte) (int, error) {
	return 0, errInjectedRead
}
func (e *readFailEngine) Write(_ context.Context, _ engine.Handle, _ int64, data []byte) (int, error) {
	return len(data), nil
}
func (e *readFailEngine) Fsync(_ context.Context, _ engine.Handle) error { return nil }
func (e *readFailEngine) Close(_ context.Context, _ engine.Handle) error { return nil }
func (e *readFailEngine) Stat(_ context.Context, target string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{Name: target}, nil
}
func (e *readFailEngine) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}
func (e *readFailEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *readFailEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *readFailEngine) Delete(_ context.Context, _ string) error { return engine.ErrUnsupported }

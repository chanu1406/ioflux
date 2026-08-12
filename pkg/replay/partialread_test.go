package replay_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/payload"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// This file covers replay of a source operation that transferred fewer bytes
// than it requested — the one dimension the qual-01 live-versus-replay
// reconciliation measured as failing.
//
// The shape is taken from that fixture: a shard whose size is not a multiple of
// the reader's block size, so the final read of every shard asks for 256 KiB and
// receives 111,392 bytes.

const (
	partialReqLen  = 262144
	partialRetLen  = 111392
	partialFileLen = 8388608 + partialRetLen // one full block, then the short tail
)

// partialReadTrace builds a two-read trace against target name: a full-length
// read at offset 0, then a read at partialOff that asks for partialReqLen and
// expects partialRetLen back.
func partialReadTrace(t *testing.T, name string) *bytes.Buffer {
	t.Helper()
	ops := []trace.Op{
		{T: 0, OpID: trace.Ptr[int64](0), S: 0, Op: trace.OpOpen,
			Tgt: trace.Ptr(0), H: trace.Ptr[int64](1), Mode: trace.ModeRead},
		{T: 1, OpID: trace.Ptr[int64](1), S: 0, Op: trace.OpRead, H: trace.Ptr[int64](1),
			Off: trace.Ptr[int64](0), Len: trace.Ptr[int64](partialReqLen)},
		{T: 2, OpID: trace.Ptr[int64](2), S: 0, Op: trace.OpRead, H: trace.Ptr[int64](1),
			Off: trace.Ptr[int64](8388608), Len: trace.Ptr[int64](partialReqLen),
			Ret: trace.Ptr[int64](partialRetLen)},
		{T: 3, OpID: trace.Ptr[int64](3), S: 0, Op: trace.OpClose, H: trace.Ptr[int64](1)},
	}
	hdr := trace.Header{
		Version:       trace.VersionPartialTransfer,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: name, Kind: trace.TargetFile}},
		Summary: trace.Summary{
			NumOps: int64(len(ops)), NumStreams: 1,
			TotalBytes:      partialReqLen + partialRetLen,
			NumPartialReads: 1,
		},
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
	return &buf
}

// requestSpy wraps an engine and records the length of every Read request, so a
// test can assert what the backend was actually asked for rather than trusting
// the run's self-report.
type requestSpy struct {
	engine.Engine
	mu       sync.Mutex
	requests []int64
}

func (s *requestSpy) Read(ctx context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	s.mu.Lock()
	s.requests = append(s.requests, length)
	s.mu.Unlock()
	return s.Engine.Read(ctx, h, off, length, buf)
}

func (s *requestSpy) seen() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.requests...)
}

// writeFile creates a file of exactly size bytes under dir.
func writeFile(t *testing.T, dir, name string, size int64) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func runPartialTrace(t *testing.T, root, name string) (*requestSpy, error) {
	t.Helper()
	inner := localfile.New(localfile.WithRoot(root))
	t.Cleanup(func() { _ = inner.Shutdown() })
	spy := &requestSpy{Engine: inner}

	r, err := trace.NewReader(partialReadTrace(t, name))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	exec, err := replay.Prepare(replay.Plan{
		Engine: spy, EngineName: "local", Mode: "asap", PrepareMode: "assume-existing",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Errors > 0 {
		return spy, &runErrors{res.Errors, res.ShortReads}
	}
	if res.ShortReads != 0 {
		t.Errorf("ShortReads = %d with no errors: the two must move together", res.ShortReads)
	}
	if res.Plan.TracePartialReads != 1 {
		t.Errorf("TracePartialReads = %d, want 1", res.Plan.TracePartialReads)
	}
	if want := int64(partialReqLen + partialRetLen); res.BytesMoved != want {
		t.Errorf("BytesMoved = %d, want %d", res.BytesMoved, want)
	}
	return spy, nil
}

type runErrors struct{ errs, short int64 }

func (e *runErrors) Error() string { return "run reported operation failures" }

// TestPartialRead_IssuesSourceRequestSize is the regression test for the
// qualification failure. Before ret existed, the trace held only the returned
// count, so replay issued a 111,392-byte request — a request the application
// never made — and reported a green, short-read-free run. The backend was
// measured on the wrong request and nothing in the evidence said so.
func TestPartialRead_IssuesSourceRequestSize(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "shard.bin", partialFileLen)

	spy, runErr := runPartialTrace(t, dir, filepath.Join(dir, "shard.bin"))
	if runErr != nil {
		t.Fatalf("run must be valid: the replay reproduced the source exactly: %v", runErr)
	}

	got := spy.seen()
	if len(got) != 2 {
		t.Fatalf("Read calls = %d, want 2", len(got))
	}
	for i, n := range got {
		if n != partialReqLen {
			t.Errorf("Read[%d] requested %d bytes, want %d: replay must issue the size the "+
				"source asked for, not the size it received", i, n, partialReqLen)
		}
	}
}

// TestPartialRead_ReturnedMismatchInvalidatesRun is the other half of the same
// property. Requesting the source's request size is only useful if the returned
// count is then checked against the source's: that comparison is what turns a
// disagreement between source and backend into a detectable failure. Here the
// replay target is longer than the source's, so the read comes back full.
func TestPartialRead_ReturnedMismatchInvalidatesRun(t *testing.T) {
	dir := t.TempDir()
	// A target long enough to satisfy the whole request, unlike the source's.
	writeFile(t, dir, "shard.bin", 8388608+partialReqLen)

	inner := localfile.New(localfile.WithRoot(dir))
	t.Cleanup(func() { _ = inner.Shutdown() })

	r, err := trace.NewReader(partialReadTrace(t, filepath.Join(dir, "shard.bin")))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	exec, err := replay.Prepare(replay.Plan{
		Engine: inner, EngineName: "local", Mode: "asap", PrepareMode: "assume-existing",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Errors != 1 {
		t.Errorf("Errors = %d, want 1: the backend returned %d where the source got %d",
			res.Errors, partialReqLen, partialRetLen)
	}
	if res.ShortReads != 1 {
		t.Errorf("ShortReads = %d, want 1", res.ShortReads)
	}
}

// TestPartialRead_ExtentUsesTransferredBytes pins that dataset preparation sizes
// a target from what the source transferred, not what it requested. Sizing from
// the request would materialize a target 150 KiB longer than the source's, the
// read would come back full, and the partial transfer the trace recorded would
// silently disappear — turning the fix into a different way of getting it wrong.
func TestPartialRead_ExtentUsesTransferredBytes(t *testing.T) {
	ops := []trace.Op{
		{T: 0, OpID: trace.Ptr[int64](0), S: 0, Op: trace.OpOpen,
			Tgt: trace.Ptr(0), H: trace.Ptr[int64](1), Mode: trace.ModeRead},
		{T: 1, OpID: trace.Ptr[int64](1), S: 0, Op: trace.OpRead, H: trace.Ptr[int64](1),
			Off: trace.Ptr[int64](8388608), Len: trace.Ptr[int64](partialReqLen),
			Ret: trace.Ptr[int64](partialRetLen)},
	}
	meta := prepare.MetadataFromOps(nil, ops, payload.Config{})
	if got, want := meta.Extents[0], int64(partialFileLen); got != want {
		t.Errorf("extent = %d, want %d (off + transferred, not off + requested)", got, want)
	}
}

// TestPartialRead_MaterializeSyntheticSizesTarget is the end-to-end form of the
// same property: prepare creates the target, and the replay of the short read
// must then succeed against it.
func TestPartialRead_MaterializeSyntheticSizesTarget(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "shard.bin")

	inner := localfile.New(localfile.WithRoot(dir))
	t.Cleanup(func() { _ = inner.Shutdown() })

	r, err := trace.NewReader(partialReadTrace(t, name))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	exec, err := replay.Prepare(replay.Plan{
		Engine: inner, EngineName: "local", Mode: "asap", PrepareMode: "materialize-synthetic",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Errors != 0 {
		t.Fatalf("Errors = %d, want 0: materialization must size the target so the "+
			"source's short read reproduces", res.Errors)
	}
	fi, err := os.Stat(name)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != partialFileLen {
		t.Errorf("materialized size = %d, want %d", fi.Size(), partialFileLen)
	}
}

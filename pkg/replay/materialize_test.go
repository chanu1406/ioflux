package replay_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// TestMaterialize_HonorsContextCancellation proves the PREPARE-cancellation fix:
// dataset preparation runs under the caller's context, so a cancelled PREPARE
// phase (or a dropped coordinator connection) stops materialization instead of
// letting a large copy run to completion.
func TestMaterialize_HonorsContextCancellation(t *testing.T) {
	buf := smallTrace(t, 1, 4)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	eng := memEngineForTrace(r.Header())

	exec, err := replay.Prepare(replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		PrepareMode: "materialize-synthetic",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := exec.Materialize(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Materialize with cancelled ctx err=%v, want context.Canceled", err)
	}
}

// TestMaterialize_Idempotent confirms Materialize runs prep once: a second call
// is a no-op returning the same stats, so the Session (PREPARE) and a later Run
// cannot double-materialize.
func TestMaterialize_Idempotent(t *testing.T) {
	buf := smallTrace(t, 1, 4)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	eng := memEngineForTrace(r.Header())

	exec, err := replay.Prepare(replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		PrepareMode: "materialize-synthetic",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	first, err := exec.Materialize(context.Background())
	if err != nil {
		t.Fatalf("Materialize (1): %v", err)
	}
	if first.Created != 4 {
		t.Fatalf("first Created=%d, want 4 (one per shard)", first.Created)
	}
	second, err := exec.Materialize(context.Background())
	if err != nil {
		t.Fatalf("Materialize (2): %v", err)
	}
	if second.Created != first.Created {
		t.Errorf("second Created=%d, want %d (idempotent, no re-create)", second.Created, first.Created)
	}
}

func TestPrepareAssignedDerivesMaterializeSizeFromUnassignedStream(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.dat")
	tgt0 := 0
	h0, h1 := int64(10), int64(20)
	readOff := int64(8 << 20)
	readLen := int64(4 << 20)
	ops := []trace.Op{
		{T: 0, OpID: trace.Ptr(int64(0)), S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h0, Mode: trace.ModeRead},
		{T: 1, OpID: trace.Ptr(int64(1)), S: 0, Op: trace.OpClose, H: &h0},
		{T: 2, OpID: trace.Ptr(int64(2)), S: 1, Op: trace.OpOpen, Tgt: &tgt0, H: &h1, Mode: trace.ModeRead},
		{T: 3, OpID: trace.Ptr(int64(3)), S: 1, Op: trace.OpRead, H: &h1, Off: &readOff, Len: &readLen},
		{T: 4, OpID: trace.Ptr(int64(4)), S: 1, Op: trace.OpClose, H: &h1},
	}
	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: target, Kind: trace.TargetFile, Size: 0}},
		Summary:       trace.Summary{NumOps: int64(len(ops)), NumStreams: 2, TotalBytes: readLen, DurationNS: 4},
	}); err != nil {
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
	exec, err := replay.PrepareAssigned(replay.Plan{
		Engine:      localfile.New(),
		EngineName:  "local",
		Mode:        "asap",
		PrepareMode: "materialize-synthetic",
	}, r, []int64{0})
	if err != nil {
		t.Fatalf("PrepareAssigned: %v", err)
	}
	if ids := exec.StreamIDs(); len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("StreamIDs=%v, want only assigned stream 0", ids)
	}
	if _, err := exec.Materialize(context.Background()); err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	fi, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat materialized target: %v", err)
	}
	if want := readOff + readLen; fi.Size() != want {
		t.Fatalf("materialized size=%d, want %d from unassigned stream extent", fi.Size(), want)
	}
}

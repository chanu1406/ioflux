package importer_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/chanuollala/ioflux/pkg/importer"
	"github.com/chanuollala/ioflux/pkg/trace"
)

func importedMeta() importer.HeaderMeta {
	return importer.HeaderMeta{
		Kind:               trace.TraceImported,
		CaptureMethod:      "import:test",
		CaptureLimitations: "test fixture; not a real capture",
		GeneratedBy:        "ioflux-import-test",
	}
}

// readBack parses a serialized trace into its header and ops.
func readBack(t *testing.T, b []byte) (trace.Header, []trace.Op) {
	t.Helper()
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()
	var ops []trace.Op
	for {
		op, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ops = append(ops, op)
	}
	return hdr, ops
}

// assertValid re-validates a serialized trace and fails on any schema error.
func assertValid(t *testing.T, b []byte) {
	t.Helper()
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	rep, err := trace.Validate(r)
	if err != nil {
		t.Fatalf("Validate I/O error: %v", err)
	}
	if !rep.OK() {
		for _, e := range rep.Errors {
			t.Logf("error: %s", e)
		}
		t.Fatalf("trace failed validation (%d error(s))", len(rep.Errors))
	}
}

func TestBuilder_MergeSortOpIDsAndSummary(t *testing.T) {
	b := importer.NewBuilder()
	t0 := b.Target("/data/a", trace.TargetFile)
	t1 := b.Target("/data/b", trace.TargetFile)

	// Stream 0: OPEN, READ, READ, CLOSE on target a (handle 0).
	b.Add(trace.Op{T: 0, S: 0, Op: trace.OpOpen, Tgt: trace.Ptr(t0), H: trace.Ptr[int64](0), Mode: trace.ModeRead})
	b.Add(trace.Op{T: 100, S: 0, Op: trace.OpRead, H: trace.Ptr[int64](0), Off: trace.Ptr[int64](0), Len: trace.Ptr[int64](4096)})
	b.Add(trace.Op{T: 200, S: 0, Op: trace.OpRead, H: trace.Ptr[int64](0), Off: trace.Ptr[int64](4096), Len: trace.Ptr[int64](4096)})
	b.Add(trace.Op{T: 300, S: 0, Op: trace.OpClose, H: trace.Ptr[int64](0)})
	// Stream 1: OPEN, READ, CLOSE on target b (handle 1), interleaved by t.
	b.Add(trace.Op{T: 50, S: 1, Op: trace.OpOpen, Tgt: trace.Ptr(t1), H: trace.Ptr[int64](1), Mode: trace.ModeRead})
	b.Add(trace.Op{T: 150, S: 1, Op: trace.OpRead, H: trace.Ptr[int64](1), Off: trace.Ptr[int64](0), Len: trace.Ptr[int64](1024)})
	b.Add(trace.Op{T: 250, S: 1, Op: trace.OpClose, H: trace.Ptr[int64](1)})

	var buf bytes.Buffer
	rep, err := b.WriteTo(&buf, importedMeta())
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	assertValid(t, buf.Bytes())

	hdr, ops := readBack(t, buf.Bytes())
	if len(ops) != 7 {
		t.Fatalf("ops = %d, want 7", len(ops))
	}
	// Global timestamps non-decreasing and op_ids 0..N-1 in order.
	var prevT int64 = -1
	for i, op := range ops {
		if op.OpID == nil || *op.OpID != int64(i) {
			t.Errorf("op[%d].op_id = %v, want %d", i, op.OpID, i)
		}
		if op.T < prevT {
			t.Errorf("op[%d].t = %d < prev %d (not non-decreasing)", i, op.T, prevT)
		}
		prevT = op.T
	}
	if hdr.Kind != trace.TraceImported {
		t.Errorf("kind = %q, want imported", hdr.Kind)
	}
	if hdr.Summary.NumOps != 7 || hdr.Summary.NumStreams != 2 {
		t.Errorf("summary num_ops=%d num_streams=%d, want 7/2", hdr.Summary.NumOps, hdr.Summary.NumStreams)
	}
	if want := int64(4096 + 4096 + 1024); hdr.Summary.TotalBytes != want {
		t.Errorf("summary total_bytes=%d, want %d", hdr.Summary.TotalBytes, want)
	}
	if hdr.Summary.DurationNS != 300 {
		t.Errorf("summary duration_ns=%d, want 300", hdr.Summary.DurationNS)
	}
	if rep.NumOps != 7 || rep.NumStreams != 2 || rep.NumTargets != 2 {
		t.Errorf("report = %+v, want 7 ops / 2 streams / 2 targets", rep)
	}
}

func TestBuilder_ClampsNonMonotonicTimestamps(t *testing.T) {
	b := importer.NewBuilder()
	tg := b.Target("/data/a", trace.TargetFile)
	b.Add(trace.Op{T: 0, S: 0, Op: trace.OpOpen, Tgt: trace.Ptr(tg), H: trace.Ptr[int64](0), Mode: trace.ModeRead})
	b.Add(trace.Op{T: 500, S: 0, Op: trace.OpRead, H: trace.Ptr[int64](0), Off: trace.Ptr[int64](0), Len: trace.Ptr[int64](16)})
	// Clock steps backwards: 300 < 500 -> clamped to 500.
	b.Add(trace.Op{T: 300, S: 0, Op: trace.OpRead, H: trace.Ptr[int64](0), Off: trace.Ptr[int64](16), Len: trace.Ptr[int64](16)})
	b.Add(trace.Op{T: 600, S: 0, Op: trace.OpClose, H: trace.Ptr[int64](0)})

	var buf bytes.Buffer
	rep, err := b.WriteTo(&buf, importedMeta())
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.TimestampClamped != 1 {
		t.Errorf("TimestampClamped = %d, want 1", rep.TimestampClamped)
	}
	_, ops := readBack(t, buf.Bytes())
	var prevT int64 = -1
	for i, op := range ops {
		if op.T < prevT {
			t.Errorf("op[%d].t = %d < prev %d", i, op.T, prevT)
		}
		prevT = op.T
	}
}

func TestBuilder_TargetInterning(t *testing.T) {
	b := importer.NewBuilder()
	if id := b.Target("/x", trace.TargetFile); id != 0 {
		t.Errorf("first target id = %d, want 0", id)
	}
	if id := b.Target("/y", trace.TargetFile); id != 1 {
		t.Errorf("second target id = %d, want 1", id)
	}
	if id := b.Target("/x", trace.TargetFile); id != 0 {
		t.Errorf("repeated target id = %d, want 0", id)
	}
}

func TestBuilder_InvalidTraceErrorsAndWritesNothing(t *testing.T) {
	b := importer.NewBuilder()
	// READ referencing a handle that was never opened -> invalid trace.
	b.Add(trace.Op{T: 0, S: 0, Op: trace.OpRead, H: trace.Ptr[int64](99), Off: trace.Ptr[int64](0), Len: trace.Ptr[int64](16)})

	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf, importedMeta()); err == nil {
		t.Fatal("WriteTo: want error for invalid trace, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("buffer wrote %d bytes; want 0 (output must be untouched on invalid trace)", buf.Len())
	}
}

func TestBuilder_RejectsImportedHeaderWithoutImportMethod(t *testing.T) {
	b := importer.NewBuilder()
	tg := b.Target("/data/a", trace.TargetFile)
	b.Add(trace.Op{T: 0, S: 0, Op: trace.OpStat, Tgt: trace.Ptr(tg)})

	meta := importedMeta()
	meta.CaptureMethod = "synthetic" // not an import:* method for an imported trace
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf, meta); err == nil {
		t.Fatal("WriteTo: want error for imported trace without import:* capture method")
	}
}

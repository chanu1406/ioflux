package transform_test

import (
	"testing"

	"github.com/chanuollala/ioflux/pkg/trace"
	"github.com/chanuollala/ioflux/pkg/transform"
)

func readOp(stream, off, length int64) trace.Op {
	return trace.Op{
		T: 0, S: stream, Op: trace.OpRead,
		H: trace.Ptr[int64](1), Off: trace.Ptr(off), Len: trace.Ptr(length),
	}
}

// coverage returns the byte ranges ops cover, in order, as (off, len) pairs.
func coverage(ops []trace.Op) [][2]int64 {
	var out [][2]int64
	for _, op := range ops {
		if op.Op != trace.OpRead && op.Op != trace.OpGet {
			continue
		}
		moved, _ := op.TransferredBytes()
		out = append(out, [2]int64{*op.Off, moved})
	}
	return out
}

func totalMoved(ops []trace.Op) int64 {
	var n int64
	for _, op := range ops {
		if op.Op != trace.OpRead && op.Op != trace.OpGet {
			continue
		}
		moved, _ := op.TransferredBytes()
		n += moved
	}
	return n
}

// The qual-01 §10 treatment: a 256 KiB read becomes four 64 KiB reads over the
// same extent.
func TestSplitReadsDividesEvenly(t *testing.T) {
	ops := []trace.Op{readOp(0, 0, 262144)}

	got, err := transform.SplitReads(ops, 65536)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 4 {
		t.Fatalf("produced %d ops, want 4", len(got))
	}
	want := [][2]int64{{0, 65536}, {65536, 65536}, {131072, 65536}, {196608, 65536}}
	for i, c := range coverage(got) {
		if c != want[i] {
			t.Errorf("piece %d covers %v, want %v", i, c, want[i])
		}
	}
}

// The extent covered and the bytes moved must be identical to the source's —
// the property FIXTURE.md §10 requires.
func TestSplitReadsPreservesExtentAndBytes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		length int64
		block  int64
	}{
		{"exact multiple", 262144, 65536},
		{"with remainder", 100000, 65536},
		{"remainder smaller than block", 70000, 65536},
		{"single byte over", 65537, 65536},
		{"block of one", 5, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := []trace.Op{readOp(0, 4096, tc.length)}

			got, err := transform.SplitReads(src, tc.block)
			if err != nil {
				t.Fatal(err)
			}

			if n := totalMoved(got); n != tc.length {
				t.Errorf("total moved = %d, want %d", n, tc.length)
			}
			// Contiguous, starting where the source did, ending where it did.
			next := int64(4096)
			for i, c := range coverage(got) {
				if c[0] != next {
					t.Errorf("piece %d starts at %d, want %d (gap or overlap)", i, c[0], next)
				}
				if c[1] > tc.block {
					t.Errorf("piece %d requests %d, exceeding the %d block", i, c[1], tc.block)
				}
				next += c[1]
			}
			if next != 4096+tc.length {
				t.Errorf("coverage ends at %d, want %d", next, 4096+tc.length)
			}
		})
	}
}

// A read already at or below the block size is left exactly as it was, so a
// trace needing no splitting is not rewritten.
func TestSplitReadsLeavesSmallReadsUntouched(t *testing.T) {
	src := []trace.Op{readOp(0, 0, 4096), readOp(0, 4096, 65536)}

	got, err := transform.SplitReads(src, 65536)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("produced %d ops, want 2", len(got))
	}
	for i := range got {
		if *got[i].Len != *src[i].Len || *got[i].Off != *src[i].Off {
			t.Errorf("op %d was rewritten: %+v", i, got[i])
		}
	}
}

// Everything that is not a read passes through, keeping the surrounding
// handle lifecycle intact.
func TestSplitReadsPassesThroughOtherOps(t *testing.T) {
	src := []trace.Op{
		{T: 0, S: 0, Op: trace.OpOpen, Tgt: trace.Ptr(0), H: trace.Ptr[int64](1), Mode: trace.ModeRead},
		readOp(0, 0, 131072),
		{T: 2, S: 0, Op: trace.OpClose, H: trace.Ptr[int64](1)},
	}

	got, err := transform.SplitReads(src, 65536)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 4 {
		t.Fatalf("produced %d ops, want 4 (open + 2 reads + close)", len(got))
	}
	if got[0].Op != trace.OpOpen || got[len(got)-1].Op != trace.OpClose {
		t.Errorf("handle lifecycle not preserved: %v … %v", got[0].Op, got[len(got)-1].Op)
	}
}

// A source short read is split over the bytes it actually moved, and each piece
// asks for exactly what it will get — asking for bytes past EOF would make the
// transformation demand a failure rather than describe a workload.
func TestSplitReadsCoversTransferredExtentOfShortRead(t *testing.T) {
	op := readOp(0, 8388608, 262144)
	op.Ret = trace.Ptr[int64](111392)

	got, err := transform.SplitReads([]trace.Op{op}, 65536)
	if err != nil {
		t.Fatal(err)
	}

	if n := totalMoved(got); n != 111392 {
		t.Errorf("total moved = %d, want the source's 111392", n)
	}
	for i, piece := range got {
		if piece.Ret != nil {
			t.Errorf("piece %d is a partial transfer; every piece should request what it receives", i)
		}
	}
	// 111392 = 65536 + 45856
	want := [][2]int64{{8388608, 65536}, {8454144, 45856}}
	for i, c := range coverage(got) {
		if c != want[i] {
			t.Errorf("piece %d covers %v, want %v", i, c, want[i])
		}
	}
}

// Splitting renumbers operation ids: the originals identified positions in the
// source trace, and leaving them would duplicate an id across every piece.
func TestSplitReadsRenumbersOpIDs(t *testing.T) {
	src := []trace.Op{readOp(0, 0, 131072), readOp(1, 0, 131072)}
	for i := range src {
		src[i].OpID = trace.Ptr(int64(i))
	}

	got, err := transform.SplitReads(src, 65536)
	if err != nil {
		t.Fatal(err)
	}

	for i, op := range got {
		if op.OpID == nil || *op.OpID != int64(i) {
			t.Errorf("op %d has id %v, want %d", i, op.OpID, i)
		}
	}
}

func TestSplitReadsRejectsBadBlock(t *testing.T) {
	for _, block := range []int64{0, -1} {
		if _, err := transform.SplitReads([]trace.Op{readOp(0, 0, 100)}, block); err == nil {
			t.Errorf("block %d accepted", block)
		}
	}
}

// Splitting must not silently drop a read it cannot place.
func TestSplitReadsRejectsReadWithoutOffset(t *testing.T) {
	op := trace.Op{T: 0, S: 0, Op: trace.OpRead, H: trace.Ptr[int64](1), Len: trace.Ptr[int64](131072)}

	if _, err := transform.SplitReads([]trace.Op{op}, 65536); err == nil {
		t.Error("a read with no offset was split anyway")
	}
}

// The header records what was done, to which trace, and recomputes the counts
// the split changed.
func TestSplitReadsHeaderRecordsLedger(t *testing.T) {
	hdr := trace.Header{
		Version: trace.TraceFormatVersion,
		Kind:    trace.TraceImported,
		Summary: trace.Summary{NumOps: 1, NumStreams: 1, TotalBytes: 262144},
	}
	ops, err := transform.SplitReads([]trace.Op{readOp(0, 0, 262144)}, 65536)
	if err != nil {
		t.Fatal(err)
	}

	got := transform.SplitReadsHeader(hdr, ops, 65536, "sha256:abc")

	if len(got.Transformations) != 1 {
		t.Fatalf("transformations = %d, want 1", len(got.Transformations))
	}
	tr := got.Transformations[0]
	if tr.Kind != trace.TransformSplitReads {
		t.Errorf("kind = %q, want %q", tr.Kind, trace.TransformSplitReads)
	}
	if tr.SourceDigest != "sha256:abc" {
		t.Errorf("source digest = %q, want it recorded", tr.SourceDigest)
	}
	if tr.Params["block"] != "65536" {
		t.Errorf("params = %v, want block=65536", tr.Params)
	}
	if got.Summary.NumOps != 4 {
		t.Errorf("summary num_ops = %d, want 4", got.Summary.NumOps)
	}
	if got.Summary.TotalBytes != 262144 {
		t.Errorf("summary total_bytes = %d, want it unchanged at 262144", got.Summary.TotalBytes)
	}
	// A header carrying a ledger must declare the version whose readers know to
	// look for one.
	if got.Version != trace.VersionTransformed {
		t.Errorf("version = %d, want %d", got.Version, trace.VersionTransformed)
	}
}

// A second transformation appends rather than replaces, so the ledger records
// the whole chain.
func TestSplitReadsHeaderAppendsToLedger(t *testing.T) {
	hdr := trace.Header{
		Version:         trace.VersionTransformed,
		Transformations: []trace.Transformation{{Kind: "earlier", SourceDigest: "sha256:aaa"}},
	}
	ops, _ := transform.SplitReads([]trace.Op{readOp(0, 0, 131072)}, 65536)

	got := transform.SplitReadsHeader(hdr, ops, 65536, "sha256:bbb")

	if len(got.Transformations) != 2 {
		t.Fatalf("transformations = %d, want 2", len(got.Transformations))
	}
	if got.Transformations[0].Kind != "earlier" {
		t.Errorf("earlier transformation lost: %+v", got.Transformations)
	}
}

func TestHeaderDerivedFrom(t *testing.T) {
	hdr := trace.Header{Transformations: []trace.Transformation{
		{Kind: trace.TransformSplitReads, SourceDigest: "sha256:aaa"},
	}}

	if !hdr.DerivedFrom("sha256:aaa") {
		t.Error("DerivedFrom failed to match the recorded source digest")
	}
	if hdr.DerivedFrom("sha256:zzz") {
		t.Error("DerivedFrom matched an unrelated digest")
	}
	// An empty digest must never match, or a result with no identity would look
	// derived from every trace.
	if hdr.DerivedFrom("") {
		t.Error("DerivedFrom matched an empty digest")
	}
	if !hdr.IsTransformed() {
		t.Error("IsTransformed = false for a trace carrying a ledger")
	}
}

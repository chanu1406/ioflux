package trainingread_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/chanuollala/ioflux/pkg/gen/trainingread"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// smallParams returns a fast, deterministic Params for testing.
// CreatedUTC is left empty so output is byte-identical across runs.
func smallParams() trainingread.Params {
	p := trainingread.DefaultParams()
	p.Shards = 8
	p.ShardSize = 256 << 10 // 256 KiB
	p.RecordSize = 32 << 10 // 32 KiB
	p.DataloaderWorkers = 2
	p.Epochs = 1
	p.Shuffle = false
	p.Seed = 1
	return p
}

func mustGenerate(t *testing.T, p trainingread.Params) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if err := trainingread.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return &buf
}

// TestDeterminism verifies that identical params produce byte-identical output.
func TestDeterminism(t *testing.T) {
	p := smallParams()
	buf1 := mustGenerate(t, p)
	buf2 := mustGenerate(t, p)
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("Generate is not deterministic with the same params and seed")
	}
}

// TestDifferentSeedsDiffer verifies that different seeds produce different output.
func TestDifferentSeedsDiffer(t *testing.T) {
	p := smallParams()
	p.Shuffle = true
	buf1 := mustGenerate(t, p)
	p.Seed = 2
	buf2 := mustGenerate(t, p)
	if bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("different seeds produced identical output")
	}
}

// TestValidatePassesOnGenerated verifies that every generated trace passes
// ioflux validate — the integration test for the full trace contract.
func TestValidatePassesOnGenerated(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*trainingread.Params)
	}{
		{"defaults-small", func(p *trainingread.Params) {}},
		{"shuffle", func(p *trainingread.Params) { p.Shuffle = true }},
		{"random-within-shard", func(p *trainingread.Params) { p.ReadWithinShard = "random" }},
		{"multi-epoch", func(p *trainingread.Params) { p.Epochs = 3 }},
		{"single-worker", func(p *trainingread.Params) { p.DataloaderWorkers = 1 }},
		{"more-workers-than-shards", func(p *trainingread.Params) { p.DataloaderWorkers = 16 }},
		{"single-shard", func(p *trainingread.Params) { p.Shards = 1; p.DataloaderWorkers = 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := smallParams()
			tc.mutate(&p)
			buf := mustGenerate(t, p)

			r, err := trace.NewReader(buf)
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
				t.Fatalf("generated trace failed validation (%d error(s))", len(rep.Errors))
			}
		})
	}
}

// TestOpCounts checks that num_streams matches DataloaderWorkers (advisory
// summary) and that the actual op count matches summary.num_ops.
func TestOpCounts(t *testing.T) {
	p := smallParams()
	// 8 shards, 2 workers → each worker gets 4 shards, both have ops.
	buf := mustGenerate(t, p)

	r, _ := trace.NewReader(buf)
	hdr := r.Header()
	rep, err := trace.Validate(r)
	if err != nil {
		t.Fatal(err)
	}

	if hdr.Summary.NumStreams != p.DataloaderWorkers {
		t.Errorf("summary.num_streams %d != DataloaderWorkers %d",
			hdr.Summary.NumStreams, p.DataloaderWorkers)
	}
	if rep.NumOpsRead != hdr.Summary.NumOps {
		t.Errorf("ops read %d != summary.num_ops %d", rep.NumOpsRead, hdr.Summary.NumOps)
	}
	// With 8 shards and 2 workers, both streams must appear.
	if got, want := len(rep.Streams), p.DataloaderWorkers; got != want {
		t.Errorf("active streams %d != %d", got, want)
	}
}

// TestTotalBytesExact checks that summary.total_bytes equals shards×shardSize×epochs
// exactly. buildReads always consumes exactly ShardSize bytes per shard.
func TestTotalBytesExact(t *testing.T) {
	cases := []struct {
		name   string
		shards int
		epochs int
	}{
		{"1epoch", 8, 1},
		{"3epochs", 4, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := smallParams()
			p.Shards = tc.shards
			p.Epochs = tc.epochs
			buf := mustGenerate(t, p)

			r, _ := trace.NewReader(buf)
			hdr := r.Header()

			want := int64(p.Shards) * p.ShardSize * int64(p.Epochs)
			if got := hdr.Summary.TotalBytes; got != want {
				t.Errorf("total_bytes %d != expected %d", got, want)
			}
		})
	}
}

// TestDistributionMeanWithinTolerance checks that the mean record size across
// all READ ops falls within ±10% of the configured RecordSize.
// Uses params where ShardSize/RecordSize is large enough that clamping bias
// is negligible (< 1.6%).
func TestDistributionMeanWithinTolerance(t *testing.T) {
	p := trainingread.DefaultParams()
	p.Shards = 32
	p.ShardSize = 1 << 20   // 1 MiB
	p.RecordSize = 16 << 10 // 16 KiB — ~64 reads/shard; clamping bias ≈1.6%
	p.DataloaderWorkers = 4
	p.Shuffle = false
	p.Epochs = 1
	p.Seed = 7

	buf := mustGenerate(t, p)
	r, _ := trace.NewReader(buf)

	var totalBytes, numReads int64
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if op.Op == trace.OpRead {
			totalBytes += *op.Len
			numReads++
		}
	}
	if numReads == 0 {
		t.Fatal("no READ ops in trace")
	}
	mean := totalBytes / numReads
	lo := p.RecordSize * 9 / 10
	hi := p.RecordSize * 11 / 10
	if mean < lo || mean > hi {
		t.Errorf("mean record size %d not within ±10%% of target %d (lo=%d hi=%d)",
			mean, p.RecordSize, lo, hi)
	}
}

// TestNoShuffleShardOrder checks that without shuffle, shards are opened in
// index order 0, 1, 2, …
func TestNoShuffleShardOrder(t *testing.T) {
	p := smallParams()
	p.DataloaderWorkers = 1 // single worker → unambiguous global open order
	p.Shuffle = false

	buf := mustGenerate(t, p)
	r, _ := trace.NewReader(buf)

	var openOrder []int
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if op.Op == trace.OpOpen {
			openOrder = append(openOrder, *op.Tgt)
		}
	}

	want := make([]int, p.Shards)
	for i := range want {
		want[i] = i
	}
	if !slices.Equal(openOrder, want) {
		t.Errorf("open order without shuffle:\n got  %v\n want %v", openOrder, want)
	}
}

// TestShuffleChangesOrder verifies that shuffle=true changes the shard access
// order relative to shuffle=false.
func TestShuffleChangesOrder(t *testing.T) {
	base := smallParams()
	base.DataloaderWorkers = 1
	base.Shards = 16

	noShuffle := base
	noShuffle.Shuffle = false

	doShuffle := base
	doShuffle.Shuffle = true

	collectOpenOrder := func(p trainingread.Params) []int {
		buf := mustGenerate(t, p)
		r, _ := trace.NewReader(buf)
		var order []int
		for {
			op, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if op.Op == trace.OpOpen {
				order = append(order, *op.Tgt)
			}
		}
		return order
	}

	unsorted := collectOpenOrder(noShuffle)
	shuffled := collectOpenOrder(doShuffle)

	if slices.Equal(unsorted, shuffled) {
		t.Error("shuffle=true produced the same shard order as shuffle=false")
	}
}

// TestGlobalTimestampsNonDecreasing checks that t values are non-decreasing
// in file order — the key invariant that lets concurrent streams be written
// to a single JSONL file without violating the trace contract.
func TestGlobalTimestampsNonDecreasing(t *testing.T) {
	p := smallParams()
	p.DataloaderWorkers = 4 // multiple streams to exercise merge-sort
	p.Shards = 16

	buf := mustGenerate(t, p)
	r, _ := trace.NewReader(buf)

	var prev int64 = -1
	lineNo := 2 // ops start at line 2
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if op.T < prev {
			t.Errorf("line %d: t=%d < previous t=%d (non-monotonic)", lineNo, op.T, prev)
		}
		prev = op.T
		lineNo++
	}
}

// TestHandleLifecycle verifies via validate that all handles in a multi-epoch,
// multi-worker trace are opened exactly once and closed exactly once.
// This exercises the handle-bookkeeping across epoch boundaries.
func TestHandleLifecycle(t *testing.T) {
	p := smallParams()
	p.Epochs = 2
	p.DataloaderWorkers = 3

	buf := mustGenerate(t, p)
	r, _ := trace.NewReader(buf)
	rep, err := trace.Validate(r)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		for _, e := range rep.Errors {
			t.Logf("error: %s", e)
		}
		t.Fatalf("handle lifecycle errors in multi-epoch trace")
	}
	// Validate also emits warnings for unclosed handles; ensure none.
	if len(rep.Warnings) > 0 {
		for _, w := range rep.Warnings {
			t.Logf("warning: %s", w)
		}
		t.Fatalf("unexpected warnings (likely unclosed handles)")
	}
}

// TestOpIDsMonotonicPerStream checks that op_ids are strictly increasing
// within each stream, which the global sequential assignment must guarantee.
func TestOpIDsMonotonicPerStream(t *testing.T) {
	p := smallParams()
	p.DataloaderWorkers = 3
	p.Shards = 9

	buf := mustGenerate(t, p)
	r, _ := trace.NewReader(buf)

	lastID := map[int64]int64{} // stream → last op_id
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if op.OpID == nil {
			t.Fatalf("op missing op_id (stream %d)", op.S)
		}
		if prev, ok := lastID[op.S]; ok && *op.OpID <= prev {
			t.Errorf("stream %d: op_id %d ≤ previous %d (not strictly increasing)",
				op.S, *op.OpID, prev)
		}
		lastID[op.S] = *op.OpID
	}
}

// TestValidateParamsErrors checks that invalid params are rejected before
// any output is produced.
func TestValidateParamsErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*trainingread.Params)
	}{
		{"zero shards", func(p *trainingread.Params) { p.Shards = 0 }},
		{"negative shard-size", func(p *trainingread.Params) { p.ShardSize = -1 }},
		{"zero record-size", func(p *trainingread.Params) { p.RecordSize = 0 }},
		{"record-size exceeds shard-size", func(p *trainingread.Params) { p.RecordSize = p.ShardSize + 1 }},
		{"unsupported dist", func(p *trainingread.Params) { p.RecordSizeDist = "uniform" }},
		{"zero epochs", func(p *trainingread.Params) { p.Epochs = 0 }},
		{"zero workers", func(p *trainingread.Params) { p.DataloaderWorkers = 0 }},
		{"invalid within-shard", func(p *trainingread.Params) { p.ReadWithinShard = "none" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := smallParams()
			tc.mutate(&p)
			var buf bytes.Buffer
			if err := trainingread.Generate(p, &buf); err == nil {
				t.Error("expected error from invalid params, got nil")
			}
		})
	}
}

// TestHeaderFields checks the generated header carries the correct metadata.
func TestHeaderFields(t *testing.T) {
	p := smallParams()
	buf := mustGenerate(t, p)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatal(err)
	}
	hdr := r.Header()

	if hdr.Version != trace.TraceFormatVersion {
		t.Errorf("version %d, want %d", hdr.Version, trace.TraceFormatVersion)
	}
	if hdr.Kind != trace.TraceSynthetic {
		t.Errorf("kind %q, want %q", hdr.Kind, trace.TraceSynthetic)
	}
	if hdr.Profile != "training-read" {
		t.Errorf("profile %q, want training-read", hdr.Profile)
	}
	if hdr.CaptureMethod != trace.CaptureSynthetic {
		t.Errorf("capture_method %q, want %q", hdr.CaptureMethod, trace.CaptureSynthetic)
	}
	if hdr.TimeUnit != trace.TimeUnitNanoseconds {
		t.Errorf("time_unit %q, want %q", hdr.TimeUnit, trace.TimeUnitNanoseconds)
	}
	if len(hdr.Targets) != p.Shards {
		t.Errorf("targets %d, want %d", len(hdr.Targets), p.Shards)
	}
	if hdr.Summary.NumStreams != p.DataloaderWorkers {
		t.Errorf("summary.num_streams %d, want %d", hdr.Summary.NumStreams, p.DataloaderWorkers)
	}
	if hdr.Summary.NumGroups != 0 {
		t.Errorf("summary.num_groups %d, want 0", hdr.Summary.NumGroups)
	}
}

// TestTargetTable verifies target IDs are sequential and names follow the
// expected naming scheme.
func TestTargetTable(t *testing.T) {
	p := smallParams()
	buf := mustGenerate(t, p)

	r, _ := trace.NewReader(buf)
	hdr := r.Header()

	for i, tgt := range hdr.Targets {
		if tgt.ID != i {
			t.Errorf("target[%d].id = %d, want %d", i, tgt.ID, i)
		}
		want := fmt.Sprintf("shard_%04d.tar", i)
		if tgt.Name != want {
			t.Errorf("target[%d].name = %q, want %q", i, tgt.Name, want)
		}
		if tgt.Kind != trace.TargetFile {
			t.Errorf("target[%d].kind = %q, want %q", i, tgt.Kind, trace.TargetFile)
		}
		if tgt.Size != p.ShardSize {
			t.Errorf("target[%d].size = %d, want %d", i, tgt.Size, p.ShardSize)
		}
	}
}

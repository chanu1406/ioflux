package checkpoint_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/chanuollala/ioflux/pkg/gen/checkpoint"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// smallParams returns a fast, deterministic Params for testing. CreatedUTC is
// left empty so output is byte-identical across runs.
func smallParams() checkpoint.Params {
	p := checkpoint.DefaultParams()
	p.ModelSize = 256 << 10 // 256 KiB
	p.WriterRanks = 4
	p.WriteBlock = 32 << 10 // 32 KiB
	p.NumCheckpoints = 1
	return p
}

func mustGenerate(t *testing.T, p checkpoint.Params) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if err := checkpoint.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return &buf
}

// readOps parses a generated trace into its header and op slice.
func readOps(t *testing.T, buf *bytes.Buffer) (trace.Header, []trace.Op) {
	t.Helper()
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()
	var ops []trace.Op
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ops = append(ops, op)
	}
	return hdr, ops
}

// TestDeterminism verifies that identical params produce byte-identical output.
func TestDeterminism(t *testing.T) {
	p := smallParams()
	p.NumCheckpoints = 3
	p.CheckpointIntervalSec = 1.5
	buf1 := mustGenerate(t, p)
	buf2 := mustGenerate(t, p)
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("Generate is not deterministic with the same params")
	}
}

// TestValidatePassesOnGenerated verifies that every generated trace passes
// validation — the integration test for the full trace contract (handle
// lifecycle, non-decreasing t, unique op_ids, in-range targets).
func TestValidatePassesOnGenerated(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*checkpoint.Params)
	}{
		{"defaults-small", func(p *checkpoint.Params) {}},
		{"fsync-final", func(p *checkpoint.Params) { p.Fsync = checkpoint.FsyncFinal; p.NumCheckpoints = 2 }},
		{"fsync-none", func(p *checkpoint.Params) { p.Fsync = checkpoint.FsyncNone }},
		{"multi-checkpoint-interval", func(p *checkpoint.Params) { p.NumCheckpoints = 3; p.CheckpointIntervalSec = 2 }},
		{"multi-checkpoint-zero-interval", func(p *checkpoint.Params) { p.NumCheckpoints = 2; p.CheckpointIntervalSec = 0 }},
		{"single-rank", func(p *checkpoint.Params) { p.WriterRanks = 1 }},
		{"write-block-larger-than-shard", func(p *checkpoint.Params) { p.WriteBlock = p.ModelSize }},
		{"uneven-split", func(p *checkpoint.Params) { p.ModelSize = 100001; p.WriterRanks = 7; p.WriteBlock = 4096 }},
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

// TestStructure checks stream count, target count, and that total WRITE bytes
// equal ModelSize × NumCheckpoints exactly.
func TestStructure(t *testing.T) {
	p := smallParams()
	p.NumCheckpoints = 3
	hdr, ops := readOps(t, mustGenerate(t, p))

	if hdr.Summary.NumStreams != p.WriterRanks {
		t.Errorf("num_streams %d != WriterRanks %d", hdr.Summary.NumStreams, p.WriterRanks)
	}
	if got, want := len(hdr.Targets), p.NumCheckpoints*p.WriterRanks; got != want {
		t.Errorf("targets %d != NumCheckpoints×WriterRanks %d", got, want)
	}

	var writeBytes int64
	streams := map[int64]bool{}
	for _, op := range ops {
		streams[op.S] = true
		if op.Op == trace.OpWrite {
			writeBytes += *op.Len
		}
	}
	if want := p.ModelSize * int64(p.NumCheckpoints); writeBytes != want {
		t.Errorf("total WRITE bytes %d != ModelSize×NumCheckpoints %d", writeBytes, want)
	}
	if want := p.ModelSize * int64(p.NumCheckpoints); hdr.Summary.TotalBytes != want {
		t.Errorf("summary.total_bytes %d != %d", hdr.Summary.TotalBytes, want)
	}
	if len(streams) != p.WriterRanks {
		t.Errorf("active streams %d != WriterRanks %d", len(streams), p.WriterRanks)
	}
}

// TestFsyncCounts verifies the three fsync policies emit FSYNC ops correctly:
// per-file once per shard file, none never, final only the last checkpoint's.
func TestFsyncCounts(t *testing.T) {
	count := func(p checkpoint.Params) int {
		_, ops := readOps(t, mustGenerate(t, p))
		n := 0
		for _, op := range ops {
			if op.Op == trace.OpFsync {
				n++
			}
		}
		return n
	}

	base := smallParams()
	base.NumCheckpoints = 3

	perFile := base
	perFile.Fsync = checkpoint.FsyncPerFile
	if got, want := count(perFile), base.NumCheckpoints*base.WriterRanks; got != want {
		t.Errorf("per-file FSYNC count %d != one per file %d", got, want)
	}

	none := base
	none.Fsync = checkpoint.FsyncNone
	if got := count(none); got != 0 {
		t.Errorf("none FSYNC count %d != 0", got)
	}

	final := base
	final.Fsync = checkpoint.FsyncFinal
	if got, want := count(final), base.WriterRanks; got != want {
		t.Errorf("final FSYNC count %d != last-checkpoint ranks %d", got, want)
	}
}

// TestBurstTiming verifies that successive checkpoints are separated by the
// interval, and that a zero interval collapses all checkpoints into one burst.
func TestBurstTiming(t *testing.T) {
	distinctT := func(ops []trace.Op) []int64 {
		seen := map[int64]bool{}
		var out []int64
		for _, op := range ops {
			if !seen[op.T] {
				seen[op.T] = true
				out = append(out, op.T)
			}
		}
		return out
	}

	t.Run("interval-separates-bursts", func(t *testing.T) {
		p := smallParams()
		p.NumCheckpoints = 2
		p.CheckpointIntervalSec = 1 // 1e9 ns
		_, ops := readOps(t, mustGenerate(t, p))
		got := distinctT(ops)
		if len(got) != 2 {
			t.Fatalf("distinct t values = %v, want 2 (one per burst)", got)
		}
		if got[0] != 0 || got[1] != 1_000_000_000 {
			t.Errorf("burst times %v, want [0 1000000000]", got)
		}
	})

	t.Run("zero-interval-single-burst", func(t *testing.T) {
		p := smallParams()
		p.NumCheckpoints = 2
		p.CheckpointIntervalSec = 0
		hdr, ops := readOps(t, mustGenerate(t, p))
		if got := distinctT(ops); len(got) != 1 || got[0] != 0 {
			t.Errorf("zero-interval distinct t = %v, want [0]", got)
		}
		// Still two checkpoints' worth of distinct files and data.
		if got, want := len(hdr.Targets), 2*p.WriterRanks; got != want {
			t.Errorf("targets %d != %d", got, want)
		}
	})
}

// TestValidateParams covers the parameter validation error cases.
func TestValidateParams(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*checkpoint.Params)
	}{
		{"zero-model-size", func(p *checkpoint.Params) { p.ModelSize = 0 }},
		{"zero-ranks", func(p *checkpoint.Params) { p.WriterRanks = 0 }},
		{"ranks-exceed-model", func(p *checkpoint.Params) { p.ModelSize = 4; p.WriterRanks = 8 }},
		{"zero-write-block", func(p *checkpoint.Params) { p.WriteBlock = 0 }},
		{"zero-checkpoints", func(p *checkpoint.Params) { p.NumCheckpoints = 0 }},
		{"negative-interval", func(p *checkpoint.Params) { p.CheckpointIntervalSec = -1 }},
		{"bad-fsync", func(p *checkpoint.Params) { p.Fsync = "sometimes" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := smallParams()
			tc.mutate(&p)
			if err := checkpoint.ValidateParams(p); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

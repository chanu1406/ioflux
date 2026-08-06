package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/trace"
)

func runTransformCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runTransform(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func readHeader(t *testing.T, path string) trace.Header {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	line, _, _ := bytes.Cut(data, []byte("\n"))
	var h trace.Header
	if err := json.Unmarshal(line, &h); err != nil {
		t.Fatal(err)
	}
	return h
}

// The output must replay the same bytes over the same targets, in more and
// smaller operations, and must record what was done to it.
func TestTransformCmd_SplitReadsProducesValidLedgeredTrace(t *testing.T) {
	dir := t.TempDir()
	in := genSmallTrace(t, dir)
	out := filepath.Join(dir, "split.ioflux")

	code, stdout, stderr := runTransformCLI([]string{
		"split-reads", "--block", "4KiB", "-o", out, in,
	})
	if code != 0 {
		t.Fatalf("exit=%d; stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "wrote "+out) {
		t.Errorf("stdout should confirm the write; got %q", stdout)
	}

	src, dst := readHeader(t, in), readHeader(t, out)

	if dst.Summary.TotalBytes != src.Summary.TotalBytes {
		t.Errorf("total bytes = %d, want the source's %d — the extent must be identical",
			dst.Summary.TotalBytes, src.Summary.TotalBytes)
	}
	if dst.Summary.NumOps <= src.Summary.NumOps {
		t.Errorf("ops = %d, want more than the source's %d", dst.Summary.NumOps, src.Summary.NumOps)
	}
	if dst.Summary.NumStreams != src.Summary.NumStreams {
		t.Errorf("streams = %d, want the source's %d", dst.Summary.NumStreams, src.Summary.NumStreams)
	}
	if len(dst.Transformations) != 1 {
		t.Fatalf("transformations = %d, want 1", len(dst.Transformations))
	}
	tr := dst.Transformations[0]
	if tr.Kind != trace.TransformSplitReads || tr.Params["block"] != "4096" {
		t.Errorf("ledger entry = %+v, want split-reads block=4096", tr)
	}
	// The ledger must tie the output to the exact bytes it came from.
	srcBytes, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	if tr.SourceDigest != trace.Digest(srcBytes) {
		t.Errorf("source digest = %q, want the input's digest", tr.SourceDigest)
	}
	// A header carrying a ledger must declare the version whose readers look for one.
	if dst.Version != trace.VersionTransformed {
		t.Errorf("version = %d, want %d", dst.Version, trace.VersionTransformed)
	}
}

// The output has to pass the validator, since a transformation that produced an
// invalid trace must fail here rather than at the start of a measured run.
func TestTransformCmd_OutputValidates(t *testing.T) {
	dir := t.TempDir()
	in := genSmallTrace(t, dir)
	out := filepath.Join(dir, "split.ioflux")

	if code, _, stderr := runTransformCLI([]string{"split-reads", "--block", "1KiB", "-o", out, in}); code != 0 {
		t.Fatalf("transform exit=%d; stderr=%s", code, stderr)
	}
	if code, _, stderr := runCLI([]string{out}); code != 0 {
		t.Errorf("transformed trace failed validation: exit=%d; stderr=%s", code, stderr)
	}
}

// A transformed trace must still replay.
func TestTransformCmd_OutputReplays(t *testing.T) {
	dir := t.TempDir()
	in := genSmallTrace(t, dir)
	out := filepath.Join(dir, "split.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runTransformCLI([]string{"split-reads", "--block", "2KiB", "-o", out, in}); code != 0 {
		t.Fatalf("transform exit=%d; stderr=%s", code, stderr)
	}
	code, _, stderr := runRunCLI([]string{"--trace", out, "--engine", "mem", "-o", resultsPath})
	if code != 0 {
		t.Fatalf("replay of a transformed trace exit=%d; stderr=%s", code, stderr)
	}
	// The result must carry the ledger, or a transformed replay could be read as
	// a replay of the source workload.
	data, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"trace_transformations"`)) {
		t.Error("result does not carry the trace's transformation ledger")
	}
	if !bytes.Contains(data, []byte(`"split-reads"`)) {
		t.Error("result does not name the transformation applied")
	}
}

func TestTransformCmd_RejectsBadUsage(t *testing.T) {
	dir := t.TempDir()
	in := genSmallTrace(t, dir)
	out := filepath.Join(dir, "o.ioflux")

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no subcommand", nil},
		{"unknown transformation", []string{"reverse-reads", "-o", out, in}},
		{"missing -o", []string{"split-reads", "--block", "4KiB", in}},
		{"zero block", []string{"split-reads", "--block", "0", "-o", out, in}},
		{"no input", []string{"split-reads", "--block", "4KiB", "-o", out}},
		{"two inputs", []string{"split-reads", "--block", "4KiB", "-o", out, in, in}},
	} {
		if code, _, _ := runTransformCLI(tc.args); code != 2 {
			t.Errorf("%s: exit=%d, want 2", tc.name, code)
		}
	}
}

// Transforming a trace that is already inconsistent would produce a
// consistent-looking output and bury the original problem.
func TestTransformCmd_RejectsInvalidInput(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.ioflux")
	if err := os.WriteFile(bad, []byte("not a trace\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runTransformCLI([]string{
		"split-reads", "--block", "4KiB", "-o", filepath.Join(dir, "o.ioflux"), bad,
	})
	if code != 1 {
		t.Errorf("exit=%d, want 1 for an invalid input trace", code)
	}
	if !strings.Contains(stderr, "ioflux transform:") {
		t.Errorf("stderr should report the failure; got %q", stderr)
	}
}

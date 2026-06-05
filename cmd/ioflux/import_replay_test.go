package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// These tests close the import → replay loop end to end: a real-format
// strace/dftracer trace is imported into .ioflux, then replayed against the
// local engine and asserted to produce an honest, kind:"imported" report with
// full op coverage. They are the proof that IOFlux's differentiator —
// trace-driven replay of a captured (non-synthetic) workload — actually works,
// not just that the parsers produce a valid file.
//
// The captured /data/ paths are rewritten into the test's own temp dir and the
// data is materialized there, so a run never touches anything outside scratch.

// A two-stream strace capture: two DataLoader-style workers (pids 2001/2002)
// each read a shard. Exercises sequential reads, a positional pread64, fd reuse
// across streams, and cursor tracking — all paths the replay must honor.
const replayStrace = `2001  12:00:00.000000 openat(AT_FDCWD, "/data/shard_000.tar", O_RDONLY) = 3 <0.000010>
2001  12:00:00.000100 read(3, "PK\3\4"..., 65536) = 65536 <0.000200>
2001  12:00:00.000400 read(3, "...."..., 65536) = 65536 <0.000200>
2001  12:00:00.000800 close(3) = 0 <0.000010>
2002  12:00:00.000050 openat(AT_FDCWD, "/data/shard_001.tar", O_RDONLY) = 3 <0.000010>
2002  12:00:00.000150 pread64(3, "...."..., 32768, 65536) = 32768 <0.000200>
2002  12:00:00.000550 read(3, "...."..., 32768) = 32768 <0.000200>
2002  12:00:00.000900 close(3) = 0 <0.000010>
`

// A two-stream DFTracer (.pfw) capture in Chrome-trace JSON, one POSIX read
// stream per pid, including a positional pread64.
const replayDFTracer = `[
{"name":"open","cat":"POSIX","ph":"X","ts":1000.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/part-0.bin","flags":0,"return_val":3}},
{"name":"read","cat":"POSIX","ph":"X","ts":1010.0,"dur":50.0,"pid":100,"tid":100,"args":{"fname":"/data/part-0.bin","fd":3,"count":8192,"return_val":8192}},
{"name":"close","cat":"POSIX","ph":"X","ts":1100.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/part-0.bin","fd":3,"return_val":0}},
{"name":"open","cat":"POSIX","ph":"X","ts":1050.0,"dur":1.0,"pid":200,"tid":200,"args":{"fname":"/data/part-1.bin","flags":0,"return_val":3}},
{"name":"pread64","cat":"POSIX","ph":"X","ts":1060.0,"dur":50.0,"pid":200,"tid":200,"args":{"fname":"/data/part-1.bin","fd":3,"offset":4096,"count":4096,"return_val":4096}},
{"name":"close","cat":"POSIX","ph":"X","ts":1120.0,"dur":1.0,"pid":200,"tid":200,"args":{"fname":"/data/part-1.bin","fd":3,"return_val":0}}
]`

// importReplay imports sample through the import CLI, then replays the produced
// .ioflux against the local engine, rewriting the captured /data/ prefix into a
// scratch dir and materializing synthetic data there. It returns the parsed
// results.json and the imported trace's header.
func importReplay(t *testing.T, source, sample string) (*results.Results, trace.Header) {
	t.Helper()
	dir := t.TempDir()
	inPath := filepath.Join(dir, "capture."+source)
	tracePath := filepath.Join(dir, "trace.ioflux")
	mapPath := filepath.Join(dir, "map.yaml")
	resultsPath := filepath.Join(dir, "results.json")
	dataDir := filepath.Join(dir, "data")

	if err := os.WriteFile(inPath, []byte(sample), 0o644); err != nil {
		t.Fatal(err)
	}

	if code, _, stderr := runImportCLI([]string{source, "-o", tracePath, inPath}); code != 0 {
		t.Fatalf("import %s exit=%d; stderr=%s", source, code, stderr)
	}

	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	hdr := validateBytes(t, traceBytes)

	// Rewrite the captured /data/ prefix into scratch so materialize-synthetic
	// writes (and the subsequent replay reads) never escape the temp dir.
	mapYAML := "target_rewrite:\n  - from: \"/data/\"\n    to: \"" + dataDir + "/\"\n"
	if err := os.WriteFile(mapPath, []byte(mapYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--mode", "asap",
		"--target-map", mapPath,
		"--prepare", "materialize-synthetic",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("run imported %s trace exit=%d; stderr=%s", source, code, stderr)
	}

	raw, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse results.json: %v", err)
	}
	return &res, hdr
}

// assertImportedReplayHonest checks that an imported trace replayed with full
// coverage, no op errors, and the honest kind/capture-method labels.
func assertImportedReplayHonest(t *testing.T, res *results.Results, hdr trace.Header, wantOps int64, wantMethod trace.CaptureMethod) {
	t.Helper()
	if hdr.Kind != trace.TraceImported {
		t.Errorf("trace header kind=%q, want imported", hdr.Kind)
	}
	if hdr.CaptureMethod != wantMethod {
		t.Errorf("capture_method=%q, want %q", hdr.CaptureMethod, wantMethod)
	}
	if res.Plan.TraceKind != string(trace.TraceImported) {
		t.Errorf("results trace_kind=%q, want imported", res.Plan.TraceKind)
	}
	if res.Errors != 0 {
		t.Errorf("errors=%d, want 0", res.Errors)
	}
	if res.Plan.NumOps != wantOps {
		t.Errorf("plan.num_ops=%d, want %d", res.Plan.NumOps, wantOps)
	}
	if got, want := res.Fidelity.Coverage.OpsIssued, res.Fidelity.Coverage.OpsInTrace; got != want {
		t.Errorf("coverage: issued=%d in_trace=%d, want equal (full coverage)", got, want)
	}
	if res.Fidelity.Coverage.OpsIssued != wantOps {
		t.Errorf("coverage.ops_issued=%d, want %d", res.Fidelity.Coverage.OpsIssued, wantOps)
	}
	if res.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("coverage.ops_skipped=%d, want 0", res.Fidelity.Coverage.OpsSkipped)
	}
	if res.BytesMoved <= 0 {
		t.Errorf("bytes_moved=%d, want > 0 (reads should have moved bytes)", res.BytesMoved)
	}
}

func TestImportReplay_Strace_EndToEnd(t *testing.T) {
	res, hdr := importReplay(t, "strace", replayStrace)
	assertImportedReplayHonest(t, res, hdr, 8, "import:strace")
}

func TestImportReplay_DFTracer_EndToEnd(t *testing.T) {
	res, hdr := importReplay(t, "dftracer", replayDFTracer)
	assertImportedReplayHonest(t, res, hdr, 6, "import:dftracer")
}

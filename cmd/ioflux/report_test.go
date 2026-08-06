package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/fidelity"
	"github.com/chanuollala/ioflux/pkg/results"
)

// testTraceDigest is the trace identity shared by every result makeTestResults
// builds, so a comparison of two unmodified fixtures is a same-workload one.
const testTraceDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

func makeTestResults() *results.Results {
	return &results.Results{
		SchemaVersion: results.SchemaVersion,
		GeneratedAt:   "2026-06-04T10:00:00Z",
		Tool:          results.Tool{Version: "0.4.0", Revision: "abc123"},
		Host:          results.Host{Hostname: "bench-01", OS: "linux", Arch: "amd64", CPUs: 28},
		Plan: results.PlanInfo{
			TracePath:          "/data/trace.ioflux",
			TraceDigest:        testTraceDigest,
			TraceKind:          "imported",
			CaptureMethod:      "import:strace",
			CaptureLimitations: "mmap page-fault I/O not captured",
			Engine:             "local",
			Mode:               "asap",
			MaxInflight:        512,
			NumStreams:         4,
			NumOps:             1024,
			TotalBytes:         67108864,
			PrepareMode:        "assume-existing",
		},
		RunEnv: results.RunEnv{
			CacheMode: "cold",
		},
		DurationNS:   2_500_000_000,
		OpsCompleted: 1024,
		BytesMoved:   67108864,
		Errors:       0,
		PerOpStats: []results.PerOpStats{
			{OpType: "READ", Count: 512, P50NS: 100_000, P90NS: 200_000, P99NS: 500_000, P999NS: 1_000_000, MaxNS: 2_000_000},
			{OpType: "OPEN", Count: 256, P50NS: 50_000, P90NS: 80_000, P99NS: 150_000, P999NS: 300_000, MaxNS: 800_000},
			{OpType: "CLOSE", Count: 256, P50NS: 20_000, P90NS: 40_000, P99NS: 80_000, P999NS: 150_000, MaxNS: 400_000},
		},
		CPU: results.CPU{
			UserNS: 12_300_000,
			SysNS:  4_500_000,
			WallNS: 2_500_000_000,
		},
		Fidelity: fidelity.FidelityReport{
			Coverage: fidelity.CoverageSummary{
				OpsInTrace: 1024,
				OpsIssued:  1024,
				OpsSkipped: 0,
			},
			ConcurrencyCheck: fidelity.ConcurrencyCheck{
				MaxPerStreamInflight: 1,
			},
			Backlog: fidelity.BacklogSummary{
				TotalEvents:           0,
				TotalBlockedNS:        0,
				PeakInflightDepth:     0,
				FractionOpsBacklogged: 0,
			},
			LowFidelity: false,
		},
	}
}

func TestReportCmd_PrintsCaptureProvenance(t *testing.T) {
	res := makeTestResults()
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, stderr := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, stderr=%s", code, stderr)
	}
	for _, want := range []string{"Source:    import:strace", "Capture limitations: mmap page-fault I/O not captured"} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q; got:\n%s", want, out)
		}
	}
}

// TestReportCmd_StatesSourcePartialReads pins that a run reproducing a source's
// short reads says so. Reproducing them correctly means the replay reports no
// short read and no error, which is indistinguishable from a workload that never
// had one — so without this line the report cannot tell the two apart.
func TestReportCmd_StatesSourcePartialReads(t *testing.T) {
	res := makeTestResults()
	res.Plan.TracePartialReads = 32
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, stderr := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, stderr=%s", code, stderr)
	}
	if !strings.Contains(out, "32 source read(s) returned less than requested") {
		t.Errorf("report does not state the source's partial reads; got:\n%s", out)
	}
	// A clean run must still read as clean.
	if !strings.Contains(out, "Execution: no detected operation failures") {
		t.Errorf("reproducing a source short read must not invalidate the run; got:\n%s", out)
	}
}

// TestReportCmd_OmitsPartialReadsWhenNone keeps the line meaningful.
func TestReportCmd_OmitsPartialReadsWhenNone(t *testing.T) {
	data, err := json.Marshal(makeTestResults())
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, _ := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	if strings.Contains(out, "returned less than requested") {
		t.Errorf("report mentions partial reads for a trace with none; got:\n%s", out)
	}
}

func runReportCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runReport(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestReportCmd_NoArgsExitsTwo(t *testing.T) {
	code, _, stderr := runReportCLI(nil)
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should contain usage; got %q", stderr)
	}
}

func TestReportCmd_HelpFlag(t *testing.T) {
	code, _, stderr := runReportCLI([]string{"-h"})
	// flag.FlagSet with ContinueOnError returns ErrHelp for -h, which we map to exit 2.
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should print usage for -h; got %q", stderr)
	}
}

func TestReportCmd_MissingFile(t *testing.T) {
	code, _, stderr := runReportCLI([]string{"/nonexistent/path/results.json"})
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "ioflux report:") {
		t.Errorf("stderr should report error; got %q", stderr)
	}
}

func TestReportCmd_BadJSON(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(p, []byte("this is not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runReportCLI([]string{p})
	if code != 1 {
		t.Fatalf("exit=%d, want 1", code)
	}
	if !strings.Contains(stderr, "parse results.json") {
		t.Errorf("stderr should mention parse error; got %q", stderr)
	}
}

func TestReportCmd_StdoutCleanOnError(t *testing.T) {
	// Errors must go to stderr only; stdout must be empty on failure.
	code, stdout, _ := runReportCLI([]string{"/nonexistent/path/results.json"})
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
	if stdout != "" {
		t.Errorf("stdout should be empty on error; got %q", stdout)
	}
}

func TestReportCmd_ValidResults(t *testing.T) {
	res := makeTestResults()
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, stderr := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}

	for _, want := range []string{
		"/data/trace.ioflux",
		"imported",
		"local",
		"asap",
		"2026-06-04",
		"READ",
		"OPEN",
		"CLOSE",
		"low-fidelity:   no",
		"Warnings:",
		// Fidelity fields now present.
		"backlog:",
		"coverage:",
		"concurrency:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, out)
		}
	}
}

// TestReportCmd_HeaderShowsProfile verifies the single-report header
// distinguishes profiles, e.g. "synthetic/checkpoint-write" vs a bare trace
// kind when no profile is recorded.
func TestReportCmd_HeaderShowsProfile(t *testing.T) {
	res := makeTestResults()
	res.Plan.TraceKind = "synthetic"
	res.Plan.Profile = "checkpoint-write"

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, stderr := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}
	if !strings.Contains(out, "synthetic/checkpoint-write") {
		t.Errorf("output should show kind/profile; got:\n%s", out)
	}
}

func TestReportCmd_MultiHostDistribution(t *testing.T) {
	res := makeTestResults()
	res.GoDeliverySkewNS = 1_500_000
	res.Hosts = []results.HostResult{
		{Hostname: "hostA", OpsCompleted: 600, BytesMoved: 40_000_000, FirstDoneNS: 800_000_000, LastDoneNS: 1_000_000_000},
		{Hostname: "hostB", OpsCompleted: 424, BytesMoved: 27_108_864, FirstDoneNS: 1_500_000_000, LastDoneNS: 2_000_000_000},
	}
	res.Straggler = &results.StragglerWindow{
		FirstDoneNS:        1_000_000_000,
		LastDoneNS:         2_000_000_000,
		SkewNS:             1_000_000_000,
		FirstDoneOpsPerSec: 800,
		LastDoneOpsPerSec:  512,
		FirstDoneGiBPerSec: 0.05,
		LastDoneGiBPerSec:  0.03,
	}

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, stderr := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}
	for _, want := range []string{
		"Hosts (2):",
		"hostA",
		"hostB",
		"straggler window:",
		"first-done:",
		"last-done:",
		"excludes straggler tail",
		"go-delivery skew:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestReportCmd_SingleHostOmitsDistribution(t *testing.T) {
	// A single-node result has no Hosts; the distribution section must not appear.
	res := makeTestResults()
	data, _ := json.Marshal(res)
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, out, _ := runReportCLI([]string{p})
	if strings.Contains(out, "Hosts (") || strings.Contains(out, "straggler window:") {
		t.Errorf("single-node report must omit the distribution section\nfull output:\n%s", out)
	}
}

func TestReportCmd_FidelityDetails(t *testing.T) {
	// Verify that completion lag and full drift stats appear when non-zero.
	res := makeTestResults()
	res.Fidelity.ScheduleDrift = fidelity.PercentileSummary{
		P99NS:  145_000_000,
		P999NS: 200_000_000,
		MaxNS:  500_000_000,
		MeanNS: 50_000_000,
	}
	res.Fidelity.CompletionLag = fidelity.PercentileSummary{
		P99NS:  150_000_000,
		P999NS: 210_000_000,
		MaxNS:  510_000_000,
		MeanNS: 55_000_000,
	}
	res.Fidelity.Backlog = fidelity.BacklogSummary{
		TotalEvents:           42,
		TotalBlockedNS:        1_000_000_000,
		PeakInflightDepth:     128,
		FractionOpsBacklogged: 0.041,
	}

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, stderr := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d; stderr=%q", code, stderr)
	}

	for _, want := range []string{
		"schedule drift:",
		"completion lag:",
		"42 event(s)",
		"peak depth 128",
		"4.1% of ops",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("fidelity output missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestReportCmd_LowFidelityWarning(t *testing.T) {
	res := makeTestResults()
	res.Fidelity.LowFidelity = true
	res.Fidelity.LowFidelityReason = "p99 schedule drift too high"

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, _ := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0 (low-fidelity is a warning, not an error)", code)
	}
	if !strings.Contains(out, "low-fidelity:   YES") {
		t.Errorf("output should flag low-fidelity; got:\n%s", out)
	}
	if !strings.Contains(out, "p99 schedule drift too high") {
		t.Errorf("output should include fidelity reason; got:\n%s", out)
	}
}

func TestReportCmd_ErrorsReportedAsWarning(t *testing.T) {
	res := makeTestResults()
	res.Errors = 3
	res.ShortReads = 1

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, _ := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0 (report just prints, does not re-evaluate)", code)
	}
	if !strings.Contains(out, "3 op error(s)") {
		t.Errorf("output should mention op errors; got:\n%s", out)
	}
	if !strings.Contains(out,
		"Execution: INVALID — 3 operation failure(s), including 1 read(s) whose returned byte count disagreed with the source") {
		t.Errorf("output should give failed operations an explicit invalid execution verdict; got:\n%s", out)
	}
}

func TestReportCmd_HistogramOverflowInvalidatesExecution(t *testing.T) {
	res := makeTestResults()
	res.HistogramOverflows = 2

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, _ := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0 (report just prints, does not re-evaluate)", code)
	}
	if !strings.Contains(out, "Execution: INVALID — 2 latency sample(s) exceeded the histogram's trackable range") {
		t.Errorf("output should give a histogram overflow an explicit invalid execution verdict; got:\n%s", out)
	}
	if !strings.Contains(out, "2 histogram overflow(s)") {
		t.Errorf("output should warn about the histogram overflow; got:\n%s", out)
	}
}

func TestReportCmd_ErrorsAndHistogramOverflowBothInvalidate(t *testing.T) {
	res := makeTestResults()
	res.Errors = 1
	res.HistogramOverflows = 1

	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, _ := runReportCLI([]string{p})
	if code != 0 {
		t.Fatalf("exit=%d, want 0", code)
	}
	if !strings.Contains(out, "Execution: INVALID — 1 operation failure(s); 1 latency sample(s) exceeded the histogram's trackable range") {
		t.Errorf("output should combine both invalidating reasons; got:\n%s", out)
	}
}

func TestReportCmd_Stdin(t *testing.T) {
	res := makeTestResults()
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}

	orig := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	w.Close()

	code, out, _ := runReportCLI([]string{"-"})
	os.Stdin = orig
	r.Close()

	if code != 0 {
		t.Fatalf("exit=%d, want 0; output=%q", code, out)
	}
	if !strings.Contains(out, "imported") {
		t.Errorf("stdin read did not produce expected output; got:\n%s", out)
	}
}

// TestReportCmd_Comparison verifies the two-file `report A.json B.json` mode:
// both reports' trace paths, profiles, and dominant-op latency tables appear,
// alongside the headline scalar delta table.
func TestReportCmd_Comparison(t *testing.T) {
	a := makeTestResults()
	a.Plan.Profile = "training-read"

	b := makeTestResults()
	b.Plan.TracePath = "/data/ckpt.json"
	b.Plan.TraceDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	b.Plan.Profile = "checkpoint-write"
	b.DurationNS = 1_000_000_000
	b.BytesMoved = 134217728
	b.PerOpStats = []results.PerOpStats{
		{OpType: "WRITE", Count: 8, P50NS: 1_000_000, P90NS: 2_000_000, P99NS: 3_000_000, P999NS: 4_000_000, MaxNS: 5_000_000},
		{OpType: "FSYNC", Count: 8, P50NS: 500_000, P90NS: 600_000, P99NS: 700_000, P999NS: 800_000, MaxNS: 900_000},
	}

	dir := t.TempDir()
	pA := filepath.Join(dir, "a.json")
	pB := filepath.Join(dir, "b.json")
	for p, res := range map[string]*results.Results{pA: a, pB: b} {
		data, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	code, out, stderr := runReportCLI([]string{pA, pB})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}

	for _, want := range []string{
		"Comparing two reports:",
		"/data/trace.ioflux",
		"/data/ckpt.json",
		"training-read",
		"checkpoint-write",
		"duration",
		"ops/s",
		"GiB/s",
		"CPU user",
		"low-fidelity",
		"A (READ) latency",
		"B (WRITE) latency",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("comparison output missing %q\nfull output:\n%s", want, out)
		}
	}
	// The two sides replayed different traces, which must be stated rather than
	// left for the reader to infer from the differing profile names.
	if !strings.Contains(out, "different traces") {
		t.Errorf("comparison of two different traces should say so; got:\n%s", out)
	}
}

// TestReportCmd_ComparisonCleanWhenFullyComparable verifies that two runs which
// agree on trace identity, engine, environment, and build are reported as
// comparable outright, with no caveats invented for fields that match.
func TestReportCmd_ComparisonCleanWhenFullyComparable(t *testing.T) {
	a := makeTestResults()
	a.Plan.ReplayEquivalence = "syscall-level"
	b := makeTestResults()
	b.Plan.ReplayEquivalence = "syscall-level"
	// A different file name for the same bytes is the same workload; identity
	// comes from the digest, not the path.
	b.Plan.TracePath = "/data/b.ioflux"

	dir := t.TempDir()
	pA := filepath.Join(dir, "a.json")
	pB := filepath.Join(dir, "b.json")
	for p, res := range map[string]*results.Results{pA: a, pB: b} {
		data, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	code, out, stderr := runReportCLI([]string{pA, pB})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}
	if !strings.Contains(out, "Eligibility: COMPARABLE\n") {
		t.Errorf("fully comparable reports should be reported as COMPARABLE; got:\n%s", out)
	}
	if strings.Contains(out, "!") {
		t.Errorf("comparable reports must not print any caveat; got:\n%s", out)
	}
}

// TestReportCmd_ComparisonFlagsEquivalenceMismatch verifies that comparing an
// object-level (coalesced write) run against a syscall-level run is flagged,
// since the delta may reflect that semantic difference rather than backend
// performance (PRD §6 honesty rule).
func TestReportCmd_ComparisonFlagsEquivalenceMismatch(t *testing.T) {
	a := makeTestResults()
	a.Plan.ReplayEquivalence = "object-level"
	b := makeTestResults()
	b.Plan.TracePath = "/data/b.json"
	b.Plan.ReplayEquivalence = "syscall-level"

	dir := t.TempDir()
	pA := filepath.Join(dir, "a.json")
	pB := filepath.Join(dir, "b.json")
	for p, res := range map[string]*results.Results{pA: a, pB: b} {
		data, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	code, out, stderr := runReportCLI([]string{pA, pB})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}
	if !strings.Contains(out, "replay equivalence: A=object-level  B=syscall-level") {
		t.Errorf("output should flag the equivalence mismatch; got:\n%s", out)
	}
}

// TestReportCmd_ComparisonFlagsFidelityMismatch verifies that comparing a
// low-fidelity run against a full-fidelity run is flagged rather than
// presented as a clean apples-to-apples delta.
func TestReportCmd_ComparisonFlagsFidelityMismatch(t *testing.T) {
	a := makeTestResults()
	b := makeTestResults()
	b.Plan.TracePath = "/data/b.json"
	b.Fidelity.LowFidelity = true
	b.Fidelity.LowFidelityReason = "schedule drift too high"

	dir := t.TempDir()
	pA := filepath.Join(dir, "a.json")
	pB := filepath.Join(dir, "b.json")
	for p, res := range map[string]*results.Results{pA: a, pB: b} {
		data, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	code, out, stderr := runReportCLI([]string{pA, pB})
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%q", code, stderr)
	}
	if !strings.Contains(out, "fidelity: A=ok  B=low") {
		t.Errorf("output should flag the fidelity mismatch; got:\n%s", out)
	}
}

// writeResultsPair writes a and b to a temp dir and returns their paths.
func writeResultsPair(t *testing.T, a, b *results.Results) (string, string) {
	t.Helper()
	dir := t.TempDir()
	pA := filepath.Join(dir, "a.json")
	pB := filepath.Join(dir, "b.json")
	for p, res := range map[string]*results.Results{pA: a, pB: b} {
		data, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return pA, pB
}

// TestReportCmd_ComparisonRefusesInvalidRun is the regression test for the
// behaviour this gate exists to fix: the same results file that the one-file
// report calls INVALID used to produce a clean-looking delta table in the
// two-file comparison. A refusal must print no delta and exit non-zero, so a
// CI step comparing two runs fails instead of reporting a speedup.
func TestReportCmd_ComparisonRefusesInvalidRun(t *testing.T) {
	a := makeTestResults()
	b := makeTestResults()
	b.Errors = 417
	b.ShortReads = 12

	pA, pB := writeResultsPair(t, a, b)
	code, out, stderr := runReportCLI([]string{pA, pB})

	if code != 1 {
		t.Fatalf("exit=%d, want 1 for a refused comparison; stderr=%q", code, stderr)
	}
	if !strings.Contains(out, "Eligibility: INCOMPARABLE") {
		t.Errorf("output should state the verdict; got:\n%s", out)
	}
	if !strings.Contains(out, "B: 417 operation failure(s)") {
		t.Errorf("output should name the blocking reason and its side; got:\n%s", out)
	}
	// The delta table is the part that gets quoted, so its absence is the
	// substance of the refusal rather than a cosmetic detail.
	for _, forbidden := range []string{"Δ (B-A)", "ops/s", "GiB/s"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("a refused comparison must not print %q; got:\n%s", forbidden, out)
		}
	}
}

// A caveated comparison still prints its numbers and still exits 0 — the
// caveats qualify the delta, they do not withdraw it.
func TestReportCmd_CaveatedComparisonStillPrintsDelta(t *testing.T) {
	a := makeTestResults()
	b := makeTestResults()
	b.RunEnv.CacheMode = "warm"

	pA, pB := writeResultsPair(t, a, b)
	code, out, stderr := runReportCLI([]string{pA, pB})

	if code != 0 {
		t.Fatalf("exit=%d, want 0 for a caveated comparison; stderr=%q", code, stderr)
	}
	if !strings.Contains(out, "Eligibility: COMPARABLE-WITH-CAVEATS") {
		t.Errorf("output should state the verdict; got:\n%s", out)
	}
	if !strings.Contains(out, "cache mode: A=cold  B=warm") {
		t.Errorf("output should name the differing field; got:\n%s", out)
	}
	if !strings.Contains(out, "Δ (B-A)") {
		t.Errorf("a caveated comparison should still print the delta; got:\n%s", out)
	}
}

// The single-run report and the comparison gate must agree about what makes a
// run invalid; they are the two readers of one definition.
func TestReportCmd_SingleAndComparisonAgreeOnInvalidity(t *testing.T) {
	a := makeTestResults()
	b := makeTestResults()
	b.HistogramOverflows = 3

	pA, pB := writeResultsPair(t, a, b)

	if code, out, _ := runReportCLI([]string{pB}); code != 0 ||
		!strings.Contains(out, "Execution: INVALID") {
		t.Errorf("single report should call the run INVALID; got:\n%s", out)
	}
	if code, _, _ := runReportCLI([]string{pA, pB}); code != 1 {
		t.Errorf("comparison exit=%d, want 1 — it must not accept a run the single report rejects", code)
	}
}

func TestDominantOpUsesCountWithPriorityTieBreak(t *testing.T) {
	res := &results.Results{PerOpStats: []results.PerOpStats{
		{OpType: "WRITE", Count: 1},
		{OpType: "READ", Count: 10},
		{OpType: "OPEN", Count: 100},
	}}
	if got := res.DominantDataOp(); got == nil || got.OpType != "READ" {
		t.Fatalf("DominantDataOp=%v, want READ by highest data-op count", got)
	}

	res.PerOpStats = []results.PerOpStats{
		{OpType: "READ", Count: 10},
		{OpType: "WRITE", Count: 10},
	}
	if got := res.DominantDataOp(); got == nil || got.OpType != "WRITE" {
		t.Fatalf("DominantDataOp=%v, want WRITE tie-break over READ", got)
	}

	res.PerOpStats = []results.PerOpStats{{OpType: "OPEN", Count: 10}}
	if got := res.DominantDataOp(); got != nil {
		t.Fatalf("DominantDataOp=%v, want nil for metadata-only stats", got)
	}
}

// TestReportCmd_TooManyArgsExitsTwo ensures more than two report paths is a
// usage error.
func TestReportCmd_TooManyArgsExitsTwo(t *testing.T) {
	code, _, stderr := runReportCLI([]string{"a.json", "b.json", "c.json"})
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should contain usage; got %q", stderr)
	}
}

func TestFmtBytes(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1048576, "1.00 MiB"},
		{1073741824, "1.00 GiB"},
	}
	for _, tc := range tests {
		if got := fmtBytes(tc.n); got != tc.want {
			t.Errorf("fmtBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestFmtDuration(t *testing.T) {
	tests := []struct {
		ns   int64
		want string
	}{
		{0, "0s"},
		{500, "500ns"},
		{1500, "1.5µs"},
		{1500000, "1.5ms"},
		{1500000000, "1.500s"},
	}
	for _, tc := range tests {
		if got := fmtDuration(tc.ns); got != tc.want {
			t.Errorf("fmtDuration(%d) = %q, want %q", tc.ns, got, tc.want)
		}
	}
}

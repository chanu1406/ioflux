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

func makeTestResults() *results.Results {
	return &results.Results{
		GeneratedAt: "2026-06-04T10:00:00Z",
		Plan: results.PlanInfo{
			TracePath:   "/data/trace.ioflux",
			TraceKind:   "imported",
			Engine:      "local",
			Mode:        "asap",
			MaxInflight: 512,
			NumStreams:  4,
			NumOps:      1024,
			TotalBytes:  67108864,
			PrepareMode: "assume-existing",
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

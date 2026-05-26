package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runRunCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runRun(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestRunCmd_BasicSmoke(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	})
	if code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "asap",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "wrote "+resultsPath) {
		t.Fatalf("stdout should confirm write, got %q", stdout)
	}
	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"errors": 0`)) {
		t.Fatalf("results should contain zero op errors, got:\n%s", got)
	}
}

func TestRunCmd_WorkersFlagRemoved(t *testing.T) {
	code, _, stderr := runRunCLI([]string{"--workers", "1"})
	if code != 2 {
		t.Fatalf("exit=%d want 2; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "flag provided but not defined") {
		t.Fatalf("stderr should report unknown flag, got %q", stderr)
	}
	if strings.Contains(runUsage, "--workers") {
		t.Fatal("runUsage should not advertise --workers")
	}
}

func TestRunCmd_PrepareRejectsMalformedTrace(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "bad.ioflux")
	resultsPath := filepath.Join(dir, "results.json")
	content := `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":"a","kind":"file","size":1024}],"summary":{"num_ops":1,"num_streams":1,"num_groups":0,"total_bytes":0,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":99,"h":1,"mode":"r"}
`
	if err := os.WriteFile(tracePath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("exit=%d want 1; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "invalid trace") || !strings.Contains(stderr, "out of range") {
		t.Fatalf("stderr should mention validation failure, got %q", stderr)
	}
}

func TestRunUsageExitCodeDocsMentionOpErrors(t *testing.T) {
	if !strings.Contains(runUsage, "completed with op errors") {
		t.Fatalf("runUsage should document op-error exit semantics, got:\n%s", runUsage)
	}
	if strings.Contains(runUsage, "engine error)") {
		t.Fatalf("runUsage still uses old engine-error wording:\n%s", runUsage)
	}
}

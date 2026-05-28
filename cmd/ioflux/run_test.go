package main

import (
	"bytes"
	"fmt"
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

func TestRunCmd_LocalEngine(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "shard.dat")
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if err := os.WriteFile(targetPath, make([]byte, 32*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":%q,"kind":"file","size":32768}],"summary":{"num_ops":3,"num_streams":1,"num_groups":0,"total_bytes":8192,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":1,"mode":"r"}
{"t":1,"op_id":1,"s":0,"op":"READ","h":1,"off":0,"len":8192}
{"t":2,"op_id":2,"s":0,"op":"CLOSE","h":1}
`, targetPath)
	if err := os.WriteFile(tracePath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--mode", "asap",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun local exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"engine": "local"`)) {
		t.Fatalf("results should record local engine, got:\n%s", got)
	}
	if !bytes.Contains(got, []byte(`"bytes_moved": 8192`)) {
		t.Fatalf("results should record local read bytes, got:\n%s", got)
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

func TestRunCmd_TimelineFlagsAccepted(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "timeline",
		"--max-inflight", "64",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("--mode timeline exit=%d want 0; stderr=%s", code, stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--mode", "scaled",
		"--speedup", "2.0",
		"--max-inflight", "32",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("--mode scaled exit=%d want 0; stderr=%s", code, stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--mode", "nonsense",
		"-o", resultsPath,
	})
	if code != 2 {
		t.Fatalf("--mode nonsense exit=%d want 2; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "unsupported mode") {
		t.Fatalf("stderr should report unsupported mode, got %q", stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--max-inflight", "0",
		"-o", resultsPath,
	})
	if code != 2 {
		t.Fatalf("--max-inflight 0 exit=%d want 2; stderr=%s", code, stderr)
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

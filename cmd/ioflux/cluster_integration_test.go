//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/results"
)

// TestCluster_TwoProcessEndToEnd is the real distributed end-to-end: it builds
// the ioflux binary, starts two `ioflux worker` processes on localhost, and runs
// `ioflux run --hosts ...` against them. Unlike the in-process tests, this
// exercises the actual compiled CLI, the process lifecycle, and TCP gRPC.
//
// Run with: go test -tags integration ./cmd/ioflux/ -run TestCluster.
func TestCluster_TwoProcessEndToEnd(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "ioflux")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build ioflux: %v\n%s", err, out)
	}

	tracePath := filepath.Join(dir, "t.ioflux")
	mustRun(t, bin, "gen", "training-read",
		"--shards", "12", "--shard-size", "128KiB", "--record-size", "16KiB",
		"--dataloader-workers", "6", "--shuffle=false", "--seed", "1", "-o", tracePath)

	addrA := startWorkerProcess(t, bin)
	addrB := startWorkerProcess(t, bin)

	distPath := filepath.Join(dir, "dist.json")
	mustRun(t, bin, "run", "--trace", tracePath, "--engine", "mem", "--mode", "asap",
		"--hosts", addrA+","+addrB, "-o", distPath)

	dist := readResults(t, distPath)
	if len(dist.Hosts) != 2 {
		t.Fatalf("dist.Hosts=%d, want 2", len(dist.Hosts))
	}
	if dist.OpsCompleted != dist.Plan.NumOps {
		t.Errorf("OpsCompleted=%d, want %d (full coverage)", dist.OpsCompleted, dist.Plan.NumOps)
	}
	if dist.Errors != 0 {
		t.Errorf("Errors=%d, want 0", dist.Errors)
	}
	if dist.Fidelity.Coverage.OpsSkipped != 0 {
		t.Errorf("coverage.ops_skipped=%d, want 0", dist.Fidelity.Coverage.OpsSkipped)
	}
	if dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		t.Errorf("max-per-stream in-flight=%d, want ≤1", dist.Fidelity.ConcurrencyCheck.MaxPerStreamInflight)
	}
}

var listeningRE = regexp.MustCompile(`listening on (\S+)`)

// startWorkerProcess starts `ioflux worker --listen 127.0.0.1:0` and returns the
// address it bound to, parsed from its startup log. The process is killed at test end.
func startWorkerProcess(t *testing.T, bin string) string {
	t.Helper()
	cmd := exec.Command(bin, "worker", "--listen", "127.0.0.1:0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	addrCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if m := listeningRE.FindStringSubmatch(sc.Text()); m != nil {
				addrCh <- m[1]
				return
			}
		}
		addrCh <- ""
	}()

	select {
	case addr := <-addrCh:
		if addr == "" {
			t.Fatal("worker exited before reporting a listen address")
		}
		return addr
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not report a listen address within 10s")
		return ""
	}
}

func mustRun(t *testing.T, bin string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, bin, args...).CombinedOutput(); err != nil {
		t.Fatalf("ioflux %v: %v\n%s", args, err, out)
	}
}

func readResults(t *testing.T, path string) results.Results {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return res
}

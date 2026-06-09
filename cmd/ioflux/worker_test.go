package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/results"
)

// startWorker runs an in-process gRPC worker on an ephemeral localhost port and
// returns its address. It is cleaned up when the test ends. This drives the real
// CLI worker path (serveWorker) over real TCP, so a distributed CLI run can dial
// it without spawning a separate process.
func startWorker(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		var log testWriter
		_ = serveWorker(ctx, lis, &log)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("serveWorker did not stop after cancel")
		}
	})
	return lis.Addr().String()
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestWorker_ServesAndShutsDown is the worker smoke test: the server serves RPCs
// (a Register round-trips the protocol version) and stops cleanly on cancel.
func TestWorker_ServesAndShutsDown(t *testing.T) {
	addr := startWorker(t)

	w, err := cluster.DialWorker(addr)
	if err != nil {
		t.Fatalf("DialWorker: %v", err)
	}
	defer w.Close()

	info, err := w.Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if info.Version != cluster.Version {
		t.Errorf("worker version=%q, want %q", info.Version, cluster.Version)
	}
	if info.Hostname == "" {
		t.Error("worker reported empty hostname")
	}
}

// TestRun_DistributedHosts is the M2 CLI end-to-end: `ioflux run --hosts ...`
// against two real in-process workers produces a merged results.json with two
// Hosts and a straggler window, and replays every op exactly once — matching a
// single-node run of the same trace.
func TestRun_DistributedHosts(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "12",
		"--shard-size", "128KiB",
		"--record-size", "16KiB",
		"--dataloader-workers", "6",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	single := runForResults(t, filepath.Join(dir, "single.json"), tracePath, "")

	addrs := startWorker(t) + "," + startWorker(t)
	dist := runForResults(t, filepath.Join(dir, "dist.json"), tracePath, addrs)

	if dist.OpsCompleted != single.OpsCompleted {
		t.Errorf("OpsCompleted: dist=%d single=%d", dist.OpsCompleted, single.OpsCompleted)
	}
	if dist.BytesMoved != single.BytesMoved {
		t.Errorf("BytesMoved: dist=%d single=%d", dist.BytesMoved, single.BytesMoved)
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
	if len(dist.Hosts) != 2 {
		t.Fatalf("dist.Hosts=%d, want 2", len(dist.Hosts))
	}
	if dist.Straggler == nil {
		t.Error("dist.Straggler is nil, want a straggler window")
	}
	if single.Hosts != nil {
		t.Errorf("single-node result must omit hosts, got %v", single.Hosts)
	}
}

// runForResults runs the CLI (single-node when hosts is empty) and returns the
// parsed results.json.
func runForResults(t *testing.T, outPath, tracePath, hosts string) results.Results {
	t.Helper()
	args := []string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "asap",
		"-o", outPath,
	}
	if hosts != "" {
		args = append(args, "--hosts", hosts)
	}
	code, stdout, stderr := runRunCLI(args)
	if code != 0 {
		t.Fatalf("runRun(hosts=%q) exit=%d; stdout=%s stderr=%s", hosts, code, stdout, stderr)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse results.json: %v", err)
	}
	return res
}

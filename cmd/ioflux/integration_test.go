//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// TestMinIO_EndToEndGenRun exercises the full gen → run pipeline against a
// live MinIO instance. Set IOFLUX_MINIO_ENDPOINT to enable.
func TestMinIO_EndToEndGenRun(t *testing.T) {
	endpoint := os.Getenv("IOFLUX_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("IOFLUX_MINIO_ENDPOINT is not set")
	}
	bucket := integEnv("IOFLUX_MINIO_BUCKET", "bench")
	accessKey := integEnv("IOFLUX_MINIO_ACCESS_KEY", "minioadmin")
	secretKey := integEnv("IOFLUX_MINIO_SECRET_KEY", "minioadmin")

	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	mapPath := filepath.Join(dir, "map.yaml")
	resultsPath := filepath.Join(dir, "results.json")
	csvPath := filepath.Join(dir, "results.csv")

	// Generate training-read trace: 4 shards × 1 MiB.
	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "4",
		"--shard-size", "1MiB",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("gen exit=%d stderr=%s", code, stderr)
	}

	// Target map: remap bare shard names into the s3://<bucket>/imagenet/ prefix.
	mapYAML := "target_rewrite:\n  - from: \"\"\n    to: \"s3://" + bucket + "/imagenet/\"\n"
	if err := os.WriteFile(mapPath, []byte(mapYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "s3",
		"--endpoint", endpoint,
		"--bucket", bucket,
		"--path-style",
		"--access-key", accessKey,
		"--secret-key", secretKey,
		"--target-map", mapPath,
		"--prepare", "materialize-synthetic",
		"--cache-mode", "cold",
		"--mode", "timeline",
		"--max-inflight", "32",
		"-o", resultsPath,
		"--csv", csvPath,
	})
	if code != 0 {
		t.Fatalf("run exit=%d want 0; stderr=%s", code, stderr)
	}

	raw, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatalf("read results.json: %v", err)
	}
	var res results.Results
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse results.json: %v", err)
	}

	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}
	// M1 acceptance: every op in the trace was issued.
	if got, want := res.Fidelity.Coverage.OpsIssued, res.Plan.NumOps; got != want {
		t.Errorf("Coverage.OpsIssued=%d, want %d (= Plan.NumOps)", got, want)
	}
	if got, want := res.Fidelity.Coverage.OpsInTrace, res.Plan.NumOps; got != want {
		t.Errorf("Coverage.OpsInTrace=%d, want %d (= Plan.NumOps)", got, want)
	}
	if skipped := res.Fidelity.Coverage.OpsSkipped; skipped != 0 {
		t.Errorf("Coverage.OpsSkipped=%d, want 0 (ops were dropped)", skipped)
	}
	// LowFidelity is informational for S3 — do not assert false.
	t.Logf("LowFidelity=%v reason=%q drift_p99=%dns",
		res.Fidelity.LowFidelity, res.Fidelity.LowFidelityReason,
		res.Fidelity.ScheduleDrift.P99NS)

	// CSV must have header row + at least one data row.
	csvData, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read results.csv: %v", err)
	}
	if n := bytes.Count(csvData, []byte("\n")); n < 2 {
		t.Errorf("CSV has %d lines, want ≥2 (header+row)", n)
	}
}

func integEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// TestCheckpointReplay_Local_EndToEnd closes the generate → replay loop for the
// write workload: a small checkpoint-write trace is generated and replayed
// against the local engine into a scratch dir, then asserted to produce an
// honest report — full coverage, no op errors, the expected bytes written, and
// the open/write/fsync/close op types of a multi-rank sharded checkpoint. It is
// the proof that write replay works end to end, not just that the generator
// produces a valid file.
func TestCheckpointReplay_Local_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "ckpt.ioflux")
	mapPath := filepath.Join(dir, "map.yaml")
	resultsPath := filepath.Join(dir, "results.json")
	scratch := filepath.Join(dir, "scratch")

	// 4 ranks × 256 KiB model, 2 checkpoints → 8 shard files, 512 KiB written.
	const modelSize = 256 << 10
	const ranks = 4
	const checkpoints = 2

	if code, _, stderr := runGenCLI([]string{
		"checkpoint-write",
		"--model-size", "256KiB",
		"--writer-ranks", "4",
		"--write-block", "32KiB",
		"--num-checkpoints", "2",
		"--fsync", "per-file",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("gen checkpoint-write exit=%d; stderr=%s", code, stderr)
	}

	// Rewrite every (relative) target into a scratch dir so the run creates files
	// only there. An empty `from` prefix matches all targets.
	mapYAML := "target_rewrite:\n  - from: \"\"\n    to: \"" + scratch + "/\"\n"
	if err := os.WriteFile(mapPath, []byte(mapYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	// No --prepare: a write trace creates its own targets via OPEN(create,trunc).
	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--mode", "asap",
		"--target-map", mapPath,
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("run checkpoint trace exit=%d; stderr=%s", code, stderr)
	}

	raw, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse results.json: %v", err)
	}

	if res.Plan.TraceKind != "synthetic" {
		t.Errorf("trace_kind=%q, want synthetic", res.Plan.TraceKind)
	}
	if res.Plan.Profile != "checkpoint-write" {
		t.Errorf("profile=%q, want checkpoint-write", res.Plan.Profile)
	}
	if res.Errors != 0 {
		t.Errorf("errors=%d, want 0", res.Errors)
	}
	if got, want := res.Fidelity.Coverage.OpsIssued, res.Fidelity.Coverage.OpsInTrace; got != want {
		t.Errorf("coverage: issued=%d in_trace=%d, want equal (full coverage)", got, want)
	}
	if want := int64(modelSize * checkpoints); res.BytesMoved != want {
		t.Errorf("bytes_moved=%d, want %d", res.BytesMoved, want)
	}

	// The open/write/fsync/close op types must all appear in the per-op stats.
	kinds := map[string]bool{}
	for _, s := range res.PerOpStats {
		kinds[s.OpType] = true
	}
	for _, want := range []string{"OPEN", "WRITE", "FSYNC", "CLOSE"} {
		if !kinds[want] {
			t.Errorf("per_op_stats missing %s op", want)
		}
	}

	// The shard files must actually exist on disk under scratch.
	entries, err := os.ReadDir(filepath.Join(scratch, "checkpoint_0000"))
	if err != nil {
		t.Errorf("checkpoint_0000 dir not created: %v", err)
	} else if len(entries) != ranks {
		t.Errorf("checkpoint_0000 has %d shard files, want %d", len(entries), ranks)
	}
}

// TestCheckpointReplay_Mem_FsyncNone replays a checkpoint-write trace
// generated with --fsync none against the mem engine. The mem engine's
// Caps.Durable is false, so it rejects FSYNC at PREPARE; a trace with no
// FSYNC ops must replay cleanly.
func TestCheckpointReplay_Mem_FsyncNone(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "ckpt.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	const modelSize = 128 << 10

	if code, _, stderr := runGenCLI([]string{
		"checkpoint-write",
		"--model-size", "128KiB",
		"--writer-ranks", "2",
		"--write-block", "32KiB",
		"--num-checkpoints", "1",
		"--fsync", "none",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("gen checkpoint-write exit=%d; stderr=%s", code, stderr)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "asap",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("run checkpoint trace (mem, fsync=none) exit=%d; stderr=%s", code, stderr)
	}

	raw, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse results.json: %v", err)
	}

	if res.Plan.Profile != "checkpoint-write" {
		t.Errorf("profile=%q, want checkpoint-write", res.Plan.Profile)
	}
	if res.Errors != 0 {
		t.Errorf("errors=%d, want 0", res.Errors)
	}
	if want := int64(modelSize); res.BytesMoved != want {
		t.Errorf("bytes_moved=%d, want %d", res.BytesMoved, want)
	}
	for _, s := range res.PerOpStats {
		if s.OpType == "FSYNC" {
			t.Errorf("per_op_stats should not contain FSYNC when --fsync none")
		}
	}
}

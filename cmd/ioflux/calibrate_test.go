package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/calibrate"
)

func runCalibrateCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runCalibrate(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// calibrateFixture generates a small trace and replays it against the mem
// engine, returning the trace path and the resulting run file. The run uses the
// mem engine so the test needs no dataset on disk; the assessment refuses it for
// exactly that reason, which is what the refusal case checks.
func calibrateFixture(t *testing.T) (dir, tracePath, runPath string) {
	t.Helper()
	dir = t.TempDir()
	tracePath = filepath.Join(dir, "trace.ioflux")
	runPath = filepath.Join(dir, "run.json")

	code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "2",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	})
	if code != 0 {
		t.Fatalf("gen exit=%d, stderr=%s", code, stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "asap",
		"-o", runPath,
	})
	if code != 0 {
		t.Fatalf("run exit=%d, stderr=%s", code, stderr)
	}
	return dir, tracePath, runPath
}

func TestCalibrateCmd_MeasuresCeiling(t *testing.T) {
	dir, tracePath, _ := calibrateFixture(t)
	outPath := filepath.Join(dir, "calibration.json")

	code, stdout, stderr := runCalibrateCLI([]string{
		"--trace", tracePath,
		"--trials", "2",
		"--warmup", "0",
		"-o", outPath,
	})
	if code != 0 {
		t.Fatalf("calibrate exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "Load-generator ceiling") {
		t.Errorf("stdout does not report a ceiling:\n%s", stdout)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read calibration: %v", err)
	}
	var cal calibrate.Calibration
	if err := json.Unmarshal(data, &cal); err != nil {
		t.Fatalf("parse calibration: %v", err)
	}

	if cal.Ceiling.Engine != calibrate.NullEngine {
		t.Errorf("ceiling engine = %q, want %q", cal.Ceiling.Engine, calibrate.NullEngine)
	}
	if cal.Ceiling.OpsPerSec <= 0 {
		t.Errorf("ceiling OpsPerSec = %v, want > 0", cal.Ceiling.OpsPerSec)
	}
	if cal.Ceiling.TraceDigest == "" {
		t.Error("ceiling carries no trace digest, so it could not be matched to a run")
	}
	// The trials are the evidence behind the median; a ceiling that discarded
	// them could not be re-examined.
	if cal.Trials == nil || len(cal.Trials.Trials) != 2 {
		t.Error("calibration did not retain its trials")
	}
	if cal.Assessment != nil {
		t.Error("assessment present without --against")
	}
}

// A calibration is measured against the null engine, so its ceiling must match
// a run of the same trace at the same concurrency and refuse one that differs.
func TestCalibrateCmd_RefusesMismatchedConcurrency(t *testing.T) {
	dir, tracePath, _ := calibrateFixture(t)
	runPath := filepath.Join(dir, "local.json")
	dataDir := filepath.Join(dir, "data")
	mapPath := filepath.Join(dir, "map.yaml")

	if err := os.WriteFile(mapPath, []byte("target_rewrite:\n  - from: \"\"\n    to: \""+dataDir+"/\"\n"), 0o644); err != nil {
		t.Fatalf("write target map: %v", err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--mode", "asap",
		"--max-inflight", "8",
		"--target-root", dataDir,
		"--target-map", mapPath,
		"--prepare", "materialize-synthetic",
		"-o", runPath,
	})
	if code != 0 {
		t.Fatalf("run exit=%d, stderr=%s", code, stderr)
	}

	// A ceiling measured at a different concurrency bounds a different thing.
	code, stdout, stderr := runCalibrateCLI([]string{
		"--against", runPath,
		"--max-inflight", "512",
		"--trials", "2",
		"--warmup", "0",
		"--max-generator-share", "90",
	})
	if code != 1 {
		t.Fatalf("calibrate exit=%d want 1 (refused); stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, string(calibrate.VerdictNotAssessed)) {
		t.Errorf("stdout does not report the refusal:\n%s", stdout)
	}
	if !strings.Contains(stdout, "max-inflight") {
		t.Errorf("refusal does not name the mismatched concurrency:\n%s", stdout)
	}
}

// A null-engine run has no backend to attribute anything to, so assessing one
// against the ceiling must be refused rather than reported as a clean 100%.
func TestCalibrateCmd_RefusesNullEngineRun(t *testing.T) {
	_, tracePath, runPath := calibrateFixture(t)

	code, stdout, stderr := runCalibrateCLI([]string{
		"--trace", tracePath,
		"--against", runPath,
		"--trials", "2",
		"--warmup", "0",
		"--max-generator-share", "90",
	})
	if code != 1 {
		t.Fatalf("calibrate exit=%d want 1 (refused); stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "no backend to attribute to") {
		t.Errorf("refusal does not name the null-engine run:\n%s", stdout)
	}
}

func TestCalibrateCmd_RequiresTrace(t *testing.T) {
	code, _, stderr := runCalibrateCLI([]string{"--trials", "1"})
	if code != 2 {
		t.Fatalf("calibrate exit=%d want 2, stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "--trace is required") {
		t.Errorf("stderr does not explain the missing trace:\n%s", stderr)
	}
}

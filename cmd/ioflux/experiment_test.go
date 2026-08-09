package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

func runExperimentCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runExperiment(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// runTestExperiment writes a config against a small generated trace and runs it,
// returning the parsed output.
func runTestExperiment(t *testing.T, configBody string) (int, string, string, *results.PairedExperiment) {
	t.Helper()
	dir := t.TempDir()
	tracePath := genSmallTrace(t, dir)
	outPath := filepath.Join(dir, "exp.json")
	cfgPath := filepath.Join(dir, "exp.yaml")

	body := strings.ReplaceAll(configBody, "TRACE", tracePath)
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runExperimentCLI([]string{"--config", cfgPath, "-o", outPath})

	var pe *results.PairedExperiment
	if data, err := os.ReadFile(outPath); err == nil {
		pe = &results.PairedExperiment{}
		if err := json.Unmarshal(data, pe); err != nil {
			t.Fatalf("output is not a paired experiment: %v", err)
		}
	}
	return code, stdout, stderr, pe
}

func TestExperimentCmd_RunsBothArmsInterleaved(t *testing.T) {
	code, stdout, stderr, pe := runTestExperiment(t, `
claim: does a tighter in-flight cap slow this down?
trials: 6
warmup: 1
seed: 7
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`)
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%s", code, stderr)
	}
	if pe == nil {
		t.Fatal("no output written")
	}

	if got := len(pe.Baseline.Trials); got != 6 {
		t.Errorf("baseline trials = %d, want 6", got)
	}
	if got := len(pe.Treatment.Trials); got != 6 {
		t.Errorf("treatment trials = %d, want 6", got)
	}
	if got := len(pe.PairOrder); got != 6 {
		t.Errorf("pair order entries = %d, want 6", got)
	}
	if pe.Claim == "" {
		t.Error("claim not carried into the output")
	}
	if pe.Seed != 7 {
		t.Errorf("seed = %d, want 7", pe.Seed)
	}
	if len(pe.TreatmentVariables) != 1 || pe.TreatmentVariables[0] != "max_inflight" {
		t.Errorf("treatment variables = %v, want [max_inflight]", pe.TreatmentVariables)
	}
	// The arms really did run with different caps.
	if pe.Baseline.Trials[0].Plan.MaxInflight != 8 || pe.Treatment.Trials[0].Plan.MaxInflight != 1 {
		t.Errorf("arms did not use their configured caps: baseline=%d treatment=%d",
			pe.Baseline.Trials[0].Plan.MaxInflight, pe.Treatment.Trials[0].Plan.MaxInflight)
	}
	if !strings.Contains(stdout, "Paired difference over 6 pair(s)") {
		t.Errorf("stdout should report the paired difference; got:\n%s", stdout)
	}
}

// The within-pair order must be drawn, not fixed: if the treatment always ran
// second, anything the first run leaves behind would land on the same arm every
// time.
func TestExperimentCmd_RandomizesWithinPairOrder(t *testing.T) {
	_, _, _, pe := runTestExperiment(t, `
trials: 12
seed: 3
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`)
	if pe == nil {
		t.Fatal("no output written")
	}
	var baselineFirst, treatmentFirst int
	for _, arm := range pe.PairOrder {
		if arm == results.ArmBaseline {
			baselineFirst++
		} else {
			treatmentFirst++
		}
	}
	if baselineFirst == 0 || treatmentFirst == 0 {
		t.Errorf("within-pair order was not randomized: %d baseline-first, %d treatment-first",
			baselineFirst, treatmentFirst)
	}
}

// The same seed must reproduce the same ordering, or the randomization is
// arbitrary rather than reproducible.
func TestExperimentCmd_SeedReproducesOrdering(t *testing.T) {
	cfg := `
trials: 8
seed: 12345
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`
	_, _, _, first := runTestExperiment(t, cfg)
	_, _, _, second := runTestExperiment(t, cfg)
	if first == nil || second == nil {
		t.Fatal("no output written")
	}
	if strings.Join(first.PairOrder, ",") != strings.Join(second.PairOrder, ",") {
		t.Errorf("same seed produced different orderings:\n %v\n %v",
			first.PairOrder, second.PairOrder)
	}
}

// An experiment whose arms are identical measures nothing, and must say so
// rather than report a clean null result.
func TestExperimentCmd_WarnsWhenNoTreatment(t *testing.T) {
	code, stdout, stderr, pe := runTestExperiment(t, `
trials: 6
seed: 1
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: TRACE
  engine: mem
treatment: {}
`)
	if code != 0 {
		t.Fatalf("exit=%d, want 0; stderr=%s", code, stderr)
	}
	if pe == nil {
		t.Fatal("no output written")
	}
	if len(pe.TreatmentVariables) != 0 {
		t.Errorf("treatment variables = %v, want none", pe.TreatmentVariables)
	}
	if !strings.Contains(stderr, "no treatment variable") {
		t.Errorf("stderr should warn about the missing treatment; got %q", stderr)
	}
	if !strings.Contains(stdout, "Treatment: none") {
		t.Errorf("report should state there was no treatment; got:\n%s", stdout)
	}
}

// The stability policy applies to an experiment exactly as it does to an
// ad-hoc comparison.
func TestExperimentCmd_RefusesUnstableArms(t *testing.T) {
	code, stdout, _, _ := runTestExperiment(t, `
trials: 6
seed: 1
policy:
  min_trials: 6
  max_cv_percent: 0.0001
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`)
	if code != 1 {
		t.Fatalf("exit=%d, want 1 for a refused experiment", code)
	}
	if !strings.Contains(stdout, "Eligibility: INCOMPARABLE") {
		t.Errorf("output should state the verdict; got:\n%s", stdout)
	}
	if strings.Contains(stdout, "Paired difference") {
		t.Errorf("a refused experiment must not print a difference; got:\n%s", stdout)
	}
}

func TestExperimentCmd_RequiresConfigAndOutput(t *testing.T) {
	if code, _, _ := runExperimentCLI(nil); code != 2 {
		t.Errorf("exit=%d, want 2 with no flags", code)
	}
	if code, _, _ := runExperimentCLI([]string{"--config", "x.yaml"}); code != 2 {
		t.Errorf("exit=%d, want 2 without -o", code)
	}
}

func TestExperimentCmd_ReportsBadConfig(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "o.json")
	code, _, stderr := runExperimentCLI([]string{"--config", filepath.Join(dir, "missing.yaml"), "-o", out})
	if code != 2 {
		t.Errorf("exit=%d, want 2 for a missing config", code)
	}
	if !strings.Contains(stderr, "ioflux experiment:") {
		t.Errorf("stderr should report the failure; got %q", stderr)
	}
}

// A saved experiment must re-print from disk with the same verdict it was
// written with.
func TestExperimentCmd_OutputReprintsThroughReport(t *testing.T) {
	dir := t.TempDir()
	tracePath := genSmallTrace(t, dir)
	outPath := filepath.Join(dir, "exp.json")
	cfgPath := filepath.Join(dir, "exp.yaml")
	body := fmt.Sprintf(`
claim: reprint check
trials: 6
seed: 5
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: %s
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`, tracePath)
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := runExperimentCLI([]string{"--config", cfgPath, "-o", outPath}); code != 0 {
		t.Fatalf("experiment exit=%d; stderr=%s", code, stderr)
	}

	code, out, stderr := runReportCLI([]string{outPath})
	if code != 0 {
		t.Fatalf("report exit=%d; stderr=%s", code, stderr)
	}
	for _, want := range []string{"Claim:", "reprint check", "Treatment:", "Paired difference"} {
		if !strings.Contains(out, want) {
			t.Errorf("re-printed experiment missing %q; got:\n%s", want, out)
		}
	}
}

// A paired experiment is already a comparison; feeding two to the comparator is
// a different question and is refused rather than mishandled.
func TestReportCmd_RefusesTwoPairedExperiments(t *testing.T) {
	dir := t.TempDir()
	tracePath := genSmallTrace(t, dir)
	cfgPath := filepath.Join(dir, "exp.yaml")
	body := fmt.Sprintf(`
trials: 6
seed: 5
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: %s
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`, tracePath)
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	a := filepath.Join(dir, "a.json")
	b := filepath.Join(dir, "b.json")
	for _, out := range []string{a, b} {
		if code, _, stderr := runExperimentCLI([]string{"--config", cfgPath, "-o", out}); code != 0 {
			t.Fatalf("experiment exit=%d; stderr=%s", code, stderr)
		}
	}

	code, _, stderr := runReportCLI([]string{a, b})
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "already a comparison") {
		t.Errorf("stderr should explain the refusal; got %q", stderr)
	}
}

// The regression gate's exit codes are the contract CI depends on, so they are
// pinned here rather than left to the printed text. Each verdict gets its own
// code: a team that blocks a release on a regression usually wants to retry on
// undecided evidence, and one code for both takes that choice away.

// No declared threshold means the difference is reported and nothing is decided.
// The absence of a gate must not read as a passing one.
func TestExperimentCmd_NoThresholdDecidesNothing(t *testing.T) {
	code, stdout, stderr, pe := runTestExperiment(t, `
trials: 6
seed: 3
policy:
  min_trials: 6
  max_cv_percent: 500
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`)
	if code != 0 {
		t.Fatalf("exit=%d, want 0 when no threshold is declared; stderr=%s", code, stderr)
	}
	if pe.Regression.Verdict != results.RegressionNotAssessed {
		t.Errorf("verdict = %q, want not_assessed", pe.Regression.Verdict)
	}
	if pe.Regression.Regressed() {
		t.Error("Regressed() true with no threshold declared")
	}
	if strings.Contains(stdout, "Regression gate:") {
		t.Errorf("no gate line should print when none was declared; got:\n%s", stdout)
	}
}

// A threshold wide enough to contain any real interval must pass and exit 0.
func TestExperimentCmd_GatePassExitsZero(t *testing.T) {
	code, stdout, stderr, pe := runTestExperiment(t, `
trials: 6
seed: 3
policy:
  min_trials: 6
  max_cv_percent: 500
  max_duration_regression_percent: 100000
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`)
	if code != 0 {
		t.Fatalf("exit=%d, want 0 for a passing gate; stderr=%s", code, stderr)
	}
	if pe.Regression.Verdict != results.RegressionPass {
		t.Fatalf("verdict = %q, want pass", pe.Regression.Verdict)
	}
	if !strings.Contains(stdout, "Regression gate: PASS") {
		t.Errorf("output should state the verdict; got:\n%s", stdout)
	}
}

// Evidence the tool already refused must not acquire a verdict by being measured
// against a threshold. This is the property that keeps a refusal from silently
// becoming a release approval.
func TestExperimentCmd_RefusedEvidenceGetsNoVerdict(t *testing.T) {
	code, stdout, _, pe := runTestExperiment(t, `
trials: 6
seed: 1
policy:
  min_trials: 6
  max_cv_percent: 0.0001
  max_duration_regression_percent: 7
run:
  trace: TRACE
  engine: mem
  max_inflight: 8
treatment:
  max_inflight: 1
`)
	if code != 1 {
		t.Fatalf("exit=%d, want 1 for refused evidence", code)
	}
	if pe.Regression.Verdict != results.RegressionNotAssessed {
		t.Errorf("verdict = %q, want not_assessed for refused evidence", pe.Regression.Verdict)
	}
	if strings.Contains(stdout, "Regression gate:") {
		t.Errorf("a refused experiment must not print a gate verdict; got:\n%s", stdout)
	}
}

// The exit-code mapping is the CI contract, pinned directly rather than through
// a replay: forcing a real regression of a known size on a shared machine makes
// the test measure the host's mood, and a gate test that flakes is worse than
// none. The verdict logic itself is covered in pkg/results.
func TestRegressionExitCodes(t *testing.T) {
	cases := []struct {
		verdict results.RegressionVerdict
		want    int
	}{
		{results.RegressionPass, 0},
		{results.RegressionNotAssessed, 0},
		{results.RegressionFail, 3},
		{results.RegressionInconclusive, 4},
	}
	for _, tc := range cases {
		if got := regressionExitCode(tc.verdict); got != tc.want {
			t.Errorf("regressionExitCode(%q) = %d, want %d", tc.verdict, got, tc.want)
		}
	}
}

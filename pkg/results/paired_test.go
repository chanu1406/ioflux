package results_test

import (
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// pairedOf builds a paired experiment from matching per-pair durations.
func pairedOf(t *testing.T, baseline, treatment []int64, treatmentVars ...string) *results.PairedExperiment {
	t.Helper()
	if len(baseline) != len(treatment) {
		t.Fatalf("arms must be the same length: %d vs %d", len(baseline), len(treatment))
	}
	bt := make([]*results.Results, 0, len(baseline))
	tt := make([]*results.Results, 0, len(treatment))
	order := make([]string, 0, len(baseline))
	for i := range baseline {
		bt = append(bt, trialWithDuration(baseline[i]))
		tt = append(tt, trialWithDuration(treatment[i]))
		order = append(order, results.ArmBaseline)
	}
	if treatmentVars == nil {
		treatmentVars = []string{"max_inflight"}
	}
	return results.BuildPaired("test claim",
		results.BuildTrialSet(bt, 0), results.BuildTrialSet(tt, 0),
		results.TrialPolicy{MinValidTrials: 6, MaxCVPercent: 0},
		42, order, treatmentVars)
}

// The paired difference is computed within pairs, which is the whole reason to
// interleave: a drift affecting both arms equally cancels.
func TestPairedDifferenceCancelsSharedDrift(t *testing.T) {
	// Both arms drift upward together; the treatment is +10 in every pair.
	base := []int64{100, 120, 140, 160, 180, 200, 220, 240}
	treat := []int64{110, 130, 150, 170, 190, 210, 230, 250}

	pe := pairedOf(t, base, treat)

	if pe.Paired.Pairs != 8 {
		t.Fatalf("pairs = %d, want 8", pe.Paired.Pairs)
	}
	// Every pairwise difference is exactly 10 despite a 2.4x drift across the run.
	if got := pe.Paired.Delta.Median; got != 10 {
		t.Errorf("median paired difference = %v, want 10", got)
	}
	if got := pe.Paired.Delta.CVPercent; got != 0 {
		t.Errorf("paired differences should have no spread here; CV = %v", got)
	}
	if !pe.Paired.ExcludesZero {
		t.Error("a consistent +10 difference should exclude zero")
	}
}

// Comparing the arms' independent medians would be swamped by the same drift
// the pairing removes — the property that justifies interleaving at all.
func TestPairedDifferenceIsSharperThanIndependentMedians(t *testing.T) {
	base := []int64{100, 120, 140, 160, 180, 200, 220, 240}
	treat := []int64{110, 130, 150, 170, 190, 210, 230, 250}

	pe := pairedOf(t, base, treat)

	// The arms' own spreads are large...
	if pe.Baseline.Summary.DurationNS.CVPercent < 20 {
		t.Fatalf("expected a wide per-arm spread, got %v", pe.Baseline.Summary.DurationNS.CVPercent)
	}
	// ...yet the paired interval is tight enough to exclude zero.
	if !pe.Paired.Delta.CI95Available || !pe.Paired.ExcludesZero {
		t.Errorf("paired interval failed to resolve a difference the arms' spread hides: %+v", pe.Paired)
	}
}

func TestPairedNoDifferenceDoesNotExcludeZero(t *testing.T) {
	vals := []int64{100, 101, 99, 100, 102, 98, 101, 99}
	pe := pairedOf(t, vals, vals)

	if pe.Paired.Delta.Median != 0 {
		t.Errorf("identical arms produced a difference: %v", pe.Paired.Delta.Median)
	}
	if pe.Paired.ExcludesZero {
		t.Error("identical arms reported a difference excluding zero")
	}
}

func TestPairedDeltaPercentIsRelativeToBaseline(t *testing.T) {
	base := []int64{100, 100, 100, 100, 100, 100}
	treat := []int64{150, 150, 150, 150, 150, 150}

	pe := pairedOf(t, base, treat)

	if pe.Paired.DeltaPercent != 50 {
		t.Errorf("delta percent = %v, want 50", pe.Paired.DeltaPercent)
	}
}

// A pair with a failed half cannot be differenced: the failure's duration is
// not a measurement of anything.
func TestPairedExcludesPairsWithAFailedHalf(t *testing.T) {
	bt := make([]*results.Results, 0, 8)
	tt := make([]*results.Results, 0, 8)
	order := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		b := trialWithDuration(100)
		tr := trialWithDuration(110)
		if i == 3 {
			tr.Errors = 1
			tr.DurationNS = 999999
		}
		bt = append(bt, b)
		tt = append(tt, tr)
		order = append(order, results.ArmBaseline)
	}

	pe := results.BuildPaired("c",
		results.BuildTrialSet(bt, 0), results.BuildTrialSet(tt, 0),
		results.TrialPolicy{MinValidTrials: 6, MaxCVPercent: 0}, 1, order, []string{"engine"})

	if pe.Paired.Pairs != 7 {
		t.Errorf("differenced pairs = %d, want 7 (the failed pair excluded)", pe.Paired.Pairs)
	}
	if pe.Paired.Delta.Median != 10 {
		t.Errorf("median difference = %v, want 10 — the failed pair must not move it", pe.Paired.Delta.Median)
	}
	// The failure still blocks the conclusion.
	if pe.Eligibility.Verdict != results.VerdictIncomparable {
		t.Errorf("verdict = %q, want %q", pe.Eligibility.Verdict, results.VerdictIncomparable)
	}
}

// A declared treatment is not uncontrolled drift; warning about the change the
// experiment exists to measure teaches readers to skip warnings.
func TestPairedDropsDeclaredTreatmentFromCaveats(t *testing.T) {
	bt := make([]*results.Results, 0, 8)
	tt := make([]*results.Results, 0, 8)
	order := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		b := trialWithDuration(100)
		tr := trialWithDuration(110)
		tr.RunEnv.CacheMode = "warm" // the declared treatment
		tr.Plan.Engine = "s3"        // NOT declared: uncontrolled drift
		bt = append(bt, b)
		tt = append(tt, tr)
		order = append(order, results.ArmBaseline)
	}

	pe := results.BuildPaired("c",
		results.BuildTrialSet(bt, 0), results.BuildTrialSet(tt, 0),
		results.TrialPolicy{MinValidTrials: 6, MaxCVPercent: 0}, 1, order, []string{"cache_mode"})

	if hasField(pe.Eligibility, "cache mode") {
		t.Errorf("declared treatment reported as a caveat; got %v", fieldsOf(pe.Eligibility))
	}
	if !hasField(pe.Eligibility, "engine") {
		t.Errorf("undeclared difference must still be reported; got %v", fieldsOf(pe.Eligibility))
	}
}

// The verdict is decided before the declared treatment is discounted, so it has
// to be recomputed afterwards — otherwise an experiment whose only difference
// was its own treatment reports CAVEATED above an empty list of caveats.
func TestPairedVerdictMatchesRemainingCaveats(t *testing.T) {
	bt := make([]*results.Results, 0, 8)
	tt := make([]*results.Results, 0, 8)
	order := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		b := trialWithDuration(100)
		tr := trialWithDuration(110)
		tr.RunEnv.CacheMode = "warm" // the one declared treatment
		bt = append(bt, b)
		tt = append(tt, tr)
		order = append(order, results.ArmBaseline)
	}

	pe := results.BuildPaired("c",
		results.BuildTrialSet(bt, 0), results.BuildTrialSet(tt, 0),
		results.TrialPolicy{MinValidTrials: 6, MaxCVPercent: 0}, 1, order, []string{"cache_mode"})

	if len(pe.Eligibility.Caveats) != 0 {
		t.Fatalf("expected no caveats once the treatment is discounted; got %v", fieldsOf(pe.Eligibility))
	}
	if pe.Eligibility.Verdict != results.VerdictComparable {
		t.Errorf("verdict = %q with no caveats remaining, want %q",
			pe.Eligibility.Verdict, results.VerdictComparable)
	}
}

// An experiment whose arms are identical measures nothing, and that is a
// property of the design rather than of the data.
func TestPairedFlagsMissingTreatment(t *testing.T) {
	vals := []int64{100, 100, 100, 100, 100, 100}
	pe := pairedOf(t, vals, vals, []string{}...)

	if !hasField(pe.Eligibility, "treatment") {
		t.Fatalf("expected a treatment caveat; got %v", fieldsOf(pe.Eligibility))
	}
	if pe.Eligibility.Verdict == results.VerdictComparable {
		t.Error("an experiment with no treatment must not read as a clean comparison")
	}
}

func TestPairedRecordsProvenanceAndDesign(t *testing.T) {
	pe := pairedOf(t, []int64{1, 2, 3, 4, 5, 6}, []int64{1, 2, 3, 4, 5, 6})

	if pe.SchemaVersion != results.SchemaVersion {
		t.Errorf("schema version = %d, want %d", pe.SchemaVersion, results.SchemaVersion)
	}
	if pe.Claim != "test claim" {
		t.Errorf("claim = %q, want it preserved", pe.Claim)
	}
	if pe.Seed != 42 {
		t.Errorf("seed = %d, want 42 — an unrecorded randomization is not reproducible", pe.Seed)
	}
	if len(pe.PairOrder) != 6 {
		t.Errorf("pair order length = %d, want 6", len(pe.PairOrder))
	}
	if pe.Tool.Version == "" || pe.Host.OS == "" {
		t.Error("paired experiment missing provenance")
	}
}

// Too few pairs to bound the difference must read as "nothing established",
// never as "no difference".
func TestPairedTooFewPairsHasNoInterval(t *testing.T) {
	pe := results.BuildPaired("c",
		results.BuildTrialSet([]*results.Results{trialWithDuration(100), trialWithDuration(100)}, 0),
		results.BuildTrialSet([]*results.Results{trialWithDuration(200), trialWithDuration(200)}, 0),
		results.TrialPolicy{MinValidTrials: 2, MaxCVPercent: 0}, 1,
		[]string{results.ArmBaseline, results.ArmBaseline}, []string{"engine"})

	if pe.Paired.Delta.CI95Available {
		t.Error("two pairs cannot bound a difference")
	}
	if pe.Paired.ExcludesZero {
		t.Error("a difference was asserted without an interval to support it")
	}
}

// The per-arm gate still applies to a paired experiment.
func TestPairedInheritsArmGate(t *testing.T) {
	pe := results.BuildPaired("c",
		results.BuildTrialSet([]*results.Results{trialWithDuration(100)}, 0),
		results.BuildTrialSet([]*results.Results{trialWithDuration(100)}, 0),
		results.DefaultTrialPolicy(), 1, []string{results.ArmBaseline}, []string{"engine"})

	if pe.Eligibility.Verdict != results.VerdictIncomparable {
		t.Fatalf("verdict = %q, want %q", pe.Eligibility.Verdict, results.VerdictIncomparable)
	}
	if !strings.Contains(strings.Join(pe.Eligibility.Blocking, "\n"), "policy requires at least 6") {
		t.Errorf("blocking should name the trial-count policy; got %v", pe.Eligibility.Blocking)
	}
}

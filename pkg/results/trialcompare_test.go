package results_test

import (
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// steadySet builds a set of n valid trials all taking d nanoseconds, so its CV
// is zero and only the property under test varies.
func steadySet(n int, d int64) *results.TrialSet {
	trials := make([]*results.Results, 0, n)
	for i := 0; i < n; i++ {
		trials = append(trials, trialWithDuration(d))
	}
	return results.BuildTrialSet(trials, 0)
}

func blockingText(tc results.TrialComparison) string {
	return strings.Join(tc.Eligibility.Blocking, "\n")
}

func TestCompareTrialSetsComparableWhenStableAndSufficient(t *testing.T) {
	a := steadySet(10, 100)
	b := steadySet(10, 100)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if tc.Eligibility.Verdict != results.VerdictComparable {
		t.Fatalf("verdict = %q, want %q; blocking=%v caveats=%v",
			tc.Eligibility.Verdict, results.VerdictComparable, tc.Eligibility.Blocking, tc.Eligibility.Caveats)
	}
	if tc.DeltaMedianNS != 0 || tc.DeltaPercent != 0 {
		t.Errorf("identical sets reported a delta: %v ns (%v%%)", tc.DeltaMedianNS, tc.DeltaPercent)
	}
}

// Too few trials cannot support a conclusion, and the refusal must say how many
// were needed rather than only that the set was rejected.
func TestCompareTrialSetsRefusesTooFewTrials(t *testing.T) {
	a := steadySet(10, 100)
	b := steadySet(3, 100)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if tc.Eligibility.Verdict != results.VerdictIncomparable {
		t.Fatalf("verdict = %q, want %q", tc.Eligibility.Verdict, results.VerdictIncomparable)
	}
	if !strings.Contains(blockingText(tc), "3 valid trial(s), policy requires at least 6") {
		t.Errorf("blocking should state the shortfall; got %q", blockingText(tc))
	}
}

// A spread wider than the difference being looked for cannot establish it.
func TestCompareTrialSetsRefusesUnstableSet(t *testing.T) {
	a := steadySet(10, 100)
	// Durations spanning 50..150 give a CV far above the 5% policy.
	noisy := []*results.Results{}
	for _, d := range []int64{50, 60, 80, 100, 120, 140, 150, 90, 110, 70} {
		noisy = append(noisy, trialWithDuration(d))
	}
	b := results.BuildTrialSet(noisy, 0)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if tc.Eligibility.Verdict != results.VerdictIncomparable {
		t.Fatalf("verdict = %q, want %q (CV was %.1f%%)",
			tc.Eligibility.Verdict, results.VerdictIncomparable, b.Summary.DurationNS.CVPercent)
	}
	if !strings.Contains(blockingText(tc), "policy allows 5.0%") {
		t.Errorf("blocking should name the policy limit; got %q", blockingText(tc))
	}
}

// Stability is only checked once there are enough trials for the number to
// mean something — otherwise a two-trial set would be rejected for a spread
// that was never measurable, hiding the real problem (too few trials).
func TestCompareTrialSetsReportsTrialCountBeforeStability(t *testing.T) {
	a := steadySet(10, 100)
	b := results.BuildTrialSet([]*results.Results{
		trialWithDuration(10), trialWithDuration(1000),
	}, 0)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	text := blockingText(tc)
	if !strings.Contains(text, "2 valid trial(s)") {
		t.Errorf("blocking should report the trial shortfall; got %q", text)
	}
	if strings.Contains(text, "policy allows") {
		t.Errorf("stability must not be judged on too few trials; got %q", text)
	}
}

// A failed trial blocks the comparison and is identified by position.
func TestCompareTrialSetsRefusesFailedTrial(t *testing.T) {
	a := steadySet(10, 100)
	trials := make([]*results.Results, 0, 10)
	for i := 0; i < 10; i++ {
		r := trialWithDuration(100)
		if i == 4 {
			r.Errors = 2
		}
		trials = append(trials, r)
	}
	b := results.BuildTrialSet(trials, 0)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if tc.Eligibility.Verdict != results.VerdictIncomparable {
		t.Fatalf("verdict = %q, want %q", tc.Eligibility.Verdict, results.VerdictIncomparable)
	}
	if !strings.Contains(blockingText(tc), "trial [5]") {
		t.Errorf("blocking should identify the failed trial's position; got %q", blockingText(tc))
	}
}

// Everything that makes two single runs incomparable still applies to sets.
func TestCompareTrialSetsInheritsPerRunCaveats(t *testing.T) {
	a := steadySet(10, 100)
	b := steadySet(10, 100)
	for _, tr := range b.Trials {
		tr.RunEnv.CacheMode = "warm"
	}

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if tc.Eligibility.Verdict != results.VerdictCaveated {
		t.Fatalf("verdict = %q, want %q", tc.Eligibility.Verdict, results.VerdictCaveated)
	}
	if !hasField(tc.Eligibility, "cache mode") {
		t.Errorf("expected the per-run cache caveat to carry through; got %v", fieldsOf(tc.Eligibility))
	}
}

// Disjoint intervals are evidence of a difference.
func TestCompareTrialSetsDetectsSeparatedMedians(t *testing.T) {
	a := steadySet(10, 100)
	b := steadySet(10, 200)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if !tc.SeparationKnown || !tc.Separated {
		t.Errorf("clearly different medians not separated: known=%v separated=%v",
			tc.SeparationKnown, tc.Separated)
	}
	if tc.DeltaMedianNS != 100 {
		t.Errorf("delta = %v, want 100", tc.DeltaMedianNS)
	}
	if tc.DeltaPercent != 100 {
		t.Errorf("delta percent = %v, want 100", tc.DeltaPercent)
	}
}

// Overlapping intervals mean "not established", never "no difference" — the
// asymmetry the report's wording depends on.
func TestCompareTrialSetsOverlappingIntervalsAreNotSeparated(t *testing.T) {
	mk := func(ds ...int64) *results.TrialSet {
		trials := make([]*results.Results, 0, len(ds))
		for _, d := range ds {
			trials = append(trials, trialWithDuration(d))
		}
		return results.BuildTrialSet(trials, 0)
	}
	a := mk(100, 102, 98, 101, 99, 103, 97, 100, 101, 99)
	b := mk(101, 103, 99, 102, 100, 104, 98, 101, 102, 100)

	tc := results.CompareTrialSets(a, b, results.DefaultTrialPolicy())

	if !tc.SeparationKnown {
		t.Fatal("separation should be computable for 10 trials a side")
	}
	if tc.Separated {
		t.Error("overlapping intervals reported as separated")
	}
}

// With too few trials to bound either median, separation carries no
// information and must be reported as unknown rather than false.
func TestCompareTrialSetsSeparationUnknownWithoutIntervals(t *testing.T) {
	policy := results.TrialPolicy{MinValidTrials: 2, MaxCVPercent: 0}
	a := steadySet(3, 100)
	b := steadySet(3, 500)

	tc := results.CompareTrialSets(a, b, policy)

	if tc.SeparationKnown {
		t.Error("separation reported as known without intervals")
	}
	if tc.Separated {
		t.Error("separation asserted without intervals to support it")
	}
}

// A zero MaxCVPercent disables the stability gate rather than rejecting
// everything, so a fixture can opt out deliberately.
func TestCompareTrialSetsZeroCVPolicyDisablesStabilityGate(t *testing.T) {
	noisy := []*results.Results{}
	for _, d := range []int64{10, 500, 20, 900, 30, 700} {
		noisy = append(noisy, trialWithDuration(d))
	}
	set := results.BuildTrialSet(noisy, 0)

	tc := results.CompareTrialSets(set, set, results.TrialPolicy{MinValidTrials: 6, MaxCVPercent: 0})

	if strings.Contains(blockingText(tc), "policy allows") {
		t.Errorf("zero CV policy should disable the stability gate; got %q", blockingText(tc))
	}
}

func TestCompareTrialSetsRefusesEmptySet(t *testing.T) {
	empty := results.BuildTrialSet(nil, 0)

	tc := results.CompareTrialSets(empty, steadySet(10, 100), results.DefaultTrialPolicy())

	if tc.Eligibility.Verdict != results.VerdictIncomparable {
		t.Errorf("verdict = %q, want %q", tc.Eligibility.Verdict, results.VerdictIncomparable)
	}
}

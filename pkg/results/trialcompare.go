package results

import "fmt"

// TrialPolicy is the rule set a trial comparison must satisfy to produce a
// conclusion. It is fixed before measured trials, not chosen after seeing them.
//
// The values here are floors, not universal defaults. A fixture declares its
// own: qualification/FIXTURE.md requires ten trials at 5% for qual-01, and a
// noisier workload or a costlier decision needs different ones.
type TrialPolicy struct {
	// MinValidTrials is the smallest number of valid trials a conclusion may
	// rest on.
	MinValidTrials int `json:"min_valid_trials"`
	// MaxCVPercent is the widest run-to-run spread a conclusion may rest on.
	MaxCVPercent float64 `json:"max_cv_percent"`
}

// DefaultTrialPolicy returns the built-in floor.
//
// Six trials is not a statistical recommendation — it is the point below which
// no 95% median interval exists at all (see medianCIBounds), so a smaller set
// cannot state its own uncertainty. Five percent follows §11.3's example, whose
// own text warns that it is calibrated to a fixture rather than universal.
func DefaultTrialPolicy() TrialPolicy {
	return TrialPolicy{MinValidTrials: 6, MaxCVPercent: 5}
}

// TrialComparison is the outcome of comparing two trial sets.
type TrialComparison struct {
	Eligibility Eligibility  `json:"eligibility"`
	Policy      TrialPolicy  `json:"policy"`
	A           TrialSummary `json:"a"`
	B           TrialSummary `json:"b"`
	// DeltaMedianNS is B's median duration minus A's; DeltaPercent expresses it
	// relative to A. Positive means B took longer.
	DeltaMedianNS float64 `json:"delta_median_ns"`
	DeltaPercent  float64 `json:"delta_percent"`
	// Separated reports whether the two medians' 95% intervals are disjoint.
	//
	// Disjoint intervals are evidence that the medians genuinely differ.
	// Overlapping ones are not evidence that they do not: this test is
	// conservative, and a real difference can still produce overlap. It is
	// reported as "not separated", never as "no difference".
	Separated bool `json:"separated"`
	// SeparationKnown is false when either side lacked the trials to form an
	// interval, in which case Separated carries no information.
	SeparationKnown bool `json:"separation_known"`
}

// CompareTrialSets checks whether two trial sets may be compared under policy
// and, if so, quantifies the difference between them.
//
// It layers on CheckEligibility rather than replacing it: everything that makes
// two single runs incomparable still makes two sets of them incomparable, and
// the set-level rules — enough trials, stable enough, no failed trial — are
// added on top.
func CompareTrialSets(a, b *TrialSet, policy TrialPolicy) TrialComparison {
	tc := TrialComparison{Policy: policy, A: a.Summary, B: b.Summary}

	repA, repB := a.Representative(), b.Representative()
	if repA == nil || repB == nil {
		tc.Eligibility = Eligibility{
			Verdict:  VerdictIncomparable,
			Blocking: []string{"a trial set contains no trials"},
		}
		return tc
	}

	// The per-run gate sees a representative trial, so the plan, environment and
	// build checks apply unchanged. Execution validity is re-checked per trial
	// below, across every trial rather than only the representative.
	tc.Eligibility = CheckEligibility(repA, repB)
	tc.Eligibility.Blocking = nil

	for _, side := range []struct {
		label string
		set   *TrialSet
	}{{"A", a}, {"B", b}} {
		tc.Eligibility.Blocking = append(tc.Eligibility.Blocking, trialSetBlockers(side.label, side.set, policy)...)
	}

	tc.Eligibility.Verdict = verdictFor(tc.Eligibility)

	da, db := a.Summary.DurationNS, b.Summary.DurationNS
	tc.DeltaMedianNS = db.Median - da.Median
	if da.Median != 0 {
		tc.DeltaPercent = tc.DeltaMedianNS / da.Median * 100
	}
	if da.CI95Available && db.CI95Available {
		tc.SeparationKnown = true
		tc.Separated = da.CI95Hi < db.CI95Lo || db.CI95Hi < da.CI95Lo
	}
	return tc
}

// trialSetBlockers returns the reasons a trial set cannot support a conclusion.
func trialSetBlockers(label string, ts *TrialSet, policy TrialPolicy) []string {
	var out []string
	s := ts.Summary

	// A failed trial is not a slow trial. Its duration measures how long the
	// failure took, so it can neither be averaged in nor quietly dropped.
	if s.FailedTrials > 0 {
		out = append(out, fmt.Sprintf("%s: %d of %d trial(s) did not execute validly (trial %v)",
			label, s.FailedTrials, s.Trials, ts.FailedTrialIndexes()))
	}
	if s.ValidTrials < policy.MinValidTrials {
		out = append(out, fmt.Sprintf("%s: %d valid trial(s), policy requires at least %d",
			label, s.ValidTrials, policy.MinValidTrials))
	}
	// Checked only when there are enough trials for the number to mean
	// something; a coefficient of variation over two runs is not a stability
	// measurement, and reporting it as one would replace a known gap with a
	// wrong answer.
	if s.ValidTrials >= policy.MinValidTrials && policy.MaxCVPercent > 0 &&
		s.DurationNS.CVPercent > policy.MaxCVPercent {
		out = append(out, fmt.Sprintf(
			"%s: duration varied by %.1f%% between trials, policy allows %.1f%% — "+
				"the run-to-run spread is wider than the difference it would be used to detect",
			label, s.DurationNS.CVPercent, policy.MaxCVPercent))
	}
	return out
}

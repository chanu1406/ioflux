package results

import (
	"fmt"
	"math"
)

// RegressionVerdict is a gate's decision about a measured difference.
//
// There are three outcomes, not two, and the third is the reason the gate is
// worth having. A gate that only answers pass/fail has to convert "these trials
// could not tell" into one of them: call it a pass and a real regression ships
// whenever the run was noisy; call it a failure and every noisy run blocks a
// release until someone learns to re-run until it goes green. Naming the
// undecided case keeps both mistakes visible.
type RegressionVerdict string

const (
	// RegressionPass means the whole interval on the difference lies within the
	// threshold: the effect is confidently no worse than the budget allows.
	RegressionPass RegressionVerdict = "pass"
	// RegressionFail means the whole interval lies beyond the threshold: the
	// effect is confidently worse than the budget allows.
	RegressionFail RegressionVerdict = "regression"
	// RegressionInconclusive means the interval spans the threshold. These trials
	// are consistent both with a passing effect and with a regression, so they
	// decide nothing. More trials or a quieter host, not a re-roll.
	RegressionInconclusive RegressionVerdict = "inconclusive"
	// RegressionNotAssessed means no decision was attempted: no threshold was
	// declared, the evidence was ineligible, or there were too few trials to
	// state an interval at all.
	RegressionNotAssessed RegressionVerdict = "not_assessed"
)

// RegressionGate is the decision a declared threshold produces from a measured
// difference, together with the numbers it was decided from.
//
// The gate is deliberately expressed against the *interval* rather than the
// point estimate. Comparing a median difference to a threshold directly would
// make the verdict flip on run-to-run noise whenever the true effect sits near
// the threshold — which is exactly where the decision matters most, and exactly
// where a flapping gate teaches people to ignore it.
type RegressionGate struct {
	Verdict RegressionVerdict `json:"verdict"`
	// ThresholdPercent is the largest duration increase that still passes.
	ThresholdPercent float64 `json:"threshold_percent"`
	// ObservedPercent is the median paired difference relative to the baseline
	// median. Positive means the treatment took longer.
	ObservedPercent float64 `json:"observed_percent"`
	// IntervalLoPercent and IntervalHiPercent bound the difference at 95%,
	// expressed in the same units as the threshold. Meaningful only when
	// IntervalAvailable.
	IntervalLoPercent float64 `json:"interval_lo_percent"`
	IntervalHiPercent float64 `json:"interval_hi_percent"`
	IntervalAvailable bool    `json:"interval_available"`
	// Reason states why the verdict is what it is, in the terms a reader needs to
	// act on it.
	Reason string `json:"reason"`
}

// Assessed reports whether a decision was actually reached.
func (g RegressionGate) Assessed() bool {
	return g.Verdict != RegressionNotAssessed
}

// Regressed reports whether the gate found a regression. It is false for
// inconclusive and unassessed evidence, so a caller that treats it as "may
// ship" must check Assessed too — an undecided gate is not a pass.
func (g RegressionGate) Regressed() bool { return g.Verdict == RegressionFail }

// EvaluateRegression decides a paired experiment against a duration-regression
// threshold, expressed as the percentage increase in the primary metric that is
// still acceptable. A threshold of 0 or less disables the gate.
//
// The metric is wall-clock duration, not throughput. The two are not
// interchangeable at the threshold: a 7% throughput loss is a 7.5% duration
// increase, and a gate that quietly swapped them would sit at a threshold
// nobody chose. Callers wanting a throughput budget should convert deliberately.
//
// Ineligible evidence is never converted into a pass. A comparison the tool
// already refused cannot acquire a verdict by being measured against a number.
func EvaluateRegression(pe *PairedExperiment, thresholdPercent float64) RegressionGate {
	g := RegressionGate{
		Verdict:          RegressionNotAssessed,
		ThresholdPercent: thresholdPercent,
	}
	if pe == nil {
		g.Reason = "no experiment to assess"
		return g
	}

	g.ObservedPercent = pe.Paired.DeltaPercent

	if thresholdPercent <= 0 {
		g.Reason = "no regression threshold declared, so no pass/fail decision was made"
		return g
	}
	if !pe.Eligibility.Comparable() {
		g.Reason = "the comparison was refused as incomparable, so no threshold decision is possible"
		return g
	}
	if len(pe.TreatmentVariables) == 0 {
		g.Reason = "the two arms were configured identically, so there is no treatment to hold to a threshold"
		return g
	}

	baseMedian := 0.0
	if pe.Baseline != nil {
		baseMedian = pe.Baseline.Summary.DurationNS.Median
	}
	if !pe.Paired.Delta.CI95Available || baseMedian == 0 {
		g.Reason = "too few pairs to bound the difference, so it cannot be placed against the threshold"
		return g
	}

	g.IntervalAvailable = true
	g.IntervalLoPercent = pe.Paired.Delta.CI95Lo / baseMedian * 100
	g.IntervalHiPercent = pe.Paired.Delta.CI95Hi / baseMedian * 100

	// A difference of exactly the threshold does not land on it in binary: 7 ms
	// against a 100 ms median divides and rescales to 7.000000000000001%. Without
	// a tolerance the gate would fire at precisely the value the team chose,
	// which — thresholds being set at round numbers — is where measurements
	// cluster. The comparison is therefore "beyond the threshold by more than
	// representation error", not "greater than".
	epsilon := math.Abs(thresholdPercent) * 1e-9
	if epsilon == 0 {
		epsilon = 1e-9
	}
	limit := thresholdPercent + epsilon

	switch {
	case g.IntervalLoPercent > limit:
		g.Verdict = RegressionFail
		g.Reason = fmt.Sprintf(
			"the whole 95%% interval (%+.1f%% … %+.1f%%) lies beyond the %g%% threshold",
			g.IntervalLoPercent, g.IntervalHiPercent, thresholdPercent)
	case g.IntervalHiPercent <= limit:
		g.Verdict = RegressionPass
		g.Reason = fmt.Sprintf(
			"the whole 95%% interval (%+.1f%% … %+.1f%%) lies within the %g%% threshold",
			g.IntervalLoPercent, g.IntervalHiPercent, thresholdPercent)
	default:
		g.Verdict = RegressionInconclusive
		g.Reason = fmt.Sprintf(
			"the 95%% interval (%+.1f%% … %+.1f%%) spans the %g%% threshold, so these pairs "+
				"are consistent both with passing and with a regression",
			g.IntervalLoPercent, g.IntervalHiPercent, thresholdPercent)
	}
	return g
}

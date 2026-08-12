package results

import (
	"fmt"
	"math"
)

// RegressionVerdict is a gate's decision about a measured difference. There are
// three outcomes rather than two: a pass/fail gate has to convert "these trials
// could not tell" into one of them, and either choice hides a real mistake.
type RegressionVerdict string

const (
	// RegressionPass means the whole interval on the difference lies within the
	// threshold: the effect is confidently no worse than the budget allows.
	RegressionPass RegressionVerdict = "pass"
	// RegressionFail means the whole interval lies beyond the threshold: the
	// effect is confidently worse than the budget allows.
	RegressionFail RegressionVerdict = "regression"
	// RegressionInconclusive means the interval spans the threshold, so these
	// trials decide nothing. The remedy is more trials or a quieter host.
	RegressionInconclusive RegressionVerdict = "inconclusive"
	// RegressionNotAssessed means no decision was attempted: no threshold was
	// declared, the evidence was ineligible, or there were too few trials to
	// state an interval at all.
	RegressionNotAssessed RegressionVerdict = "not_assessed"
)

// RegressionGate is the decision a declared threshold produces from a measured
// difference, together with the numbers it was decided from. It is judged
// against the interval, not the median: a median comparison flips on noise
// whenever the true effect sits near the threshold.
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
	Reason            string  `json:"reason"`
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
// The metric is wall-clock duration, not throughput: a 7% throughput loss is a
// 7.5% duration increase, so callers wanting a throughput budget convert
// deliberately. Ineligible evidence never becomes a pass.
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

	// 7 ms against a 100 ms median rescales to 7.000000000000001%, so an exact
	// threshold would fire at the very value the team chose. Compare beyond the
	// threshold by more than representation error, not simply greater than.
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

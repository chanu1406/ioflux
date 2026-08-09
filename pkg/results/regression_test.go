package results_test

import (
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// gateFor builds a paired experiment whose differenced pairs carry a known
// interval, so the gate can be exercised against exact boundaries rather than
// against whatever a replay happened to produce.
//
// baseMedian fixes the denominator; loNS/hiNS fix the interval on the
// difference. Both arms are otherwise identical and valid.
func gateFor(t *testing.T, baseMedian, loNS, hiNS float64, threshold float64) results.RegressionGate {
	t.Helper()
	pe := &results.PairedExperiment{
		TreatmentVariables: []string{"trace"},
		Eligibility:        results.Eligibility{Verdict: results.VerdictComparable},
		Baseline: &results.TrialSet{
			Summary: results.TrialSummary{DurationNS: results.MetricSummary{Median: baseMedian}},
		},
		Paired: results.PairedSummary{
			Pairs: 10,
			Delta: results.MetricSummary{
				Median:        (loNS + hiNS) / 2,
				CI95Lo:        loNS,
				CI95Hi:        hiNS,
				CI95Available: true,
			},
		},
	}
	if baseMedian != 0 {
		pe.Paired.DeltaPercent = pe.Paired.Delta.Median / baseMedian * 100
	}
	return results.EvaluateRegression(pe, threshold)
}

func TestRegressionGateVerdicts(t *testing.T) {
	const base = 100_000_000 // 100 ms, so 1 ms of delta is 1%

	cases := []struct {
		name      string
		lo, hi    float64
		threshold float64
		want      results.RegressionVerdict
	}{
		{
			// Entirely inside the budget, including a treatment that is faster.
			name: "interval within threshold passes",
			lo:   -3_000_000, hi: 2_000_000, threshold: 7,
			want: results.RegressionPass,
		},
		{
			// Entirely beyond the budget: a real regression, confidently.
			name: "interval beyond threshold fails",
			lo:   9_000_000, hi: 14_000_000, threshold: 7,
			want: results.RegressionFail,
		},
		{
			// The case a two-outcome gate has to get wrong. The median is 7.5%,
			// past the threshold, but the interval reaches down to 2% — these
			// pairs are consistent with passing and with regressing.
			name: "interval spanning threshold is inconclusive",
			lo:   2_000_000, hi: 13_000_000, threshold: 7,
			want: results.RegressionInconclusive,
		},
		{
			// A large regression still spans the threshold if the interval is
			// wide enough. Magnitude does not substitute for precision.
			name: "huge but imprecise effect is inconclusive",
			lo:   5_000_000, hi: 90_000_000, threshold: 7,
			want: results.RegressionInconclusive,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := gateFor(t, base, tc.lo, tc.hi, tc.threshold)
			if g.Verdict != tc.want {
				t.Errorf("verdict = %q, want %q (interval %+.1f%%…%+.1f%%, threshold %.1f%%)",
					g.Verdict, tc.want, g.IntervalLoPercent, g.IntervalHiPercent, tc.threshold)
			}
			if g.Reason == "" {
				t.Error("verdict carries no reason")
			}
		})
	}
}

// The boundary itself: a threshold of 7% must admit an effect of exactly 7% and
// reject one just past it. Stated as a test because "at most" versus "less than"
// is the kind of detail that silently shifts a team's gate by one trial's noise.
func TestRegressionGateBoundaryIsInclusive(t *testing.T) {
	const base = 100_000_000

	if g := gateFor(t, base, 1_000_000, 7_000_000, 7); g.Verdict != results.RegressionPass {
		t.Errorf("interval ending exactly at the threshold = %q, want pass", g.Verdict)
	}
	if g := gateFor(t, base, 7_000_001, 9_000_000, 7); g.Verdict != results.RegressionFail {
		t.Errorf("interval starting just past the threshold = %q, want regression", g.Verdict)
	}
	// Exactly at the threshold on the low side is not yet "beyond" it.
	if g := gateFor(t, base, 7_000_000, 9_000_000, 7); g.Verdict != results.RegressionInconclusive {
		t.Errorf("interval starting exactly at the threshold = %q, want inconclusive", g.Verdict)
	}
}

// Ineligible evidence must never acquire a verdict. A comparison the tool
// already refused cannot become a pass by being measured against a number.
func TestRegressionGateRefusesIneligibleEvidence(t *testing.T) {
	const base = 100_000_000

	pe := &results.PairedExperiment{
		TreatmentVariables: []string{"trace"},
		Eligibility:        results.Eligibility{Verdict: results.VerdictIncomparable},
		Baseline: &results.TrialSet{
			Summary: results.TrialSummary{DurationNS: results.MetricSummary{Median: base}},
		},
		Paired: results.PairedSummary{
			Pairs: 10,
			Delta: results.MetricSummary{CI95Lo: -1e6, CI95Hi: 1e6, CI95Available: true},
		},
	}
	g := results.EvaluateRegression(pe, 7)
	if g.Verdict != results.RegressionNotAssessed {
		t.Errorf("incomparable evidence produced verdict %q, want not_assessed", g.Verdict)
	}
	if g.Assessed() {
		t.Error("Assessed() true for incomparable evidence")
	}
	if g.Regressed() {
		t.Error("Regressed() true for incomparable evidence")
	}
}

// Without an interval there is nothing to place against the threshold. Falling
// back to the median would state a decision the trials cannot support.
func TestRegressionGateNeedsAnInterval(t *testing.T) {
	pe := &results.PairedExperiment{
		TreatmentVariables: []string{"trace"},
		Eligibility:        results.Eligibility{Verdict: results.VerdictComparable},
		Baseline: &results.TrialSet{
			Summary: results.TrialSummary{DurationNS: results.MetricSummary{Median: 100_000_000}},
		},
		Paired: results.PairedSummary{
			Pairs: 3,
			Delta: results.MetricSummary{Median: 50_000_000, CI95Available: false},
		},
	}
	if g := results.EvaluateRegression(pe, 7); g.Verdict != results.RegressionNotAssessed {
		t.Errorf("verdict without an interval = %q, want not_assessed", g.Verdict)
	}
}

// No declared threshold means no decision — not a silent pass.
func TestRegressionGateOffByDefault(t *testing.T) {
	g := gateFor(t, 100_000_000, 40_000_000, 60_000_000, 0)
	if g.Verdict != results.RegressionNotAssessed {
		t.Errorf("verdict without a threshold = %q, want not_assessed", g.Verdict)
	}
	if g.Regressed() {
		t.Error("a 50% slowdown reported as a regression with no threshold declared")
	}
}

// An experiment whose arms are identical has no treatment to hold to a
// threshold; any difference is run-to-run variation.
func TestRegressionGateRefusesWhenArmsIdentical(t *testing.T) {
	pe := &results.PairedExperiment{
		TreatmentVariables: nil,
		Eligibility:        results.Eligibility{Verdict: results.VerdictComparable},
		Baseline: &results.TrialSet{
			Summary: results.TrialSummary{DurationNS: results.MetricSummary{Median: 100_000_000}},
		},
		Paired: results.PairedSummary{
			Pairs: 10,
			Delta: results.MetricSummary{CI95Lo: 9e6, CI95Hi: 12e6, CI95Available: true},
		},
	}
	if g := results.EvaluateRegression(pe, 7); g.Verdict != results.RegressionNotAssessed {
		t.Errorf("verdict with no treatment = %q, want not_assessed", g.Verdict)
	}
}

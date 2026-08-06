package results

import (
	"math"
	"sort"
	"time"
)

// TrialSet is the output of running one configuration repeatedly. One run is
// not evidence: a single duration cannot be told apart from noise, so a
// decision needs a distribution and a statement of how stable it was.
//
// The individual trials are kept, not just their summary. A reader who doubts
// the summary must be able to recompute it, and an excluded trial must remain
// visible rather than disappearing into an aggregate.
type TrialSet struct {
	SchemaVersion int    `json:"result_schema_version,omitempty"`
	GeneratedAt   string `json:"generated_at"`
	Tool          Tool   `json:"tool,omitempty"`
	Host          Host   `json:"host,omitempty"`
	// WarmupTrials counts trials run before measurement began and deliberately
	// excluded from Summary. They are not retained: their purpose was to change
	// the machine's state, not to be evidence about it.
	WarmupTrials int `json:"warmup_trials,omitempty"`
	// Trials holds every measured trial in execution order, including ones whose
	// execution was invalid.
	Trials  []*Results   `json:"trials"`
	Summary TrialSummary `json:"summary"`
}

// TrialSummary aggregates a trial set's measured trials.
//
// Duration is the primary metric: it is what the workload produces, and
// §11.2's warning applies — a p99 operation latency inside one run is not the
// p99 of trial outcomes, and neither substitutes for an interval on the metric
// a decision is made from. GiBPerSec is the same measurement expressed the way
// people read it, derived from the same trials, so the two cannot be searched
// against each other for a more favourable answer.
type TrialSummary struct {
	Trials       int           `json:"trials"`
	ValidTrials  int           `json:"valid_trials"`
	FailedTrials int           `json:"failed_trials"`
	DurationNS   MetricSummary `json:"duration_ns"`
	GiBPerSec    MetricSummary `json:"gib_per_sec"`
}

// MetricSummary describes the distribution of one metric over the valid trials.
type MetricSummary struct {
	N      int     `json:"n"`
	Median float64 `json:"median"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"stddev"`
	// CVPercent is the coefficient of variation, the stability measure a
	// comparison is gated on: a spread this wide cannot support a claim about a
	// difference narrower than itself.
	CVPercent float64 `json:"cv_percent"`
	Min       float64 `json:"min"`
	Max       float64 `json:"max"`
	// CI95Lo and CI95Hi bound the median at 95% confidence, by order statistics
	// rather than a normal assumption — replay durations are bounded below and
	// have a long right tail, which is exactly where a symmetric interval
	// misleads. CI95Available is false when there were too few trials to form an
	// interval at all (see medianCIBounds), and the bounds are then meaningless.
	CI95Lo        float64 `json:"ci95_lo,omitempty"`
	CI95Hi        float64 `json:"ci95_hi,omitempty"`
	CI95Available bool    `json:"ci95_available"`
}

// BuildTrialSet assembles a trial set from measured trials in execution order.
// Trials whose execution was invalid are retained and counted but excluded from
// the statistics, because a failed run measures nothing and averaging it in
// would move the result toward whatever the failure happened to cost.
func BuildTrialSet(trials []*Results, warmup int) *TrialSet {
	ts := &TrialSet{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Tool:          CurrentTool(),
		Host:          CurrentHost(),
		WarmupTrials:  warmup,
		Trials:        trials,
	}

	durations := make([]float64, 0, len(trials))
	rates := make([]float64, 0, len(trials))
	failed := 0
	for _, t := range trials {
		if len(t.ExecutionInvalidReasons()) > 0 {
			failed++
			continue
		}
		durations = append(durations, float64(t.DurationNS))
		_, gib := t.Throughput()
		rates = append(rates, gib)
	}

	ts.Summary = TrialSummary{
		Trials:       len(trials),
		ValidTrials:  len(durations),
		FailedTrials: failed,
		DurationNS:   summarize(durations),
		GiBPerSec:    summarize(rates),
	}
	return ts
}

// Representative returns the first trial, whose plan and environment stand for
// the set: every trial in a set replays one plan against one configuration.
// Returns nil for an empty set.
func (ts *TrialSet) Representative() *Results {
	if len(ts.Trials) == 0 {
		return nil
	}
	return ts.Trials[0]
}

// FailedTrialIndexes lists the 1-based positions of trials whose execution was
// invalid, so a report can point at them rather than only counting them.
func (ts *TrialSet) FailedTrialIndexes() []int {
	var out []int
	for i, t := range ts.Trials {
		if len(t.ExecutionInvalidReasons()) > 0 {
			out = append(out, i+1)
		}
	}
	return out
}

// summarize computes the distribution summary of xs. It does not mutate xs.
func summarize(xs []float64) MetricSummary {
	m := MetricSummary{N: len(xs)}
	if len(xs) == 0 {
		return m
	}

	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)

	m.Min = sorted[0]
	m.Max = sorted[len(sorted)-1]
	m.Median = median(sorted)

	var sum float64
	for _, x := range sorted {
		sum += x
	}
	m.Mean = sum / float64(len(sorted))

	// Sample standard deviation (n−1): these trials are a sample of the runs the
	// configuration could produce, not the entire population of them. One trial
	// has no spread to estimate.
	if len(sorted) > 1 {
		var ss float64
		for _, x := range sorted {
			d := x - m.Mean
			ss += d * d
		}
		m.StdDev = math.Sqrt(ss / float64(len(sorted)-1))
	}
	if m.Mean != 0 {
		m.CVPercent = m.StdDev / math.Abs(m.Mean) * 100
	}

	if lo, hi, ok := medianCIBounds(len(sorted)); ok {
		m.CI95Lo, m.CI95Hi, m.CI95Available = sorted[lo], sorted[hi], true
	}
	return m
}

// median returns the median of an already-sorted, non-empty slice.
func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// medianCIBounds returns the 0-based indexes into a sorted sample of n values
// that bound its median at ≥95% confidence, and whether such a pair exists.
//
// The interval is the distribution-free order-statistic one: with the median as
// the population centre each observation falls above it with probability 1/2,
// so the count below is Binomial(n, 1/2), and [x_(k), x_(n+1-k)] covers the
// median whenever k is the largest index with P(X < k) ≤ 0.025.
//
// Below n = 6 no such k exists — even the widest interval, the full range, has
// coverage under 95% — and the honest answer is that the sample cannot support
// an interval rather than a narrower one computed by assuming normality.
func medianCIBounds(n int) (lo, hi int, ok bool) {
	if n < 6 {
		return 0, 0, false
	}
	// Accumulate P(X = i) for X ~ Binomial(n, 1/2) in log space, which stays
	// exact for sample sizes where the binomial coefficient itself would
	// overflow a float64.
	logHalfPow := float64(n) * math.Log(0.5)
	lnFactN, _ := math.Lgamma(float64(n + 1))
	var cum float64
	best := -1
	for i := 0; i < n; i++ {
		lnFactI, _ := math.Lgamma(float64(i + 1))
		lnFactNI, _ := math.Lgamma(float64(n - i + 1))
		cum += math.Exp(lnFactN - lnFactI - lnFactNI + logHalfPow)
		// cum is now P(X ≤ i) = P(X < i+1), so k = i+1 is admissible.
		if cum > 0.025 {
			break
		}
		best = i + 1
	}
	if best < 1 {
		return 0, 0, false
	}
	// k is 1-based; convert to 0-based indexes into the sorted sample.
	return best - 1, n - best, true
}

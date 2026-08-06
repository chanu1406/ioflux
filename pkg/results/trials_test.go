package results_test

import (
	"math"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// trialWithDuration builds a valid trial that took d nanoseconds and moved a
// fixed number of bytes, so throughput varies only with duration.
func trialWithDuration(d int64) *results.Results {
	r := comparableResult()
	r.DurationNS = d
	r.OpsCompleted = 1000
	r.BytesMoved = 1 << 30
	return r
}

func trialSetOf(durations ...int64) *results.TrialSet {
	trials := make([]*results.Results, 0, len(durations))
	for _, d := range durations {
		trials = append(trials, trialWithDuration(d))
	}
	return results.BuildTrialSet(trials, 0)
}

func approx(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %v, want %v (±%v)", name, got, want, tol)
	}
}

// Known-value check against hand-computed statistics, so an error in the
// implementation cannot hide behind agreement with itself.
func TestTrialSummaryStatistics(t *testing.T) {
	// Mean 30, sample stddev sqrt(250) ≈ 15.8114, median 30.
	ts := trialSetOf(10, 20, 30, 40, 50)
	d := ts.Summary.DurationNS

	if d.N != 5 {
		t.Errorf("N = %d, want 5", d.N)
	}
	approx(t, "median", d.Median, 30, 1e-9)
	approx(t, "mean", d.Mean, 30, 1e-9)
	approx(t, "stddev", d.StdDev, math.Sqrt(250), 1e-9)
	approx(t, "cv", d.CVPercent, math.Sqrt(250)/30*100, 1e-9)
	approx(t, "min", d.Min, 10, 1e-9)
	approx(t, "max", d.Max, 50, 1e-9)
}

// An even-sized sample takes the mean of the two central values.
func TestTrialSummaryMedianOfEvenSample(t *testing.T) {
	ts := trialSetOf(10, 20, 30, 40)
	approx(t, "median", ts.Summary.DurationNS.Median, 25, 1e-9)
}

// Order is an artifact of when trials ran, never of their values.
func TestTrialSummaryIsOrderIndependent(t *testing.T) {
	ascending := trialSetOf(10, 20, 30, 40, 50, 60).Summary.DurationNS
	shuffled := trialSetOf(40, 10, 60, 30, 50, 20).Summary.DurationNS

	if ascending != shuffled {
		t.Errorf("summary depends on trial order:\n ascending %+v\n shuffled  %+v", ascending, shuffled)
	}
}

// A single trial has no spread to estimate; reporting one would invent
// precision the sample cannot support.
func TestTrialSummarySingleTrialHasNoSpread(t *testing.T) {
	d := trialSetOf(42).Summary.DurationNS

	if d.StdDev != 0 || d.CVPercent != 0 {
		t.Errorf("single trial reported spread: stddev=%v cv=%v", d.StdDev, d.CVPercent)
	}
	if d.CI95Available {
		t.Error("single trial reported a confidence interval")
	}
	approx(t, "median", d.Median, 42, 1e-9)
}

// Identical trials have zero spread — the boundary where a CV computation
// divides by a mean that is not itself zero.
func TestTrialSummaryIdenticalTrials(t *testing.T) {
	d := trialSetOf(100, 100, 100, 100, 100, 100).Summary.DurationNS

	if d.StdDev != 0 || d.CVPercent != 0 {
		t.Errorf("identical trials reported spread: stddev=%v cv=%v", d.StdDev, d.CVPercent)
	}
	if !d.CI95Available || d.CI95Lo != 100 || d.CI95Hi != 100 {
		t.Errorf("CI over identical trials = [%v, %v] available=%v, want [100,100] available",
			d.CI95Lo, d.CI95Hi, d.CI95Available)
	}
}

// The median interval comes from order statistics. For n = 10 the admissible
// bound is k = 2, so the interval runs from the 2nd smallest to the 2nd
// largest; asserting the exact bounds pins the binomial arithmetic.
func TestMedianIntervalUsesOrderStatistics(t *testing.T) {
	ts := trialSetOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	d := ts.Summary.DurationNS

	if !d.CI95Available {
		t.Fatal("no interval for 10 trials")
	}
	approx(t, "ci lo", d.CI95Lo, 2, 1e-9)
	approx(t, "ci hi", d.CI95Hi, 9, 1e-9)
}

// Six trials is the smallest sample admitting a 95% interval; five is not, and
// the tool must say so rather than narrow the interval by assuming normality.
func TestMedianIntervalAvailabilityThreshold(t *testing.T) {
	for n := 1; n <= 5; n++ {
		durations := make([]int64, n)
		for i := range durations {
			durations[i] = int64(i + 1)
		}
		if trialSetOf(durations...).Summary.DurationNS.CI95Available {
			t.Errorf("n=%d reported an interval; too few trials to bound the median", n)
		}
	}
	// n = 6: k = 1, so the interval is the full range.
	d := trialSetOf(1, 2, 3, 4, 5, 6).Summary.DurationNS
	if !d.CI95Available {
		t.Fatal("n=6 reported no interval")
	}
	approx(t, "ci lo", d.CI95Lo, 1, 1e-9)
	approx(t, "ci hi", d.CI95Hi, 6, 1e-9)
}

// The interval must contain the median and widen with the sample's spread.
func TestMedianIntervalBracketsTheMedian(t *testing.T) {
	for _, durations := range [][]int64{
		{1, 2, 3, 4, 5, 6},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{5, 5, 5, 100, 100, 100, 100, 5, 5, 5, 50, 50},
	} {
		d := trialSetOf(durations...).Summary.DurationNS
		if !d.CI95Available {
			t.Fatalf("no interval for %d trials", len(durations))
		}
		if d.CI95Lo > d.Median || d.CI95Hi < d.Median {
			t.Errorf("interval [%v, %v] does not contain median %v", d.CI95Lo, d.CI95Hi, d.Median)
		}
	}
}

// A failed trial measures how long a failure took. It is counted and kept, but
// must not move the statistics.
func TestBuildTrialSetExcludesFailedTrialsFromStatistics(t *testing.T) {
	good := []*results.Results{
		trialWithDuration(100), trialWithDuration(100), trialWithDuration(100),
		trialWithDuration(100), trialWithDuration(100), trialWithDuration(100),
	}
	failed := trialWithDuration(999999)
	failed.Errors = 3
	trials := append(good, failed)

	ts := results.BuildTrialSet(trials, 0)

	if ts.Summary.Trials != 7 || ts.Summary.ValidTrials != 6 || ts.Summary.FailedTrials != 1 {
		t.Errorf("counts = %d total / %d valid / %d failed, want 7/6/1",
			ts.Summary.Trials, ts.Summary.ValidTrials, ts.Summary.FailedTrials)
	}
	approx(t, "median", ts.Summary.DurationNS.Median, 100, 1e-9)
	approx(t, "max", ts.Summary.DurationNS.Max, 100, 1e-9)
	// The failed trial is retained as evidence, not dropped.
	if len(ts.Trials) != 7 {
		t.Errorf("trials retained = %d, want 7", len(ts.Trials))
	}
	if got := ts.FailedTrialIndexes(); len(got) != 1 || got[0] != 7 {
		t.Errorf("failed trial indexes = %v, want [7]", got)
	}
}

func TestBuildTrialSetWithOnlyFailedTrials(t *testing.T) {
	failed := trialWithDuration(10)
	failed.Errors = 1

	ts := results.BuildTrialSet([]*results.Results{failed}, 0)

	if ts.Summary.ValidTrials != 0 {
		t.Errorf("valid trials = %d, want 0", ts.Summary.ValidTrials)
	}
	if ts.Summary.DurationNS.N != 0 || ts.Summary.DurationNS.CI95Available {
		t.Error("summary computed statistics with no valid trials")
	}
}

func TestBuildTrialSetRecordsProvenance(t *testing.T) {
	ts := trialSetOf(1, 2, 3)

	if ts.SchemaVersion != results.SchemaVersion {
		t.Errorf("schema version = %d, want %d", ts.SchemaVersion, results.SchemaVersion)
	}
	if ts.Tool.Version == "" || ts.Host.OS == "" {
		t.Errorf("trial set missing provenance: tool=%+v host=%+v", ts.Tool, ts.Host)
	}
	if ts.Representative() == nil {
		t.Error("Representative() = nil for a non-empty set")
	}
}

// Throughput is derived from the same trials as duration, so a slower median
// duration must correspond to a lower median throughput. The two cannot be
// searched against each other for a friendlier answer.
func TestThroughputSummaryTracksDuration(t *testing.T) {
	fast := trialSetOf(1_000_000_000, 1_000_000_000, 1_000_000_000,
		1_000_000_000, 1_000_000_000, 1_000_000_000)
	slow := trialSetOf(2_000_000_000, 2_000_000_000, 2_000_000_000,
		2_000_000_000, 2_000_000_000, 2_000_000_000)

	if !(fast.Summary.GiBPerSec.Median > slow.Summary.GiBPerSec.Median) {
		t.Errorf("faster trials did not report higher throughput: %v vs %v",
			fast.Summary.GiBPerSec.Median, slow.Summary.GiBPerSec.Median)
	}
	// 1 GiB in 1 s.
	approx(t, "gib/s", fast.Summary.GiBPerSec.Median, 1.0, 1e-9)
}

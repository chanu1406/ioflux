package calibrate_test

import (
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/calibrate"
	"github.com/chanuollala/ioflux/pkg/results"
)

const digest = "sha256:aaaa"

// boundedPolicy is the default stability policy with a share bound added, so a
// test that means to exercise the bound is not silently refused for instability
// it never set up.
func boundedPolicy(share float64) calibrate.Policy {
	p := calibrate.DefaultPolicy()
	p.MaxGeneratorSharePercent = share
	return p
}

// stable marks a ceiling as resting on evidence that meets the default policy,
// which is the precondition for any verdict at all.
func stable(c calibrate.Ceiling) calibrate.Ceiling {
	c.Trials = calibrate.DefaultPolicy().MinCeilingTrials
	c.CVPercent = 1
	return c
}

// ceilingRun builds a null-engine result standing in for a calibration replay:
// opsCompleted operations in durationNS, at the given concurrency.
func ceilingRun(ops int64, durationNS int64, maxInflight int) *results.Results {
	return &results.Results{
		SchemaVersion: results.SchemaVersion,
		Plan: results.PlanInfo{
			Engine:      calibrate.NullEngine,
			Mode:        "asap",
			TraceDigest: digest,
			MaxInflight: maxInflight,
			NumStreams:  4,
		},
		OpsCompleted: ops,
		DurationNS:   durationNS,
	}
}

// measuredRun builds a real-backend result over the same trace and concurrency,
// so only the property under test differs from the ceiling.
func measuredRun(ops int64, durationNS int64, maxInflight int) *results.Results {
	return &results.Results{
		SchemaVersion: results.SchemaVersion,
		Plan: results.PlanInfo{
			Engine:      "local",
			Mode:        "asap",
			TraceDigest: digest,
			MaxInflight: maxInflight,
			NumStreams:  4,
		},
		OpsCompleted: ops,
		DurationNS:   durationNS,
	}
}

func TestCeilingFromRates(t *testing.T) {
	// 1,000 ops in 100 ms is 10,000 ops/s and 100 µs per op.
	c := calibrate.CeilingFrom(ceilingRun(1000, 100_000_000, 512), 100_000_000)

	if c.OpsPerSec != 10_000 {
		t.Errorf("OpsPerSec = %v, want 10000", c.OpsPerSec)
	}
	if c.NsPerOp != 100_000 {
		t.Errorf("NsPerOp = %v, want 100000", c.NsPerOp)
	}
	if c.TraceDigest != digest {
		t.Errorf("TraceDigest = %q, want %q", c.TraceDigest, digest)
	}
}

// CeilingFrom takes the duration it is given rather than the run's own, because
// a multi-trial calibration is represented by its median.
func TestCeilingUsesSuppliedDuration(t *testing.T) {
	run := ceilingRun(1000, 50_000_000, 512) // the run's own duration is faster
	c := calibrate.CeilingFrom(run, 100_000_000)

	if c.DurationNS != 100_000_000 {
		t.Fatalf("DurationNS = %d, want the supplied 100000000", c.DurationNS)
	}
	if c.OpsPerSec != 10_000 {
		t.Errorf("OpsPerSec = %v, want 10000 from the supplied median", c.OpsPerSec)
	}
}

func TestCeilingFromEmptyRunRatesStayZero(t *testing.T) {
	c := calibrate.CeilingFrom(ceilingRun(0, 0, 512), 0)
	if c.OpsPerSec != 0 || c.NsPerOp != 0 {
		t.Errorf("rates = %v/%v, want 0/0 for a run that completed nothing", c.OpsPerSec, c.NsPerOp)
	}
}

func TestAssessVerdicts(t *testing.T) {
	// Ceiling: 1,000 ops in 100 ms = 10,000 ops/s.
	c := stable(calibrate.CeilingFrom(ceilingRun(1000, 100_000_000, 512), 100_000_000))

	cases := []struct {
		name        string
		runDuration int64
		bound       float64
		want        calibrate.Verdict
		wantShare   float64
	}{
		{
			// 1,000 ops in 1 s = 1,000 ops/s, a tenth of the ceiling.
			name:        "well below the ceiling is attributable",
			runDuration: 1_000_000_000, bound: 25,
			want: calibrate.VerdictAttributable, wantShare: 10,
		},
		{
			// 1,000 ops in 125 ms = 8,000 ops/s, 80% of the ceiling.
			name:        "close to the ceiling is not attributable",
			runDuration: 125_000_000, bound: 25,
			want: calibrate.VerdictNotAttributable, wantShare: 80,
		},
		{
			// Exactly at the bound still passes: the bound is a limit, not a
			// value to be exceeded before it applies.
			name:        "share exactly at the bound is attributable",
			runDuration: 400_000_000, bound: 25,
			want: calibrate.VerdictAttributable, wantShare: 25,
		},
		{
			name:        "no bound declared decides nothing",
			runDuration: 1_000_000_000, bound: 0,
			want: calibrate.VerdictNotAssessed, wantShare: 10,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := calibrate.Assess(measuredRun(1000, tc.runDuration, 512), tc.runDuration, c, boundedPolicy(tc.bound))

			if a.Verdict != tc.want {
				t.Errorf("Verdict = %q, want %q (%s)", a.Verdict, tc.want, a.Explanation)
			}
			if a.GeneratorSharePercent != tc.wantShare {
				t.Errorf("GeneratorSharePercent = %v, want %v", a.GeneratorSharePercent, tc.wantShare)
			}
			if len(a.Blocking) > 0 {
				t.Errorf("unexpected blocking: %v", a.Blocking)
			}
		})
	}
}

// Each of these makes the share meaningless rather than merely uncertain, so
// each must refuse the comparison outright.
func TestAssessRefusals(t *testing.T) {
	c := stable(calibrate.CeilingFrom(ceilingRun(1000, 100_000_000, 512), 100_000_000))

	cases := []struct {
		name     string
		mutate   func(run *results.Results, c *calibrate.Ceiling)
		wantWord string
	}{
		{
			name:     "ceiling measured against a real engine",
			mutate:   func(_ *results.Results, c *calibrate.Ceiling) { c.Engine = "local" },
			wantWord: "real I/O",
		},
		{
			name:     "ceiling measured on a different trace",
			mutate:   func(_ *results.Results, c *calibrate.Ceiling) { c.TraceDigest = "sha256:bbbb" },
			wantWord: "different trace",
		},
		{
			name:     "run has no recorded trace identity",
			mutate:   func(run *results.Results, _ *calibrate.Ceiling) { run.Plan.TraceDigest = "" },
			wantWord: "unrecorded",
		},
		{
			name:     "concurrency differs",
			mutate:   func(_ *results.Results, c *calibrate.Ceiling) { c.MaxInflight = 4 },
			wantWord: "max-inflight",
		},
		{
			name:     "run was paced by the trace",
			mutate:   func(run *results.Results, _ *calibrate.Ceiling) { run.Plan.Mode = "timeline" },
			wantWord: "schedule rather than by the backend",
		},
		{
			name:     "run did not execute validly",
			mutate:   func(run *results.Results, _ *calibrate.Ceiling) { run.Errors = 3 },
			wantWord: "not a valid measurement",
		},
		{
			name:     "run is itself a null-engine replay",
			mutate:   func(run *results.Results, _ *calibrate.Ceiling) { run.Plan.Engine = calibrate.NullEngine },
			wantWord: "no backend to attribute to",
		},
		{
			name:     "ceiling rests on too few trials",
			mutate:   func(_ *results.Results, c *calibrate.Ceiling) { c.Trials = 2 },
			wantWord: "fewer than the",
		},
		{
			name:     "calibration was too unstable to bound anything",
			mutate:   func(_ *results.Results, c *calibrate.Ceiling) { c.CVPercent = 45 },
			wantWord: "does not bound anything",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := measuredRun(1000, 1_000_000_000, 512)
			ceiling := c
			tc.mutate(run, &ceiling)

			// A generous bound: were the refusal not honored, this evidence would
			// otherwise sail through as attributable.
			a := calibrate.Assess(run, 1_000_000_000, ceiling, boundedPolicy(90))

			if a.Verdict != calibrate.VerdictNotAssessed {
				t.Errorf("Verdict = %q, want %q", a.Verdict, calibrate.VerdictNotAssessed)
			}
			if len(a.Blocking) == 0 {
				t.Fatal("expected a blocking reason, got none")
			}
			if !containsWord(a.Blocking, tc.wantWord) {
				t.Errorf("blocking %v, want one mentioning %q", a.Blocking, tc.wantWord)
			}
		})
	}
}

// The property that matters most: refused evidence must never acquire an
// approval, however favourable the numbers underneath it look. This is the
// calibration counterpart of the regression gate's refusal control.
func TestRefusedEvidenceIsNeverAttributable(t *testing.T) {
	c := stable(calibrate.CeilingFrom(ceilingRun(1000, 100_000_000, 512), 100_000_000))

	// A run 1000× slower than the ceiling — as clean an attribution as could
	// exist — but its operations failed, so it measured nothing.
	run := measuredRun(1000, 100_000_000_000, 512)
	run.Errors = 1

	a := calibrate.Assess(run, 100_000_000_000, c, boundedPolicy(90))

	if a.Verdict == calibrate.VerdictAttributable {
		t.Fatal("an invalid run was certified attributable")
	}
	if a.GeneratorSharePercent != 0 {
		t.Errorf("GeneratorSharePercent = %v, want 0: a refused comparison reports no share",
			a.GeneratorSharePercent)
	}
}

func TestCPUSaturation(t *testing.T) {
	c := stable(calibrate.CeilingFrom(ceilingRun(1000, 100_000_000, 512), 100_000_000))

	run := measuredRun(1000, 1_000_000_000, 512)
	run.Host = results.Host{CPUs: 4}
	// 2 s of CPU over 1 s of wall on 4 cores is half the machine.
	run.CPU = results.CPU{UserNS: 1_500_000_000, SysNS: 500_000_000, WallNS: 1_000_000_000}

	a := calibrate.Assess(run, 1_000_000_000, c, boundedPolicy(25))

	if a.CPUSaturationPercent != 50 {
		t.Errorf("CPUSaturationPercent = %v, want 50", a.CPUSaturationPercent)
	}
}

// CPU figures are absent from older results and from platforms where getrusage
// fails; a missing input must report nothing rather than a fabricated 0%.
func TestCPUSaturationUnrecorded(t *testing.T) {
	c := stable(calibrate.CeilingFrom(ceilingRun(1000, 100_000_000, 512), 100_000_000))
	run := measuredRun(1000, 1_000_000_000, 512)

	a := calibrate.Assess(run, 1_000_000_000, c, boundedPolicy(25))

	if a.CPUSaturationPercent != 0 {
		t.Errorf("CPUSaturationPercent = %v, want 0 when CPUs and wall time are unrecorded",
			a.CPUSaturationPercent)
	}
}

func containsWord(haystack []string, word string) bool {
	for _, s := range haystack {
		if strings.Contains(s, word) {
			return true
		}
	}
	return false
}

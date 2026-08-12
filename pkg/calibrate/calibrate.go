// Package calibrate measures the load generator's own throughput ceiling, so a
// run's timing can be attributed to its backend rather than to IOFlux.
//
// The ceiling comes from replaying a trace against the null (mem) engine, which
// performs no I/O. A measured run's op rate as a fraction of that ceiling is the
// generator's share of the result.
package calibrate

import (
	"fmt"

	"github.com/chanuollala/ioflux/pkg/results"
)

// SchemaVersion is the calibration_schema_version written into every artifact.
const SchemaVersion = 1

// NullEngine is the engine a ceiling must be measured against. Any other engine
// performs real I/O, so its rate is not a bound on the generator.
const NullEngine = "mem"

// Verdict classifies whether a run's timing may be attributed to its backend.
type Verdict string

const (
	VerdictAttributable    Verdict = "attributable"
	VerdictNotAttributable Verdict = "not_attributable"
	VerdictNotAssessed     Verdict = "not_assessed"
)

// Calibration is the artifact `ioflux calibrate` writes.
type Calibration struct {
	SchemaVersion int          `json:"calibration_schema_version"`
	GeneratedAt   string       `json:"generated_at"`
	Tool          results.Tool `json:"tool,omitempty"`
	Host          results.Host `json:"host,omitempty"`
	Policy        Policy       `json:"policy"`
	Ceiling       Ceiling      `json:"ceiling"`
	// Assessment is present only when a measured run was supplied to assess.
	Assessment   *Assessment       `json:"assessment,omitempty"`
	AssessedPath string            `json:"assessed_path,omitempty"`
	Trials       *results.TrialSet `json:"trials,omitempty"`
}

// Ceiling is the load generator's op rate for one trace shape at one
// concurrency.
type Ceiling struct {
	Engine string `json:"engine"`
	// TraceDigest identifies the trace measured, so a ceiling cannot be applied
	// to a run of a different workload.
	TraceDigest string  `json:"trace_digest,omitempty"`
	MaxInflight int     `json:"max_inflight"`
	Streams     int     `json:"num_streams"`
	Ops         int64   `json:"ops"`
	DurationNS  int64   `json:"duration_ns"`
	OpsPerSec   float64 `json:"ops_per_sec"`
	NsPerOp     float64 `json:"ns_per_op"`
	Trials      int     `json:"trials,omitempty"`
	// CVPercent is the spread across the calibration's own trials.
	CVPercent float64      `json:"cv_percent"`
	Tool      results.Tool `json:"tool,omitempty"`
	Host      results.Host `json:"host,omitempty"`
}

// Policy is the set of limits an attribution decision must meet.
type Policy struct {
	// MaxGeneratorSharePercent is the largest generator share that still permits
	// attribution. 0 declares no bound, which reports the share and decides
	// nothing.
	MaxGeneratorSharePercent float64 `json:"max_generator_share_percent,omitempty"`
	MinCeilingTrials         int     `json:"min_ceiling_trials"`
	MaxCeilingCVPercent      float64 `json:"max_ceiling_cv_percent"`
}

// DefaultPolicy returns the stability limits a calibration must meet, matching
// the trial policy a comparison is gated on. It declares no share bound.
func DefaultPolicy() Policy {
	tp := results.DefaultTrialPolicy()
	return Policy{
		MinCeilingTrials:    tp.MinValidTrials,
		MaxCeilingCVPercent: tp.MaxCVPercent,
	}
}

// CeilingFrom derives a ceiling from a null-engine calibration run.
//
// durationNS is the duration to represent the run by: the median across trials,
// not the fastest. An overstated ceiling understates the generator's share of a
// measured run, which is the error that would wrongly reassure.
func CeilingFrom(r *results.Results, durationNS int64) Ceiling {
	c := Ceiling{
		Engine:      r.Plan.Engine,
		TraceDigest: r.Plan.TraceDigest,
		MaxInflight: r.Plan.MaxInflight,
		Streams:     r.Plan.NumStreams,
		Ops:         r.OpsCompleted,
		DurationNS:  durationNS,
		Tool:        r.Tool,
		Host:        r.Host,
	}
	if durationNS > 0 && r.OpsCompleted > 0 {
		c.OpsPerSec = float64(r.OpsCompleted) / (float64(durationNS) / 1e9)
		c.NsPerOp = float64(durationNS) / float64(r.OpsCompleted)
	}
	return c
}

// Assessment is the outcome of comparing a measured run against a ceiling.
type Assessment struct {
	Verdict Verdict `json:"verdict"`
	// Blocking lists the reasons the two could not be compared. A non-empty
	// Blocking always yields VerdictNotAssessed.
	Blocking []string `json:"blocking,omitempty"`

	RunOpsPerSec     float64 `json:"run_ops_per_sec,omitempty"`
	CeilingOpsPerSec float64 `json:"ceiling_ops_per_sec,omitempty"`
	// GeneratorSharePercent is the run's op rate as a percentage of the ceiling.
	GeneratorSharePercent float64 `json:"generator_share_percent,omitempty"`
	BoundPercent          float64 `json:"bound_percent,omitempty"`
	// CPUSaturationPercent is the run's CPU time over the wall time it had across
	// every core. Independent of the op-rate share: a run can sit far below the
	// ceiling and still be CPU-bound.
	CPUSaturationPercent float64 `json:"cpu_saturation_percent,omitempty"`

	Explanation string `json:"explanation"`
}

// Assess compares a measured run against a ceiling. runDurationNS is the
// duration representing the run, median across trials when several were
// measured.
func Assess(run *results.Results, runDurationNS int64, c Ceiling, policy Policy) Assessment {
	a := Assessment{
		Verdict:          VerdictNotAssessed,
		CeilingOpsPerSec: c.OpsPerSec,
	}
	if runDurationNS > 0 && run.OpsCompleted > 0 {
		a.RunOpsPerSec = float64(run.OpsCompleted) / (float64(runDurationNS) / 1e9)
	}
	a.CPUSaturationPercent = cpuSaturation(run)
	a.Blocking = blockers(run, c, policy)

	if len(a.Blocking) > 0 {
		a.Explanation = "the run and the ceiling could not be compared, so no attribution decision is possible"
		return a
	}

	boundPercent := policy.MaxGeneratorSharePercent
	a.GeneratorSharePercent = a.RunOpsPerSec / c.OpsPerSec * 100
	if boundPercent <= 0 {
		a.Explanation = fmt.Sprintf(
			"the run reached %.1f%% of the load generator's ceiling; no bound was declared, so nothing is decided",
			a.GeneratorSharePercent)
		return a
	}

	a.BoundPercent = boundPercent
	if a.GeneratorSharePercent <= boundPercent {
		a.Verdict = VerdictAttributable
		a.Explanation = fmt.Sprintf(
			"the run reached %.1f%% of the load generator's ceiling, within the %.1f%% bound, "+
				"so its timing is dominated by the backend",
			a.GeneratorSharePercent, boundPercent)
		return a
	}
	a.Verdict = VerdictNotAttributable
	a.Explanation = fmt.Sprintf(
		"the run reached %.1f%% of the load generator's ceiling, beyond the %.1f%% bound, "+
			"so its timing measures IOFlux as well as the backend",
		a.GeneratorSharePercent, boundPercent)
	return a
}

// blockers returns the reasons run and c cannot be compared. Each makes the
// share meaningless rather than uncertain, so each refuses the comparison
// instead of qualifying it.
func blockers(run *results.Results, c Ceiling, policy Policy) []string {
	var b []string

	if policy.MinCeilingTrials > 0 && c.Trials < policy.MinCeilingTrials {
		b = append(b, fmt.Sprintf(
			"the ceiling rests on %d valid trial(s), fewer than the %d required",
			c.Trials, policy.MinCeilingTrials))
	}
	if policy.MaxCeilingCVPercent > 0 && c.CVPercent > policy.MaxCeilingCVPercent {
		b = append(b, fmt.Sprintf(
			"the calibration's own spread is %.1f%% CV, wider than the %.1f%% policy: "+
				"a ceiling this unstable does not bound anything",
			c.CVPercent, policy.MaxCeilingCVPercent))
	}

	if c.Engine != NullEngine {
		b = append(b, fmt.Sprintf(
			"the ceiling was measured against the %q engine, not %q: a ceiling that "+
				"performed real I/O does not bound the load generator", c.Engine, NullEngine))
	}
	if run.Plan.Engine == NullEngine {
		b = append(b, "the run being assessed is itself a null-engine replay, which has no backend to attribute to")
	}

	switch {
	case c.TraceDigest == "" || run.Plan.TraceDigest == "":
		b = append(b, "the trace identity of the run or the ceiling is unrecorded, "+
			"so they cannot be confirmed to describe the same workload")
	case c.TraceDigest != run.Plan.TraceDigest:
		b = append(b, "the ceiling was measured on a different trace than the run, "+
			"so it bounds a different workload")
	}

	if c.MaxInflight != run.Plan.MaxInflight {
		b = append(b, fmt.Sprintf(
			"the ceiling was measured at max-inflight %d and the run at %d: "+
				"the generator's capacity depends on the concurrency it was given",
			c.MaxInflight, run.Plan.MaxInflight))
	}

	// In timeline and scaled modes the arrival schedule comes from the trace, so
	// the run's op rate reflects its pacing rather than what the backend allowed.
	if run.Plan.Mode != "asap" {
		b = append(b, fmt.Sprintf(
			"the run replayed in %q mode, where the op rate is set by the trace's "+
				"schedule rather than by the backend", run.Plan.Mode))
	}

	for _, reason := range run.ExecutionInvalidReasons() {
		b = append(b, "the run is not a valid measurement: "+reason)
	}

	if c.OpsPerSec <= 0 {
		b = append(b, "the ceiling recorded no completed operations, so it bounds nothing")
	}
	return b
}

// cpuSaturation returns the run's CPU time as a percentage of the wall time it
// had across every core, or 0 when either input is unrecorded.
func cpuSaturation(run *results.Results) float64 {
	cpus := run.Host.CPUs
	if cpus <= 0 || run.CPU.WallNS <= 0 {
		return 0
	}
	busy := float64(run.CPU.UserNS + run.CPU.SysNS)
	return busy / (float64(run.CPU.WallNS) * float64(cpus)) * 100
}

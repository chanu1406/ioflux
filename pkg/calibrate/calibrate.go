// Package calibrate answers the question plan.md §8.5 requires to be settled
// before any result is attributed to a backend: did IOFlux itself become the
// bottleneck the run blamed on storage?
//
// The method is a null-engine replay. The same trace, at the same concurrency,
// is replayed against the in-process mem engine, which performs no I/O. What
// remains is the load generator's own cost — scheduling, dispatch, buffer
// handling, payload movement, and accounting. The op rate that replay achieves
// is a ceiling: no replay of that trace at that concurrency can exceed it,
// whatever backend sits underneath.
//
// A measured run's rate as a fraction of that ceiling is the generator's share
// of the result. A run at 5% of the ceiling was limited by its backend, and a
// delta between two such runs is a property of those backends. A run at 90% was
// substantially limited by IOFlux, and a delta between two such runs is mostly
// a property of IOFlux.
//
// The ceiling is deliberately conservative in one direction. The mem engine
// still copies each transfer's bytes, so the ceiling includes payload handling
// rather than isolating pure scheduling. That understates the generator's raw
// dispatch capacity, which overstates its measured share — an error that
// produces a warning where none was needed, never a reassurance that was not
// earned.
package calibrate

import (
	"fmt"

	"github.com/chanuollala/ioflux/pkg/results"
)

// SchemaVersion is the calibration_schema_version written into every artifact.
const SchemaVersion = 1

// Calibration is the artifact `ioflux calibrate` writes: the ceiling, the
// null-engine trials it was derived from, and the assessment of a measured run
// against it when one was supplied.
//
// The trials are retained rather than summarized away because a ceiling is
// evidence like any other measurement — one that cannot be re-examined is a
// number to be taken on trust.
type Calibration struct {
	SchemaVersion int          `json:"calibration_schema_version"`
	GeneratedAt   string       `json:"generated_at"`
	Tool          results.Tool `json:"tool,omitempty"`
	Host          results.Host `json:"host,omitempty"`
	Policy        Policy       `json:"policy"`
	Ceiling       Ceiling      `json:"ceiling"`
	// Assessment is present only when a measured run was supplied to assess.
	Assessment *Assessment `json:"assessment,omitempty"`
	// AssessedPath records which results file was assessed, for provenance.
	AssessedPath string `json:"assessed_path,omitempty"`
	// Trials is the null-engine replay the ceiling came from.
	Trials *results.TrialSet `json:"trials,omitempty"`
}

// Verdict classifies whether a run's timing may be attributed to its backend.
//
// As with the regression gate, "no bound was declared" and "the evidence was
// refused" are not approvals, and neither collapses into a pass.
type Verdict string

const (
	// VerdictAttributable means the generator's share stayed within the declared
	// bound: the run's timing is dominated by its backend.
	VerdictAttributable Verdict = "attributable"
	// VerdictNotAttributable means the share exceeded the bound. The run measured
	// IOFlux as much as it measured storage, and a backend conclusion drawn from
	// it is not supported.
	VerdictNotAttributable Verdict = "not_attributable"
	// VerdictNotAssessed means no bound was declared, or the evidence could not
	// support a decision at all.
	VerdictNotAssessed Verdict = "not_assessed"
)

// NullEngine is the engine a ceiling must have been measured against. Any other
// engine performs real I/O, so its rate is not a bound on the generator.
const NullEngine = "mem"

// Ceiling is the load generator's measured op rate for one trace shape at one
// concurrency, established by a null-engine replay.
type Ceiling struct {
	Engine string `json:"engine"`
	// TraceDigest identifies the trace the ceiling was measured on. A ceiling
	// from a different workload bounds nothing about this one, and the digest is
	// what makes that checkable rather than assumed from a file name.
	TraceDigest string `json:"trace_digest,omitempty"`
	MaxInflight int    `json:"max_inflight"`
	Streams     int    `json:"num_streams"`
	Ops         int64  `json:"ops"`
	// DurationNS is the duration the ceiling is derived from — the median across
	// trials when several were measured.
	DurationNS int64   `json:"duration_ns"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	NsPerOp    float64 `json:"ns_per_op"`
	// Trials and CVPercent describe the evidence behind the ceiling. A ceiling
	// whose own trials disagree by more than the share being measured cannot
	// establish that share, so both travel with it rather than being discarded
	// once the median is taken.
	Trials    int          `json:"trials,omitempty"`
	CVPercent float64      `json:"cv_percent"`
	Tool      results.Tool `json:"tool,omitempty"`
	Host      results.Host `json:"host,omitempty"`
}

// Policy is the set of limits an attribution decision must meet. As with the
// regression gate's threshold, these are floors calibrated to a fixture rather
// than universal values, which is why they are declared rather than assumed.
type Policy struct {
	// MaxGeneratorSharePercent is the largest generator share that still permits
	// attribution. 0 or negative declares no bound, which reports the share and
	// decides nothing.
	MaxGeneratorSharePercent float64 `json:"max_generator_share_percent,omitempty"`
	// MinCeilingTrials is the fewest valid calibration trials a ceiling may rest
	// on.
	MinCeilingTrials int `json:"min_ceiling_trials"`
	// MaxCeilingCVPercent is the widest run-to-run spread the calibration itself
	// may have. A ceiling measured on a host too noisy to repeat is not a bound;
	// it is one sample of a moving quantity.
	MaxCeilingCVPercent float64 `json:"max_ceiling_cv_percent"`
}

// DefaultPolicy returns the stability limits a calibration must meet, matching
// the trial policy a comparison is already gated on so the two cannot disagree
// about what counts as stable evidence. It declares no share bound: what share
// is tolerable depends on the fixture and on what the result will be used for.
func DefaultPolicy() Policy {
	tp := results.DefaultTrialPolicy()
	return Policy{
		MinCeilingTrials:    tp.MinValidTrials,
		MaxCeilingCVPercent: tp.MaxCVPercent,
	}
}

// CeilingFrom derives a ceiling from a null-engine calibration run.
//
// durationNS is the duration the run should be represented by: the median
// across trials when several were measured, not the fastest. The fastest
// overstates what the generator can sustain, and an overstated ceiling
// understates its share of a measured run — the direction of error that would
// wrongly reassure. The median errs the other way.
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
	// Blocking lists the reasons the two could not be compared at all. A
	// non-empty Blocking always yields VerdictNotAssessed.
	Blocking []string `json:"blocking,omitempty"`

	RunOpsPerSec     float64 `json:"run_ops_per_sec,omitempty"`
	CeilingOpsPerSec float64 `json:"ceiling_ops_per_sec,omitempty"`
	// GeneratorSharePercent is the run's op rate as a percentage of the ceiling.
	GeneratorSharePercent float64 `json:"generator_share_percent,omitempty"`
	// BoundPercent is the declared limit on that share, 0 when none was declared.
	BoundPercent float64 `json:"bound_percent,omitempty"`
	// CPUSaturationPercent is the run's own CPU time as a percentage of the wall
	// time it had available across every core. It is an independent signal: a run
	// can sit far below the op-rate ceiling and still be CPU-bound, because the
	// ceiling is measured for one trace shape while CPU is shared with everything
	// else on the host.
	CPUSaturationPercent float64 `json:"cpu_saturation_percent,omitempty"`

	Explanation string `json:"explanation"`
}

// Assess compares a measured run against a ceiling and returns whether the
// run's timing may be attributed to its backend.
//
// runDurationNS is the duration representing the run, median across trials when
// several were measured.
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

// blockers returns the reasons run and c cannot be compared. Each one makes the
// share meaningless rather than merely uncertain, which is why they refuse the
// comparison instead of qualifying it.
func blockers(run *results.Results, c Ceiling, policy Policy) []string {
	var b []string

	// A ceiling is evidence, and unstable evidence is refused here for the same
	// reason it is refused in a comparison: if the calibration's own spread is
	// wider than the share being measured, the share is a number the next run
	// would contradict.
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

	// Trace identity comes from the digest, so a ceiling measured on a differently
	// named copy of the same bytes still applies, and one measured on a different
	// workload never does. An absent digest is unverifiable, not agreement.
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
	// the run's op rate reflects the pacing it was told to keep rather than the
	// rate its backend allowed. Comparing that to a ceiling measures the trace.
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

// cpuSaturation returns the run's own CPU time as a percentage of the wall time
// it had across every core, or 0 when the inputs to that ratio are unrecorded.
func cpuSaturation(run *results.Results) float64 {
	cpus := run.Host.CPUs
	if cpus <= 0 || run.CPU.WallNS <= 0 {
		return 0
	}
	busy := float64(run.CPU.UserNS + run.CPU.SysNS)
	return busy / (float64(run.CPU.WallNS) * float64(cpus)) * 100
}

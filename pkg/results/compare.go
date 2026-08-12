package results

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Verdict classifies whether two results may be read as a comparison. Three
// outcomes, because a differing backend is what a storage experiment is for
// while a run that failed its operations is not a measurement at all: the first
// is a caveat, the second a refusal.
type Verdict string

const (
	// VerdictComparable means nothing was found that changes what a delta means.
	VerdictComparable Verdict = "comparable"
	// VerdictCaveated means the delta is real but measures something other than,
	// or in addition to, the backend. The caveats say what.
	VerdictCaveated Verdict = "comparable-with-caveats"
	// VerdictIncomparable means at least one side is not a valid measurement, so
	// no delta between them supports a conclusion.
	VerdictIncomparable Verdict = "incomparable"
)

// Difference is one property on which two results disagree, with a note
// explaining what the disagreement does to the comparison. The note is the
// point: a reader who is told only that two values differ still has to work out
// whether it matters.
type Difference struct {
	Field string `json:"field"`
	A     string `json:"a"`
	B     string `json:"b"`
	Note  string `json:"note"`
}

// Eligibility is the outcome of the comparison gate.
type Eligibility struct {
	Verdict Verdict `json:"verdict"`
	// Blocking lists the reasons a comparison must be refused, each prefixed
	// with the side it came from.
	Blocking []string `json:"blocking,omitempty"`
	// Caveats lists differences that change what a delta means without making
	// the comparison meaningless.
	Caveats []Difference `json:"caveats,omitempty"`
}

// Comparable reports whether deltas between the two runs may be presented at
// all. It is false only for VerdictIncomparable; a caveated comparison still
// carries information, provided the caveats travel with it.
func (e Eligibility) Comparable() bool { return e.Verdict != VerdictIncomparable }

// ExecutionInvalidReasons returns the reasons this run's execution is not a
// valid measurement, or nil when it is. It is the single definition of "invalid
// execution" shared by the single-run report and the comparison gate, so the
// two can never disagree about whether a result is usable — which they did when
// each decided for itself.
func (r *Results) ExecutionInvalidReasons() []string {
	var reasons []string
	if r.Errors > 0 {
		reason := fmt.Sprintf("%d operation failure(s)", r.Errors)
		if r.ShortReads > 0 {
			reason += fmt.Sprintf(", including %d read(s) whose returned byte count "+
				"disagreed with the source", r.ShortReads)
		}
		reasons = append(reasons, reason)
	}
	if r.HistogramOverflows > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d latency sample(s) exceeded the histogram's trackable range", r.HistogramOverflows))
	}
	return reasons
}

// CheckEligibility decides whether results a and b may be compared, and on what
// terms. It never mutates either result.
func CheckEligibility(a, b *Results) Eligibility {
	var e Eligibility

	// A run that did not execute validly cannot serve as either side of a
	// comparison, whatever else agrees.
	for _, side := range []struct {
		label string
		res   *Results
	}{{"A", a}, {"B", b}} {
		for _, reason := range side.res.ExecutionInvalidReasons() {
			e.Blocking = append(e.Blocking, side.label+": "+reason)
		}
	}

	// Results predating this schema omit fields that cannot be distinguished
	// from a genuinely empty value, so say so once rather than emitting a
	// spurious difference for each one.
	bothCurrent := a.SchemaVersion >= SchemaVersion && b.SchemaVersion >= SchemaVersion
	if !bothCurrent {
		e.Caveats = append(e.Caveats, Difference{
			Field: "result schema",
			A:     schemaLabel(a),
			B:     schemaLabel(b),
			Note: "a result predating schema " + strconv.Itoa(SchemaVersion) + " does not carry trace " +
				"identity, containment root, host, or build, so those cannot be checked and are not " +
				"reported as agreeing",
		})
	}

	e.checkTraceIdentity(a, b)

	e.diff("engine", a.Plan.Engine, b.Plan.Engine,
		"the runs replayed against different storage engines; the delta includes that difference")
	e.diff("cache mode", a.RunEnv.CacheMode, b.RunEnv.CacheMode,
		"the runs began from different cache states, which usually dominates a read workload's timing")
	e.diff("replay mode", a.Plan.Mode, b.Plan.Mode,
		"the runs issued operations on different schedules, so their latencies measure different things")
	e.diff("max-inflight", itoa(a.Plan.MaxInflight), itoa(b.Plan.MaxInflight),
		"the load-generator concurrency cap differs, so the offered load differs")
	e.diff("prepare", a.Plan.PrepareMode, b.Plan.PrepareMode,
		"the datasets were prepared differently, so the runs may not have read equivalent data")
	e.diff("fill", fillLabel(a), fillLabel(b),
		"target contents were generated differently, which changes what a compressing or "+
			"deduplicating backend has to store")
	e.diff("bucket", a.Plan.Bucket, b.Plan.Bucket,
		"the runs addressed different buckets")
	e.diff("endpoint", a.Plan.Endpoint, b.Plan.Endpoint,
		"the runs addressed different endpoints")

	if speedupRelevant(a) || speedupRelevant(b) {
		e.diff("speedup", ftoa(a.Plan.SpeedupFactor), ftoa(b.Plan.SpeedupFactor),
			"scaled mode replayed the trace's timeline at different rates")
	}

	if bothCurrent {
		e.diff("target root", rootLabel(a), rootLabel(b),
			"the runs were confined differently, so they were not able to touch the same data")
		e.diff("host", a.Host.Hostname, b.Host.Hostname,
			"the runs executed on different hosts, so the delta includes every difference between them")
		e.diff("build", toolLabel(a), toolLabel(b),
			"the runs were measured by different builds of ioflux, so a change in the tool cannot be "+
				"separated from a change in the backend")
	}

	e.diff("replay equivalence", a.Plan.ReplayEquivalence, b.Plan.ReplayEquivalence,
		"one side's writes were coalesced into object-level PUTs while the other replayed at the "+
			"syscall level; the delta may reflect that transformation rather than backend performance")

	if a.Fidelity.LowFidelity != b.Fidelity.LowFidelity {
		e.Caveats = append(e.Caveats, Difference{
			Field: "fidelity",
			A:     fidelityLabel(a),
			B:     fidelityLabel(b),
			Note: "one side did not keep to the trace's schedule, so its timing reflects the replay " +
				"falling behind as well as the backend",
		})
	}

	e.Verdict = verdictFor(e)
	return e
}

// verdictFor derives the verdict from what a gate found. It is shared so a
// caller that adds or removes findings after the fact — a paired experiment
// discounting its own declared treatment — cannot leave the verdict disagreeing
// with the reasons printed beneath it.
func verdictFor(e Eligibility) Verdict {
	switch {
	case len(e.Blocking) > 0:
		return VerdictIncomparable
	case len(e.Caveats) > 0:
		return VerdictCaveated
	default:
		return VerdictComparable
	}
}

// checkTraceIdentity compares what the two runs replayed. A differing digest is
// a caveat rather than a refusal because comparing two workloads is a supported
// use (a checkpoint-write report against a training-read one); what must not
// happen is presenting that delta as though it measured the backend.
func (e *Eligibility) checkTraceIdentity(a, b *Results) {
	da, db := a.Plan.TraceDigest, b.Plan.TraceDigest
	if da == "" || db == "" {
		e.Caveats = append(e.Caveats, Difference{
			Field: "trace",
			A:     digestLabel(a),
			B:     digestLabel(b),
			Note: "trace identity is missing on at least one side, so whether both runs replayed the " +
				"same workload cannot be verified — the file names agreeing is not evidence that the " +
				"bytes did",
		})
		return
	}
	if da == db {
		return
	}
	// Two differing digests look identical whether one trace is a declared
	// transformation of the other or the two are unrelated workloads. The ledger
	// is what tells them apart, and the distinction matters: "the treatment
	// replayed the same workload with smaller reads" supports a conclusion that
	// "these are two different workloads" does not.
	if note := derivationNote(a, b); note != "" {
		e.Caveats = append(e.Caveats, Difference{
			Field: "trace",
			A:     digestLabel(a),
			B:     digestLabel(b),
			Note:  note,
		})
		return
	}
	e.Caveats = append(e.Caveats, Difference{
		Field: "trace",
		A:     digestLabel(a),
		B:     digestLabel(b),
		Note: "the runs replayed different traces, so the delta measures the difference between two " +
			"workloads and not between two backends",
	})
}

// derivationNote describes how one side's trace was derived from the other's,
// or "" when neither was.
func derivationNote(a, b *Results) string {
	if desc := transformDescription(b, a.Plan.TraceDigest); desc != "" {
		return "B replayed a declared transformation of A's trace (" + desc +
			"), so the difference measures that change and not two unrelated workloads"
	}
	if desc := transformDescription(a, b.Plan.TraceDigest); desc != "" {
		return "A replayed a declared transformation of B's trace (" + desc +
			"), so the difference measures that change and not two unrelated workloads"
	}
	return ""
}

// TransformationOf renders the transformations in r's trace that were applied
// to sourceDigest, or "" when r's trace was not derived from it. It lets a
// report state that one run replayed a declared transformation of another's
// trace rather than an unrelated file.
func TransformationOf(r *Results, sourceDigest string) string {
	return transformDescription(r, sourceDigest)
}

// transformDescription renders the transformations in r's trace that were
// applied to sourceDigest, or "" when none were.
func transformDescription(r *Results, sourceDigest string) string {
	if sourceDigest == "" {
		return ""
	}
	var parts []string
	for _, t := range r.Plan.TraceTransformations {
		if t.SourceDigest != sourceDigest {
			continue
		}
		desc := t.Kind
		if len(t.Params) > 0 {
			keys := make([]string, 0, len(t.Params))
			for k := range t.Params {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				desc += " " + k + "=" + t.Params[k]
			}
		}
		parts = append(parts, desc)
	}
	return strings.Join(parts, ", ")
}

// diff records a caveat when two values disagree.
func (e *Eligibility) diff(field, av, bv, note string) {
	if av == bv {
		return
	}
	e.Caveats = append(e.Caveats, Difference{Field: field, A: unset(av), B: unset(bv), Note: note})
}

// speedupRelevant reports whether a run's speedup factor affected its schedule.
// Outside scaled mode the field is carried but unused, and reporting a
// difference in an ignored field would be noise.
func speedupRelevant(r *Results) bool { return r.Plan.Mode == "scaled" }

// digestLabel renders a trace digest short enough to read in a table: the
// algorithm prefix plus the first 12 hex characters. The full digest stays in
// the results file for anyone who needs to verify it exactly.
func digestLabel(r *Results) string {
	d := r.Plan.TraceDigest
	if d == "" {
		return "(not recorded)"
	}
	if i := strings.IndexByte(d, ':'); i >= 0 && len(d) > i+13 {
		return d[:i+13]
	}
	return d
}

func schemaLabel(r *Results) string {
	if r.SchemaVersion == 0 {
		return "pre-versioned"
	}
	return strconv.Itoa(r.SchemaVersion)
}

func rootLabel(r *Results) string {
	if r.Plan.TargetRoot == "" {
		return "(unconfined)"
	}
	return r.Plan.TargetRoot
}

func toolLabel(r *Results) string {
	if r.Tool.Revision != "" {
		return r.Tool.Version + " (" + r.Tool.Revision + ")"
	}
	return r.Tool.Version
}

func fillLabel(r *Results) string {
	if r.Plan.FillMode == "" {
		return ""
	}
	return fmt.Sprintf("%s/%d", r.Plan.FillMode, r.Plan.FillSeed)
}

func fidelityLabel(r *Results) string {
	if !r.Fidelity.LowFidelity {
		return "ok"
	}
	if r.Fidelity.LowFidelityCategory != "" {
		return "low [" + r.Fidelity.LowFidelityCategory + "]"
	}
	return "low"
}

func unset(s string) string {
	if s == "" {
		return "(unset)"
	}
	return s
}

func itoa(n int) string { return strconv.Itoa(n) }

func ftoa(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

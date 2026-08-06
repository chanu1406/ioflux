package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chanuollala/ioflux/pkg/results"
)

const reportUsage = `Usage:
  ioflux report <results.json>
  ioflux report <a.json> <b.json>

Pretty-print a saved run report or trial set. Pass - to read from stdin.

Given two files, check whether they may be compared and, if so, print the
comparison. Two single runs are compared on their headline scalars; two trial
sets are compared statistically — median, coefficient of variation, and a 95%
interval on each median — under a policy fixed by the flags below.

Flags:
  --min-trials <n>   Minimum valid trials per side of a trial-set comparison
                     (default 6, the point below which no interval exists).
  --max-cv <pct>     Maximum duration coefficient of variation per side
                     (default 5). A spread wider than the difference being
                     looked for cannot support a conclusion about it.

Both defaults are floors, not universal values; a fixture should declare its own.

The comparison is gated. A run that did not execute validly cannot be either
side of a comparison, so the delta is refused rather than printed. Differences
that change what a delta means — a different trace, engine, cache state, host,
or build — are reported as caveats above the numbers they qualify.

Exit codes:
  0   report printed, or comparison printed (with or without caveats)
  1   parse error, or comparison refused as incomparable
  2   usage error or I/O failure
`

// runReport is the entry point for the `report` subcommand.
func runReport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, reportUsage) }
	defaults := results.DefaultTrialPolicy()
	minTrials := fs.Int("min-trials", defaults.MinValidTrials,
		"minimum valid trials each side of a trial-set comparison must have")
	maxCV := fs.Float64("max-cv", defaults.MaxCVPercent,
		"maximum duration coefficient of variation (percent) each side may have")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	policy := results.TrialPolicy{MinValidTrials: *minTrials, MaxCVPercent: *maxCV}

	switch fs.NArg() {
	case 1:
		art, code := loadArtifact(fs.Arg(0), stderr)
		if code != 0 {
			return code
		}
		switch {
		case art.paired != nil:
			// A paired experiment already carries its own verdict; re-printing it
			// must reach the same conclusion, including the refusal.
			if !printPairedReport(stdout, art.paired) {
				return 1
			}
		case art.trials != nil:
			printTrialSetReport(stdout, art.trials)
		default:
			printRunReport(stdout, art.single)
		}
		return 0
	case 2:
		a, code := loadArtifact(fs.Arg(0), stderr)
		if code != 0 {
			return code
		}
		b, code := loadArtifact(fs.Arg(1), stderr)
		if code != 0 {
			return code
		}
		// A paired experiment is already a comparison and carries its own verdict;
		// comparing two of them is a different question (was the treatment's
		// effect the same on two occasions?) that this command does not answer.
		if a.paired != nil || b.paired != nil {
			fmt.Fprintln(stderr, "ioflux report: a paired experiment is already a comparison — "+
				"print it on its own with `ioflux report <file>`")
			return 2
		}
		// Comparing a trial set against a single run would silently reduce the
		// set to one number, which is the thing repeated trials exist to prevent.
		if (a.trials == nil) != (b.trials == nil) {
			fmt.Fprintln(stderr, "ioflux report: cannot compare a trial set against a single run — "+
				"re-run the single side with --trials so both carry a distribution")
			return 2
		}
		if a.trials != nil {
			if !printTrialComparison(stdout, a.trials, b.trials, policy) {
				return 1
			}
			return 0
		}
		if !printComparison(stdout, a.single, b.single) {
			return 1
		}
		return 0
	default:
		fmt.Fprint(stderr, reportUsage)
		return 2
	}
}

// artifact is either a single run's results or a set of trials; `ioflux report`
// accepts both and one file is exactly one of them.
type artifact struct {
	single *results.Results
	trials *results.TrialSet
	paired *results.PairedExperiment
}

// loadArtifact reads a results file and determines which kind it is. A trial
// set is identified by carrying a trials array: a single result has no such
// field, so the two are distinguishable without a discriminator that older
// files would lack.
func loadArtifact(path string, stderr io.Writer) (artifact, int) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		fmt.Fprintf(stderr, "ioflux report: %v\n", err)
		return artifact{}, 2
	}

	var probe struct {
		Trials    []json.RawMessage `json:"trials"`
		Baseline  json.RawMessage   `json:"baseline"`
		Treatment json.RawMessage   `json:"treatment"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		fmt.Fprintf(stderr, "ioflux report: parse %s: %v\n", path, err)
		return artifact{}, 1
	}
	if probe.Baseline != nil && probe.Treatment != nil {
		var pe results.PairedExperiment
		if err := json.Unmarshal(data, &pe); err != nil {
			fmt.Fprintf(stderr, "ioflux report: parse paired experiment: %v\n", err)
			return artifact{}, 1
		}
		if pe.Baseline == nil || pe.Treatment == nil ||
			len(pe.Baseline.Trials) == 0 || len(pe.Treatment.Trials) == 0 {
			fmt.Fprintf(stderr, "ioflux report: %s is a paired experiment with an empty arm\n", path)
			return artifact{}, 1
		}
		return artifact{paired: &pe}, 0
	}
	if probe.Trials != nil {
		var ts results.TrialSet
		if err := json.Unmarshal(data, &ts); err != nil {
			fmt.Fprintf(stderr, "ioflux report: parse trial set: %v\n", err)
			return artifact{}, 1
		}
		if len(ts.Trials) == 0 {
			fmt.Fprintf(stderr, "ioflux report: %s is a trial set containing no trials\n", path)
			return artifact{}, 1
		}
		return artifact{trials: &ts}, 0
	}

	var res results.Results
	if err := json.Unmarshal(data, &res); err != nil {
		fmt.Fprintf(stderr, "ioflux report: parse results.json: %v\n", err)
		return artifact{}, 1
	}
	return artifact{single: &res}, 0
}

func printRunReport(w io.Writer, res *results.Results) {
	plan := res.Plan
	env := res.RunEnv

	// --- Header ---
	kind := plan.TraceKind
	if plan.Profile != "" {
		kind = kind + "/" + plan.Profile
	}
	fmt.Fprintf(w, "Trace:     %s\n", plan.TracePath)
	fmt.Fprintf(w, "           [%s · %d stream(s) · %d op(s) · %s]\n",
		kind,
		plan.NumStreams,
		plan.NumOps,
		fmtBytes(plan.TotalBytes),
	)
	if plan.CaptureMethod != "" {
		fmt.Fprintf(w, "Source:    %s\n", plan.CaptureMethod)
	}
	if plan.TracePartialReads > 0 {
		// Stated explicitly because reproducing these is invisible otherwise: the
		// replay matched the source and so reports no short read, which is
		// indistinguishable from a workload that had none.
		fmt.Fprintf(w, "           [%d source read(s) returned less than requested; "+
			"replay issues the source's request size and requires its returned size]\n",
			plan.TracePartialReads)
	}
	if plan.CaptureLimitations != "" {
		fmt.Fprintf(w, "Capture limitations: %s\n", plan.CaptureLimitations)
	}
	fmt.Fprintf(w, "Engine:    %s   mode: %s   max-inflight: %d\n",
		plan.Engine, plan.Mode, plan.MaxInflight)
	if env.CacheMode != "" {
		fmt.Fprintf(w, "Cache:     %s\n", env.CacheMode)
	}
	if plan.PrepareMode != "" {
		fmt.Fprintf(w, "Prepare:   %s\n", plan.PrepareMode)
	}
	fmt.Fprintf(w, "Run:       %s   duration: %s\n",
		res.GeneratedAt, fmtDuration(res.DurationNS))
	invalidReasons := res.ExecutionInvalidReasons()
	if len(invalidReasons) > 0 {
		fmt.Fprintf(w, "Execution: INVALID — %s\n", strings.Join(invalidReasons, "; "))
	} else {
		fmt.Fprintln(w, "Execution: no detected operation failures")
	}

	// --- Throughput ---
	fmt.Fprintln(w)
	opsPerSec, gibPerSec := throughput(res)
	fmt.Fprintf(w, "Throughput:  %.1f ops/s   %.3f GiB/s\n", opsPerSec, gibPerSec)
	fmt.Fprintf(w, "             %d ops completed   %s moved\n",
		res.OpsCompleted, fmtBytes(res.BytesMoved))

	// --- Per-op latency table ---
	if len(res.PerOpStats) > 0 {
		fmt.Fprintln(w)
		printOpStatsTable(w, "Latency (µs):", res.PerOpStats)
	}

	// In timeline/scaled mode the headline latency is coordinated-omission
	// corrected (completion − intended arrival); the service-time table shows
	// what the backend itself did, so the two are worth separating. In asap
	// mode they are the same measurement, so the second table is omitted.
	if (plan.Mode == "timeline" || plan.Mode == "scaled") && len(res.ServiceTimeStats) > 0 {
		fmt.Fprintln(w)
		printOpStatsTable(w, "Service time (µs, excludes schedule wait):", res.ServiceTimeStats)
	}

	// --- CPU ---
	fmt.Fprintln(w)
	fmt.Fprintf(w, "CPU:  user %s   sys %s   wall %s\n",
		fmtDuration(res.CPU.UserNS),
		fmtDuration(res.CPU.SysNS),
		fmtDuration(res.CPU.WallNS),
	)

	// --- Distribution (multi-host runs only) ---
	if len(res.Hosts) > 1 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Hosts (%d):\n", len(res.Hosts))
		fmt.Fprintf(w, "  %-20s %10s %12s %10s %10s\n",
			"Host", "Ops", "Bytes", "1st-done", "last-done")
		for _, h := range res.Hosts {
			name := h.Hostname
			if name == "" {
				name = "(unnamed)"
			}
			fmt.Fprintf(w, "  %-20s %10d %12s %10s %10s\n",
				name, h.OpsCompleted, fmtBytes(h.BytesMoved),
				fmtDuration(h.FirstDoneNS), fmtDuration(h.LastDoneNS))
		}
		if sw := res.Straggler; sw != nil {
			fmt.Fprintf(w, "  straggler window:  first-done %s   last-done %s   skew %s\n",
				fmtDuration(sw.FirstDoneNS), fmtDuration(sw.LastDoneNS), fmtDuration(sw.SkewNS))
			fmt.Fprintf(w, "  first-done:        %.1f ops/s   %.3f GiB/s   (excludes straggler tail)\n",
				sw.FirstDoneOpsPerSec, sw.FirstDoneGiBPerSec)
			fmt.Fprintf(w, "  last-done:         %.1f ops/s   %.3f GiB/s\n",
				sw.LastDoneOpsPerSec, sw.LastDoneGiBPerSec)
		}
		if res.GoDeliverySkewNS > 0 {
			fmt.Fprintf(w, "  go-delivery skew:  %s\n", fmtDuration(res.GoDeliverySkewNS))
		}
	}

	// --- Fidelity ---
	fid := res.Fidelity
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Fidelity:\n")

	issuePct := 100.0
	if fid.Coverage.OpsInTrace > 0 {
		issuePct = float64(fid.Coverage.OpsIssued) / float64(fid.Coverage.OpsInTrace) * 100
	}
	fmt.Fprintf(w, "  coverage:       %d/%d ops (%.1f%%)\n",
		fid.Coverage.OpsIssued, fid.Coverage.OpsInTrace, issuePct)

	// Structural guarantee, not an independent measurement: the scheduler
	// dispatches each (non-grouped) stream one op at a time, so this can only
	// exceed 1 if the scheduler itself has a bug.
	ccStatus := "guaranteed by scheduler"
	if fid.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		ccStatus = fmt.Sprintf("SCHEDULER BUG: max per-stream in-flight = %d (streams: %v)",
			fid.ConcurrencyCheck.MaxPerStreamInflight, fid.ConcurrencyCheck.Violations)
	}
	fmt.Fprintf(w, "  concurrency:    max-per-stream %d [%s]\n",
		fid.ConcurrencyCheck.MaxPerStreamInflight, ccStatus)

	fmt.Fprintf(w, "  backlog:        %d event(s)   %s blocked   %.1f%% of ops   peak depth %d\n",
		fid.Backlog.TotalEvents,
		fmtDuration(fid.Backlog.TotalBlockedNS),
		fid.Backlog.FractionOpsBacklogged*100,
		fid.Backlog.PeakInflightDepth,
	)

	if fid.ScheduleDrift.P99NS > 0 || fid.ScheduleDrift.MaxNS > 0 {
		fmt.Fprintf(w, "  schedule drift: p99 %s   p999 %s   max %s   mean %s\n",
			fmtDuration(fid.ScheduleDrift.P99NS),
			fmtDuration(fid.ScheduleDrift.P999NS),
			fmtDuration(fid.ScheduleDrift.MaxNS),
			fmtDuration(int64(fid.ScheduleDrift.MeanNS)),
		)
	}
	if fid.CompletionLag.P99NS > 0 || fid.CompletionLag.MaxNS > 0 {
		fmt.Fprintf(w, "  completion lag: p99 %s   p999 %s   max %s   mean %s\n",
			fmtDuration(fid.CompletionLag.P99NS),
			fmtDuration(fid.CompletionLag.P999NS),
			fmtDuration(fid.CompletionLag.MaxNS),
			fmtDuration(int64(fid.CompletionLag.MeanNS)),
		)
	}

	if fid.LowFidelity {
		if fid.LowFidelityCategory != "" {
			fmt.Fprintf(w, "  low-fidelity:   YES [%s] — %s\n", fid.LowFidelityCategory, fid.LowFidelityReason)
		} else {
			fmt.Fprintf(w, "  low-fidelity:   YES — %s\n", fid.LowFidelityReason)
		}
	} else {
		fmt.Fprintf(w, "  low-fidelity:   no\n")
	}

	// --- Warnings ---
	var warnings []string
	if res.Errors > 0 {
		warnings = append(warnings, fmt.Sprintf("%d op error(s)", res.Errors))
	}
	if res.ShortReads > 0 {
		// Not necessarily *fewer*: the trace records what the source received, so a
		// target longer than the source's returns more than expected and is just as
		// much a disagreement. Naming only the undersized case would misdescribe
		// half the failures this check exists to catch.
		warnings = append(warnings, fmt.Sprintf(
			"%d read(s) returned a different number of bytes than the source did "+
				"(mismatched target sizes?)", res.ShortReads))
	}
	if res.HistogramOverflows > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"%d histogram overflow(s): latency sample(s) exceeded the 100s trackable range and were excluded "+
				"from every percentile (op still completed; possible hang or stall)", res.HistogramOverflows))
	}
	warnings = append(warnings, env.EngineLimitations...)
	warnings = append(warnings, env.CacheLimitations...)

	fmt.Fprintln(w)
	if len(warnings) == 0 {
		fmt.Fprintf(w, "Warnings:  none\n")
	} else {
		fmt.Fprintf(w, "Warnings:\n")
		for _, msg := range warnings {
			fmt.Fprintf(w, "  ! %s\n", msg)
		}
	}
}

// throughput returns ops/s and GiB/s for a completed run.
func throughput(res *results.Results) (opsPerSec, gibPerSec float64) { return res.Throughput() }

// printComparison prints the eligibility verdict for two run reports and, when
// the verdict permits it, a side-by-side delta of their headline scalars
// followed by each side's dominant data-op latency table.
//
// It returns false when the comparison was refused. A refusal prints no deltas
// at all: the throughput numbers are the part of this output that gets quoted,
// and printing them under a warning has repeatedly proved to be the same thing
// as printing them without one.
func printComparison(w io.Writer, a, b *results.Results) bool {
	elig := results.CheckEligibility(a, b)

	fmt.Fprintf(w, "Comparing two reports:\n")
	fmt.Fprintf(w, "  A: %s\n", describeSide(a))
	fmt.Fprintf(w, "  B: %s\n", describeSide(b))

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Eligibility: %s\n", strings.ToUpper(string(elig.Verdict)))

	if !elig.Comparable() {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Refusing to report a delta — at least one run is not a valid measurement:\n")
		for _, reason := range elig.Blocking {
			fmt.Fprintf(w, "  ! %s\n", reason)
		}
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Inspect each run on its own with `ioflux report <file>`.\n")
		return false
	}

	printComparisonCaveats(w, elig, "backend", "the two runs agree on trace, engine, environment, and build")

	fmt.Fprintln(w)
	fmt.Fprintf(w, "%-14s %16s %16s %16s\n", "", "A", "B", "Δ (B-A)")
	row := func(label, av, bv, dv string) {
		fmt.Fprintf(w, "%-14s %16s %16s %16s\n", label, av, bv, dv)
	}
	row2 := func(label, av, bv string) {
		fmt.Fprintf(w, "%-14s %16s %16s\n", label, av, bv)
	}

	row2("kind", a.Plan.TraceKind, b.Plan.TraceKind)
	row2("profile", orDash(a.Plan.Profile), orDash(b.Plan.Profile))
	// Engine and cache state belong in the table for the same reason the delta
	// does: they are what a reader needs in order to know what the delta is a
	// delta of.
	row2("engine", orDash(a.Plan.Engine), orDash(b.Plan.Engine))
	row2("cache", orDash(a.RunEnv.CacheMode), orDash(b.RunEnv.CacheMode))
	row2("mode", a.Plan.Mode, b.Plan.Mode)
	row2("max-inflight", fmt.Sprint(a.Plan.MaxInflight), fmt.Sprint(b.Plan.MaxInflight))
	row2("equivalence", orDash(a.Plan.ReplayEquivalence), orDash(b.Plan.ReplayEquivalence))

	row("duration", fmtDuration(a.DurationNS), fmtDuration(b.DurationNS), fmtSignedDuration(b.DurationNS-a.DurationNS))

	aOps, aGiB := throughput(a)
	bOps, bGiB := throughput(b)
	row("ops/s", fmt.Sprintf("%.1f", aOps), fmt.Sprintf("%.1f", bOps), fmt.Sprintf("%+.1f", bOps-aOps))
	row("GiB/s", fmt.Sprintf("%.3f", aGiB), fmt.Sprintf("%.3f", bGiB), fmt.Sprintf("%+.3f", bGiB-aGiB))

	row("CPU user", fmtDuration(a.CPU.UserNS), fmtDuration(b.CPU.UserNS), fmtSignedDuration(b.CPU.UserNS-a.CPU.UserNS))
	row("CPU sys", fmtDuration(a.CPU.SysNS), fmtDuration(b.CPU.SysNS), fmtSignedDuration(b.CPU.SysNS-a.CPU.SysNS))
	row("CPU wall", fmtDuration(a.CPU.WallNS), fmtDuration(b.CPU.WallNS), fmtSignedDuration(b.CPU.WallNS-a.CPU.WallNS))

	row2("low-fidelity", lowFidelityLabel(a), lowFidelityLabel(b))

	fmt.Fprintln(w)
	printDominantOpLatency(w, "A", a)
	printDominantOpLatency(w, "B", b)
	return true
}

// printTrialSetReport prints one trial set: what was replayed, the distribution
// over its trials, and any trial that did not execute validly.
func printTrialSetReport(w io.Writer, ts *results.TrialSet) {
	rep := ts.Representative()
	s := ts.Summary

	fmt.Fprintf(w, "Trace:     %s\n", describeSide(rep))
	fmt.Fprintf(w, "Engine:    %s   mode: %s   cache: %s\n",
		rep.Plan.Engine, rep.Plan.Mode, orDash(rep.RunEnv.CacheMode))
	fmt.Fprintf(w, "Trials:    %d measured", s.Trials)
	if ts.WarmupTrials > 0 {
		fmt.Fprintf(w, ", %d warmup (discarded)", ts.WarmupTrials)
	}
	fmt.Fprintln(w)

	if s.FailedTrials > 0 {
		fmt.Fprintf(w, "Execution: INVALID — %d of %d trial(s) failed: trial %v\n",
			s.FailedTrials, s.Trials, ts.FailedTrialIndexes())
	} else {
		fmt.Fprintln(w, "Execution: no detected operation failures")
	}

	if s.ValidTrials == 0 {
		fmt.Fprintln(w, "\nNo valid trials — nothing to summarize.")
		return
	}

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Duration over %d valid trial(s):\n", s.ValidTrials)
	printMetricSummary(w, s.DurationNS, func(v float64) string { return fmtDuration(int64(v)) })

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Throughput over %d valid trial(s):\n", s.ValidTrials)
	printMetricSummary(w, s.GiBPerSec, func(v float64) string { return fmt.Sprintf("%.3f GiB/s", v) })

	fmt.Fprintln(w)
	printStabilityNote(w, s.DurationNS)
}

// printMetricSummary renders one metric's distribution, formatting values with
// fmtVal so durations and rates read naturally.
func printMetricSummary(w io.Writer, m results.MetricSummary, fmtVal func(float64) string) {
	fmt.Fprintf(w, "  median:  %s\n", fmtVal(m.Median))
	fmt.Fprintf(w, "  mean:    %s   stddev %s   CV %.1f%%\n",
		fmtVal(m.Mean), fmtVal(m.StdDev), m.CVPercent)
	fmt.Fprintf(w, "  range:   %s … %s\n", fmtVal(m.Min), fmtVal(m.Max))
	if m.CI95Available {
		fmt.Fprintf(w, "  95%% CI:  %s … %s (on the median)\n", fmtVal(m.CI95Lo), fmtVal(m.CI95Hi))
	} else {
		fmt.Fprintf(w, "  95%% CI:  unavailable — %d trial(s) is too few to bound the median\n", m.N)
	}
}

// printStabilityNote states what the observed spread does to a comparison,
// because a coefficient of variation is only meaningful against the size of the
// difference someone intends to detect.
func printStabilityNote(w io.Writer, m results.MetricSummary) {
	if m.N < 2 {
		return
	}
	fmt.Fprintf(w, "Stability: run-to-run spread is %.1f%%; a difference smaller than that "+
		"cannot be distinguished from noise by these trials.\n", m.CVPercent)
}

// printTrialComparison prints the gated statistical comparison of two trial
// sets, returning false when the comparison was refused.
func printTrialComparison(w io.Writer, a, b *results.TrialSet, policy results.TrialPolicy) bool {
	tc := results.CompareTrialSets(a, b, policy)

	fmt.Fprintf(w, "Comparing two trial sets:\n")
	fmt.Fprintf(w, "  A: %s  (%d valid trial(s))\n", describeSide(a.Representative()), tc.A.ValidTrials)
	fmt.Fprintf(w, "  B: %s  (%d valid trial(s))\n", describeSide(b.Representative()), tc.B.ValidTrials)
	fmt.Fprintf(w, "  policy: at least %d valid trial(s) per side, CV at most %.1f%%\n",
		policy.MinValidTrials, policy.MaxCVPercent)

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Eligibility: %s\n", strings.ToUpper(string(tc.Eligibility.Verdict)))

	if !tc.Eligibility.Comparable() {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Refusing to report a difference:\n")
		for _, reason := range tc.Eligibility.Blocking {
			fmt.Fprintf(w, "  ! %s\n", reason)
		}
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Inspect each set on its own with `ioflux report <file>`.\n")
		return false
	}

	printComparisonCaveats(w, tc.Eligibility, "backend", "the two runs agree on trace, engine, environment, and build")

	fmt.Fprintln(w)
	fmt.Fprintf(w, "%-14s %18s %18s\n", "duration", "A", "B")
	fmt.Fprintf(w, "%-14s %18s %18s\n", "  median",
		fmtDuration(int64(tc.A.DurationNS.Median)), fmtDuration(int64(tc.B.DurationNS.Median)))
	fmt.Fprintf(w, "%-14s %18s %18s\n", "  CV",
		fmt.Sprintf("%.1f%%", tc.A.DurationNS.CVPercent), fmt.Sprintf("%.1f%%", tc.B.DurationNS.CVPercent))
	fmt.Fprintf(w, "%-14s %18s %18s\n", "  95% CI",
		ciLabel(tc.A.DurationNS), ciLabel(tc.B.DurationNS))

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Difference:  %s (%+.1f%%) in median duration\n",
		fmtSignedDuration(int64(tc.DeltaMedianNS)), tc.DeltaPercent)
	fmt.Fprintf(w, "             %s\n", separationSentence(tc))
	return true
}

// ciLabel renders a metric's interval, or says it has none.
func ciLabel(m results.MetricSummary) string {
	if !m.CI95Available {
		return "unavailable"
	}
	return fmt.Sprintf("%s … %s", fmtDuration(int64(m.CI95Lo)), fmtDuration(int64(m.CI95Hi)))
}

// separationSentence states what the intervals do and do not establish. The
// asymmetry is deliberate: disjoint intervals are evidence of a difference,
// while overlapping ones are not evidence of its absence.
func separationSentence(tc results.TrialComparison) string {
	switch {
	case !tc.SeparationKnown:
		return "too few trials to bound either median, so this difference is not established."
	case tc.Separated:
		return "the two intervals do not overlap, so the difference is unlikely to be noise."
	default:
		return "the two intervals overlap, so these trials do not establish a difference — " +
			"which is not the same as showing there is none."
	}
}

// printPairedReport prints a paired experiment, returning false when the
// comparison was refused.
func printPairedReport(w io.Writer, pe *results.PairedExperiment) bool {
	fmt.Fprintln(w)
	if pe.Claim != "" {
		fmt.Fprintf(w, "Claim:     %s\n", pe.Claim)
	}
	fmt.Fprintf(w, "Trace:     %s\n", describeSide(pe.Baseline.Representative()))
	if len(pe.TreatmentVariables) == 0 {
		fmt.Fprintf(w, "Treatment: none — the two arms were configured identically\n")
	} else {
		fmt.Fprintf(w, "Treatment: %v\n", pe.TreatmentVariables)
		for _, name := range pe.TreatmentVariables {
			fmt.Fprintf(w, "             %-16s baseline %-20s treatment %s\n", name,
				armFieldValue(pe.Baseline, name), armFieldValue(pe.Treatment, name))
		}
	}
	if desc := results.TransformationOf(
		pe.Treatment.Representative(), pe.Baseline.Representative().Plan.TraceDigest); desc != "" {
		// Without this the treatment reads as an unrelated file swapped in; the
		// ledger is what makes it the same workload under a declared change.
		fmt.Fprintf(w, "             the treatment trace is a declared transformation of the baseline's: %s\n", desc)
	}
	fmt.Fprintf(w, "Pairs:     %d measured, interleaved (seed %d)\n", len(pe.PairOrder), pe.Seed)
	fmt.Fprintf(w, "Policy:    at least %d valid trial(s) per arm, CV at most %.1f%%\n",
		pe.Policy.MinValidTrials, pe.Policy.MaxCVPercent)

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Eligibility: %s\n", strings.ToUpper(string(pe.Eligibility.Verdict)))

	if !pe.Eligibility.Comparable() {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Refusing to report a difference:\n")
		for _, reason := range pe.Eligibility.Blocking {
			fmt.Fprintf(w, "  ! %s\n", reason)
		}
		return false
	}

	printComparisonCaveats(w, pe.Eligibility, "treatment", "nothing differs between the arms beyond the declared treatment")

	b, tr := pe.Baseline.Summary.DurationNS, pe.Treatment.Summary.DurationNS
	fmt.Fprintln(w)
	fmt.Fprintf(w, "%-14s %18s %18s\n", "duration", "baseline", "treatment")
	fmt.Fprintf(w, "%-14s %18s %18s\n", "  median",
		fmtDuration(int64(b.Median)), fmtDuration(int64(tr.Median)))
	fmt.Fprintf(w, "%-14s %18s %18s\n", "  CV",
		fmt.Sprintf("%.1f%%", b.CVPercent), fmt.Sprintf("%.1f%%", tr.CVPercent))

	p := pe.Paired
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Paired difference over %d pair(s) (treatment - baseline):\n", p.Pairs)
	fmt.Fprintf(w, "  median:  %s (%+.1f%%)\n", fmtSignedDuration(int64(p.Delta.Median)), p.DeltaPercent)
	if p.Delta.CI95Available {
		fmt.Fprintf(w, "  95%% CI:  %s … %s\n",
			fmtSignedDuration(int64(p.Delta.CI95Lo)), fmtSignedDuration(int64(p.Delta.CI95Hi)))
	} else {
		fmt.Fprintf(w, "  95%% CI:  unavailable — %d pair(s) is too few to bound the difference\n", p.Pairs)
	}
	fmt.Fprintf(w, "  %s\n", pairedVerdictSentence(p))
	return true
}

// pairedVerdictSentence states what the interval on the paired difference does
// and does not establish.
func pairedVerdictSentence(p results.PairedSummary) string {
	switch {
	case !p.Delta.CI95Available:
		return "too few pairs to bound the difference, so nothing is established."
	case p.ExcludesZero:
		return "the interval excludes zero, so the treatment changed the workload's duration."
	default:
		return "the interval includes zero, so these pairs do not establish a difference — " +
			"which is not the same as showing there is none."
	}
}

// armFieldValue reports one arm's value for a named setting, read back from the
// result rather than from the config, so the report shows what ran.
func armFieldValue(ts *results.TrialSet, name string) string {
	rep := ts.Representative()
	if rep == nil {
		return "?"
	}
	switch name {
	case "engine":
		return rep.Plan.Engine
	case "mode":
		return rep.Plan.Mode
	case "max_inflight":
		return fmt.Sprint(rep.Plan.MaxInflight)
	case "cache_mode":
		return orDash(rep.RunEnv.CacheMode)
	case "target_root":
		return orDash(rep.Plan.TargetRoot)
	case "trace":
		return rep.Plan.TracePath
	case "prepare":
		return orDash(rep.Plan.PrepareMode)
	case "bucket":
		return orDash(rep.Plan.Bucket)
	case "endpoint":
		return orDash(rep.Plan.Endpoint)
	case "fill":
		return orDash(rep.Plan.FillMode)
	case "fill_seed":
		return fmt.Sprint(rep.Plan.FillSeed)
	case "speedup":
		return fmt.Sprint(rep.Plan.SpeedupFactor)
	default:
		// A setting the result does not echo back (a target map path, a host
		// list). Naming it is still useful; inventing a value would not be.
		return "(not echoed in results)"
	}
}

// printComparisonCaveats renders the differences that change what the delta
// below them means. subject names what the delta would otherwise be credited
// to — the backend for an ad-hoc comparison, the treatment for an experiment.
func printComparisonCaveats(w io.Writer, elig results.Eligibility, subject, cleanNote string) {
	if len(elig.Caveats) == 0 {
		fmt.Fprintf(w, "  %s\n", cleanNote)
		return
	}
	fmt.Fprintf(w, "  the difference below is not attributable to the %s alone:\n", subject)
	for _, c := range elig.Caveats {
		fmt.Fprintf(w, "  ! %s: A=%s  B=%s\n", c.Field, c.A, c.B)
		fmt.Fprintf(w, "      %s\n", c.Note)
	}
}

// describeSide labels one side of a comparison by the trace it replayed, with
// its identity where recorded.
func describeSide(res *results.Results) string {
	if d := res.Plan.TraceDigest; d != "" {
		short := d
		if i := strings.IndexByte(d, ':'); i >= 0 && len(d) > i+13 {
			short = d[:i+13]
		}
		return fmt.Sprintf("%s [%s]", res.Plan.TracePath, short)
	}
	return res.Plan.TracePath
}

// orDash returns s, or "-" if it is empty (e.g. a field not recorded by an
// older results.json).
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// lowFidelityLabel summarizes a run's low-fidelity flag and category for the
// comparison table.
func lowFidelityLabel(res *results.Results) string {
	if !res.Fidelity.LowFidelity {
		return "no"
	}
	if res.Fidelity.LowFidelityCategory != "" {
		return fmt.Sprintf("YES [%s]", res.Fidelity.LowFidelityCategory)
	}
	return "YES"
}

// printDominantOpLatency prints the dominant data-op's latency table for one
// side of a comparison, labeled "A" or "B".
func printDominantOpLatency(w io.Writer, label string, res *results.Results) {
	op := res.DominantDataOp()
	if op == nil {
		fmt.Fprintf(w, "%s: no data ops\n", label)
		return
	}
	printOpStatsTable(w, fmt.Sprintf("%s (%s) latency (µs):", label, op.OpType), []results.PerOpStats{*op})
}

// printOpStatsTable renders one per-op percentile table under the given title.
func printOpStatsTable(w io.Writer, title string, stats []results.PerOpStats) {
	fmt.Fprintf(w, "%s\n", title)
	fmt.Fprintf(w, "  %-8s %8s %8s %8s %8s %8s %8s\n",
		"Op", "Count", "p50", "p90", "p99", "p999", "max")
	for _, s := range stats {
		fmt.Fprintf(w, "  %-8s %8d %8s %8s %8s %8s %8s\n",
			s.OpType,
			s.Count,
			fmtUS(s.P50NS),
			fmtUS(s.P90NS),
			fmtUS(s.P99NS),
			fmtUS(s.P999NS),
			fmtUS(s.MaxNS),
		)
	}
}

// fmtBytes formats a byte count as a human-readable string.
func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// fmtDuration formats a nanosecond count as a human-readable duration.
func fmtDuration(ns int64) string {
	if ns == 0 {
		return "0s"
	}
	switch {
	case ns >= 1_000_000_000:
		return fmt.Sprintf("%.3fs", float64(ns)/1e9)
	case ns >= 1_000_000:
		return fmt.Sprintf("%.1fms", float64(ns)/1e6)
	case ns >= 1_000:
		return fmt.Sprintf("%.1fµs", float64(ns)/1e3)
	default:
		return fmt.Sprintf("%dns", ns)
	}
}

// fmtSignedDuration formats a nanosecond delta with an explicit +/- sign, for
// the Δ column of a comparison table.
func fmtSignedDuration(ns int64) string {
	switch {
	case ns > 0:
		return "+" + fmtDuration(ns)
	case ns < 0:
		return "-" + fmtDuration(-ns)
	default:
		return fmtDuration(0)
	}
}

// fmtUS formats a nanosecond value as a microsecond string for the latency table.
func fmtUS(ns int64) string {
	if ns == 0 {
		return "-"
	}
	us := float64(ns) / 1e3
	if us >= 1000 {
		return fmt.Sprintf("%.0f", us)
	}
	if us >= 10 {
		return fmt.Sprintf("%.1f", us)
	}
	return fmt.Sprintf("%.2f", us)
}

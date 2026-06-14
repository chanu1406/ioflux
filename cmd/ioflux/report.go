package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/chanuollala/ioflux/pkg/results"
)

const reportUsage = `Usage:
  ioflux report <results.json>
  ioflux report <a.json> <b.json>

Pretty-print a saved run report. Pass - to read from stdin.

Given two reports, print a side-by-side comparison of their headline scalars
(throughput, CPU, duration, fidelity) and each side's dominant data-op latency
— e.g. to compare a checkpoint-write report against a training-read report.

Exit codes:
  0   report printed
  1   parse error
  2   usage error or I/O failure
`

// runReport is the entry point for the `report` subcommand.
func runReport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, reportUsage) }
	if err := fs.Parse(args); err != nil {
		return 2
	}

	switch fs.NArg() {
	case 1:
		res, code := loadResults(fs.Arg(0), stderr)
		if code != 0 {
			return code
		}
		printRunReport(stdout, res)
		return 0
	case 2:
		a, code := loadResults(fs.Arg(0), stderr)
		if code != 0 {
			return code
		}
		b, code := loadResults(fs.Arg(1), stderr)
		if code != 0 {
			return code
		}
		printComparison(stdout, a, b)
		return 0
	default:
		fmt.Fprint(stderr, reportUsage)
		return 2
	}
}

// loadResults reads and parses a results.json file, or stdin if path is "-".
// On error it writes a message to stderr and returns the exit code to use (2
// for an I/O failure, 1 for a parse error); the returned code is 0 on success.
func loadResults(path string, stderr io.Writer) (*results.Results, int) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		fmt.Fprintf(stderr, "ioflux report: %v\n", err)
		return nil, 2
	}

	var res results.Results
	if err := json.Unmarshal(data, &res); err != nil {
		fmt.Fprintf(stderr, "ioflux report: parse results.json: %v\n", err)
		return nil, 1
	}
	return &res, 0
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

	ccStatus := "ok"
	if fid.ConcurrencyCheck.MaxPerStreamInflight > 1 {
		ccStatus = fmt.Sprintf("VIOLATION: max per-stream in-flight = %d (streams: %v)",
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
		warnings = append(warnings, fmt.Sprintf(
			"%d short read(s): backend returned fewer bytes than the trace requested (undersized targets?)", res.ShortReads))
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

// throughput returns ops/s and GiB/s for a completed run, or (0, 0) if the
// run has no recorded duration.
func throughput(res *results.Results) (opsPerSec, gibPerSec float64) {
	if res.DurationNS <= 0 {
		return 0, 0
	}
	secs := float64(res.DurationNS) / 1e9
	return float64(res.OpsCompleted) / secs, float64(res.BytesMoved) / float64(1<<30) / secs
}

// dominantOp returns the PerOpStats entry for the first data-moving op type
// present, in priority order WRITE, READ, PUT, GET — the op whose latency
// best characterizes the run's workload. It returns nil if none are present
// (e.g. a metadata-only trace).
func dominantOp(res *results.Results) *results.PerOpStats {
	for _, kind := range []string{"WRITE", "READ", "PUT", "GET"} {
		for i := range res.PerOpStats {
			if res.PerOpStats[i].OpType == kind {
				return &res.PerOpStats[i]
			}
		}
	}
	return nil
}

// printComparison prints a side-by-side delta of two run reports' headline
// scalars, followed by each side's dominant data-op latency table. It is
// used to compare e.g. a checkpoint-write report against a training-read
// report.
func printComparison(w io.Writer, a, b *results.Results) {
	fmt.Fprintf(w, "Comparing two reports:\n")
	fmt.Fprintf(w, "  A: %s\n", a.Plan.TracePath)
	fmt.Fprintf(w, "  B: %s\n", b.Plan.TracePath)

	fmt.Fprintln(w)
	fmt.Fprintf(w, "%-14s %16s %16s %16s\n", "", "A", "B", "Δ (B-A)")
	row := func(label, av, bv, dv string) {
		fmt.Fprintf(w, "%-14s %16s %16s %16s\n", label, av, bv, dv)
	}
	row2 := func(label, av, bv string) {
		fmt.Fprintf(w, "%-14s %16s %16s\n", label, av, bv)
	}

	row2("kind", a.Plan.TraceKind, b.Plan.TraceKind)
	row2("profile", profileOrDash(a.Plan.Profile), profileOrDash(b.Plan.Profile))
	row2("mode", a.Plan.Mode, b.Plan.Mode)

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
}

// profileOrDash returns the trace profile, or "-" if it was not recorded
// (e.g. an older results.json predating the profile field).
func profileOrDash(profile string) string {
	if profile == "" {
		return "-"
	}
	return profile
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
	op := dominantOp(res)
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

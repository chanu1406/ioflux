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

Pretty-print a saved run report. Pass - to read from stdin.

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
	if fs.NArg() != 1 {
		fmt.Fprint(stderr, reportUsage)
		return 2
	}
	path := fs.Arg(0)

	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		fmt.Fprintf(stderr, "ioflux report: %v\n", err)
		return 2
	}

	var res results.Results
	if err := json.Unmarshal(data, &res); err != nil {
		fmt.Fprintf(stderr, "ioflux report: parse results.json: %v\n", err)
		return 1
	}

	printRunReport(stdout, &res)
	return 0
}

func printRunReport(w io.Writer, res *results.Results) {
	plan := res.Plan
	env := res.RunEnv

	// --- Header ---
	fmt.Fprintf(w, "Trace:     %s\n", plan.TracePath)
	fmt.Fprintf(w, "           [%s · %d stream(s) · %d op(s) · %s]\n",
		plan.TraceKind,
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
	var opsPerSec, gibPerSec float64
	if res.DurationNS > 0 {
		secs := float64(res.DurationNS) / 1e9
		opsPerSec = float64(res.OpsCompleted) / secs
		gibPerSec = float64(res.BytesMoved) / float64(1<<30) / secs
	}
	fmt.Fprintf(w, "Throughput:  %.1f ops/s   %.3f GiB/s\n", opsPerSec, gibPerSec)
	fmt.Fprintf(w, "             %d ops completed   %s moved\n",
		res.OpsCompleted, fmtBytes(res.BytesMoved))

	// --- Per-op latency table ---
	if len(res.PerOpStats) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Latency (µs):\n")
		fmt.Fprintf(w, "  %-8s %8s %8s %8s %8s %8s %8s\n",
			"Op", "Count", "p50", "p90", "p99", "p999", "max")
		for _, s := range res.PerOpStats {
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

	// --- CPU ---
	fmt.Fprintln(w)
	fmt.Fprintf(w, "CPU:  user %s   sys %s   wall %s\n",
		fmtDuration(res.CPU.UserNS),
		fmtDuration(res.CPU.SysNS),
		fmtDuration(res.CPU.WallNS),
	)

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
		fmt.Fprintf(w, "  low-fidelity:   YES — %s\n", fid.LowFidelityReason)
	} else {
		fmt.Fprintf(w, "  low-fidelity:   no\n")
	}

	// --- Warnings ---
	var warnings []string
	if res.Errors > 0 {
		warnings = append(warnings, fmt.Sprintf("%d op error(s)", res.Errors))
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

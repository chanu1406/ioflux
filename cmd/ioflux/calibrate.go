package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/chanuollala/ioflux/pkg/calibrate"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/results"
)

const calibrateUsage = `Usage:
  ioflux calibrate --trace trace.ioflux [flags] [-o calibration.json]
  ioflux calibrate --against results.json [flags] [-o calibration.json]

Measure the load generator's own ceiling for a trace, so a run's timing can be
attributed to its backend rather than to IOFlux.

The trace is replayed at the same concurrency against the null (mem) engine,
which performs no I/O. A measured run's op rate as a fraction of that ceiling is
the generator's share of the result.

Flags:
  --trace <path>        Path to a .ioflux trace file. Required unless --against
                        is given, which supplies it.
  --against <path>      A results.json or trial set to assess against the
                        ceiling. Its concurrency is adopted so the two match.
  --max-inflight <n>    Concurrent in-flight op cap (default 512, or the
                        assessed run's).
  --trials <n>          Number of measured calibration trials (default 10). The
                        ceiling comes from their median duration, not the
                        fastest.
  --warmup <n>          Unmeasured trials to run first (default 2).
  --max-generator-share <pct>
                        Largest generator share that still permits attribution.
                        Omitted, the share is reported and nothing is decided.
  --min-trials <n>      Fewest valid calibration trials a ceiling may rest on
                        (default 6).
  --max-ceiling-cv <pct>
                        Widest spread the calibration itself may have (default
                        5). An unstable calibration is refused rather than used.
  -o <path>             Write the calibration artifact here (optional; - for
                        stdout).

The null engine holds the trace's dataset in memory, so a trace whose targets
exceed available RAM will not complete.

Exit codes:
  0   ceiling measured; attributable, or reported with no bound declared
  1   the run and the ceiling could not be compared, so nothing was decided
  2   usage error or I/O failure
  3   not attributable: the generator's share exceeded the declared bound
`

// runCalibrate is the entry point for the `calibrate` subcommand.
func runCalibrate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("calibrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, calibrateUsage) }

	defaults := calibrate.DefaultPolicy()

	var (
		tracePath   string
		againstPath string
		maxInflight int
		trials      int
		warmup      int
		policy      calibrate.Policy
		outPath     string
	)
	fs.StringVar(&tracePath, "trace", "", "path to .ioflux trace file")
	fs.StringVar(&againstPath, "against", "", "results.json or trial set to assess against the ceiling")
	fs.IntVar(&maxInflight, "max-inflight", 0, "concurrent in-flight op cap (0 = default or the assessed run's)")
	fs.IntVar(&trials, "trials", 10, "number of measured calibration trials")
	fs.IntVar(&warmup, "warmup", 2, "number of unmeasured warmup trials")
	fs.Float64Var(&policy.MaxGeneratorSharePercent, "max-generator-share", 0,
		"largest generator share permitting attribution, in percent")
	fs.IntVar(&policy.MinCeilingTrials, "min-trials", defaults.MinCeilingTrials,
		"fewest valid calibration trials a ceiling may rest on")
	fs.Float64Var(&policy.MaxCeilingCVPercent, "max-ceiling-cv", defaults.MaxCeilingCVPercent,
		"widest run-to-run spread the calibration itself may have, in percent")
	fs.StringVar(&outPath, "o", "", "write the calibration artifact here (- for stdout)")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if trials < 1 {
		fmt.Fprintf(stderr, "ioflux calibrate: --trials must be >= 1, got %d\n", trials)
		return 2
	}
	if warmup < 0 {
		fmt.Fprintf(stderr, "ioflux calibrate: --warmup must be >= 0, got %d\n", warmup)
		return 2
	}

	// The assessed run supplies the trace and concurrency the ceiling must match.
	var assessed *results.Results
	var assessedDurationNS int64
	if againstPath != "" {
		art, code := loadArtifact(againstPath, stderr)
		if code != 0 {
			return code
		}
		switch {
		case art.paired != nil:
			fmt.Fprintf(stderr, "ioflux calibrate: %s is a paired experiment with two arms; "+
				"assess each arm's own results file\n", againstPath)
			return 2
		case art.trials != nil:
			if art.trials.Summary.ValidTrials == 0 {
				fmt.Fprintf(stderr, "ioflux calibrate: %s has no valid trials to assess\n", againstPath)
				return 1
			}
			// The median covers the valid trials only, so the trial standing for the
			// set must be valid too.
			assessed = firstValidTrial(art.trials)
			assessedDurationNS = int64(art.trials.Summary.DurationNS.Median)
		default:
			assessed = art.single
			assessedDurationNS = art.single.DurationNS
		}
		if tracePath == "" {
			tracePath = assessed.Plan.TracePath
		}
		if maxInflight == 0 {
			maxInflight = assessed.Plan.MaxInflight
		}
	}

	if tracePath == "" {
		fmt.Fprintln(stderr, "ioflux calibrate: --trace is required (or --against, to take it from a run)")
		fmt.Fprint(stderr, calibrateUsage)
		return 2
	}
	if maxInflight == 0 {
		maxInflight = 512
	}

	// The mem engine creates each object on first touch, so without a PREPARE
	// pass the dataset is allocated inside the measured window and the ceiling
	// measures Go's allocator. It has no page cache, so no recipe applies.
	settings := defaultRunSettings()
	settings.TracePath = tracePath
	settings.EngineName = calibrate.NullEngine
	settings.Mode = "asap"
	settings.MaxInflight = maxInflight
	settings.CacheMode = ""
	settings.PrepareMode = string(prepare.ModeAssumeExisting)

	plan, perr := buildPlan(settings)
	if perr != nil {
		fmt.Fprintf(stderr, "ioflux calibrate: %v\n", perr)
		return perr.Code
	}

	measured := make([]*results.Results, 0, trials)
	for i := 0; i < warmup+trials; i++ {
		// Each trial allocates a fresh copy of the dataset. Collecting first keeps
		// trial N from running against trial N-1's garbage, which otherwise
		// dominates the spread.
		runtime.GC()

		if warmup+trials > 1 {
			label := fmt.Sprintf("trial %d/%d", i-warmup+1, trials)
			if i < warmup {
				label = fmt.Sprintf("warmup %d/%d", i+1, warmup)
			}
			fmt.Fprintf(stderr, "ioflux calibrate: %s\n", label)
		}
		res, err := runOnce(context.Background(), plan, "", stderr)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux calibrate: %v\n", err)
			return 1
		}
		if i >= warmup {
			measured = append(measured, res)
		}
	}

	ts := results.BuildTrialSet(measured, warmup)
	if ts.Summary.ValidTrials == 0 {
		fmt.Fprintln(stderr, "ioflux calibrate: every calibration trial failed; no ceiling was established")
		return 1
	}

	// The op count must come from a valid trial, since the median does.
	ceiling := calibrate.CeilingFrom(firstValidTrial(ts), int64(ts.Summary.DurationNS.Median))
	ceiling.Trials = ts.Summary.ValidTrials
	ceiling.CVPercent = ts.Summary.DurationNS.CVPercent

	cal := &calibrate.Calibration{
		SchemaVersion: calibrate.SchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Tool:          results.CurrentTool(),
		Host:          results.CurrentHost(),
		Policy:        policy,
		Ceiling:       ceiling,
		Trials:        ts,
	}
	if assessed != nil {
		a := calibrate.Assess(assessed, assessedDurationNS, ceiling, policy)
		cal.Assessment = &a
		cal.AssessedPath = againstPath
	}

	if outPath != "" {
		if outPath == "-" {
			if err := results.WriteJSON(stdout, cal); err != nil {
				fmt.Fprintf(stderr, "ioflux calibrate: write calibration: %v\n", err)
				return 2
			}
		} else {
			if err := results.WriteJSONFile(outPath, cal); err != nil {
				fmt.Fprintf(stderr, "ioflux calibrate: %v\n", err)
				return 2
			}
			fmt.Fprintf(stdout, "wrote %s\n", outPath)
		}
	}

	printCalibration(stdout, cal)

	if cal.Assessment == nil {
		return 0
	}
	switch cal.Assessment.Verdict {
	case calibrate.VerdictNotAttributable:
		return 3
	case calibrate.VerdictNotAssessed:
		if len(cal.Assessment.Blocking) > 0 {
			return 1
		}
	}
	return 0
}

// firstValidTrial returns the first trial whose execution was valid, falling
// back to the set's representative when none is.
func firstValidTrial(ts *results.TrialSet) *results.Results {
	for _, t := range ts.Trials {
		if len(t.ExecutionInvalidReasons()) == 0 {
			return t
		}
	}
	return ts.Representative()
}

// printCalibration writes the human-readable summary.
func printCalibration(w io.Writer, cal *calibrate.Calibration) {
	c := cal.Ceiling
	fmt.Fprintf(w, "\nLoad-generator ceiling (%s engine, %d valid trial(s))\n", c.Engine, c.Trials)
	fmt.Fprintf(w, "  max-inflight        %d over %d stream(s)\n", c.MaxInflight, c.Streams)
	fmt.Fprintf(w, "  median duration     %s for %d ops\n", fmtDuration(c.DurationNS), c.Ops)
	fmt.Fprintf(w, "  ceiling             %s ops/s   (%s per op)\n",
		fmtCount(c.OpsPerSec), fmtDuration(int64(c.NsPerOp)))
	fmt.Fprintf(w, "  trial CV            %.1f%%\n", c.CVPercent)

	a := cal.Assessment
	if a == nil {
		fmt.Fprintf(w, "\nNo run assessed. Pass --against results.json to compare a measured run\n"+
			"against this ceiling.\n")
		return
	}

	fmt.Fprintf(w, "\nAttribution of %s\n", cal.AssessedPath)
	if len(a.Blocking) > 0 {
		fmt.Fprintf(w, "  verdict             %s\n", a.Verdict)
		for _, b := range a.Blocking {
			fmt.Fprintf(w, "    - %s\n", b)
		}
		fmt.Fprintf(w, "  %s\n", a.Explanation)
		return
	}

	fmt.Fprintf(w, "  run rate            %s ops/s\n", fmtCount(a.RunOpsPerSec))
	fmt.Fprintf(w, "  generator share     %.1f%% of ceiling", a.GeneratorSharePercent)
	if a.BoundPercent > 0 {
		fmt.Fprintf(w, "   (bound %.1f%%)", a.BoundPercent)
	}
	fmt.Fprintln(w)
	if a.CPUSaturationPercent > 0 {
		fmt.Fprintf(w, "  client CPU          %.1f%% of all cores during the run\n", a.CPUSaturationPercent)
	}
	fmt.Fprintf(w, "  verdict             %s\n", a.Verdict)
	fmt.Fprintf(w, "  %s\n", a.Explanation)
}

// fmtCount renders a rate compactly.
func fmtCount(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

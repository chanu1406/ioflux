package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/results"
)

const experimentUsage = `Usage:
  ioflux experiment --config experiment.yaml -o experiment.json

Run two configurations interleaved and compare them pair by pair.

Running one configuration to completion and then the other assigns any drift in
the machine — thermal throttling, a neighbouring tenant, a cache warming up — to
whichever arm ran second, where it cannot be told apart from the treatment's
effect. This alternates them instead, and differences each pair, so whatever the
two shared cancels out.

Flags:
  --config <path>   Experiment config (required)
  -o <path>         Output path (required; use - for stdout)

Config:
  claim: what this experiment is meant to answer   # free text, copied to output
  trials: 10                                       # measured pairs
  warmup: 2                                        # unmeasured rounds, discarded
  seed: 42                                         # within-pair ordering draw
  policy:
    min_trials: 10
    max_cv_percent: 5
  run:                    # shared by both arms
    trace: workload.ioflux
    engine: local
    target_root: ./data
    target_map: map.yaml
    cache_mode: cold
  baseline: {}            # overrides for the baseline arm
  treatment:              # overrides for the treatment arm
    max_inflight: 4       # whatever differs here is the treatment variable

The treatment variable is derived from what actually differs between the two
resolved configurations, so a treatment block that changes nothing is reported
as having no treatment rather than as a clean result.

S3 credentials are not config fields. A config file travels with its results;
credentials come from the environment instead.

Exit codes:
  0   experiment completed and a difference was reported
  1   a replay failed, or the comparison was refused as incomparable
  2   usage error or I/O failure
`

// runExperiment is the entry point for the `experiment` subcommand.
func runExperiment(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("experiment", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, experimentUsage) }
	configPath := fs.String("config", "", "experiment config file (required)")
	outPath := fs.String("o", "", "output path (required; - for stdout)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" || *outPath == "" {
		fmt.Fprintln(stderr, "ioflux experiment: --config and -o are required")
		fmt.Fprint(stderr, experimentUsage)
		return 2
	}

	cfg, err := loadExpConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux experiment: %v\n", err)
		return 2
	}

	baseSettings, treatSettings := cfg.arms()
	basePlan, perr := buildPlan(baseSettings)
	if perr != nil {
		fmt.Fprintf(stderr, "ioflux experiment: baseline: %v\n", perr)
		return perr.Code
	}
	treatPlan, perr := buildPlan(treatSettings)
	if perr != nil {
		fmt.Fprintf(stderr, "ioflux experiment: treatment: %v\n", perr)
		return perr.Code
	}

	treatmentVars := treatmentVariables(baseSettings, treatSettings)
	if len(treatmentVars) == 0 {
		fmt.Fprintln(stderr, "ioflux experiment: warning: the two arms are configured identically, "+
			"so this experiment has no treatment variable")
	} else {
		fmt.Fprintf(stderr, "ioflux experiment: treatment variable(s): %v\n", treatmentVars)
	}

	// Preflight both arms' workers so an unreachable host fails before any trial
	// has run.
	for _, s := range []runSettings{baseSettings, treatSettings} {
		probe, err := buildWorkers(s.Hosts)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux experiment: %v\n", err)
			return 2
		}
		closeWorkers(probe)
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	ctx := context.Background()

	// Warmup rounds run both arms and are discarded; their purpose is to change
	// the machine's state, not to be evidence about it.
	for i := 0; i < cfg.Warmup; i++ {
		fmt.Fprintf(stderr, "ioflux experiment: warmup %d/%d\n", i+1, cfg.Warmup)
		for _, arm := range orderedArms(rng) {
			if _, err := runArm(ctx, arm, basePlan, treatPlan, baseSettings, treatSettings, stderr); err != nil {
				fmt.Fprintf(stderr, "ioflux experiment: warmup %s: %v\n", arm, err)
				return 1
			}
		}
	}

	baseTrials := make([]*results.Results, 0, cfg.Trials)
	treatTrials := make([]*results.Results, 0, cfg.Trials)
	pairOrder := make([]string, 0, cfg.Trials)

	for i := 0; i < cfg.Trials; i++ {
		order := orderedArms(rng)
		fmt.Fprintf(stderr, "ioflux experiment: pair %d/%d (%s first)\n", i+1, cfg.Trials, order[0])
		pairOrder = append(pairOrder, order[0])

		for _, arm := range order {
			res, err := runArm(ctx, arm, basePlan, treatPlan, baseSettings, treatSettings, stderr)
			if err != nil {
				fmt.Fprintf(stderr, "ioflux experiment: pair %d %s: %v\n", i+1, arm, err)
				return 1
			}
			if arm == results.ArmBaseline {
				baseTrials = append(baseTrials, res)
			} else {
				treatTrials = append(treatTrials, res)
			}
		}
	}

	pe := results.BuildPaired(
		cfg.Claim,
		results.BuildTrialSet(baseTrials, cfg.Warmup),
		results.BuildTrialSet(treatTrials, cfg.Warmup),
		cfg.policy(), cfg.Seed, pairOrder, treatmentVars,
	)

	if *outPath == "-" {
		if err := results.WriteJSON(stdout, pe); err != nil {
			fmt.Fprintf(stderr, "ioflux experiment: write: %v\n", err)
			return 2
		}
	} else {
		if err := results.WriteJSONFile(*outPath, pe); err != nil {
			fmt.Fprintf(stderr, "ioflux experiment: %v\n", err)
			return 2
		}
		fmt.Fprintf(stdout, "wrote %s\n", *outPath)
	}

	if !printPairedReport(stdout, pe) {
		return 1
	}
	return 0
}

// orderedArms draws the order the two arms run in for one round. Alternating
// strictly would make every treatment run follow a baseline run, so anything
// the first run leaves behind — a warmed cache, a settled allocator — would land
// on the same arm every time.
func orderedArms(rng *rand.Rand) [2]string {
	if rng.Intn(2) == 0 {
		return [2]string{results.ArmBaseline, results.ArmTreatment}
	}
	return [2]string{results.ArmTreatment, results.ArmBaseline}
}

// runArm executes one arm once.
func runArm(
	ctx context.Context,
	arm string,
	basePlan, treatPlan cluster.Plan,
	baseSettings, treatSettings runSettings,
	stderr io.Writer,
) (*results.Results, error) {
	if arm == results.ArmBaseline {
		return runOnce(ctx, basePlan, baseSettings.Hosts, stderr)
	}
	return runOnce(ctx, treatPlan, treatSettings.Hosts, stderr)
}

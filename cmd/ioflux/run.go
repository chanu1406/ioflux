package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/engine"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
	"github.com/chanuollala/ioflux/pkg/payload"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const runUsage = `Usage:
  ioflux run --trace trace.ioflux --engine mem|local|s3 [flags] -o results.json

Replay a trace against a storage engine and emit results.json.

Flags:
  --trace <path>        Path to a .ioflux trace file (required)
  --engine <name>       Storage engine: mem | local | s3 (default mem)
  --mode <mode>         Replay mode: asap | think | timeline | scaled (default asap)
  --max-inflight <n>    Worker-global concurrent in-flight op cap (default 512)
  --speedup <f>         Timeline scaling factor for --mode scaled (default 1.0)
  --target-map <path>   Path to a YAML target-map config (optional)
  --allow-passthrough   Allow targets that match no rule to pass through unchanged
  --prepare <mode>      Dataset prep mode: assume-existing | materialize-synthetic | materialize-from-source
  --prepare-scope <s>   Dataset prep scope: shared | per-worker (default: s3 shared, mem/local per-worker)
  --source-root <path>  Local source path for --prepare materialize-from-source
  --cache-mode <mode>   Cache state: cold | warm (default cold)
  --fill <mode>         Write/materialization payload fill: seeded | zero (default seeded)
  --fill-seed <n>       Seed for deterministic payload fill (default 1)
  --hosts <list>        Comma-separated worker addresses (e.g. hostA:7800,hostB:7800).
                        Omit for a single-node run (an in-process worker).
  -o <path>             Output path for results.json (required; use - for stdout).
                        Written atomically via a temporary file in the same directory,
                        so a failed write leaves the previous results intact; this
                        needs write access to the directory, not only to the file.
  --csv <path>          Append a CSV row to this file (optional; header written once).
                        With --trials, one row per measured trial.

Repeated trials:
  --trials <n>          Number of measured trials (default 1). Above 1, -o receives a
                        trial set — every trial plus the distribution over them
                        (median, coefficient of variation, 95% interval on the median)
                        — instead of a single result. One run cannot be told apart
                        from noise, so a comparison of two trial sets is what supports
                        a regression decision.
  --warmup <n>          Unmeasured trials to run first (default 0). They change the
                        machine's state and are discarded, not recorded.

                        Each trial re-runs PREPARE, so --cache-mode cold evicts before
                        every trial rather than only the first.

S3 flags:
  --endpoint <url>                 S3-compatible endpoint override (optional)
  --region <name>                  S3 region (default us-east-1)
  --bucket <name>                  S3 bucket (required for --engine s3)
  --path-style                     Use path-style S3 addressing (MinIO/Ceph often need this)
  --access-key <key>               Static S3 access key (optional)
  --secret-key <secret>            Static S3 secret key (optional)
  --session-token <token>          Static S3 session token (optional)
  --s3-multipart-threshold <size>  Multipart threshold (default 64MiB)
  --s3-multipart-part-size <size>  Multipart part size (default 16MiB; minimum 5MiB)

Containment:
  --target-root <loc>   Confine every target to one place, so a trace cannot read or
                        overwrite data elsewhere. Unset means no containment, which the
                        report records as a limitation. Cannot be combined with --hosts;
                        set it on each worker instead. Unsupported for --engine mem,
                        which has no persistent namespace to confine.
                        local: a directory, enforced by the OS. Targets escaping via
                          "..", an absolute path, or a symlink are rejected. Created if
                          missing. Absolute symlinks are rejected even when they point
                          back inside.
                        s3:    a key prefix within --bucket. Keys outside it, and keys
                          carrying a ".." segment, are rejected.

Local engine flags:
  --allow-direct        Honor O_DIRECT opens from the trace (Linux only; default off)
  --direct-fallback     Fall back to buffered I/O when O_DIRECT is unsupported by the
                        filesystem rather than failing the open
  --direct-align <n>    Block alignment for O_DIRECT (bytes; 0 = auto-detect)

Engine notes:
  mem     In-process zero-I/O engine. All data is held in memory; no disk I/O.
  local   Local filesystem engine using platform file APIs.
  s3      S3-compatible object engine. File-shaped reads use Range GETs.

Exit codes:
  0   replay completed; results.json written
  1   replay rejected before dispatch (bad trace, caps mismatch, a target outside
      --target-root) or completed with op errors (including a latency sample outside
      the histogram's trackable range). With --trials, any trial doing so.
  2   usage error or I/O failure
`

// runRun is the entry point for the `run` subcommand.
func runRun(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, runUsage) }

	var (
		tracePath        string
		engineName       string
		mode             string
		maxInflight      int
		speedup          float64
		outPath          string
		csvPath          string
		hosts            string
		targetMapPath    string
		allowPassthrough bool
		prepareMode      string
		prepareScope     string
		sourceRoot       string
		fillMode         string
		fillSeed         int64
		targetRoot       string
		allowDirect      bool
		directFallback   bool
		directAlign      int64
		trials           int
		warmup           int
		s3Cfg            s3engine.Config
	)
	fs.StringVar(&tracePath, "trace", "", "path to .ioflux trace file (required)")
	fs.StringVar(&engineName, "engine", "mem", "storage engine (mem | local | s3)")
	fs.StringVar(&mode, "mode", "asap", "replay mode: asap | think | timeline | scaled")
	fs.IntVar(&maxInflight, "max-inflight", 512, "worker-global concurrent in-flight op cap")
	fs.Float64Var(&speedup, "speedup", 1.0, "timeline scaling factor for --mode scaled")
	fs.StringVar(&outPath, "o", "", "output path for results.json (required; - for stdout)")
	fs.StringVar(&csvPath, "csv", "", "append a CSV row to this file (optional)")
	fs.StringVar(&hosts, "hosts", "", "comma-separated worker addresses; empty = single-node in-process worker")
	fs.StringVar(&targetMapPath, "target-map", "", "path to YAML target-map config (optional)")
	fs.BoolVar(&allowPassthrough, "allow-passthrough", false, "allow unmatched targets to pass through unchanged")
	fs.StringVar(&prepareMode, "prepare", "", "dataset prep mode: assume-existing | materialize-synthetic | materialize-from-source")
	fs.StringVar(&prepareScope, "prepare-scope", "", "dataset prep scope: shared | per-worker")
	fs.StringVar(&sourceRoot, "source-root", "", "local source path for --prepare materialize-from-source")
	fs.StringVar(&fillMode, "fill", string(payload.ModeSeeded), "payload fill mode: seeded | zero")
	fs.Int64Var(&fillSeed, "fill-seed", payload.DefaultSeed, "seed for deterministic payload fill")
	fs.StringVar(&targetRoot, "target-root", "", "confine every local-engine target to this directory")
	fs.BoolVar(&allowDirect, "allow-direct", false, "enable O_DIRECT for trace OPEN ops carrying the direct flag (local engine, Linux only)")
	fs.BoolVar(&directFallback, "direct-fallback", false, "fall back to buffered I/O when O_DIRECT is unsupported by the filesystem")
	fs.Int64Var(&directAlign, "direct-align", 0, "O_DIRECT block alignment in bytes (0 = auto-detect from filesystem)")
	fs.StringVar(&s3Cfg.Endpoint, "endpoint", "", "S3-compatible endpoint override")
	fs.StringVar(&s3Cfg.Region, "region", "", "S3 region")
	fs.StringVar(&s3Cfg.Bucket, "bucket", "", "S3 bucket")
	fs.BoolVar(&s3Cfg.PathStyle, "path-style", false, "use path-style S3 addressing")
	fs.StringVar(&s3Cfg.AccessKey, "access-key", "", "static S3 access key")
	fs.StringVar(&s3Cfg.SecretKey, "secret-key", "", "static S3 secret key")
	fs.StringVar(&s3Cfg.SessionToken, "session-token", "", "static S3 session token")
	fs.Var(newBytesFlag(&s3Cfg.MultipartThreshold), "s3-multipart-threshold", "S3 multipart threshold")
	fs.Var(newBytesFlag(&s3Cfg.MultipartPartSize), "s3-multipart-part-size", "S3 multipart part size")

	var cacheMode string
	fs.StringVar(&cacheMode, "cache-mode", "cold", "cache state: cold | warm (default cold)")
	fs.IntVar(&trials, "trials", 1, "number of measured trials to run")
	fs.IntVar(&warmup, "warmup", 0, "number of unmeasured warmup trials to run first")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if tracePath == "" {
		fmt.Fprintln(stderr, "ioflux run: --trace is required")
		fmt.Fprint(stderr, runUsage)
		return 2
	}
	if outPath == "" {
		fmt.Fprintln(stderr, "ioflux run: -o is required")
		fmt.Fprint(stderr, runUsage)
		return 2
	}
	if trials < 1 {
		fmt.Fprintf(stderr, "ioflux run: --trials must be >= 1, got %d\n", trials)
		return 2
	}
	if warmup < 0 {
		fmt.Fprintf(stderr, "ioflux run: --warmup must be >= 0, got %d\n", warmup)
		return 2
	}

	plan, perr := buildPlan(runSettings{
		TracePath:        tracePath,
		EngineName:       engineName,
		Mode:             mode,
		MaxInflight:      maxInflight,
		Speedup:          speedup,
		TargetMapPath:    targetMapPath,
		AllowPassthrough: allowPassthrough,
		PrepareMode:      prepareMode,
		PrepareScope:     prepareScope,
		SourceRoot:       sourceRoot,
		CacheMode:        cacheMode,
		FillMode:         fillMode,
		FillSeed:         fillSeed,
		TargetRoot:       targetRoot,
		AllowDirect:      allowDirect,
		DirectFallback:   directFallback,
		DirectAlign:      directAlign,
		Hosts:            hosts,
		S3:               s3Cfg,
	})
	if perr != nil {
		fmt.Fprintf(stderr, "ioflux run: %v\n", perr)
		return perr.Code
	}

	// Preflight the worker set so an unreachable host fails immediately as a
	// usage/IO error, rather than after the first trial has already run.
	probe, err := buildWorkers(hosts)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: %v\n", err)
		return 2
	}
	closeWorkers(probe)

	// Warmups change the machine's state before measurement; they are run and
	// discarded. Each trial gets a fresh worker set so no state carries between
	// them, and re-runs PREPARE, which re-applies the cache recipe — the reason
	// a repeated cold-recipe trial is cold rather than warm after the first.
	measured := make([]*results.Results, 0, trials)
	for i := 0; i < warmup+trials; i++ {
		if warmup+trials > 1 {
			label := fmt.Sprintf("trial %d/%d", i-warmup+1, trials)
			if i < warmup {
				label = fmt.Sprintf("warmup %d/%d", i+1, warmup)
			}
			fmt.Fprintf(stderr, "ioflux run: %s\n", label)
		}
		res, err := runOnce(context.Background(), plan, hosts, stderr)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux run: %v\n", err)
			return 1
		}
		if i >= warmup {
			measured = append(measured, res)
		}
	}

	// A single measured trial stays a plain result; repeated trials produce a
	// trial set, which carries the distribution a decision actually needs.
	var trialSet *results.TrialSet
	var payload any = measured[0]
	if trials > 1 {
		trialSet = results.BuildTrialSet(measured, warmup)
		payload = trialSet
	}

	// Write output. The file path goes through an atomic write so a failure
	// leaves the previous results intact rather than a truncated file, and
	// "wrote ..." is printed only once the new results are durable.
	if outPath == "-" {
		if err := results.WriteJSON(stdout, payload); err != nil {
			fmt.Fprintf(stderr, "ioflux run: write results: %v\n", err)
			return 2
		}
	} else {
		if err := results.WriteJSONFile(outPath, payload); err != nil {
			fmt.Fprintf(stderr, "ioflux run: %v\n", err)
			return 2
		}
		fmt.Fprintf(stdout, "wrote %s\n", outPath)
	}

	if csvPath != "" {
		for _, res := range measured {
			if err := results.AppendCSV(csvPath, res); err != nil {
				fmt.Fprintf(stderr, "ioflux run: write csv: %v\n", err)
				return 2
			}
		}
	}

	invalid := false
	for i, res := range measured {
		if res.Fidelity.LowFidelity {
			fmt.Fprintf(stderr, "ioflux run: warning: %slow-fidelity replay: %s\n",
				trialPrefix(i, trials), res.Fidelity.LowFidelityReason)
		}
		for _, reason := range res.ExecutionInvalidReasons() {
			fmt.Fprintf(stderr, "ioflux run: %s%s\n", trialPrefix(i, trials), reason)
			invalid = true
		}
	}
	if trialSet != nil {
		printTrialRunSummary(stderr, trialSet)
	}
	if invalid {
		fmt.Fprintf(stderr, "ioflux run: see %s for per-operation detail\n", outPath)
		return 1
	}
	return 0
}

// trialPrefix labels a message with the trial it came from, and labels nothing
// when there was only one.
func trialPrefix(i, trials int) string {
	if trials <= 1 {
		return ""
	}
	return fmt.Sprintf("trial %d: ", i+1)
}

// printTrialRunSummary reports the measured distribution at the end of a
// multi-trial run, so the headline numbers are visible without a second command.
func printTrialRunSummary(w io.Writer, ts *results.TrialSet) {
	s := ts.Summary
	fmt.Fprintf(w, "ioflux run: %d valid trial(s), %d failed\n", s.ValidTrials, s.FailedTrials)
	if s.ValidTrials == 0 {
		return
	}
	fmt.Fprintf(w, "ioflux run: duration median %s   CV %.1f%%   min %s   max %s\n",
		fmtDuration(int64(s.DurationNS.Median)), s.DurationNS.CVPercent,
		fmtDuration(int64(s.DurationNS.Min)), fmtDuration(int64(s.DurationNS.Max)))
}

// runOnce executes one complete replay — its own workers, its own PREPARE and
// cache recipe, its own RUN — and returns its results.
func runOnce(ctx context.Context, plan cluster.Plan, hosts string, stderr io.Writer) (*results.Results, error) {
	workers, err := buildWorkers(hosts)
	if err != nil {
		return nil, err
	}
	defer closeWorkers(workers)

	coord := &cluster.Coordinator{}
	// Live per-host progress + warnings only for distributed runs, so single-node
	// output stays quiet and unchanged.
	if len(workers) > 1 {
		var pmu sync.Mutex
		coord.Progress = func(host string, ops, b int64) {
			pmu.Lock()
			defer pmu.Unlock()
			fmt.Fprintf(stderr, "  [%s] %d ops   %s\n", host, ops, fmtBytes(b))
		}
		coord.Logf = func(format string, args ...any) {
			fmt.Fprintf(stderr, "ioflux run: "+format+"\n", args...)
		}
	}

	return coord.Run(ctx, plan, workers)
}

// buildWorkers returns the workers for a run. An empty hosts string yields a
// single in-process worker (single-node); otherwise each comma-separated address
// is dialed as a gRPC worker. Both paths drive the same Coordinator code.
func buildWorkers(hosts string) ([]cluster.Worker, error) {
	addrs := splitHosts(hosts)
	if len(addrs) == 0 {
		return []cluster.Worker{cluster.NewLocalWorker(cluster.NewSession())}, nil
	}
	workers := make([]cluster.Worker, 0, len(addrs))
	for _, addr := range addrs {
		w, err := cluster.DialWorker(addr)
		if err != nil {
			closeWorkers(workers)
			return nil, fmt.Errorf("dial worker %q: %w", addr, err)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// splitHosts parses a comma-separated host list, trimming spaces and dropping
// empty entries.
func splitHosts(hosts string) []string {
	var addrs []string
	for _, h := range strings.Split(hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			addrs = append(addrs, h)
		}
	}
	return addrs
}

func closeWorkers(workers []cluster.Worker) {
	for _, w := range workers {
		_ = w.Close()
	}
}

func buildRunEngine(spec cluster.EngineSpec, hdr trace.Header) (engine.Engine, string, error) {
	if spec.TargetSizes == nil {
		spec.TargetSizes = make(map[string]int64, len(hdr.Targets))
		for _, tgt := range hdr.Targets {
			spec.TargetSizes[tgt.Name] = tgt.Size
		}
	}
	return cluster.BuildEngine(spec)
}

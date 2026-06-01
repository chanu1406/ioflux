package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const runUsage = `Usage:
  ioflux run --trace trace.ioflux --engine mem|local|s3 [flags] -o results.json

Replay a trace against a storage engine and emit results.json.

Flags:
  --trace <path>        Path to a .ioflux trace file (required)
  --engine <name>       Storage engine: mem | local | s3 (default mem)
  --mode <mode>         Replay mode: asap | timeline | scaled (default asap)
  --max-inflight <n>    Worker-global concurrent in-flight op cap (default 512)
  --speedup <f>         Timeline scaling factor for --mode scaled (default 1.0)
  --target-map <path>   Path to a YAML target-map config (optional)
  --allow-passthrough   Allow targets that match no rule to pass through unchanged
  --prepare <mode>      Dataset prep mode: assume-existing | materialize-synthetic | materialize-from-source
  --source-root <path>  Local source path for --prepare materialize-from-source
  --cache-mode <mode>   Cache state: cold | warm (default cold)
  -o <path>             Output path for results.json (required; use - for stdout)
  --csv <path>          Append a CSV row to this file (optional; header written once)

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

Engine notes:
  mem     In-process zero-I/O engine. All data is held in memory; no disk I/O.
  local   Local filesystem engine using platform file APIs.
  s3      S3-compatible object engine. File-shaped reads use Range GETs.

Exit codes:
  0   replay completed; results.json written
  1   replay rejected before dispatch (bad trace, caps mismatch) or completed with op errors
  2   usage error or I/O failure
`

type runEngineConfig struct {
	Name      string
	CacheMode string

	S3 s3engine.Config
}

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
		targetMapPath    string
		allowPassthrough bool
		prepareMode      string
		sourceRoot       string
		s3Cfg            s3engine.Config
	)
	fs.StringVar(&tracePath, "trace", "", "path to .ioflux trace file (required)")
	fs.StringVar(&engineName, "engine", "mem", "storage engine (mem | local | s3)")
	fs.StringVar(&mode, "mode", "asap", "replay mode: asap | timeline | scaled")
	fs.IntVar(&maxInflight, "max-inflight", 512, "worker-global concurrent in-flight op cap")
	fs.Float64Var(&speedup, "speedup", 1.0, "timeline scaling factor for --mode scaled")
	fs.StringVar(&outPath, "o", "", "output path for results.json (required; - for stdout)")
	fs.StringVar(&csvPath, "csv", "", "append a CSV row to this file (optional)")
	fs.StringVar(&targetMapPath, "target-map", "", "path to YAML target-map config (optional)")
	fs.BoolVar(&allowPassthrough, "allow-passthrough", false, "allow unmatched targets to pass through unchanged")
	fs.StringVar(&prepareMode, "prepare", "", "dataset prep mode: assume-existing | materialize-synthetic | materialize-from-source")
	fs.StringVar(&sourceRoot, "source-root", "", "local source path for --prepare materialize-from-source")
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
	switch mode {
	case "asap", "timeline", "scaled":
	default:
		fmt.Fprintf(stderr, "ioflux run: unsupported mode %q (want asap | timeline | scaled)\n", mode)
		return 2
	}
	if maxInflight <= 0 {
		fmt.Fprintf(stderr, "ioflux run: --max-inflight must be > 0, got %d\n", maxInflight)
		return 2
	}
	if mode == "scaled" && speedup <= 0 {
		fmt.Fprintf(stderr, "ioflux run: --speedup must be > 0 for --mode scaled, got %v\n", speedup)
		return 2
	}

	// Open and parse trace.
	f, err := os.Open(tracePath)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: open trace: %v\n", err)
		return 2
	}
	defer f.Close()

	r, err := trace.NewReader(f)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: parse trace: %v\n", err)
		return 1
	}
	hdr := r.Header()

	eng, bucket, err := buildRunEngine(runEngineConfig{
		Name:      engineName,
		CacheMode: cacheMode,
		S3:        s3Cfg,
	}, hdr)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: %v\n", err)
		return 2
	}

	var tmap *targetmap.Map
	if targetMapPath != "" {
		tmap, err = targetmap.Load(targetMapPath)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux run: %v\n", err)
			return 1
		}
		if allowPassthrough {
			tmap.AllowPassthrough = true
		}
	}

	plan := replay.Plan{
		TracePath:     tracePath,
		Engine:        eng,
		EngineName:    engineName,
		Mode:          mode,
		MaxInflight:   maxInflight,
		SpeedupFactor: speedup,
		TargetMap:     tmap,
		Bucket:        bucket,
		PrepareMode:   prepareMode,
		SourceRoot:    sourceRoot,
		CacheMode:     cacheMode,
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: prepare: %v\n", err)
		return 1
	}

	res, err := exec.Run(context.Background())
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: replay: %v\n", err)
		return 1
	}

	// Write output.
	var w io.Writer
	if outPath == "-" {
		w = stdout
	} else {
		out, err := os.Create(outPath)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux run: create output: %v\n", err)
			return 2
		}
		defer out.Close()
		w = out
	}

	if err := results.WriteJSON(w, res); err != nil {
		fmt.Fprintf(stderr, "ioflux run: write results: %v\n", err)
		return 2
	}

	if outPath != "-" {
		fmt.Fprintf(stdout, "wrote %s\n", outPath)
	}

	if csvPath != "" {
		if err := results.AppendCSV(csvPath, res); err != nil {
			fmt.Fprintf(stderr, "ioflux run: write csv: %v\n", err)
			return 2
		}
	}

	if res.Fidelity.LowFidelity {
		fmt.Fprintf(stderr, "ioflux run: warning: low-fidelity replay: %s\n", res.Fidelity.LowFidelityReason)
	}
	if res.Errors > 0 {
		fmt.Fprintf(stderr, "ioflux run: %d op(s) failed; see results.errors\n", res.Errors)
		return 1
	}
	return 0
}

func buildRunEngine(cfg runEngineConfig, hdr trace.Header) (engine.Engine, string, error) {
	switch cfg.Name {
	case "mem":
		sizeMap := make(map[string]int64, len(hdr.Targets))
		for _, tgt := range hdr.Targets {
			sizeMap[tgt.Name] = tgt.Size
		}
		return mem.New(mem.WithSizeFunc(func(target string) int64 {
			if sz, ok := sizeMap[target]; ok && sz > 0 {
				return sz
			}
			return 64 << 20
		})), "", nil
	case "local":
		return localfile.New(), "", nil
	case "s3":
		cfg.S3.DisableHTTPKeepAlive = cfg.CacheMode == "cold"
		eng, err := s3engine.New(cfg.S3)
		if err != nil {
			return nil, "", err
		}
		return eng, cfg.S3.Bucket, nil
	default:
		return nil, "", fmt.Errorf("unsupported engine %q (currently supported: mem, local, s3)", cfg.Name)
	}
}

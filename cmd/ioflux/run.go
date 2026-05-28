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
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const runUsage = `Usage:
  ioflux run --trace trace.ioflux --engine mem|local [flags] -o results.json

Replay a trace against a storage engine and emit results.json.

Flags:
  --trace <path>        Path to a .ioflux trace file (required)
  --engine <name>       Storage engine: mem | local (default mem)
  --mode <mode>         Replay mode: asap | timeline | scaled (default asap)
  --max-inflight <n>    Worker-global concurrent in-flight op cap (default 512)
  --speedup <f>         Timeline scaling factor for --mode scaled (default 1.0)
  -o <path>             Output path for results.json (required; use - for stdout)

Engine notes:
  mem     In-process zero-I/O engine. All data is held in memory; no disk I/O.
  local   Local filesystem engine using platform file APIs.

Exit codes:
  0   replay completed; results.json written
  1   replay rejected before dispatch (bad trace, caps mismatch) or completed with op errors
  2   usage error or I/O failure
`

// runRun is the entry point for the `run` subcommand.
func runRun(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, runUsage) }

	var (
		tracePath   string
		engineName  string
		mode        string
		maxInflight int
		speedup     float64
		outPath     string
	)
	fs.StringVar(&tracePath, "trace", "", "path to .ioflux trace file (required)")
	fs.StringVar(&engineName, "engine", "mem", "storage engine (mem | local)")
	fs.StringVar(&mode, "mode", "asap", "replay mode: asap | timeline | scaled")
	fs.IntVar(&maxInflight, "max-inflight", 512, "worker-global concurrent in-flight op cap")
	fs.Float64Var(&speedup, "speedup", 1.0, "timeline scaling factor for --mode scaled")
	fs.StringVar(&outPath, "o", "", "output path for results.json (required; - for stdout)")

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

	eng, err := buildRunEngine(engineName, hdr)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux run: %v\n", err)
		return 2
	}

	plan := replay.Plan{
		TracePath:     tracePath,
		Engine:        eng,
		EngineName:    engineName,
		Mode:          mode,
		MaxInflight:   maxInflight,
		SpeedupFactor: speedup,
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
	if res.Errors > 0 {
		fmt.Fprintf(stderr, "ioflux run: %d op(s) failed; see results.errors\n", res.Errors)
		return 1
	}
	return 0
}

func buildRunEngine(name string, hdr trace.Header) (engine.Engine, error) {
	switch name {
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
		})), nil
	case "local":
		return localfile.New(), nil
	default:
		return nil, fmt.Errorf("unsupported engine %q (currently supported: mem, local)", name)
	}
}

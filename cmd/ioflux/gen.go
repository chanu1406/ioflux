package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/chanuollala/ioflux/pkg/gen/checkpoint"
	"github.com/chanuollala/ioflux/pkg/gen/trainingread"
)

const genUsage = `ioflux gen — generate a synthetic trace.

Usage:
  ioflux gen <profile> [flags] -o trace.ioflux

Profiles:
  training-read     Sharded WebDataset-style multi-worker read workload.
  checkpoint-write  Multi-rank sharded checkpoint write workload.

Run 'ioflux gen <profile> -h' for profile-specific flags.
`

const genTrainingReadUsage = `Usage:
  ioflux gen training-read [flags] -o trace.ioflux

Generate a synthetic training-read trace (sharded WebDataset-style).

Flags:
  -o <path>                 Output file (required; use - for stdout)
  --shards <n>              Number of shard files (default 1024)
  --shard-size <size>       Size of each shard (default 64MiB; accepts KiB/MiB/GiB or bytes)
  --record-size <size>      Mean read size within a shard (default 512KiB; accepts KiB/MiB/GiB or bytes)
  --record-size-dist <d>    Record size distribution: lognormal (default lognormal)
  --epochs <n>              Number of passes over the dataset (default 1)
  --dataloader-workers <n>  Concurrent DataLoader worker streams (default 8)
  --prefetch-depth <n>      Outstanding shards per worker (accepted, not yet modeled)
  --shuffle=<bool>          Shuffle shard order each epoch (default true; use --shuffle=false to disable)
  --read-within-shard <s>   Access pattern within a shard: sequential|random (default sequential)
  --seed <n>                PRNG seed for reproducibility (default 42)

Size arguments accept a plain integer (bytes) or a suffix: KiB, MiB, GiB (binary),
KB, MB, GB (decimal), or K/M/G (binary aliases).

Output is byte-identical for the same flags and --seed. The trace header does not
include a timestamp so that reproducible trace artifacts can be compared directly.

Exit code:
  0   trace written successfully
  1   generation error (invalid params)
  2   usage error or I/O failure
`

const genCheckpointUsage = `Usage:
  ioflux gen checkpoint-write [flags] -o trace.ioflux

Generate a synthetic checkpoint-write trace (multi-rank sharded checkpoint).

The model is split into one shard per writer rank; each rank writes its shard as
a file (open, write, optional fsync, close). Several checkpoints may be emitted,
separated by --checkpoint-interval, to model periodic checkpointing.

Flags:
  -o <path>                  Output file (required; use - for stdout)
  --model-size <size>        Total checkpoint size (default 16GiB; accepts KiB/MiB/GiB or bytes)
  --writer-ranks <n>         Concurrent writer streams / shards (default 8)
  --write-block <size>       Write call size (default 4MiB; accepts KiB/MiB/GiB or bytes)
  --num-checkpoints <n>      Number of checkpoint bursts (default 1)
  --checkpoint-interval <s>  Seconds between bursts (default 0; 0 = single dense burst)
  --fsync <policy>           Durability: per-file | final | none (default per-file)

Size arguments accept a plain integer (bytes) or a suffix: KiB, MiB, GiB (binary),
KB, MB, GB (decimal), or K/M/G (binary aliases).

Output is byte-identical for the same flags. The trace header does not include a
timestamp so that reproducible trace artifacts can be compared directly.

Exit code:
  0   trace written successfully
  1   generation error (invalid params)
  2   usage error or I/O failure
`

// runGen is the entry point for the `gen` subcommand. It dispatches to a
// per-profile generator.
func runGen(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, genUsage)
		return 2
	}
	switch args[0] {
	case "training-read":
		return runGenTrainingRead(args[1:], stdout, stderr)
	case "checkpoint-write":
		return runGenCheckpoint(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "ioflux gen: unknown profile %q\n\nSupported profiles: training-read, checkpoint-write\n", args[0])
		return 2
	}
}

// runGenTrainingRead generates a synthetic training-read trace.
func runGenTrainingRead(args []string, stdout, stderr io.Writer) int {
	p := trainingread.DefaultParams()

	fs := flag.NewFlagSet("gen training-read", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, genTrainingReadUsage) }

	var out string
	fs.StringVar(&out, "o", "", "output file (required; - for stdout)")
	fs.IntVar(&p.Shards, "shards", p.Shards, "number of shard files")
	fs.Var(newBytesFlag(&p.ShardSize), "shard-size", "shard size (e.g. 64MiB)")
	fs.Var(newBytesFlag(&p.RecordSize), "record-size", "mean record size (e.g. 512KiB)")
	fs.StringVar(&p.RecordSizeDist, "record-size-dist", p.RecordSizeDist, "record size distribution")
	fs.IntVar(&p.Epochs, "epochs", p.Epochs, "number of epochs")
	fs.IntVar(&p.DataloaderWorkers, "dataloader-workers", p.DataloaderWorkers, "dataloader worker count")
	fs.IntVar(&p.PrefetchDepth, "prefetch-depth", p.PrefetchDepth, "outstanding shards per worker")
	fs.BoolVar(&p.Shuffle, "shuffle", p.Shuffle, "shuffle shard order each epoch")
	fs.StringVar(&p.ReadWithinShard, "read-within-shard", p.ReadWithinShard, "sequential or random")
	fs.Int64Var(&p.Seed, "seed", p.Seed, "PRNG seed")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if out == "" {
		fmt.Fprintln(stderr, "ioflux gen: -o is required")
		fmt.Fprint(stderr, genTrainingReadUsage)
		return 2
	}

	// Validate params before touching the output file. This prevents
	// truncating an existing trace when the user passes invalid flags.
	if err := trainingread.ValidateParams(p); err != nil {
		fmt.Fprintf(stderr, "ioflux gen: %v\n", err)
		return 1
	}

	var w io.Writer
	if out == "-" {
		w = stdout
	} else {
		f, err := os.Create(out)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux gen: %v\n", err)
			return 2
		}
		defer f.Close()
		w = f
	}

	if err := trainingread.Generate(p, w); err != nil {
		fmt.Fprintf(stderr, "ioflux gen: %v\n", err)
		return 2
	}

	if out != "-" {
		fmt.Fprintf(stdout, "wrote %s\n", out)
	}
	return 0
}

// runGenCheckpoint generates a synthetic checkpoint-write trace.
func runGenCheckpoint(args []string, stdout, stderr io.Writer) int {
	p := checkpoint.DefaultParams()

	fs := flag.NewFlagSet("gen checkpoint-write", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, genCheckpointUsage) }

	var out string
	fs.StringVar(&out, "o", "", "output file (required; - for stdout)")
	fs.Var(newBytesFlag(&p.ModelSize), "model-size", "total checkpoint size (e.g. 16GiB)")
	fs.IntVar(&p.WriterRanks, "writer-ranks", p.WriterRanks, "concurrent writer streams / shards")
	fs.Var(newBytesFlag(&p.WriteBlock), "write-block", "write call size (e.g. 4MiB)")
	fs.IntVar(&p.NumCheckpoints, "num-checkpoints", p.NumCheckpoints, "number of checkpoint bursts")
	fs.Float64Var(&p.CheckpointIntervalSec, "checkpoint-interval", p.CheckpointIntervalSec, "seconds between bursts (0 = single burst)")
	fs.StringVar(&p.Fsync, "fsync", p.Fsync, "durability: per-file | final | none")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if out == "" {
		fmt.Fprintln(stderr, "ioflux gen: -o is required")
		fmt.Fprint(stderr, genCheckpointUsage)
		return 2
	}

	// Validate params before touching the output file so invalid flags never
	// truncate an existing trace.
	if err := checkpoint.ValidateParams(p); err != nil {
		fmt.Fprintf(stderr, "ioflux gen: %v\n", err)
		return 1
	}

	var w io.Writer
	if out == "-" {
		w = stdout
	} else {
		f, err := os.Create(out)
		if err != nil {
			fmt.Fprintf(stderr, "ioflux gen: %v\n", err)
			return 2
		}
		defer f.Close()
		w = f
	}

	if err := checkpoint.Generate(p, w); err != nil {
		fmt.Fprintf(stderr, "ioflux gen: %v\n", err)
		return 2
	}

	if out != "-" {
		fmt.Fprintf(stdout, "wrote %s\n", out)
	}
	return 0
}

// bytesFlag implements flag.Value for size arguments that accept an integer
// number of bytes or a human-readable suffix such as KiB, MiB, GiB.
type bytesFlag struct{ p *int64 }

func newBytesFlag(p *int64) *bytesFlag { return &bytesFlag{p} }

func (f *bytesFlag) String() string {
	if f.p == nil {
		return "0"
	}
	return strconv.FormatInt(*f.p, 10)
}

func (f *bytesFlag) Set(s string) error {
	n, err := parseBytes(s)
	if err != nil {
		return err
	}
	*f.p = n
	return nil
}

// parseBytes parses a size string. Accepted forms:
//   - Plain non-negative integer: "4194304" → 4194304 bytes
//   - Binary suffixes (powers of 1024): KiB, MiB, GiB (case-insensitive)
//   - Decimal suffixes (powers of 1000): KB, MB, GB (case-insensitive)
//   - Short binary aliases: K, M, G (case-insensitive; treated as KiB/MiB/GiB)
func parseBytes(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("size must be non-negative, got %d", n)
		}
		return n, nil
	}
	// Order matters: longer suffixes must come before shorter prefixes of the
	// same letter (e.g. "GiB" before "G") to avoid mis-stripping.
	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"GB", 1_000_000_000},
		{"MB", 1_000_000},
		{"KB", 1_000},
		{"G", 1 << 30},
		{"M", 1 << 20},
		{"K", 1 << 10},
	}
	su := strings.ToUpper(s)
	for _, m := range multipliers {
		mu := strings.ToUpper(m.suffix)
		if strings.HasSuffix(su, mu) {
			// Strip suffix from the original string to preserve case in the
			// numeric part for the error message.
			numStr := strings.TrimSpace(s[:len(s)-len(m.suffix)])
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("size %q: invalid numeric part: %w", s, err)
			}
			if n < 0 {
				return 0, fmt.Errorf("size %q: must be non-negative", s)
			}
			return n * m.mult, nil
		}
	}
	return 0, fmt.Errorf("size %q: unrecognized format (use bytes or a KiB/MiB/GiB suffix)", s)
}

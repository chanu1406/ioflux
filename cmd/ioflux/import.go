package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/chanuollala/ioflux/pkg/importer"
	"github.com/chanuollala/ioflux/pkg/importer/strace"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const importUsage = `Usage:
  ioflux import <source> [-o trace.ioflux] <input>

Translate an external trace into the IOFlux trace format.

Sources:
  strace   strace -T -tt -f output (file syscalls)

Flags:
  -o <path>   Output file (required; use - for stdout)

The input is the final positional argument (use - for stdin); a .gz input is
decompressed automatically. Flags must precede the input path. An existing -o
file is left untouched if the input fails to parse.

Exit code:
  0   trace written successfully
  1   parse/format error in the input
  2   usage error or I/O failure
`

type importFunc func(io.Reader, io.Writer) (importer.Report, error)

var importSources = map[string]importFunc{
	"strace": strace.Import,
}

// runImport is the entry point for the `import` subcommand.
func runImport(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, importUsage)
		return 2
	}
	source := args[0]
	imp, ok := importSources[source]
	if !ok {
		fmt.Fprintf(stderr, "ioflux import: unknown source %q\n\nSupported sources: strace\n", source)
		return 2
	}
	args = args[1:]

	fs := flag.NewFlagSet("import "+source, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, importUsage) }
	var out string
	fs.StringVar(&out, "o", "", "output file (required; - for stdout)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if out == "" {
		fmt.Fprintln(stderr, "ioflux import: -o is required")
		fmt.Fprint(stderr, importUsage)
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "ioflux import: exactly one input file is required (use - for stdin; flags must precede it)")
		fmt.Fprint(stderr, importUsage)
		return 2
	}

	in, closeIn, err := openImportInput(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "ioflux import: %v\n", err)
		return 2
	}

	// Parse into a buffer first; only touch the output once it validates, so a
	// malformed input never truncates an existing -o file.
	var buf bytes.Buffer
	rep, err := imp(in, &buf)
	closeIn()
	if err != nil {
		fmt.Fprintf(stderr, "ioflux import: %v\n", err)
		return 1
	}

	if code := writeImportOutput(out, buf.Bytes(), stdout, stderr); code != 0 {
		return code
	}

	printImportSummary(stderr, source, buf.Bytes(), rep)
	if out != "-" {
		fmt.Fprintf(stdout, "wrote %s\n", out)
	}
	return 0
}

// openImportInput opens the import source (a file or stdin) and transparently
// decompresses gzip-compressed input.
func openImportInput(path string) (io.Reader, func(), error) {
	var raw io.Reader
	cleanup := func() {}
	if path == "-" {
		raw = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		raw = f
		cleanup = func() { f.Close() }
	}

	br := bufio.NewReader(raw)
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("gzip: %w", err)
		}
		prev := cleanup
		return gz, func() { gz.Close(); prev() }, nil
	}
	return br, cleanup, nil
}

// writeImportOutput writes the validated trace bytes to out (or stdout for -).
// It returns a non-zero exit code on I/O failure, 0 on success.
func writeImportOutput(out string, data []byte, stdout, stderr io.Writer) int {
	if out == "-" {
		if _, err := stdout.Write(data); err != nil {
			fmt.Fprintf(stderr, "ioflux import: %v\n", err)
			return 2
		}
		return 0
	}
	f, err := os.Create(out)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux import: %v\n", err)
		return 2
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		fmt.Fprintf(stderr, "ioflux import: %v\n", err)
		return 2
	}
	if err := f.Close(); err != nil {
		fmt.Fprintf(stderr, "ioflux import: %v\n", err)
		return 2
	}
	return 0
}

// printImportSummary writes an honest account of the import to stderr: counts,
// everything skipped and why, any clamped timestamps, and the trace's declared
// capture limitations.
func printImportSummary(stderr io.Writer, source string, data []byte, rep importer.Report) {
	fmt.Fprintf(stderr, "imported %d op(s) across %d stream(s), %d target(s) via %s\n",
		rep.NumOps, rep.NumStreams, rep.NumTargets, source)
	if rep.SkippedOps > 0 {
		fmt.Fprintf(stderr, "skipped %d op(s):\n", rep.SkippedOps)
		reasons := make([]string, 0, len(rep.SkippedReasons))
		for r := range rep.SkippedReasons {
			reasons = append(reasons, r)
		}
		sort.Strings(reasons)
		for _, r := range reasons {
			fmt.Fprintf(stderr, "  %-24s %d\n", r, rep.SkippedReasons[r])
		}
	}
	if rep.TimestampClamped > 0 {
		fmt.Fprintf(stderr, "timestamps clamped (non-monotonic source clock): %d\n", rep.TimestampClamped)
	}
	if r, err := trace.NewReader(bytes.NewReader(data)); err == nil {
		if lim := r.Header().CaptureLimitations; lim != "" {
			fmt.Fprintf(stderr, "capture limitations: %s\n", lim)
		}
	}
}

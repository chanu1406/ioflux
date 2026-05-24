package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/chanuollala/ioflux/pkg/trace"
)

const validateUsage = `Usage:
  ioflux validate <trace.ioflux>

Reads the given .ioflux file and reports any schema or invariant violations.

Exit code:
  0   trace is valid (may have warnings)
  1   trace has one or more errors
  2   usage error or I/O failure (file not found, etc.)
`

// runValidate is the entry point for the `validate` subcommand. It is split
// from main() so tests can drive it with captured stdout/stderr.
func runValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, validateUsage) }
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprint(stderr, validateUsage)
		return 2
	}
	path := fs.Arg(0)

	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux validate: %v\n", err)
		return 2
	}
	defer f.Close()

	r, err := trace.NewReader(f)
	if err != nil {
		// Parse failure of the header counts as an invalid trace, not a
		// usage error: the file exists and is readable but is not a
		// well-formed .ioflux. Exit 1 so CI catches it.
		fmt.Fprintf(stdout, "ioflux trace: %s\n", path)
		fmt.Fprintf(stdout, "  parse error: %v\nINVALID\n", err)
		return 1
	}

	rep, err := trace.Validate(r)
	if err != nil {
		// I/O failure mid-iteration — distinct from invariant violation.
		fmt.Fprintf(stderr, "ioflux validate: %v\n", err)
		return 2
	}

	printReport(stdout, path, rep)
	if !rep.OK() {
		return 1
	}
	return 0
}

func printReport(w io.Writer, path string, rep trace.Report) {
	h := rep.Header
	fmt.Fprintf(w, "ioflux trace: %s\n", path)
	fmt.Fprintf(w, "  version          %d\n", h.Version)
	fmt.Fprintf(w, "  kind             %s\n", h.Kind)
	if h.Profile != "" {
		fmt.Fprintf(w, "  profile          %s\n", h.Profile)
	}
	if h.CaptureMethod != "" {
		fmt.Fprintf(w, "  capture_method   %s\n", h.CaptureMethod)
	}
	fmt.Fprintf(w, "  time_unit        %s\n", h.TimeUnit)
	fmt.Fprintf(w, "  targets          %d\n", len(h.Targets))
	fmt.Fprintf(w, "  ops              %d\n", rep.NumOpsRead)
	fmt.Fprintf(w, "  streams          %d\n", len(rep.Streams))

	if len(rep.Errors) > 0 {
		fmt.Fprintln(w, "errors:")
		for _, e := range rep.Errors {
			fmt.Fprintf(w, "  %s\n", e)
		}
	}
	if len(rep.Warnings) > 0 {
		fmt.Fprintln(w, "warnings:")
		for _, x := range rep.Warnings {
			fmt.Fprintf(w, "  %s\n", x)
		}
	}
	if rep.OK() {
		fmt.Fprintln(w, "OK")
	} else {
		fmt.Fprintf(w, "INVALID: %d error(s), %d warning(s)\n",
			len(rep.Errors), len(rep.Warnings))
	}
}

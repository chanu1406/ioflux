package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/chanuollala/ioflux/pkg/trace"
	"github.com/chanuollala/ioflux/pkg/transform"
)

const transformUsage = `Usage:
  ioflux transform split-reads --block <size> -o out.ioflux in.ioflux

Apply a declared transformation to a trace.

A transformed trace no longer describes the workload that was captured. The
change is recorded in the output's header — what was done, with what parameters,
and the digest of the trace it came from — so a replay of it can never present
itself as an exact replay of the source, and a comparison can tell a declared
transformation apart from an unrelated workload.

Transformations:
  split-reads --block <size>
        Divide every READ/GET larger than <size> into requests of at most that
        size, over identical extents. Targets, offsets covered, per-stream
        order, and total bytes transferred are unchanged; the number of
        operations and their sizes are not. This models the same workload read
        with a smaller block size.

Sizes accept a plain integer (bytes) or a suffix: KiB, MiB, GiB, KB, MB, GB, K/M/G.

Exit codes:
  0   trace written
  1   input trace invalid, or the transformation could not be applied
  2   usage error or I/O failure
`

// runTransform is the entry point for the `transform` subcommand.
func runTransform(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, transformUsage)
		return 2
	}
	switch args[0] {
	case "split-reads":
		return runSplitReads(args[1:], stdout, stderr)
	case "-h", "--help":
		fmt.Fprint(stderr, transformUsage)
		return 2
	default:
		fmt.Fprintf(stderr, "ioflux transform: unknown transformation %q\n\n", args[0])
		fmt.Fprint(stderr, transformUsage)
		return 2
	}
}

func runSplitReads(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("transform split-reads", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, transformUsage) }
	var block int64
	fs.Var(newBytesFlag(&block), "block", "maximum bytes per produced read")
	outPath := fs.String("o", "", "output trace path (required; - for stdout)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *outPath == "" {
		fmt.Fprintln(stderr, "ioflux transform: -o is required")
		return 2
	}
	if block <= 0 {
		fmt.Fprintln(stderr, "ioflux transform: --block must be > 0")
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "ioflux transform: exactly one input trace is required")
		return 2
	}
	inPath := fs.Arg(0)

	srcBytes, err := os.ReadFile(inPath)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux transform: %v\n", err)
		return 2
	}

	// Validate the input before transforming it. A transformation of a trace
	// that was already inconsistent would produce a consistent-looking output
	// and bury the original problem.
	rep, err := validateTraceBytes(srcBytes)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux transform: read %s: %v\n", inPath, err)
		return 1
	}
	if !rep.OK() {
		fmt.Fprintf(stderr, "ioflux transform: %s is not a valid trace:\n", inPath)
		for _, issue := range rep.Errors {
			fmt.Fprintf(stderr, "  %s\n", issue)
		}
		return 1
	}

	hdr, ops, err := readTrace(srcBytes)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux transform: %v\n", err)
		return 1
	}

	split, err := transform.SplitReads(ops, block)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux transform: %v\n", err)
		return 1
	}
	outHdr := transform.SplitReadsHeader(hdr, split, block, trace.Digest(srcBytes))

	var buf bytes.Buffer
	if err := writeTrace(&buf, outHdr, split); err != nil {
		fmt.Fprintf(stderr, "ioflux transform: write: %v\n", err)
		return 2
	}
	// Validate the output too: a transformation that produced an invalid trace
	// must fail here rather than at the start of a measured run.
	if outRep, err := validateTraceBytes(buf.Bytes()); err != nil || !outRep.OK() {
		fmt.Fprintf(stderr, "ioflux transform: produced an invalid trace (this is a bug):\n")
		if err != nil {
			fmt.Fprintf(stderr, "  %v\n", err)
		} else {
			for _, issue := range outRep.Errors {
				fmt.Fprintf(stderr, "  %s\n", issue)
			}
		}
		return 1
	}

	if *outPath == "-" {
		if _, err := stdout.Write(buf.Bytes()); err != nil {
			fmt.Fprintf(stderr, "ioflux transform: write: %v\n", err)
			return 2
		}
	} else {
		if err := os.WriteFile(*outPath, buf.Bytes(), 0o644); err != nil {
			fmt.Fprintf(stderr, "ioflux transform: %v\n", err)
			return 2
		}
		fmt.Fprintf(stdout, "wrote %s\n", *outPath)
	}

	fmt.Fprintf(stderr, "ioflux transform: %d op(s) -> %d op(s); %s unchanged\n",
		len(ops), len(split), fmtBytes(outHdr.Summary.TotalBytes))
	return 0
}

// validateTraceBytes validates a complete trace held in memory.
func validateTraceBytes(data []byte) (trace.Report, error) {
	r, err := trace.NewReader(bytes.NewReader(data))
	if err != nil {
		return trace.Report{}, err
	}
	return trace.Validate(r)
}

// writeTrace serializes a header and its operations.
func writeTrace(w io.Writer, hdr trace.Header, ops []trace.Op) error {
	tw := trace.NewWriter(w)
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	for _, op := range ops {
		if err := tw.WriteOp(op); err != nil {
			return err
		}
	}
	return nil
}

// readTrace parses a trace's header and every operation into memory.
func readTrace(data []byte) (trace.Header, []trace.Op, error) {
	r, err := trace.NewReader(bytes.NewReader(data))
	if err != nil {
		return trace.Header{}, nil, err
	}
	hdr := r.Header()
	var ops []trace.Op
	for {
		op, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return trace.Header{}, nil, err
		}
		ops = append(ops, op)
	}
	return hdr, ops, nil
}

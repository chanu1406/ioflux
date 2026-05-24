// Command ioflux is the IOFlux CLI. This early build wires the validate
// subcommand for checking .ioflux trace files.
package main

import (
	"fmt"
	"os"
)

const usage = `ioflux — distributed AI-storage workload profiler and replay engine

Usage:
  ioflux <command> [flags]

Commands:
  validate <trace.ioflux>     Validate a trace against the schema and invariants.

Run 'ioflux <command> -h' for command-specific help.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "validate":
		os.Exit(runValidate(args, os.Stdout, os.Stderr))
	case "-h", "--help", "help":
		fmt.Print(usage)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "ioflux: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
}

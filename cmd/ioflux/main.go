// Command ioflux is the IOFlux CLI.
package main

import (
	"fmt"
	"os"
)

const usage = `ioflux — distributed AI-storage workload profiler and replay engine

Usage:
  ioflux <command> [flags]

Commands:
  gen      <profile> [flags]  Generate a synthetic trace.
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
	case "gen":
		os.Exit(runGen(args, os.Stdout, os.Stderr))
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

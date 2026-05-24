# IOFlux

IOFlux is a trace driven storage workload replay tool for AI systems. It is
designed to capture, generate, validate, and replay storage access patterns
such as training data reads, checkpoint writes, and object store ingestion.

A trace is a portable JSONL recording of storage operations over time: opens,
reads, writes, closes, object GETs, and related metadata. IOFlux uses traces to
benchmark storage backends against concrete AI workload behavior rather than generic
synthetic I/O patterns.

The goal is to benchmark storage backends against a concrete workload trace,
not just a generic synthetic profile. IOFlux is intended to work with POSIX
filesystems and S3-compatible object stores, with reports focused on
throughput, latency, straggler behavior, and replay fidelity.

This repository is in early development. The current build includes the
`.ioflux` JSONL trace reader/writer, trace validation logic, and the
`ioflux validate` subcommand.

## Quick start

```bash
go build -o bin/ioflux ./cmd/ioflux
bin/ioflux validate pkg/trace/testdata/minimal_valid.ioflux
```

Expected output:

```
ioflux trace: pkg/trace/testdata/minimal_valid.ioflux
  version          1
  kind             synthetic
  profile          training-read
  time_unit        ns
  targets          2
  ops              6
  streams          1
OK
```

Exit codes: `0` if the trace is valid, `1` if it has invariant violations,
`2` on usage or I/O failure. Warnings (e.g., a stream that opens a file but
never closes it) do not affect the exit code.

## Development

```bash
go test ./... -race        # unit tests under the race detector
go vet ./...               # static checks
gofmt -l .                 # formatting check (empty = clean)
```

Requires Go 1.22 or newer.

# IOFlux

IOFlux is a trace driven storage workload replay tool for AI workloads. It
generates, imports, validates, and replays storage access patterns such as
training data reads and checkpoint writes, then reports how a backend handled
them.

A trace is a portable JSONL recording of storage operations over time: opens,
reads, writes, closes, object GETs, and related metadata. IOFlux replays a trace
against a real backend so you can benchmark storage against a concrete workload
rather than a generic synthetic profile.

IOFlux works with POSIX filesystems and S3-compatible object stores, with reports
focused on throughput, latency, straggler behavior, and replay fidelity. Traces
can be generated synthetically or imported from strace and DFTracer output. Live
capture and multi-host distribution are not implemented yet.

## Commands

- `ioflux gen training-read [flags]` generates a synthetic trace.
- `ioflux import strace|dftracer <file>` imports an external trace into .ioflux.
- `ioflux validate <trace>` checks a trace against the schema and invariants.
- `ioflux run [flags]` replays a trace against the mem, local, or s3 engine.
- `ioflux report <results.json>` prints a saved run report.

Run `ioflux <command> -h` for the flags of each command.

## Quick start

Build the binary:

```bash
go build -o bin/ioflux ./cmd/ioflux
```

Import an strace capture, replay it against the local filesystem, and print the
report:

```bash
bin/ioflux import strace -o run.ioflux capture.strace
bin/ioflux run --trace run.ioflux --engine local \
  --prepare materialize-synthetic --target-map map.yaml -o results.json
bin/ioflux report results.json
```

The target map rewrites the captured paths onto the replay backend, so the run
only touches data you choose:

```yaml
target_rewrite:
  - from: "/mnt/dataset/imagenet/"
    to: "./scratch/"
```

Exit codes: `0` on success, `1` on a trace or replay error, `2` on usage or I/O
failure.

## Development

```bash
go test ./... -race        # unit tests under the race detector
go vet ./...               # static checks
gofmt -l .                 # formatting check (empty = clean)
```

Requires Go 1.22 or newer.

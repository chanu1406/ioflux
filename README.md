# IOFlux

IOFlux is a trace driven storage workload replay tool for AI workloads. It
generates synthetic training-read traces, imports real traces (strace,
DFTracer), validates them, and replays any of them against a storage backend,
then reports how the backend handled them. (A synthetic checkpoint-write
generator is planned; write-heavy workloads can already be replayed from
imported traces today.)

A trace is a portable JSONL recording of storage operations over time: opens,
reads, writes, closes, object GETs, and related metadata. IOFlux replays a trace
against a real backend so you can benchmark storage against a concrete workload
rather than a generic synthetic profile.

IOFlux works with POSIX filesystems and S3-compatible object stores, with reports
focused on throughput, latency, straggler behavior, and replay fidelity. Traces
can be generated synthetically or imported from strace and DFTracer output. A run
can be distributed across multiple hosts, which replay disjoint subsets of the
trace's streams and report per-host throughput alongside merged percentiles. Live
process capture is not implemented yet.

## Commands

- `ioflux gen training-read [flags]` generates a synthetic trace.
- `ioflux import strace|dftracer <file>` imports an external trace into .ioflux.
- `ioflux validate <trace>` checks a trace against the schema and invariants.
- `ioflux run [flags]` replays a trace against the mem, local, or s3 engine.
- `ioflux worker --listen :7800` runs a replay worker for distributed runs.
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

## Distributed runs

Start a worker on each host, then point a run at them with `--hosts`:

```bash
# on each host
bin/ioflux worker --listen :7800

# from the coordinator
bin/ioflux run --trace t.ioflux --engine local \
  --prepare materialize-synthetic --target-map map.yaml \
  --hosts hostA:7800,hostB:7800 -o results.json
bin/ioflux report results.json
```

The coordinator partitions the trace's streams round-robin across the workers,
synchronizes them through `PREPARE`/`RUN`/`DONE` barriers, and merges the
per-host HDR histograms losslessly, so the reported percentiles come from one
global distribution rather than averaged per-host numbers. The report adds a
per-host table and a first-done/last-done straggler window. A worker failure
aborts the whole run (v1 has no failover). Omitting `--hosts` runs single-node
through the same code path via one in-process worker.

> **Security:** the coordinator/worker gRPC transport is plaintext and
> unauthenticated, and the plan it sends carries the trace bytes and any S3
> credentials. Run workers only on a trusted network (e.g. a private cluster
> subnet or over an SSH tunnel/VPN); do not expose `ioflux worker` on an
> untrusted network. TLS/mTLS is not implemented in v1.

## Development

```bash
go test ./... -race        # unit tests under the race detector
go vet ./...               # static checks
gofmt -l .                 # formatting check (empty = clean)
```

Requires Go 1.22 or newer.

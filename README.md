# IOFlux

IOFlux is a trace-driven storage workload replay tool for AI systems. It turns
synthetic or imported I/O traces into repeatable workloads that can be run
against POSIX filesystems and S3-compatible object stores.

Use IOFlux to evaluate how a storage backend handles a specific workload—not a
generic benchmark profile—and compare throughput, latency, straggler behavior,
and replay fidelity across runs.

## Features

- Generate training-read and checkpoint-write workloads.
- Import file I/O captured by `strace` or DFTracer.
- Validate portable, JSONL-based `.ioflux` traces.
- Replay against in-memory, local filesystem, and S3-compatible backends.
- Run in maximum-throughput, original-timeline, or scaled-timeline modes.
- Distribute streams across multiple replay workers.
- Export machine-readable JSON, optional CSV, and human-readable reports.

## Requirements

- Go 1.25 or newer
- Linux for `O_DIRECT` support and the qualification harness
- `strace` or DFTracer only when importing traces from those tools

## Build

```bash
go build -o bin/ioflux ./cmd/ioflux
```

## Quick start

Generate a small training-read trace, materialize its dataset, replay it against
the local filesystem, and print the report:

```bash
bin/ioflux gen training-read \
  --shards 8 \
  --shard-size 16MiB \
  --record-size 256KiB \
  --dataloader-workers 4 \
  -o training.ioflux

mkdir -p ioflux-data
```

Create a target map that places every generated shard inside that directory:

```yaml
# local-map.yaml
target_rewrite:
  - from: ""
    to: "./ioflux-data/"
```

```bash
bin/ioflux run \
  --trace training.ioflux \
  --engine local \
  --target-root ./ioflux-data \
  --target-map local-map.yaml \
  --prepare materialize-synthetic \
  -o results.json

bin/ioflux report results.json
```

Run `bin/ioflux <command> -h` for all available options.

## Commands

| Command | Description |
| --- | --- |
| `ioflux gen training-read` | Generate a sharded training-data read trace. |
| `ioflux gen checkpoint-write` | Generate a multi-rank checkpoint write trace. |
| `ioflux import strace` | Import a `strace` capture. |
| `ioflux import dftracer` | Import a DFTracer capture. |
| `ioflux validate` | Validate a `.ioflux` trace and its invariants. |
| `ioflux run` | Replay a trace against the `mem`, `local`, or `s3` engine. |
| `ioflux worker` | Start a worker for distributed replay. |
| `ioflux report` | Print one report, or compare two reports if they are comparable. |

## Importing a real trace

Convert a capture, map its source paths into a controlled replay namespace,
then run it:

```bash
bin/ioflux import strace -o workload.ioflux capture.strace
bin/ioflux validate workload.ioflux
```

```yaml
# map.yaml
target_rewrite:
  - from: "/mnt/dataset/"
    to: "./ioflux-data/dataset/"
```

```bash
mkdir -p ioflux-data

bin/ioflux run \
  --trace workload.ioflux \
  --engine local \
  --target-map map.yaml \
  --target-root ./ioflux-data \
  --prepare materialize-synthetic \
  -o results.json
```

Imported traces retain the limitations of their capture method. IOFlux records
those limitations and replay-fidelity information in the trace and result
metadata.

## Comparing two runs

Replay the same trace before and after a change, then compare the two results:

```bash
bin/ioflux report before.json after.json
```

A delta between two runs is only meaningful if the runs were comparable, so the
comparison is checked before it is printed. Every result records what produced
it — a digest of the trace bytes, the engine and its configuration, the cache
recipe, the containment root, the host, and the ioflux build — and the
comparison reports one of three outcomes:

- **comparable** — the runs agree on all of it, and the delta is attributable to
  what changed between them.
- **comparable with caveats** — something else differs too. The difference is
  named above the numbers it qualifies: a different trace means the delta
  measures two workloads rather than two backends, a different cache recipe
  usually dominates a read workload's timing, and so on. The delta is still
  printed.
- **incomparable** — at least one run is not a valid measurement, because
  operations failed or latency samples fell outside the histogram's range. No
  delta is printed and the command exits `1`, so a comparison in CI fails rather
  than reporting a speedup that a broken run produced.

Trace identity comes from the digest rather than the file name, so the same
bytes under a different path still compare as one workload. Results written
before this metadata existed compare with an explicit note that their identity
could not be verified — never as agreement.

## Target safety

Trace targets may come from imported or hand-edited input. For local replay,
use `--target-root` to confine file access to a dedicated directory. For S3,
the same option confines access to a key prefix within the configured bucket.
Runs without a target root are allowed but reported as unconfined.

## Distributed replay

Distributed replay: Start one worker per host, then pass their
addresses to `ioflux run`:

```bash
# On each worker host
bin/ioflux worker --listen :7800 --target-root /srv/ioflux-data

# On the coordinator
bin/ioflux run \
  --trace workload.ioflux \
  --engine local \
  --target-map cluster-map.yaml \
  --hosts host-a:7800,host-b:7800 \
  -o results.json
```

For this example, `cluster-map.yaml` must rewrite targets beneath
`/srv/ioflux-data/` on every worker host.

Workers use plaintext, unauthenticated gRPC and may receive trace data and S3
credentials. Run them only on a trusted private network, VPN, or SSH tunnel.

## Project status

This is an experimental systems project intended for controlled workload
replay and storage research. Distributed execution and cross-protocol replay
are not yet production-qualified. See
[`qualification/RESULTS.md`](qualification/RESULTS.md) for measured fidelity
results and known limitations.

## Development

```bash
go test ./... -race
go vet ./...
gofmt -l .
```

The end-to-end real-trace walkthrough is documented in
[`examples/README.md`](examples/README.md).

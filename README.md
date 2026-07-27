# IOFlux

IOFlux is a trace-driven storage workload replay prototype for AI workloads. It
generates synthetic training-read and checkpoint-write traces, imports real
traces (strace, DFTracer), validates them, and replays any of them against a
storage backend, then reports how the backend handled them.

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

## Current qualification boundary

IOFlux is useful today for controlled experiments and simple, synchronous,
same-domain replays. It is not yet a production-qualified benchmark for storage
purchasing, capacity planning, or whole-application reproduction. In particular,
imported traces retain capture-method limitations, short transfers make a run
fail, a latency sample outside the histogram's 100s trackable range (e.g. a
hang or stall) also fails the run instead of silently vanishing from every
percentile, and saved reports identify detected operation failures as invalid
execution. Importer loss manifests, environment comparability, result-schema
versioning, and full workload qualification are still in progress.

Target names come from the trace, which is imported or hand-edited input, so
`--target-root` confines the local engine to one directory: replay and dataset
preparation reject a target that resolves outside it — via `..`, an absolute
path, or a symlink — instead of reading or overwriting data elsewhere on the
host. Containment is enforced by the OS, not by string comparison. Without it a
run is unconfined, and the report says so (see
[Target containment](#target-containment)).

Distributed execution and POSIX-to-object replay are experimental. They are
valuable implementation and research paths, but their output must not be treated
as qualified cluster-wide or exact cross-protocol evidence yet.

## Commands

- `ioflux gen training-read [flags]` generates a synthetic sharded read trace.
- `ioflux gen checkpoint-write [flags]` generates a synthetic multi-rank
  checkpoint write trace.
- `ioflux import strace|dftracer <file>` imports an external trace into .ioflux.
- `ioflux validate <trace>` checks a trace against the schema and invariants.
- `ioflux run [flags]` replays a trace against the mem, local, or s3 engine.
- `ioflux worker --listen :7800` runs a replay worker for distributed runs.
- `ioflux report <results.json>` prints a saved run report, or
  `ioflux report <a.json> <b.json>` compares two reports side by side.

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
  --prepare materialize-synthetic --target-map map.yaml \
  --target-root ./scratch -o results.json
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

## Target containment

A trace's target names are untrusted input: an imported capture records absolute
production paths, and a hand-edited trace can contain anything. Dataset
preparation opens every write target with `CREATE|TRUNC`, so a target that
escapes the directory you meant to use — via `allow_passthrough`, a `..`
sequence, or a rule that does not match what you expected — would truncate real
data.

`--target-root <dir>` confines the local engine to one directory. Every target is
resolved (relative names against the working directory) and must land inside the
root; anything else fails the run before a file is opened, created, or
truncated:

```bash
bin/ioflux run --trace run.ioflux --engine local --target-root ./scratch \
  --prepare materialize-synthetic --target-map map.yaml -o results.json
```

Enforcement is done by the operating system, not by comparing strings: IOFlux
holds the root open as a directory handle and performs every file operation
relative to it (`os.Root`), so neither a `..` component nor a symlink can leave
the root — including a symlink placed inside the root that points outside it,
which a text-based check would happily follow.

Notes and limits:

- An **absolute symlink is rejected even when it points back inside the root**,
  because an absolute link cannot be resolved relative to a root. Relative
  symlinks that stay inside the root work normally, including ones that
  traverse `..` without leaving. If a dataset uses absolute symlinks internally,
  either relativize them or run without `--target-root`.
- A root confines *path resolution*, not the filesystem. Bind mounts, `/proc`
  files, and device nodes reachable inside the root remain reachable. The run
  records this in `run_env.engine_limitations` rather than implying a stronger
  guarantee.
- Without `--target-root`, the run is unconfined and records
  `no target root configured: …` in `run_env.engine_limitations`, which the
  report prints under Warnings. The guarantee IOFlux makes is not that targets
  are always confined — it is that a saved report never leaves the question
  unstated.
- Only the `local` engine can enforce a root. Passing `--target-root` with
  `--engine mem` or `--engine s3` is a usage error, not a silently ignored flag.
  Object-prefix containment for S3 is not implemented yet.
- `--target-root` cannot be combined with `--hosts`: the root is not part of the
  worker protocol, so a coordinator-side value would not apply on a remote
  worker. Set it on each worker instead (see below).

## Checkpoint-write workloads

`ioflux gen checkpoint-write` generates a synthetic multi-rank sharded
checkpoint write: each writer rank opens its shard, writes it in
`--write-block`-sized chunks, optionally fsyncs, and closes — repeated every
`--checkpoint-interval` seconds for `--num-checkpoints` bursts. This models
FSDP/DeepSpeed-style checkpoint I/O storms.

```bash
bin/ioflux gen checkpoint-write --model-size 64MiB --writer-ranks 4 \
  --write-block 1MiB --fsync per-file -o ckpt.ioflux

cat > ckpt-map.yaml <<'EOF'
target_rewrite:
  - from: ""
    to: "scratch/"
EOF

bin/ioflux run --trace ckpt.ioflux --engine local --mode asap \
  --target-map ckpt-map.yaml -o ckpt.json
bin/ioflux report ckpt.json
```

No `--prepare` is needed: a write trace's targets are created by the run
itself (`OPEN(create|trunc)`). Compare the write run against a training-read
report side by side — throughput, CPU, duration, and fidelity deltas, plus
each side's dominant data-op latency (`WRITE` vs `READ`):

```bash
bin/ioflux report ckpt.json results.json
```

Checkpoint-write replays op-for-op as `OPEN(create|trunc)` → `WRITE*` →
`[FSYNC]` → `CLOSE` against the `local` engine, and against `mem` with
`--fsync none` (the mem engine has no durable storage to fsync). Against S3,
eligible sequential full-coverage writes are coalesced into one whole-object
PUT, using multipart upload above the configured threshold. Results label that
path `replay_equivalence: object-level`; it is an experimental cross-domain
transformation, not an exact POSIX replay or durability comparison.

## Distributed runs (experimental)

Start a worker on each host, then point a run at them with `--hosts`:

```bash
# on each host
bin/ioflux worker --listen :7800 --target-root /srv/ioflux-scratch

# from the coordinator
bin/ioflux run --trace t.ioflux --engine local \
  --prepare materialize-synthetic --target-map map.yaml \
  --hosts hostA:7800,hostB:7800 -o results.json
bin/ioflux report results.json
```

A worker's `--target-root` is worker-local policy: it confines every target the
worker replays regardless of what the coordinator's plan asked for, because the
host that owns the data — not the coordinator — decides what a plan may touch.

The coordinator partitions the trace's streams round-robin across the workers,
synchronizes them through `PREPARE`/`RUN`/`DONE` barriers, and merges the
per-host HDR histograms losslessly, so the reported percentiles come from one
global distribution rather than averaged per-host numbers. The report adds a
per-host table and a first-done/last-done straggler window. A worker failure
aborts the whole run (v1 has no failover). This mode is a functional prototype,
not a qualified production distributed benchmark: placement semantics, clock
and start-skew calibration, authentication, partial evidence, and real
multi-host qualification remain incomplete. Omitting `--hosts` runs single-node
through the same code path via one in-process worker.

> **Security:** the coordinator/worker gRPC transport is plaintext and
> unauthenticated, and the plan it sends carries the trace bytes and any S3
> credentials. Run workers only on a trusted network (e.g. a private cluster
> subnet or over an SSH tunnel/VPN); do not expose `ioflux worker` on an
> untrusted network. TLS/mTLS is not implemented in v1. `--target-root` bounds
> what a plan can reach on the worker's filesystem, but it is not a substitute
> for authentication: an unauthenticated worker still accepts runs from anyone
> who can reach the port.

## Development

```bash
go test ./... -race        # unit tests under the race detector
go vet ./...               # static checks
gofmt -l .                 # formatting check (empty = clean)
```

Requires Go 1.25 or newer (`--target-root` containment uses `os.Root`, whose
`MkdirAll` landed in 1.25).

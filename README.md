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
| `ioflux transform` | Apply a declared transformation to a trace. |
| `ioflux run` | Replay a trace against the `mem`, `local`, or `s3` engine. |
| `ioflux calibrate` | Measure the load generator's own ceiling for a trace. |
| `ioflux experiment` | Run two configurations interleaved and compare them pairwise. |
| `ioflux worker` | Start a worker for distributed replay. |
| `ioflux report` | Print one report or trial set, or compare two if they are comparable. |

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

Replay the same trace before and after a change, then compare the two results.
One run of each is not enough to tell a real difference from noise, so measure
each side repeatedly:

```bash
bin/ioflux run --trace workload.ioflux --engine local --trials 10 --warmup 2 \
  --target-root ./ioflux-data --target-map map.yaml -o before.json

# ...make the change...

bin/ioflux run --trace workload.ioflux --engine local --trials 10 --warmup 2 \
  --target-root ./ioflux-data --target-map map.yaml -o after.json

bin/ioflux report before.json after.json
```

With `--trials`, the output file holds every trial plus the distribution over
them — median, coefficient of variation, and a 95% interval on the median. The
interval is computed from order statistics rather than assuming a normal
distribution, and below six trials none is reported at all, because no interval
of that confidence exists for a smaller sample.

Comparing two trial sets reports the difference in median duration and whether
the two intervals overlap. Non-overlapping intervals are evidence of a real
difference; overlapping ones mean these trials did not establish one, which is
not the same as showing there is none.

Two policy limits must be met before a difference is reported at all, both
adjustable and both floors rather than universal values:

```bash
bin/ioflux report --min-trials 10 --max-cv 5 before.json after.json
```

`--max-cv` is the important one. If a configuration's own run-to-run spread is
wider than the difference being looked for, no number of trials makes the
comparison mean anything, and the comparison is refused rather than printed.

## Is the tool the bottleneck?

A throughput number is only a fact about storage if storage is what limited it.
IOFlux has its own per-op cost — scheduling, dispatch, buffer handling,
accounting — and against a fast enough backend that cost stops being negligible.
Two runs compared under those conditions differ by however much the tool got in
the way, which is not a property of either backend.

`ioflux calibrate` measures the ceiling that cost implies. It replays the same
trace, at the same concurrency, against the null (`mem`) engine, which performs
no I/O. Whatever that replay takes is IOFlux alone, and no replay of that trace
can go faster against any backend:

```bash
bin/ioflux calibrate --against results.json --max-generator-share 25
```

```
Load-generator ceiling (mem engine, 10 valid trial(s))
  max-inflight        512 over 4 stream(s)
  median duration     5.9ms for 4131 ops
  ceiling             700.8k ops/s   (1.4µs per op)
  trial CV            2.0%

Attribution of results.json
  run rate            406.7k ops/s
  generator share     58.0% of ceiling   (bound 25.0%)
  client CPU          32.6% of all cores during the run
  verdict             not_attributable
```

The generator share is the run's op rate as a fraction of that ceiling. A run at
5% of the ceiling was limited by its backend, and a delta between two such runs
is a property of those backends. The run above reached 58%, so its timing
measures IOFlux as much as it measures storage — which is the expected outcome
for a warm-cache local read, where the filesystem is returning data at close to
memory speed.

The ceiling is taken from the trials' median rather than their fastest. The
fastest overstates what the generator can sustain, and an overstated ceiling
understates its share of a measured run — the direction of error that would
wrongly reassure.

There is no default share bound, for the same reason there is no default
regression threshold: what share is tolerable depends on the fixture and on what
the result will be used for. Without one the share is reported and nothing is
decided.

Stability is checked before anything is concluded. A ceiling whose own trials
disagree by more than the policy allows is refused rather than used, because a
ceiling that the next run would contradict bounds nothing:

```
  verdict             not_assessed
    - the calibration's own spread is 35.8% CV, wider than the 5.0% policy:
      a ceiling this unstable does not bound anything
```

The same refusals apply as elsewhere: a ceiling measured on a different trace,
at a different concurrency, or against an engine that performed real I/O is
rejected rather than caveated, and a run that failed operations acquires no
verdict. A paced replay (`timeline` or `scaled`) is refused too — its op rate is
set by the trace's schedule rather than by the backend, so comparing it to a
ceiling measures the trace.

Verdicts exit `0` for `attributable`, `3` for `not_attributable`, and `1` when
the evidence was refused.

## Paired experiments

Running one configuration to completion and then the other assigns any drift in
the machine — thermal throttling, a neighbouring tenant, a cache warming up — to
whichever arm ran second, where it cannot be told apart from the change being
measured. When the thing you are changing is a replay setting rather than the
machine itself, run the two arms interleaved instead:

```yaml
# experiment.yaml
claim: does capping in-flight I/O at 4 slow this workload down?
trials: 10
warmup: 2
seed: 42
policy:
  min_trials: 10
  max_cv_percent: 5

run:                       # shared by both arms
  trace: workload.ioflux
  engine: local
  target_root: ./ioflux-data
  target_map: map.yaml
  cache_mode: cold
  max_inflight: 16

baseline: {}               # no overrides
treatment:
  max_inflight: 4          # whatever differs here is the treatment
```

```bash
bin/ioflux experiment --config experiment.yaml -o experiment.json
```

The arms alternate in a randomized within-pair order — recorded via `seed`, so
the ordering is reproducible — and each pair is differenced. Whatever the two
runs shared cancels, which resolves differences that the arms' own spread would
otherwise hide.

The treatment variable is derived from what actually differs between the two
resolved configurations, not from which keys the treatment block lists. A
treatment that restates a value it already had is reported as no treatment,
rather than as a clean null result. A misspelled key is an error, since it would
otherwise silently produce an experiment with nothing in it.

Because the treatment is declared, it is not also reported as uncontrolled
drift; only differences nobody chose are. S3 credentials are not config fields —
a config file travels with its results, so credentials come from the environment.

### Turning the difference into a decision

Reporting an effect size is not the same as deciding whether to ship. Add a
threshold and the experiment renders a verdict:

```yaml
policy:
  min_trials: 10
  max_cv_percent: 5
  max_duration_regression_percent: 7   # a 7% slowdown is the most that passes
```

```
Regression gate: REGRESSION (threshold 7.0%)
  the whole 95% interval (+56.0% … +65.2%) lies beyond the 7.0% threshold
```

The verdict is judged against the **whole 95% interval**, not the median. A gate
that compared the median to the threshold would flip on run-to-run noise
whenever the true effect sat near the threshold — which is exactly where the
decision matters, and exactly where a flapping gate teaches people to ignore it.
That produces three outcomes rather than two:

| Verdict | Meaning | Exit |
|---|---|---|
| `pass` | the whole interval is within the threshold | 0 |
| `regression` | the whole interval is beyond it | 3 |
| `inconclusive` | the interval spans it — these trials decide nothing | 4 |
| `not_assessed` | no threshold declared, or the evidence was refused | 0 / 1 |

`inconclusive` is the outcome a two-way gate has to guess at, and guessing is
how a real regression ships on a noisy day. The remedy is more pairs or a
quieter host — not re-running until it comes back green. If your threshold is
narrower than your measurement's own precision, every run will land here; that
is the tool telling you the budget is tighter than the evidence can support.

Evidence the tool refuses as incomparable never acquires a verdict. A comparison
that was rejected for instability cannot become a release approval by being
measured against a number.

There is no default threshold. Thresholds are calibrated to a fixture and to the
cost of the decision, so omitting one reports the difference and decides nothing
rather than applying a number nobody chose.

### Changing the workload itself

Some treatments are a change to the workload rather than to a setting — reading
the same data in smaller blocks, for instance. `ioflux transform` produces a new
trace for those:

```bash
bin/ioflux transform split-reads --block 64KiB -o small-reads.ioflux workload.ioflux
```

`split-reads` divides every read larger than the block into requests of at most
that size, over identical extents: same targets, same offsets covered, same
total bytes, more and smaller operations.

The output records what was done to it — the transformation, its parameters, and
a digest of the trace it came from — in the trace header, so it travels with the
file. A replay of a transformed trace carries that ledger into its results, and
a comparison uses it to tell "the treatment is the same workload read
differently" apart from "the two runs replayed unrelated traces", which are
otherwise indistinguishable when all you have is two differing digests.

Use it as the treatment by pointing the two arms at the two traces:

```yaml
run:
  trace: workload.ioflux
  # ...
baseline: {}
treatment:
  trace: small-reads.ioflux
```

Interleaving only applies when the change is a replay setting or the trace. If the treatment
is the machine itself — a kernel, a mount option, a firmware revision — the two
arms cannot be alternated, and `--trials` with two separate runs is the option;
that comparison does not control for drift, and its caveats say so.

A delta between two runs is also only meaningful if the runs were comparable at
all, so the comparison is checked before it is printed. Every result records what produced
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
  operations failed, latency samples fell outside the histogram's range, or a
  trial set was too small or too unstable for its policy. No delta is printed
  and the command exits `1`, so a comparison in CI fails rather than reporting a
  speedup that a broken or noisy run produced.

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

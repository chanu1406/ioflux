# qual-01: first live-versus-replay comparison

Fixture: [FIXTURE.md](FIXTURE.md). Reproduce with `qualification/qualify.sh`
(≈40 s end to end). Machine-readable evidence:
`$IOFLUX_QUAL_WORK/evidence/reconcile-cold.json`.

This is the first time anyone has checked whether an IOFlux replay reproduces the
workload it claims to replay. Nothing below was tuned to agree; every comparison
reports its residual, and two dimensions came out **negative**.

§1–§14 are that first comparison. **§15 is the FIXTURE.md §10 controlled
regression**, run later against schema v2 (`qualification/qual10.sh`); it
supersedes §4 and restates §14's requested-length row.

---

## 1. Headline

**For the declared claim, replay reproduced the workload exactly. For two
dimensions it provably did not, and both are now disclosed by the tool.**

| §10.1 dimension | Verdict | Residual |
|---|---|---|
| Oracle self-consistency (3 independent instruments) | **match** | 0 |
| Capture adequacy vs independent oracle | **match** | 0 of 1,184 ops |
| Import adequacy vs independent oracle | **match** | 0, plus 32 *declared* drops |
| Target identities | **match** | 0 of 1,152 |
| Offsets | **match** | 0 of 1,152 |
| Returned lengths | **match** | 0 of 1,152 |
| Per-stream ordering | **match** (exact) | 0 |
| Peak outstanding I/O | **match** | 4 vs 4 |
| Mean outstanding I/O | **match** (within noise) | −0.05 … +0.31 across runs |
| Block-device bytes served | **match** (exact) | **0 bytes** |
| **Source requested lengths** | **MISMATCH** | **32 of 1,056 reads** |
| **Syscall form** | **MISMATCH** | all 1,056 reads, all 32 stats |

The claim this supports, stated narrowly:

> For a synchronous, non-mmap, private-descriptor POSIX read workload, an strace
> capture imported by IOFlux and replayed in `asap` mode against the same
> filesystem reproduces the source's targets, offsets, transferred lengths,
> per-stream order, outstanding-I/O depth, and block-device read volume exactly.
> It does **not** reproduce the source's request *sizes* where the source read
> short, nor the *form* of the syscalls issued.

It supports nothing about timing fidelity, arrival pacing, repeatability, mmap,
async I/O, shared descriptors, writes, or whole-application reproduction.

## 2. The oracle held up (§9.4)

Coverage is measured against an independent denominator, not against "what the
importer recognized". All three legs agree, per worker, exactly:

| Instrument | Measured | Expected | |
|---|---|---|---|
| Analytic traversal → application journal | 296 entries, element-wise | 296 | exact |
| Kernel `syscr` (read syscalls) | 272 | 272 | exact |
| Kernel `rchar` (bytes via syscalls) | 68,000,000 | 68,000,000 | exact |
| Kernel `read_bytes` (device) | 68,026,368 | 68,026,368 | exact |

Analytic totals: 32 OPEN, 32 FSTAT, 1,088 READ (1,024 full + **32 short** + 32
EOF), 32 CLOSE = **1,184 syscall-visible operations**, 272,000,000 bytes.

`read_bytes` matching the page-rounded dataset size to the byte is also the
**verification** of the cold recipe: had any page survived
`posix_fadvise(DONTNEED)`, device bytes would have fallen short.

## 3. Acquisition decision (§9.1)

### Selected: strace 6.8

Measured against the fixture, `strace -f -tt -T -y -s 0`:

- **Coverage: 1,184 / 1,184 operations (100%), zero unexplained drops.** Exact
  agreement with the analytic oracle on operation count, kind, target path,
  offset, requested length, returned length, and per-stream order — all four
  workers, element by element.
- **Fields available per read:** fd *and* its path (`-y`), **requested count**,
  returned count, entry timestamp, duration. Outcomes and errors are visible.
- **Offsets:** a sequential `read(2)` carries none; reconstructed from a per-fd
  cursor. Verified correct against the analytic oracle (0 mismatches of 1,056).
- **Capture overhead: 1.21×–1.34×** measured across 4 trials (per-worker phase
  time from the application's own clock: ~88–90 ms untraced → ~107–126 ms
  traced). Notably *far* below the "commonly 10–100×" the importer's own
  limitation text warns about — because this workload is dominated by data
  movement, not syscall count. **That text is right to be pessimistic in general
  and should not be relaxed on the strength of one favourable fixture.**
- **Trace size:** 336 KB of strace log for 1,184 relevant syscalls (a further
  ~2,100 lines are interpreter I/O outside the declared scope).
- **Operational complexity:** low; one command, no build, no privileges.

One caveat measured, not assumed: strace splits most syscalls into
`<unfinished ...>` / `<... resumed>` halves under `-f`. Both the importer and the
reconciler rejoin them; the entry timestamp comes from the unfinished half and
the duration from the resumed half. Verified empirically that `-tt` is the
**entry** time (`clock_nanosleep` probe: t + dur = next line's t).

### Documented fallback: DFTracer — coverage and overhead NOT MEASURED

I could not measure it, and that is the finding:

- `pydftracer==2.0.4` from PyPI is a **pure-Python wheel with no native
  extension**. `dftracer.dftracer` is absent, so `common.profiler` silently binds
  a `NoOpProfiler` and records nothing.
- No `libdftracer_preload.so` anywhere in the distribution.
- The **sdist ships `python/` only** — no `CMakeLists.txt`, no C++ sources, no
  preload target. The POSIX interceptor lives in a separate C++ project requiring
  a from-source CMake build or spack.

So DFTracer's POSIX capture is **not obtainable from a pinned package install**
on this host. That is a real operational-complexity cost of exactly the kind
§9.1 asks to weigh, and it is why the spike stopped there rather than expanding
into a toolchain build.

Two structural facts also count against it for *this* fixture, independent of
obtainability — both already documented in IOFlux's own importer:

1. The 2.x hashed format **records byte counts but no offsets**; the importer
   reconstructs them from per-file sequential cursors. §10.1 requires offset
   agreement, so offsets would be inferred rather than observed.
2. Its POSIX event vocabulary has **no `stat`/`fstat`**. This fixture issues 32
   `fstat` calls, which would be invisible — an 2.7% coverage hole against the
   independent denominator before measuring anything.

### Rejected alternatives

| Candidate | Reason |
|---|---|
| DFTracer (PyPI) | No native interceptor obtainable; no offsets in 2.x; no stat/fstat vocabulary. Fallback, pending a source build. |
| eBPF / bpftrace | Not attempted. Needs privileges the fixture is designed not to need; revisit when capture must scale beyond ptrace overhead. |
| `LD_PRELOAD` shim (custom) | Not attempted. §9.1 and §24 say qualify an existing source before building capture. |
| PyTorch-native profiler | Out of scope: this fixture has no PyTorch, deliberately (FIXTURE.md §1). |

## 4. Finding 1 — source requested lengths are lost (MISMATCH)

> **Superseded — see §15.2.** Trace schema v2 added the requested-length field
> this section calls for. Re-running the fixture against it, all 1,056 reads now
> agree with the oracle on requested *and* returned length. The section is kept
> as the record of how the gap was found; its verdict is no longer current.

This is the finding the fixture was designed to expose, and it reproduced.

| | |
|---|---|
| Trace READ ops compared | 1,056 |
| `len` == source **returned** bytes | **1,056 / 1,056** |
| `len` == source **requested** bytes | 1,024 / 1,056 |
| Indistinguishable (full transfer) | 1,024 |
| **Discriminating reads** | **32** |

On the last read of every shard the source called `read(fd, buf, 262144)` and got
111,392 back. The trace records `len: 111392`. Replay therefore issues
`pread64(..., 111392, 8388608)` — a **request the size of the source's result**.

Root cause is structural, not a parser bug: `trace.Op` has a single `Len` field
(`pkg/trace/types.go:203`) and there is nowhere to put the requested count.
strace *does* capture it; the IR cannot hold it.

Consequences, stated plainly:

- The source's **request shape** is not reproduced for these ops. A storage
  system that behaves differently for a 256 KiB request than a 111 KiB one would
  be measured on the wrong request.
- The replay reports **zero short reads and a green result** — truthfully, since
  the replay itself read nothing short. The short read has been silently
  *converted into a full read of a smaller size*. That is exactly the class of
  false confidence plan.md §1 is about.

**Fixed in this change (disclosure, not the schema):** both importers now detect
the loss and count it. The strace import of this fixture reports:

```
imported with loss (op present, source property not representable):
  short_transfer_requested_len_dropped 32
```

32 — independently confirmed by the oracle. The count is written into the trace
header notes (`lossy: short_transfer_requested_len_dropped=32`) so it travels
with the trace permanently (§9.3), and the capture-limitations text now states
the loss, so it appears in **every saved result and every `ioflux report`**.

A real fix needs a requested-length field in trace schema v2 (roadmap Phase 1).
Deliberately not attempted here.

## 5. Finding 2 — syscall form differs (MISMATCH)

Observed by stracing the replay itself rather than trusting its self-report:

| Source issued | Replay issued |
|---|---|
| `read(fd, buf, n)` — sequential, advances the file cursor | `pread64(fd, buf, n, off)` — positional, no cursor |
| `fstat(fd)` — on the open descriptor | `newfstatat(rootfd, "shard_NNNN.bin", …)` — path-based |
| `lseek` | not replayed at all |

Offsets and lengths are **exact**; only the form differs. It comes from
`LocalFileEngine` using `File.ReadAt`/`WriteAt` (`pkg/engine/localfile/localfile.go`),
which is a deliberate design choice — it is what makes concurrent ops on one
handle safe without locking. But `Executor.ReplayEquivalence()` labels such a run
`syscall-level`, and for these ops that label is stronger than the evidence.

Why it can matter: positional and sequential reads take different kernel paths,
interact differently with per-file readahead state, and a workload whose
correctness depends on cursor position is not modelled at all.

**Fixed in this change (disclosure):** `LocalFileEngine` now reports the
substitution unconditionally as an engine limitation, so every report says:

```
! replay is positional: READ/WRITE are issued as pread/pwrite at explicit offsets
  and never move a file cursor, so a source workload's sequential read/write calls
  and its lseek calls are not reproduced in form (offsets and lengths are); STAT
  is issued against the target path, not as fstat on an open descriptor
```

Reclassifying `ReplayEquivalence` is Phase 1 semantic-domain work, not done here.

## 6. Declared drop — 32 EOF reads

The application issues one `read()` returning 0 per shard to terminate its loop.
The importer drops these (`eof_read`, 32 — 8 per worker). This is *declared and
explained*, not unexplained loss: 1,184 source ops → 1,152 trace ops, and the
difference is exactly the 32 EOF reads.

It is still a behavioural difference: replay performs **one fewer read per
read-to-EOF loop** than the application did. On a metadata-sensitive or
high-latency backend, 32 fewer round trips is not nothing. Now stated in the
capture-limitations text.

## 7. Execution: replay did what the trace said (match)

Replay's syscalls, observed by an independent strace and an independent parser:

| | |
|---|---|
| Ops the trace specified | 1,152 |
| Ops observed in the measured run | **1,152** |
| Operation-kind mismatches | 0 |
| Target-path mismatches | **0** |
| Offset mismatches | **0** |
| Requested-length mismatches (vs trace) | **0** |
| Returned-length mismatches | **0** |
| Per-stream order | **exact**, all 4 streams |
| Unattributed run-phase operations | **0** |

Two methodological points, because they are what make this number trustworthy:

- **Preparation was separated from the measured run, not ignored.** `assume-existing`
  stats and the cold recipe's sync/fadvise/close produce 197 further dataset
  syscalls. The boundary is the last `fadvise`, plus that target's trailing
  `close`. Both phases' counts are reported, so the split is auditable rather
  than assumed.
- **Per-stream order was recovered from targets, not thread ids.** Replay runs
  one goroutine per stream over a shared thread pool, so an OS tid identifies
  nothing. The fixture's static disjoint shard→worker assignment makes the shard
  index in a path name its stream unambiguously.

## 8. Outstanding-I/O depth (match, within run-to-run noise)

Reconstructed on both sides from `[entry, entry+duration]` spans:

| | Live | Replay |
|---|---|---|
| Read spans | 1,088 | 1,056 (the 32 EOF reads are absent) |
| **Peak depth** | **4** | **4** |
| Mean depth while busy | 2.71 | 2.82 |
| Time share at depth 1 / 2 / 3 / 4 | .10 / .29 / .41 / .20 | .09 / .25 / .41 / .25 |

Peak matches exactly. The mean-depth residual moved between −0.05 and +0.31
across three runs, i.e. it is **run-to-run noise around zero, not a measured
agreement to 0.1**. Stating a tolerance would require repeated trials, which this
single-trial reconciliation does not have.

Both sides are structurally capped at 4 for different reasons — live by having
4 sequential processes, replay by `--max-inflight 4`. The agreement therefore
confirms the cap was set correctly; it is **not** evidence that IOFlux *recovered*
the source's concurrency. §8.2 is explicit that `max-inflight` must not be
mistaken for recovered source concurrency, and it is not here.

## 9. Block-device work (exact match)

The strongest single result, because it shows the replay caused the same
*physical* work rather than merely finishing in a similar time:

| | Device bytes read |
|---|---|
| Expected (dataset, rounded up per file to 4 KiB pages) | 272,105,472 |
| **Live, cold recipe** (`/proc/self/io` `read_bytes`) | **272,105,472** |
| **Replay, cold recipe** (`RUSAGE_CHILDREN.ru_inblock`) | **272,105,472** |
| Live-vs-replay residual | **0 bytes** |
| Replay, warm recipe | **0** |

Two independent instruments, byte-for-byte agreement. The warm run's 0 also
double-confirms that the cold recipe's 272 MB was real device traffic.

## 10. Timing — reported, and explicitly not a fidelity claim

| | |
|---|---|
| Live read phase, untraced, cold (per worker) | 88 / 90 / 89 / 88 ms |
| Live read phase, under strace, cold | 125 / 126 / 113 / 113 ms |
| Replay, untraced, cold | **88.0 ms** (2.878 GiB/s) |
| Replay, untraced, warm | **12.9 ms** (19.629 GiB/s) |

Replay cold (88.0 ms) landing on top of the untraced live phase (≈89 ms) is
**a coincidence of two unrelated overheads, not evidence of timing fidelity.**
The live phase includes CPython per-read interpreter cost and the fixture's
per-block compute; the replay does neither, and instead pays Go scheduler and
bookkeeping cost. Two different overheads summing to a similar total proves
nothing. Do not cite it as timing agreement.

The cold/warm ratio (6.8×) is meaningful in a way the absolute numbers are not:
it confirms the recipes produce genuinely different cache states.

## 11. What could not be measured

Named rather than passed over in silence:

- **Inter-arrival distribution and issue-time error.** The source clock is
  ptrace-distorted (1.2–1.3× here) and replay ran in `asap`, so no arrival-time
  claim is defensible for this fixture. A timing envelope (§8.4) needs a capture
  method that does not perturb the timeline.
- **Think/compute interval reproduction.** The fixture is closed-loop; replay
  performs no application compute, so its pacing structure is not reproduced.
  This is the gap §8.3 predicts and it is real here.
- **Repeated-trial stability, effect size, confidence intervals.** Single-trial
  reconciliation. No stability claim is made.
- **Load-generator calibration** (§8.5): no null/spy-engine baseline was taken,
  so IOFlux's own scheduler overhead is unquantified. Replay CPU was 21.8 ms user
  / 140.3 ms sys against 95 ms wall on 28 CPUs — no evidence of saturation, but
  absence of evidence is not calibration.
- **The seeded controlled regression** (FIXTURE.md §10) is specified but not
  implemented; it needs the repeated-trial machinery of roadmap Phase 3.

## 12. A note on the method itself

The reconciler initially reported 16 path mismatches and a spurious extra
operation on the replay side. Both were bugs in **my comparison harness**, not in
IOFlux:

1. The harness keyed its fd→path table by `(pid, fd)`. strace's pid column is a
   *tid*, but a descriptor table is shared across a process's threads — so for a
   multi-threaded Go replay (goroutine opens on one thread, reads from another)
   the table went stale and resolved reads to whatever file another thread had
   open on that fd number. The `-y` decoration is kernel truth and now always
   wins. Note this failure mode is invisible on the single-threaded live side,
   i.e. it fails *only* where a false disagreement is most expensive.
2. The prepare/run boundary missed the cold recipe's trailing `close`.

Recording this because it is the point: a reconciliation that can only produce
agreement proves nothing. This one produced disagreements, three of which were my
own error and two of which were real properties of the tool.

## 13. Changes made to IOFlux

Disclosure only. No behaviour was altered to make numbers agree, and no schema
change was attempted.

| Change | Why |
|---|---|
| `importer.Report.Lossy` + `Builder.Note` / `LossySummary` | Distinguish "op absent" (skip) from "op present, source property lost". Merging them would either understate coverage or hide the loss. |
| strace + dftracer importers count `short_transfer_requested_len_dropped` | Finding 1: makes the loss countable. Cross-checked against the oracle (32). |
| Both importers' capture-limitations text | States the requested-length loss, the EOF-read drop, and positional replay, so it reaches every saved result. |
| dftracer capture-limitations text | Adds the missing-`stat`/`fstat` fact, found while scoring the candidate. |
| Header notes carry the lossy counts | §9.3: the information must travel with the trace, not live in one terminal. |
| `ioflux import` prints lossy counts | Separately from skips. |
| `LocalFileEngine` reports the positional-replay substitution | Finding 2. |

Tests added: 3 in `pkg/importer/strace`, 2 in `pkg/importer/dftracer`, 1 in
`pkg/engine/localfile` — each pinning a disclosure, including negative cases so
the loss counter stays meaningful when nothing was lost.

## 14. Where this leaves the milestone

Against §10.2's qualification goal:

| §10.2 requirement | Status |
|---|---|
| Relevant source ops accounted for vs declared oracle: 100% | **met** (1,184/1,184) |
| Unexplained drops: 0 | **met** (32 drops, all declared and explained) |
| Required per-lane / dependency order agreement: exact | **met** |
| Requested byte agreement within declared bound | **NOT met** — 32 of 1,056 reads (**now met** under schema v2 — §15.2) |
| Returned byte agreement within declared bound | **met** (exact) |
| Outstanding-I/O waveform within declared bound | **partly** — peak exact; no bound declared for the mean, and single-trial data cannot support one |
| Replay timing within supported envelope | **not assessed** — no envelope declared for this fixture |
| Unsupported semantics detected: none | **met** — no mmap, no async, no shared descriptors, no appends |

So: the premise holds for the narrow claim, and the machinery works well enough
to find its own limits. The blocking gap is a schema one — the trace IR cannot
represent a requested length — and it is Phase 1 work, now documented rather than
silent.

---

# 15. The §10 controlled regression: 256 KiB vs 64 KiB reads

FIXTURE.md §10 predeclared a treatment and a threshold before any result
existed. This is the first time it has been run. Reproduce with
`qualification/qual10.sh` (after `qualify.sh`, ≈5 s). Machine-readable evidence:
`$IOFLUX_QUAL_WORK/evidence/qual10*.json` (§15.7 lists which run is which).

**Headline: the predeclared expectation did not hold.** The tool found no
regression — not a small one, not one below threshold. The measured paired
difference is −1.6% (treatment *faster*) with a 95% interval spanning zero,
against a predicted ≥ +7% slowdown. The comparison was eligible, so this is a
result about the workload, not a refusal to measure.

## 15.1 Baseline re-established first

`qualify.sh` was re-run before anything else, because §10's comparison is only
worth running on a fixture that still reconciles. It does, and on two dimensions
it now reconciles *better* than §1 recorded:

| §10.1 dimension | §1 verdict | Now |
|---|---|---|
| Source **requested** lengths | MISMATCH (32 of 1,056) | **match** (0 of 1,056) |
| Source **returned** lengths | match | **match** (now scored separately) |
| Import adequacy vs oracle | match | **match**, requested dimension now included |
| Replay executed what the trace said | match | **match** (1,152 of 1,152, after §15.6) |
| Syscall form | MISMATCH | **MISMATCH** — unchanged, see §5 |

The import no longer reports `short_transfer_requested_len_dropped`. What it
reports instead, on this fixture:

```
imported 1152 op(s) across 4 stream(s), 32 target(s) via strace
skipped 86 op(s):
  eof_read                 32
  unparsed_line            54
```

No `imported with loss` block at all — the loss it used to declare no longer
occurs. The two skip counts:

- **`eof_read` 32** — unchanged and still declared (§6).
- **`unparsed_line` 54** — *not* dropped operations. These are orphan
  `<... resumed>` halves whose `<unfinished ...>` partner was an interpreter
  syscall (`/proc/<pid>/stat`, `/dev/null`) that `qualify.sh`'s dataset-scope
  grep removed while the filter's `resumed>` clause kept the tail. The count
  varies run to run (53, then 54) because it tracks interpreter activity, not
  fixture activity. That they consume no dataset operation is established
  independently, not asserted: capture-vs-oracle remains 1,184 of 1,184.

## 15.2 Finding 1 is resolved (schema v2)

The 32 discriminating reads now carry both counts:

```json
{"op":"READ","off":8388608,"len":262144,"ret":111392,...}
```

| | §4 (v1) | Now (v2) |
|---|---|---|
| `len` == source **requested** | 1,024 / 1,056 | **1,056 / 1,056** |
| `ret` == source **returned** | — (no field) | **1,056 / 1,056** |
| Replay's actual request, observed by strace | `pread64(…, 111392, …)` | **`pread64(…, 262144, …)` → 111392** |

Replay now issues the request the application actually made and requires the
short return the source got. §14's "requested byte agreement: NOT met" is met.

## 15.3 The transformation and its verification

```
ioflux transform split-reads --block 64KiB -o qual01-64k.ioflux qual01.ioflux
  1152 op(s) -> 4256 op(s); 259.40 MiB unchanged
```

Verified against the source trace rather than taken on trust — every property
§10 requires be identical, checked element-wise across all 32 targets:

| Property | Source | Treatment | |
|---|---|---|---|
| Total bytes | 272,000,000 | 272,000,000 | **identical** |
| Targets | 32 | 32 | **identical** (same ids, same names) |
| Extent covered, per target | `[0, 8500000)` | `[0, 8500000)` | **identical**, all 32 |
| Bytes transferred, per target | 8,500,000 | 8,500,000 | **identical**, all 32 |
| Per-stream OPEN/STAT/CLOSE order | — | — | **identical** |
| READ operations | 1,056 | 4,160 | 3.94× |
| All operations | 1,152 | 4,256 | — |
| Read sizes | 1,056 × 256 KiB | 4,128 × 64 KiB + 32 × 45,856 B | — |
| **Device bytes read, cold** | **272,105,472** | **272,105,472** | **identical** |

The header carries the ledger, and its `source_digest` is exactly
`sha256sum qual01.ioflux`:

```json
"transformations": [{"kind":"split-reads","params":{"block":"65536"},
  "source_digest":"sha256:f1c3f143c5bf04dd95854296210a59d53216e41f3f8ef5ff83a93ffec7670abd",
  "applied_utc":"2026-08-09T03:59:12Z", "note":"…divided into requests of at
  most that size over identical extents"}]
```

The experiment's own output resolves that digest and reports the arms as
related-by-declared-transformation rather than as two unrelated traces.

**One declared deviation from a literal 64 KiB reader, stated because it is not
nothing.** The 32 tail reads request **45,856 bytes**, not 65,536. A real reader
with a 64 KiB block would ask for 65,536 at offset 8,454,144 and receive 45,856
— a short read. `split-reads` instead divides the extent actually transferred
and emits full reads, so the trace's partial-read count goes 32 → **0**. This is
deliberate and documented (`pkg/transform/splitreads.go:25`), and it does not
affect extents, targets, or total bytes — but it means the treatment arm is not
byte-for-byte the request stream a 64 KiB reader would issue, and it converts a
short read into a full read of a smaller size, which is the shape §4 was about.
The stated reason ("asking for bytes the file does not have would make the
transformation demand a failure") is weaker now than when it was written: with
`ret` in schema v2 the trace can record an expected short return, so the
faithful split is representable and would not demand a failure. Left unchanged
here — changing the treatment after seeing the result is exactly what §10's
predeclaration exists to prevent. Flagged for the transform's own follow-up.

Affected: 32 of 4,160 reads (0.8%), 1,467,392 of 272,000,000 bytes (0.5%),
request shape only.

## 15.4 What was measured

Both arms read the **same dataset directory** under `--target-root` confinement,
`prepare: assume-existing`, `cache_mode: cold`, `max_inflight: 4`, 2 warmup
rounds discarded, 10 measured pairs interleaved with within-pair order drawn
from seed 42 (`baseline, baseline, treatment, treatment, treatment, baseline,
baseline, treatment, treatment, treatment`).

| duration (cold, wall-clock) | baseline (256 KiB) | treatment (64 KiB) |
|---|---|---|
| **median** | **91.4 ms** | **89.9 ms** |
| **CV** | **2.19%** | **1.75%** |
| mean | 91.6 ms | 90.5 ms |
| min … max | 88.9 … 94.8 ms | 88.9 … 92.7 ms |
| 95% CI of the median | 89.1 … 94.1 ms | 89.0 … 92.5 ms |
| throughput (median) | 2.773 GiB/s | 2.818 GiB/s |
| ops completed | 1,152 | 4,256 |
| bytes moved | 272,000,000 | 272,000,000 |
| valid trials / failed | 10 / 0 | 10 / 0 |

**Paired difference (treatment − baseline), 10 pairs:**

| | |
|---|---|
| median | **−1.45 ms (−1.6%)** |
| 95% CI | **−3.50 ms … +1.69 ms** |
| excludes zero | **no** |

**Eligibility: COMPARABLE.** Both arms cleared the policy — ≥ 10 valid trials
(10 and 10) and CV ≤ 5% (2.19% and 1.75%) — and the only thing differing between
the resolved configurations is the declared treatment variable (`trace`). The
comparison was not refused, so the numbers above are the result.

**The CV gate was not the binding constraint.** It was expected to be: the
replay path's trial-to-trial stability had never been measured, and only the
live phase (88/90/89/88 ms) suggested ~1% was achievable. Measured over 10
interleaved trials the replay path holds **1.75–2.19%** CV, comfortably inside
the 5% the fixture demanded. That is a new fact about this host and this path,
and it is what makes the null result interpretable rather than a measurement
failure.

## 15.5 Was the predeclared 7% expectation met? No.

| | |
|---|---|
| Predicted | cold-recipe throughput regression **≥ +7%** |
| Measured | **−1.6%** (treatment faster), 95% CI −3.8% … +1.8% |
| Verdict | **not met** — and the interval excludes +7% by a wide margin |

The interval includes zero, so these pairs do not establish a difference in
*either* direction. What they do establish is an upper bound: a +7% regression
is inconsistent with this data. The prediction is not merely unconfirmed; the
effect it predicted is outside what was observed.

Per FIXTURE.md's own instruction, the prediction has not been edited. Nothing
was retuned — threshold, trial count, and seed are as specified in §10, and the
reported run was not selected from a set: both runs performed are in §15.7.

**Why the mechanism did not bite**, from the run's own instrumentation rather
than speculation. §10 predicted per-op overhead and reduced readahead
effectiveness. Both were real and both were absorbed:

| | baseline | treatment | |
|---|---|---|---|
| READ operations | 1,056 | 4,160 | **3.94×** |
| Mean READ latency | 331.8 µs | 83.1 µs | **0.250×** |
| Product (total read service time) | — | — | **≈ 1.00** |
| CPU sys (median) | 129.6 ms | 122.4 ms | unchanged |
| CPU user (median) | 13.2 ms | 16.5 ms | +3.3 ms |
| Device bytes | 272,105,472 | 272,105,472 | **identical** |

Per-operation latency fell by almost exactly the factor the operation count
rose. The workload moves 272 MB in ~90 ms — **~3.0 GB/s**, at this NVMe device's
streaming rate — so wall-clock is bounded by device bandwidth, and the 3,104
extra syscalls cost user-space time (+3.3 ms) that is spread across 4 concurrent
streams on 28 CPUs and never reaches the critical path. Kernel time did not rise
at all: it is dominated by moving 272 MB, not by syscall entry. Readahead
effectiveness is unchanged in the only way this fixture can observe it — device
bytes are identical to the byte, so the smaller requests did not cause the block
layer to fetch more.

This is a claim about *this* fixture on *this* host: a bandwidth-bound
sequential read of a page-cache-evicted dataset on local NVMe. It is not a claim
that request size never matters.

## 15.6 A bug found and fixed: one trace op became two syscalls

Found because the re-run baseline did **not** reproduce §7, which is the whole
reason §7 exists. Reported plainly rather than worked around:

`replay_vs_trace` came back MISMATCH — 1,184 operations observed against 1,152
specified, with 852 offset mismatches cascading behind the divergence. The 32
extra were:

```
pread64(…/shard_0000.bin, "", 150752, 8500000) = 0
```

**Cause.** `LocalFileEngine.Read` used `os.File.ReadAt`, which is documented to
retry until the caller's buffer is full. Under schema v1 the engine asked for
111,392 bytes and one syscall filled it. Under v2 it correctly asks for the
source's 262,144, gets 111,392, and `ReadAt` issues a *second*, zero-byte read at
offset 8,500,000 — an offset the trace never contained — before returning EOF.
So the v2 fidelity fix introduced a new infidelity one layer down: one recorded
operation replayed as two syscalls.

It is worth being precise about how mild the symptom was and how badly it would
have read. The extra call transfers nothing and costs microseconds; the run
still reported `coverage: 1152/1152 ops (100.0%)` and no errors. Only an
independent strace of the replay showed it — the same method that found §5.

**Fix.** `readAtOnce` issues exactly one `pread(2)` and returns a partial result
as-is, letting the caller convert it to `engine.ErrShortRead`; EINTR is the only
retried condition. Applied to the buffered path and the O_DIRECT path; the
read-modify-write inside `alignedWriteAt` still wants a full fill and keeps
`ReadAt`. Non-unix builds keep `ReadAt` behind a documented fallback.

Verified at the syscall level, not by self-report — dataset `pread64` calls in
the traced replay:

| | before | after |
|---|---|---|
| Total | 1,088 | **1,056** (= trace READ ops, exactly) |
| Returning 262,144 | 1,024 | 1,024 |
| Returning 111,392 | 32 | 32 |
| **Returning 0** | **32** | **0** |

`replay_vs_trace` returns to 1,152 of 1,152 with zero residual on every
dimension, restoring §7. Three tests pin it (`pread_unix_test.go`), counting
syscalls through an injectable `pread` rather than inferring them from byte
counts — including the full-read and at-EOF cases, so the assertion keeps its
meaning when nothing is short.

## 15.7 Two harness corrections, in the spirit of §12

Neither is an IOFlux defect; both produced *false* disagreements, which is the
failure mode §12 warns about.

1. **`reconcile.py` was stale against schema v2.** It projected trace reads as
   `requested = len, returned = len` — correct when there was one length field,
   wrong once `ret` exists. It therefore reported the 32 short reads as
   returning 262,144 and flagged both `trace_vs_oracle` and `replay_vs_trace` as
   MISMATCH while the trace, the oracle, and the replay all agreed on 111,392.
   Now reads `ret` (absent means a full transfer), scores the requested
   dimension it previously had to skip, and reports
   `source_returned_length_preserved` alongside the requested one.
2. **`TrialPolicy` serialized as Go field names.** `MinValidTrials` /
   `MaxCVPercent` in a results schema that is snake_case everywhere else. Now
   `min_valid_trials` / `max_cv_percent`. Cosmetic, but these files are the
   durable evidence artifact.

The second required rebuilding the binary mid-way, and `qual10.sh` was then
executed once more to verify it reproduces end to end. So the experiment ran
**three times**, every time with the identical config and seed. All three are
reported, because all three were run:

| run | baseline median (CV) | treatment median (CV) | paired median | 95% CI | verdict |
|---|---|---|---|---|---|
| 1 (pre-fix binary) | 91.0 ms (2.14%) | 90.5 ms (2.70%) | −0.44 ms (−0.5%) | −4.45 … +1.89 ms | comparable, includes zero |
| **2 (reported above)** | **91.4 ms (2.19%)** | **89.9 ms (1.75%)** | **−1.45 ms (−1.6%)** | **−3.50 … +1.69 ms** | comparable, includes zero |
| 3 (`qual10.sh` check) | 89.5 ms (2.13%) | 89.1 ms (2.47%) | −0.18 ms (−0.2%) | −4.28 … +2.35 ms | comparable, includes zero |

Three independent 10-pair runs: every arm's CV between 1.75% and 2.70%, every
paired interval containing zero, every point estimate between −0.2% and −1.6%,
and not one anywhere near +7%. Run 2 is quoted above because it came from the
committed binary, not because it was preferred — run 3 is the weakest of the
three for the treatment and changes no conclusion.

Evidence files: `evidence/qual10-run1.json`, `evidence/qual10.json` (run 2),
`evidence/qual10-run3.json`.

## 15.8 What this does not measure

Named rather than passed over, as §11:

- **Why the treatment is faster, if it is.** All three runs put the point
  estimate slightly negative (−0.5%, −1.6%, −0.2%) and all three intervals
  include zero. That is consistent with no effect, and with a small real effect
  this design cannot resolve. Nothing here establishes that 64 KiB reads are
  *faster*, and the −1.6% should not be quoted as if it were an effect.
- **Sensitivity.** §10 wanted an effect "small enough to test sensitivity", and
  with no effect to detect, the design's *power* is untested. These 10 pairs
  bound the effect at roughly ±3.5 ms (±3.8%); whether the machinery would
  detect a genuine 7% regression is **not established by this run** — it would
  need a treatment with a known-nonzero effect. That is the single most
  important thing §10 set out to test and it remains open.
- **Whether the fixture's prediction is wrong in general.** This measures one
  host, one NVMe device, one filesystem, at ~3 GB/s with 4 concurrent streams
  and no application compute. On a device where per-op cost is a larger share of
  service time — network storage, a slower disk, a single stream — the predicted
  regression may well appear. §10's expectation is falsified *here*; it is not
  shown to be wrong everywhere.
- **The application-level equivalent.** The replay performs no CPython
  interpreter work and no per-block compute, so this does not predict what the
  live fixture would do at a 64 KiB block size. The closed-loop gap of §11
  applies unchanged.
- **The 45,856-byte tail requests** (§15.3) are a known deviation from a literal
  64 KiB reader, unmeasured in effect. At 0.5% of bytes they cannot plausibly
  account for a −1.6% difference, but that is an argument, not a measurement.
- **Anything about warm-cache behaviour**, or about request size under memory
  pressure — the cold recipe and FIXTURE.md §4's caveat both still apply.

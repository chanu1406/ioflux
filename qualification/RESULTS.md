# qual-01: first live-versus-replay comparison

Fixture: [FIXTURE.md](FIXTURE.md). Reproduce with `qualification/qualify.sh`
(≈40 s end to end). Machine-readable evidence:
`$IOFLUX_QUAL_WORK/evidence/reconcile-cold.json`.

This is the first time anyone has checked whether an IOFlux replay reproduces the
workload it claims to replay. Nothing below was tuned to agree; every comparison
reports its residual, and two dimensions came out **negative**.

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
| Requested byte agreement within declared bound | **NOT met** — 32 of 1,056 reads |
| Returned byte agreement within declared bound | **met** (exact) |
| Outstanding-I/O waveform within declared bound | **partly** — peak exact; no bound declared for the mean, and single-trial data cannot support one |
| Replay timing within supported envelope | **not assessed** — no envelope declared for this fixture |
| Unsupported semantics detected: none | **met** — no mmap, no async, no shared descriptors, no appends |

So: the premise holds for the narrow claim, and the machinery works well enough
to find its own limits. The blocking gap is a schema one — the trace IR cannot
represent a requested length — and it is Phase 1 work, now documented rather than
silent.

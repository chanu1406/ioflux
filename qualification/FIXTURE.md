# Qualification fixture `qual-01`

The one concrete fixture required by plan.md §5 ("Required Fixture Definition")
and §24 ("First Qualification Fixture"). Everything below is pinned. Changing
any pinned value produces a different fixture and invalidates comparison against
archived `qual-01` evidence.

Run it with `qualification/qualify.sh`. Results: [RESULTS.md](RESULTS.md).

---

## 1. Application and framework version

A WebDataset-shaped sharded reader: `qualification/fixture/reader.py`.

| | |
|---|---|
| Interpreter | CPython **3.12.3** (`main, Mar 23 2026, 19:04:32) [GCC 13.3.0]`) |
| Framework | CPython standard library only — `os`, `multiprocessing` (fork) |
| Third-party deps | **none** |
| Worker model | 4 × `multiprocessing.Process`, fork start method (pinned explicitly) |
| I/O calls | `os.open` / `os.fstat` / `os.read` / `os.close` on raw descriptors |

Two properties are deliberate and load-bearing:

- **Raw descriptors, no buffered file objects.** `os.read(fd, n)` is exactly one
  `read(2)`, which makes the application-level journal 1:1 with the syscall
  stream. Buffered I/O would hide syscalls from application-level accounting and
  make oracle leg 2 (below) impossible.
- **`Process`, not `Pool`.** A `Pool` worker reads its task from a pipe, which
  would place non-dataset `read(2)` calls inside the measured window. `Process`
  args arrive across `fork`, so the window contains dataset I/O only.

### Why not PyTorch

plan.md §5 offers "a fixed PyTorch WebDataset-style reader" as *an example*. It
is rejected as the **first** fixture, with reasons:

1. A PyTorch `DataLoader` moves worker→parent tensors over file-backed **mmap**
   in `/dev/shm` plus a shared-memory allocator. §9.2 requires "no mmap" and
   §10.3 makes mmap detection an automatic qualification **failure**. It cannot
   be the fixture that first establishes the method.
2. ~2.5 GB of install against a "small enough to iterate on in minutes"
   requirement.

PyTorch belongs to a later fixture, once the mmap semantic domain exists
(roadmap Phase 5). Nothing here assumes it never will.

## 2. Dataset format, target count, total size

| | |
|---|---|
| Layout | 32 flat shard files, `shard_0000.bin` … `shard_0031.bin` |
| Shard size | **8,500,000 B** each (uniform) |
| Total | **272,000,000 B** (259.40 MiB) |
| Content | seeded-random per shard (`random.Random(1000+i).randbytes`), fsynced |

Content is pseudorandom rather than zeros so a compressing or deduplicating
filesystem cannot make the read workload cheaper than it looks. Each shard is
fsynced at creation so its pages are clean, which is what lets the cold recipe's
`posix_fadvise(DONTNEED)` evict them.

**The shard size is deliberately not a multiple of the block size.** 8,500,000 =
32 × 262,144 + 111,392. The final read of every shard therefore requests 262,144
and returns 111,392. This is the case that *exercises* the requested-vs-returned
dimension of §10.1 instead of hiding it; an aligned size would make every read
full-length and the dimension unmeasurable. It is the single most informative
choice in the fixture — see RESULTS.md §4.

## 3. Worker count and assignment

4 workers. Assignment is static and deterministic: worker `w` owns
`shards[w::4]`, i.e. 8 shards each, read in ascending shard order.

Disjoint static assignment is what makes the shard index in a path identify its
stream unambiguously, which the reconciler relies on to recover per-stream
ordering from the replay side (where goroutines migrate across OS threads and a
thread id identifies nothing).

## 4. Does the dataset fit in client memory

**Yes, easily** — 259.40 MiB against 31.9 GiB of RAM (≈26 GiB free).

Stated plainly because it matters: cache state in this fixture comes from an
**explicit recipe**, never from the dataset being too large to cache. Building a
dataset that genuinely exceeds page cache on this host would need >30 GB and
violate the minutes-to-iterate constraint. Consequences:

- A `cold` result is only as good as the recipe (`fsync` + `posix_fadvise(
  POSIX_FADV_DONTNEED)` per shard), and the recipe is verified after the fact
  from `/proc/<pid>/io` `read_bytes` — see oracle leg 3.
- This fixture cannot support any claim about page-cache **pressure** or
  eviction behaviour under memory contention.

## 5. Compute / think time between I/O operations

Per block: a bounded, deterministic "decode" stand-in — the sum of every 64th
byte of the block just read (4,096 additions per 256 KiB block).

The causal model is therefore **closed-loop**: read *n+1* is issued only after
read *n* completes and its compute finishes. Declared here because §8.3 requires
the fixture to state whether it is open-loop, closed-loop, or mixed, and §11.1
forbids describing results from one model as if they proved the other.

There is no inter-batch sleep and no synthetic delay.

## 6. Filesystem, mount, and storage target

| | |
|---|---|
| Device | `/dev/nvme0n1p6` |
| Filesystem | **ext4**, `rw,relatime` |
| Kernel | Linux **6.17.0-35-generic** (Ubuntu 24.04 base), x86_64 |
| CPU | 28 logical CPUs |
| RAM | 31.9 GiB |
| Dataset path | `$IOFLUX_QUAL_WORK/dataset` (default `/tmp/ioflux-qual01/dataset`) |

Replay reads **the same directory** the live run read. The fixture is read-only,
so this removes dataset materialization as a confound entirely: live and replay
touch identical bytes in identical blocks on identical extents. The engine is
confined to that directory with `--target-root`.

## 7. Acquisition candidates evaluated

Evaluated against this fixture by `qualification/spike.sh`:

1. **strace 6.8** — `-f -tt -T -y -s 0 -e trace=<file syscalls>`, via
   `ioflux import strace`.
2. **DFTracer** (`pydftracer` 2.0.4 from PyPI) — via `ioflux import dftracer`.

Outcome, evidence, and the rejected-alternative record are in RESULTS.md §3.

## 8. Independent measurement oracle

Three legs. **None of them is the IOFlux importer** — §9.4 explicitly forbids
defining coverage as "all operations the importer happened to recognize".

| Leg | Instrument | Establishes | Independent of |
|---|---|---|---|
| 1 | **Analytic expected traversal** (`oracle.py`) — derived from the pinned constants, observes nothing | op count, kinds, order, offsets, requested & returned lengths | everything; computed before any run |
| 2 | **Application-level journal** (`reader.py`) — the reader logs every read it issues | op count, order, per-op offset/requested/returned | strace, the importer, the kernel counters |
| 3 | **Kernel counters** — `/proc/self/io` `syscr` / `rchar` / `read_bytes` deltas around the read phase, per worker | read-syscall count, bytes read via syscalls, bytes fetched from the block device | strace, the importer, the application |

Leg 3's sampling cost is measured, not assumed: three samples are taken (two
back-to-back before the phase, one after), because reading `/proc/self/io` is
itself `read(2)` traffic, and the file grows as its counters gain digits — so the
snapshot's own byte length is recorded and subtracted rather than estimated.

Leg 3 also **verifies the cache recipe**: expected device bytes under a cold
recipe are `shards_per_worker × roundup(8,500,000, 4096)` = 68,026,368, and if
pages had survived eviction `read_bytes` would fall below that.

No leg is assumed perfect. RESULTS.md states which leg establishes each claim.

## 9. Primary performance metric

**Wall-clock duration of the replayed read phase** under the cold recipe
(equivalently GiB/s over 272,000,000 B), with median and CV over repeated
trials.

Chosen because it is the quantity the fixture's workload actually produces, and
because per-op latency percentiles cannot serve as a decision metric here —
§11.2 is explicit that a p99 operation latency is not the p99 of trial outcomes.

Secondary/diagnostic only: per-op latency percentiles, CPU user/sys, outstanding-
I/O depth, backlog events.

## 10. Controlled regression IOFlux must detect

**Not yet implemented** — it belongs to the milestone this work precedes
(roadmap Phase 3), and repeated-trial machinery does not exist yet. The
specification is fixed here so it cannot be chosen after seeing results:

> **Treatment:** re-run the identical trace against the identical dataset with
> the replay's per-op read size reduced from 256 KiB to 64 KiB by an explicit
> declared trace transformation (4× more, 4× smaller reads; identical total
> bytes, identical targets, identical offsets covered).
>
> **Predeclared expectation:** a cold-recipe throughput regression, driven by
> per-op overhead and reduced readahead effectiveness, large enough to clear a
> 7% threshold while remaining small enough to test sensitivity.
>
> **Decision metric:** median cold-recipe wall-clock duration over ≥10 paired,
> interleaved trials, with CV ≤ 5% required for the comparison to be eligible.

Chosen over an injected artificial delay because it is a request-shape change of
the kind teams actually make, and over a mount-option change because it would
not be reproducible on an arbitrary reviewer's host.

## 11. Declared scope of the resulting trace

The capture is filtered to the dataset directory. This is a **declared scope
reduction**, not a loss: the Python interpreter's own `.so` / `.pyc` / locale
reads are outside the claim. The claim this fixture supports is about the
**dataset read path**, not about whole-application reproduction.

The filter is verified rather than trusted: reconciliation section B parses the
**unfiltered** capture with an independent parser, so anything the filter dropped
that the fixture actually did would appear as missing coverage.

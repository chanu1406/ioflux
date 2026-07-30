"""qual-01 fixture application: a WebDataset-shaped sharded shard reader.

Synchronous, non-mmap, private-descriptor POSIX reads. Each of WORKERS forked
worker processes owns a static, deterministic slice of the shard list and reads
its shards end to end in BLOCK_BYTES blocks, doing a bounded per-block
"decode" so the workload is closed-loop rather than a pure syscall generator.

Two properties make this fixture usable as its own oracle:

  1. Reads go through os.open/os.read on a raw descriptor, so the
     application-level journal below is exactly 1:1 with read(2). A buffered
     file object would hide syscalls from application-level accounting.

  2. /proc/self/io is sampled around the read phase, giving a kernel-maintained
     operation and byte count that is independent of both the journal and any
     external tracer. Three samples are taken (two back-to-back before the
     phase, one after) so the cost of sampling is measured rather than assumed:
     reading /proc/self/io is itself read(2) traffic, and s1-s0 calibrates it
     out of s2-s1.

Workers are multiprocessing.Process, not Pool: a Pool worker reads its task
from a pipe, which would put non-dataset read(2) calls inside the measured
window. Process args arrive across fork, so the window stays clean.

Usage: reader.py <dataset-dir> <out-dir> [workers]
"""

import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import spec  # noqa: E402

IO_FIELDS = ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes")


def io_snapshot() -> dict:
    """Read /proc/self/io with exactly one read(2) so sampling cost is stable.

    `_nbytes` records how many bytes this snapshot's own read(2) returned. The
    proc file grows as its counters gain digits, so a snapshot cannot be
    calibrated out of a later rchar delta by assuming all snapshots cost the
    same; the actual length is needed to make the byte accounting exact.
    """
    fd = os.open("/proc/self/io", os.O_RDONLY)
    try:
        raw = os.read(fd, 4096)
    finally:
        os.close(fd)
    out = {"_nbytes": len(raw)}
    for line in raw.decode().splitlines():
        key, _, val = line.partition(":")
        if key in IO_FIELDS:
            out[key] = int(val.strip())
    return out


def worker(worker_idx: int, dataset_dir: str, out_dir: str) -> None:
    paths = [
        os.path.join(dataset_dir, spec.shard_name(i))
        for i in spec.shards_for_worker(worker_idx)
    ]
    journal = []
    checksum = 0
    stride = spec.COMPUTE_STRIDE
    block = spec.BLOCK_BYTES

    # ---- measured window begins; nothing below reads anything but the dataset
    s0 = io_snapshot()
    s1 = io_snapshot()
    t0 = time.monotonic_ns()

    for path in paths:
        fd = os.open(path, os.O_RDONLY)
        journal.append({"op": "OPEN", "path": path})
        st = os.fstat(fd)
        journal.append({"op": "FSTAT", "path": path, "size": st.st_size})
        off = 0
        while True:
            buf = os.read(fd, block)
            journal.append(
                {
                    "op": "READ",
                    "path": path,
                    "off": off,
                    "requested": block,
                    "returned": len(buf),
                }
            )
            if not buf:
                break
            off += len(buf)
            checksum = (checksum + sum(buf[::stride])) & 0xFFFFFFFFFFFFFFFF
        os.close(fd)
        journal.append({"op": "CLOSE", "path": path})

    t1 = time.monotonic_ns()
    s2 = io_snapshot()
    # ---- measured window ends

    out = {
        "fixture": spec.FIXTURE_ID,
        "worker": worker_idx,
        "pid": os.getpid(),
        "phase_ns": t1 - t0,
        "checksum": checksum,
        "io_samples": {"s0": s0, "s1": s1, "s2": s2},
        "journal": journal,
    }
    dest = Path(out_dir) / f"worker-{worker_idx}.json"
    tmp = dest.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(out))
    os.replace(tmp, dest)


def main() -> None:
    if len(sys.argv) not in (3, 4):
        raise SystemExit("usage: reader.py <dataset-dir> <out-dir> [workers]")
    dataset_dir = os.path.abspath(sys.argv[1])
    out_dir = os.path.abspath(sys.argv[2])
    workers = int(sys.argv[3]) if len(sys.argv) == 4 else spec.WORKERS
    if workers != spec.WORKERS:
        raise SystemExit(
            f"worker count {workers} differs from the pinned fixture value "
            f"{spec.WORKERS}; the shard assignment is part of the contract"
        )
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    ctx = mp.get_context("fork")
    procs = [
        ctx.Process(target=worker, args=(w, dataset_dir, out_dir))
        for w in range(workers)
    ]
    wall0 = time.monotonic_ns()
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    wall1 = time.monotonic_ns()

    failed = [(w, p.exitcode) for w, p in enumerate(procs) if p.exitcode != 0]
    if failed:
        raise SystemExit(f"worker failures (worker, exitcode): {failed}")

    manifest = {
        "fixture": spec.FIXTURE_ID,
        "dataset_dir": dataset_dir,
        "workers": workers,
        "parent_pid": os.getpid(),
        "wall_ns": wall1 - wall0,
        "spec": {
            "shards": spec.SHARDS,
            "shard_bytes": spec.SHARD_BYTES,
            "block_bytes": spec.BLOCK_BYTES,
            "compute_stride": spec.COMPUTE_STRIDE,
        },
    }
    (Path(out_dir) / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"read phase wall: {(wall1 - wall0) / 1e6:.1f} ms ({workers} workers)")


if __name__ == "__main__":
    main()

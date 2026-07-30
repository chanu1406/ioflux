"""Cache-state recipes for the live leg of qual-01.

These mirror what pkg/cache does for the replay leg (sync, then
posix_fadvise(POSIX_FADV_DONTNEED)) so the live and replay legs are subject to
the same recipe rather than to two different notions of "cold".

Per §15.1 a recipe is not a guarantee. What this does and does not cover:
  does    -- drops this file's clean page-cache pages
  does not-- device, controller, or filesystem-metadata caches
Verification is not done here; it is done after the fact from the read_bytes
delta in /proc/self/io, which shows whether the reads actually reached the
block device.

Usage: cachectl.py cold|warm <dataset-dir>
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import spec  # noqa: E402


def shard_paths(root: Path):
    return [root / spec.shard_name(i) for i in range(spec.SHARDS)]


def cold(root: Path) -> None:
    for path in shard_paths(root):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def warm(root: Path) -> None:
    buf_size = 1 << 20
    for path in shard_paths(root):
        fd = os.open(path, os.O_RDONLY)
        try:
            while os.read(fd, buf_size):
                pass
        finally:
            os.close(fd)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: cachectl.py cold|warm <dataset-dir>")
    mode, root = sys.argv[1], Path(sys.argv[2])
    if mode == "cold":
        cold(root)
    elif mode == "warm":
        warm(root)
    else:
        raise SystemExit(f"unknown cache mode {mode!r}")
    print(f"cache recipe {mode} applied to {spec.SHARDS} shards in {root}")


if __name__ == "__main__":
    main()

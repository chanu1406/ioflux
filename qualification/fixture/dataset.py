"""Create the qual-01 dataset deterministically.

Content is seeded-random (not zeros) so a compressing or deduplicating
filesystem cannot make the read workload cheaper than it looks. Each shard is
fsynced so its pages are clean, which is what makes the cold recipe's
posix_fadvise(DONTNEED) able to evict them.

Usage: dataset.py <dataset-dir>
"""

import os
import random
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import spec  # noqa: E402

CHUNK = 1 << 20


def build(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for i in range(spec.SHARDS):
        path = root / spec.shard_name(i)
        base = random.Random(1000 + i).randbytes(CHUNK)
        remaining = spec.SHARD_BYTES
        with open(path, "wb") as f:
            while remaining > 0:
                n = min(CHUNK, remaining)
                f.write(base[:n])
                remaining -= n
            f.flush()
            os.fsync(f.fileno())
        actual = path.stat().st_size
        if actual != spec.SHARD_BYTES:
            raise SystemExit(f"{path}: wrote {actual} bytes, want {spec.SHARD_BYTES}")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: dataset.py <dataset-dir>")
    root = Path(sys.argv[1])
    build(root)
    print(
        f"dataset: {spec.SHARDS} shards x {spec.SHARD_BYTES} B "
        f"= {spec.DATASET_TOTAL_BYTES} B at {root}"
    )


if __name__ == "__main__":
    main()

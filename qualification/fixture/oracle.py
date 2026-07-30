"""Analytic expected traversal for qual-01 -- oracle leg 1.

This is derived purely from the pinned fixture constants. It observes nothing:
no tracer, no application journal, no importer. It is the denominator §9.4
requires, computed before anything runs.

Emitted entries use exactly the shape reader.py journals, so the reconciler can
compare the two sequences element by element.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import spec  # noqa: E402


def expected_worker_sequence(worker: int, dataset_dir: str):
    """The complete syscall-visible operation sequence worker `worker` must issue."""
    seq = []
    for i in spec.shards_for_worker(worker):
        path = os.path.join(dataset_dir, spec.shard_name(i))
        seq.append({"op": "OPEN", "path": path})
        seq.append({"op": "FSTAT", "path": path, "size": spec.SHARD_BYTES})
        off = 0
        while off < spec.SHARD_BYTES:
            returned = min(spec.BLOCK_BYTES, spec.SHARD_BYTES - off)
            seq.append(
                {
                    "op": "READ",
                    "path": path,
                    "off": off,
                    "requested": spec.BLOCK_BYTES,
                    "returned": returned,
                }
            )
            off += returned
        # The loop-terminating read: still a real read(2) the application issued.
        seq.append(
            {
                "op": "READ",
                "path": path,
                "off": spec.SHARD_BYTES,
                "requested": spec.BLOCK_BYTES,
                "returned": 0,
            }
        )
        seq.append({"op": "CLOSE", "path": path})
    return seq


def expected_all(dataset_dir: str):
    return {w: expected_worker_sequence(w, dataset_dir) for w in range(spec.WORKERS)}


def totals(dataset_dir: str = "/x"):
    """Aggregate expected counts, independent of dataset location."""
    per_op = {}
    read_syscalls = 0
    read_bytes = 0
    eof_reads = 0
    short_reads = 0
    for w in range(spec.WORKERS):
        for e in expected_worker_sequence(w, dataset_dir):
            per_op[e["op"]] = per_op.get(e["op"], 0) + 1
            if e["op"] == "READ":
                read_syscalls += 1
                read_bytes += e["returned"]
                if e["returned"] == 0:
                    eof_reads += 1
                elif e["returned"] < e["requested"]:
                    short_reads += 1
    return {
        "per_op": per_op,
        "read_syscalls": read_syscalls,
        "read_bytes": read_bytes,
        "eof_reads": eof_reads,
        "short_reads": short_reads,
        "data_reads": read_syscalls - eof_reads,
    }


def main() -> None:
    t = totals()
    print(f"fixture {spec.FIXTURE_ID} analytic oracle")
    for op, n in sorted(t["per_op"].items()):
        print(f"  {op:6s} {n}")
    print(f"  read syscalls        {t['read_syscalls']}")
    print(f"    of which full      {t['data_reads'] - t['short_reads']}")
    print(f"    of which short     {t['short_reads']}")
    print(f"    of which EOF (0 B) {t['eof_reads']}")
    print(f"  read bytes           {t['read_bytes']}")


if __name__ == "__main__":
    main()

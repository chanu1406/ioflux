"""Measure a child process's block-device read volume -- oracle leg 3, replay side.

The live legs sample /proc/self/io inside each worker. A replay is a separate
process that has exited by the time we would like to read its counters, so its
device traffic is taken from getrusage(RUSAGE_CHILDREN).ru_inblock instead, which
Linux populates from the same task I/O accounting (in 512-byte units).

This is what makes the live-versus-replay block-device comparison of §10.1
possible at all: without it, replay-side device work is only inferable from
timing.

Usage: devbytes.py <out.json> -- <command> [args...]
"""

import json
import os
import resource
import subprocess
import sys
import time


def main() -> None:
    if "--" not in sys.argv:
        raise SystemExit("usage: devbytes.py <out.json> -- <command> [args...]")
    split = sys.argv.index("--")
    out_path = sys.argv[1]
    cmd = sys.argv[split + 1 :]
    if not cmd:
        raise SystemExit("no command given")

    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.monotonic_ns()
    proc = subprocess.run(cmd)
    t1 = time.monotonic_ns()
    after = resource.getrusage(resource.RUSAGE_CHILDREN)

    result = {
        "command": cmd,
        "exit_code": proc.returncode,
        "wall_ns": t1 - t0,
        "ru_inblock_delta": after.ru_inblock - before.ru_inblock,
        "device_read_bytes": (after.ru_inblock - before.ru_inblock) * 512,
        "ru_oublock_delta": after.ru_oublock - before.ru_oublock,
        "user_ns": int((after.ru_utime - before.ru_utime) * 1e9),
        "sys_ns": int((after.ru_stime - before.ru_stime) * 1e9),
    }
    with open(out_path, "w") as fh:
        json.dump(result, fh, indent=2)
    print(
        f"device read: {result['device_read_bytes']} B "
        f"({result['ru_inblock_delta']} x 512 B blocks) in {result['wall_ns'] / 1e6:.1f} ms"
    )
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()

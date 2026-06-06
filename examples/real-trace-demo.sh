#!/usr/bin/env bash
# IOFlux real-trace demo: capture a live DataLoader-shaped read workload with
# strace, import it, and replay the SAME trace against the local filesystem (and
# optionally S3/MinIO), inspecting coverage, concurrency, and replay fidelity.
#
# This is the hardened version of the "capture -> import -> replay" recipe. Two
# fixes make it actually run on a real Python process:
#
#   1. strace records ALL file I/O the interpreter makes (~160 .so/.pyc/locale
#      files), not just dataset shards. Mapping only the dataset prefix then
#      rejects every other target at PREPARE ("matched no rule"); adding
#      --allow-passthrough instead makes materialize-from-source try to WRITE
#      stand-ins over /usr/lib and /etc (EACCES, or destructive as root).
#      FIX: capture with `strace -y` (decorates each fd with its path) and keep
#      only lines naming the dataset -> a clean, dataset-only trace.
#
#   2. Invoke the reader with an ABSOLUTE dataset path so OPEN records absolute
#      target names that the target-map prefix matches. Filter on the shard stem
#      ('dataset/shard_') PLUS all 'resumed>' lines so interleaved
#      <unfinished>/<resumed> syscall halves (from -f) are both kept; the
#      importer safely skips any orphaned resume.
#
# Requirements: go, python3, strace. The optional S3 leg needs Docker + the
# bundled MinIO fixture (test/integration/docker-compose.yml).
#
# Usage:
#   examples/real-trace-demo.sh           # local legs only
#   S3=1 examples/real-trace-demo.sh      # also replay against MinIO (start the
#                                          # fixture first; see the S3 block below)
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
BIN="$REPO_ROOT/bin/ioflux"
WORK=${IOFLUX_DEMO_WORK:-/tmp/ioflux-real}
WORKERS=4

rm -rf "$WORK"; mkdir -p "$WORK/dataset" "$WORK/replay"
cd "$WORK"

echo "== build =="
( cd "$REPO_ROOT" && go build -o bin/ioflux ./cmd/ioflux )

echo "== create dataset (16 x 16MiB) =="
python3 - "$WORK/dataset" <<'PY'
import sys
from pathlib import Path
root = Path(sys.argv[1])
for i in range(16):
    with (root / f"shard_{i:04d}.bin").open("wb") as f:
        chunk = bytes([i % 251]) * (1024 * 1024)
        for _ in range(16):
            f.write(chunk)
PY

cat > read_shards.py <<'PY'
import multiprocessing as mp, os, sys
from pathlib import Path
root = Path(sys.argv[1]); workers = int(sys.argv[2]); block = 512 * 1024
shards = sorted(root.glob("shard_*.bin"))
def read_file(path):
    total = 0
    with open(path, "rb", buffering=0) as f:
        while (data := os.read(f.fileno(), block)):
            total += len(data)
    return total
def worker(paths): return sum(read_file(p) for p in paths)
chunks = [shards[i::workers] for i in range(workers)]
with mp.Pool(workers) as pool:
    print(sum(pool.map(worker, chunks)))
PY

echo "== capture (strace -y, ABSOLUTE dataset path) =="
strace -f -tt -T -s 0 -y \
  -e trace=open,openat,openat2,read,pread64,close,lseek,fstat,stat,statx,newfstatat \
  -o capture.strace \
  python3 read_shards.py "$WORK/dataset" "$WORKERS"

echo "== footgun fix: keep dataset I/O only (both split halves) =="
grep -E 'dataset/shard_|resumed>' capture.strace > capture.dataset.strace
echo "   raw $(wc -l < capture.strace) lines -> dataset-only $(wc -l < capture.dataset.strace) lines"

echo "== import + validate =="
"$BIN" import strace -o imported.ioflux capture.dataset.strace
"$BIN" validate imported.ioflux

printf 'target_rewrite:\n  - from: "%s/dataset/"\n    to: "%s/replay/"\n' "$WORK" "$WORK" > map.yaml

echo "== LOCAL . timeline . cold =="
"$BIN" run --trace imported.ioflux --engine local --target-map map.yaml \
  --prepare materialize-from-source --source-root / --cache-mode cold --mode timeline \
  --max-inflight "$WORKERS" -o local-timeline.json --csv results.csv || true
"$BIN" report local-timeline.json

echo "== LOCAL . asap . warm =="
"$BIN" run --trace imported.ioflux --engine local --target-map map.yaml \
  --prepare assume-existing --cache-mode warm --mode asap \
  --max-inflight "$WORKERS" -o local-asap.json --csv results.csv
"$BIN" report local-asap.json

# ---- Optional S3/MinIO leg ----
# Start the fixture first, from the repo root:
#   docker compose -f test/integration/docker-compose.yml up -d
# then re-run with S3=1.
if [ "${S3:-0}" = "1" ]; then
  printf 'target_rewrite:\n  - from: "%s/dataset/"\n    to: "s3://bench/replay/"\n' "$WORK" > s3-map.yaml
  echo "== S3/MinIO . timeline . cold =="
  "$BIN" run --trace imported.ioflux --engine s3 \
    --endpoint http://127.0.0.1:9000 --bucket bench --path-style \
    --access-key minioadmin --secret-key minioadmin \
    --target-map s3-map.yaml --prepare materialize-from-source --source-root / \
    --cache-mode cold --mode timeline --max-inflight "$WORKERS" \
    -o s3-timeline.json --csv results.csv || true
  "$BIN" report s3-timeline.json
fi

echo "== CSV series =="
column -t -s, results.csv | cut -c1-140

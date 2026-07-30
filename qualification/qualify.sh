#!/usr/bin/env bash
# qual-01 end-to-end qualification run: live -> capture -> import -> replay -> reconcile.
#
# Produces the evidence behind qualification/RESULTS.md. Every number in that
# document comes from a run of this script; nothing is hand-entered.
#
# Requirements: go, python3 >= 3.11, strace, Linux (posix_fadvise + /proc/<pid>/io).
#
# Usage:  qualification/qualify.sh
#         IOFLUX_QUAL_WORK=/path qualification/qualify.sh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
FIX="$SCRIPT_DIR/fixture"
BIN="$REPO_ROOT/bin/ioflux"
W=${IOFLUX_QUAL_WORK:-/tmp/ioflux-qual01}

# The syscalls the fixture can issue, plus the cache-control calls that mark the
# boundary between a replay's preparation phase and its measured run.
TRACED=openat,openat2,open,read,pread64,close,fstat,newfstatat,statx,lseek,fadvise64,fsync

rm -rf "$W"
mkdir -p "$W"/{dataset,live/baseline,live/strace,capture,replay,evidence}

echo "== build =="
( cd "$REPO_ROOT" && go build -o bin/ioflux ./cmd/ioflux )
strace -V | head -1
python3 -VV | head -1

echo
echo "== analytic oracle (leg 1: no observation at all) =="
python3 "$FIX/oracle.py"

echo
echo "== dataset =="
python3 "$FIX/dataset.py" "$W/dataset"

echo
echo "== live run A: untraced baseline (capture-overhead denominator) =="
python3 "$FIX/cachectl.py" cold "$W/dataset"
python3 "$FIX/reader.py" "$W/dataset" "$W/live/baseline"

echo
echo "== live run B: under strace (the capture that becomes the trace) =="
python3 "$FIX/cachectl.py" cold "$W/dataset"
strace -f -tt -T -y -s 0 -e "trace=$TRACED" -o "$W/capture/live.strace" \
  python3 "$FIX/reader.py" "$W/dataset" "$W/live/strace"

# Scope the trace to dataset I/O. This is a DECLARED scope reduction, not an
# accident: the interpreter's own .so/.pyc reads are out of the claim. Section B
# of the reconciliation parses the UNFILTERED capture, so if this filter dropped
# anything the fixture did, the coverage comparison would show it.
# Both halves of a split syscall must survive: keep every 'resumed>' line.
grep -E "$W/dataset/shard_|resumed>" "$W/capture/live.strace" \
  > "$W/capture/live.dataset.strace"
echo "   capture $(wc -l < "$W/capture/live.strace") lines -> dataset scope $(wc -l < "$W/capture/live.dataset.strace") lines"

echo
echo "== import + validate =="
"$BIN" import strace -o "$W/capture/qual01.ioflux" "$W/capture/live.dataset.strace"
"$BIN" validate "$W/capture/qual01.ioflux"

# Replay reads the SAME dataset directory. The fixture is read-only, so this
# removes materialization as a confound: live and replay touch identical bytes on
# identical blocks. --target-root confines the engine to that directory.
#
# asap, not timeline: the source clock is ptrace-distorted, so no arrival-time
# claim is defensible. --max-inflight 4 matches the 4 sequential live workers.
echo
echo "== replay C: under strace (observed independently, for reconciliation) =="
strace -f -tt -T -y -s 0 -e "trace=$TRACED" -o "$W/capture/replay.strace" \
  "$BIN" run --trace "$W/capture/qual01.ioflux" --engine local \
    --target-root "$W/dataset" --prepare assume-existing --cache-mode cold \
    --mode asap --max-inflight 4 -o "$W/replay/traced-cold.json"

echo
echo "== replay D/E: untraced, cold and warm, with block-device accounting =="
# ru_inblock from RUSAGE_CHILDREN gives the replay's device read volume, which is
# the only way to compare block-layer work against the live legs' /proc/self/io.
for leg in cold warm; do
  python3 "$FIX/devbytes.py" "$W/evidence/devbytes-replay-$leg.json" -- \
    "$BIN" run --trace "$W/capture/qual01.ioflux" --engine local \
      --target-root "$W/dataset" --prepare assume-existing --cache-mode "$leg" \
      --mode asap --max-inflight 4 -o "$W/replay/untraced-$leg.json"
done

echo
echo "== live run F: untraced under the same device accounting (for symmetry) =="
python3 "$FIX/cachectl.py" cold "$W/dataset"
mkdir -p "$W/live/devbytes"
python3 "$FIX/devbytes.py" "$W/evidence/devbytes-live-cold.json" -- \
  python3 "$FIX/reader.py" "$W/dataset" "$W/live/devbytes"
"$BIN" report "$W/replay/untraced-cold.json"

echo
echo "== reconcile =="
python3 "$FIX/reconcile.py" \
  --dataset "$W/dataset" \
  --journals "$W/live/strace" \
  --baseline-journals "$W/live/baseline" \
  --live-strace "$W/capture/live.strace" \
  --trace "$W/capture/qual01.ioflux" \
  --replay-strace "$W/capture/replay.strace" \
  --replay-root "$W/dataset" \
  --replay-results "$W/replay/untraced-cold.json" \
  --devbytes-live "$W/evidence/devbytes-live-cold.json" \
  --devbytes-replay "$W/evidence/devbytes-replay-cold.json" \
  --devbytes-replay-warm "$W/evidence/devbytes-replay-warm.json" \
  --out "$W/evidence/reconcile-cold.json"

echo
echo "evidence: $W/evidence/reconcile-cold.json"

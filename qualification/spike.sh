#!/usr/bin/env bash
# Bounded acquisition spike for qual-01 (plan.md §9.1).
#
# Evaluates each candidate acquisition source against the SAME fixture and
# records what it sees, what it misses, and what it costs. The strace leg's
# coverage is scored by qualify.sh's reconciliation; this script measures the
# overhead denominator and probes each candidate's obtainability.
#
# Usage: qualification/spike.sh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
FIX="$SCRIPT_DIR/fixture"
W=${IOFLUX_QUAL_WORK:-/tmp/ioflux-qual01-spike}
TRACED=openat,openat2,open,read,pread64,close,fstat,newfstatat,statx,lseek,fadvise64,fsync
REPEATS=${REPEATS:-3}

rm -rf "$W"; mkdir -p "$W"/{dataset,out}
python3 "$FIX/dataset.py" "$W/dataset"

phase_mean() { # mean per-worker phase_ns across the journals in $1
  python3 -c "
import json,glob,sys
js=[json.load(open(p)) for p in glob.glob(sys.argv[1]+'/worker-*.json')]
print(int(sum(j['phase_ns'] for j in js)/len(js)))" "$1"
}

echo
echo "=== candidate 1: strace $(strace -V | head -1 | awk '{print $NF}') ==="
for i in $(seq 1 "$REPEATS"); do
  rm -rf "$W/out/base-$i" "$W/out/str-$i"; mkdir -p "$W/out/base-$i" "$W/out/str-$i"
  python3 "$FIX/cachectl.py" cold "$W/dataset" >/dev/null
  python3 "$FIX/reader.py" "$W/dataset" "$W/out/base-$i" >/dev/null
  python3 "$FIX/cachectl.py" cold "$W/dataset" >/dev/null
  strace -f -tt -T -y -s 0 -e "trace=$TRACED" -o "$W/out/strace-$i.log" \
    python3 "$FIX/reader.py" "$W/dataset" "$W/out/str-$i" >/dev/null
  b=$(phase_mean "$W/out/base-$i"); s=$(phase_mean "$W/out/str-$i")
  printf "  trial %d: untraced %6.1f ms   straced %6.1f ms   overhead %.2fx   log %s\n" \
    "$i" "$(echo "$b/1000000" | bc -l)" "$(echo "$s/1000000" | bc -l)" \
    "$(echo "$s/$b" | bc -l)" "$(du -h "$W/out/strace-$i.log" | cut -f1)"
done
echo "  fields available per read: fd(+path via -y), requested count, returned count, entry ts, duration"
echo "  offsets: sequential reads carry none; reconstructed from the per-fd cursor"

echo
echo "=== candidate 2: DFTracer (pydftracer, PyPI) ==="
VENV="$W/dfvenv"
python3 -m venv "$VENV" >/dev/null
"$VENV/bin/pip" install -q pydftracer==2.0.4 typing_extensions 2>&1 | tail -2 || true
"$VENV/bin/python" - <<'PY' || true
import importlib.util, sys
print("  pydftracer installed:", importlib.util.find_spec("dftracer") is not None)
native = importlib.util.find_spec("dftracer.dftracer")
print("  native POSIX interceptor module (dftracer.dftracer):", "PRESENT" if native else "ABSENT")
try:
    import dftracer.python.common as common
    from dftracer.python.logger import dftracer as _  # binds common.profiler
    print("  bound profiler:", type(common.profiler).__name__)
except Exception as e:
    print("  import failed:", e)
PY
echo -n "  libdftracer_preload.so in distribution: "
if find "$VENV" -name 'libdftracer*' | grep -q .; then echo PRESENT; else echo ABSENT; fi
echo -n "  dftracer_split / native build files in sdist: "
"$VENV/bin/pip" download --no-binary :all: --no-deps -d "$W/out/dfsrc" pydftracer==2.0.4 >/dev/null 2>&1 || true
if tar tzf "$W/out/dfsrc"/pydftracer-*.tar.gz 2>/dev/null | grep -qiE 'CMakeLists|\.cpp$|preload'; then
  echo PRESENT
else
  echo "ABSENT (sdist ships python/ only)"
fi
echo "  => POSIX-level capture cannot be obtained from a pinned PyPI install;"
echo "     it requires a from-source CMake build of the separate C++ dftracer project."
echo "     Coverage and overhead therefore NOT MEASURED for this candidate."

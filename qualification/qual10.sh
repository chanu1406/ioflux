#!/usr/bin/env bash
# qual-01 §10 controlled regression: 256 KiB vs 64 KiB reads, paired and interleaved.
#
# Produces the evidence behind qualification/RESULTS.md §15. Requires a prior
# qualification/qualify.sh run in the same work directory: this reuses that
# run's capture (qual01.ioflux) and its dataset, so both arms read the identical
# bytes on identical extents.
#
# Usage:  qualification/qual10.sh
#         IOFLUX_QUAL_WORK=/path qualification/qual10.sh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
BIN="$REPO_ROOT/bin/ioflux"
W=${IOFLUX_QUAL_WORK:-/tmp/ioflux-qual01}
SRC="$W/capture/qual01.ioflux"

if [[ ! -f "$SRC" ]]; then
  echo "missing $SRC — run qualification/qualify.sh first" >&2
  exit 2
fi

( cd "$REPO_ROOT" && go build -o bin/ioflux ./cmd/ioflux )

echo "== treatment trace: split every read to 64 KiB =="
# A declared transformation, not a re-capture: identical targets, identical
# offsets covered, identical total bytes; only the request size changes. The
# output header carries the ledger entry and the source trace's digest.
"$BIN" transform split-reads --block 64KiB -o "$W/capture/qual01-64k.ioflux" "$SRC"
"$BIN" validate "$W/capture/qual01-64k.ioflux"
echo "   source digest: $(sha256sum "$SRC" | cut -d' ' -f1)"

echo
echo "== experiment config (FIXTURE.md §10, verbatim thresholds) =="
cat > "$W/qual10.yaml" <<YAML
claim: does reducing the read size from 256KiB to 64KiB slow the workload down?
trials: 10
warmup: 2
seed: 42
policy:
  min_trials: 10
  max_cv_percent: 5
run:
  trace: $W/capture/qual01.ioflux
  engine: local
  target_root: $W/dataset
  prepare: assume-existing
  cache_mode: cold
  max_inflight: 4
baseline: {}
treatment:
  trace: $W/capture/qual01-64k.ioflux
YAML
cat "$W/qual10.yaml"

echo
echo "== run =="
"$BIN" experiment --config "$W/qual10.yaml" -o "$W/qual10.json"

echo
echo "evidence: $W/qual10.json"

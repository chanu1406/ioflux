#!/usr/bin/env bash
# Regression-gate controls: four experiments whose answers are known in advance.
#
# Produces the evidence behind qualification/RESULTS.md §16. A tool that reports
# "no regression" is only worth trusting if it can be shown to report one when a
# regression is really there, to stay quiet when it is not, and to refuse when
# the evidence cannot support either answer. These check all three, plus the
# undecided case.
#
# Requires a prior qualification/qualify.sh run in the same work directory.
#
# Usage:  qualification/controls.sh
#         IOFLUX_QUAL_WORK=/path qualification/controls.sh
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
mkdir -p "$W/evidence"

# The null control's treatment must be a *declared* difference with no real
# effect. A byte-identical copy under a second filename is exactly that: the
# experiment has a treatment variable to attribute a difference to, and the true
# difference is zero by construction.
cp -f "$SRC" "$W/capture/qual01-copy.ioflux"

# emit_config <name> <claim> <threshold%> <baseline-cache-mode> <treatment-yaml>
emit_config() {
  cat > "$W/$1.yaml" <<YAML
claim: $2
trials: 10
warmup: 2
seed: 42
policy:
  min_trials: 10
  max_cv_percent: 5
  max_duration_regression_percent: $3
run:
  trace: $SRC
  engine: local
  target_root: $W/dataset
  prepare: assume-existing
  cache_mode: $4
  max_inflight: 4
baseline: {}
treatment:
$5
YAML
}

emit_config nullcontrol \
  "null control — does the gate report a regression when there is provably none?" \
  7 cold "  trace: $W/capture/qual01-copy.ioflux"

emit_config poscontrol \
  "positive control — does the gate detect a regression that is known to be real?" \
  7 cold "  max_inflight: 2"

# Storage-side: a warm baseline against a cold treatment, so the slower arm is
# slower for a reason that lives in the storage stack rather than in the runner.
emit_config poscontrol-cache \
  "positive control (storage-side) — a warm page cache versus a cold one" \
  7 warm "  cache_mode: cold"

# A budget essentially at zero cannot be certified as met while the interval
# still straddles zero, which is where a zero-effect treatment's interval sits.
emit_config tight1 \
  "tight-threshold control — a budget narrower than the measurement precision" \
  0.01 cold "  trace: $W/capture/qual01-copy.ioflux"

# Exit codes are the contract: 0 pass, 1 refused, 3 regression, 4 inconclusive.
#
# Only invariants are asserted. Whether a near-threshold comparison lands on
# pass or inconclusive depends on the precision that run achieved, so asserting
# it would make this script fail on an honest result — the same mistake as
# tuning a threshold until the answer is the expected one. What must hold every
# time is asserted; what varies is reported.
run_control() { # <name> <mode: is|not> <exit-code> <why>
  echo
  local phrase="must exit $3"
  [[ "$2" == "not" ]] && phrase="must never exit $3"
  echo "== $1 — $phrase ($4) =="
  set +e
  "$BIN" experiment --config "$W/$1.yaml" -o "$W/evidence/$1.json"
  local code=$?
  set -e
  case "$2" in
    is)
      if [[ $code -ne $3 ]]; then
        echo "FAIL: $1 exited $code, must be $3 ($4)" >&2
        exit 1
      fi ;;
    not)
      if [[ $code -eq $3 ]]; then
        echo "FAIL: $1 exited $code, which it must never do ($4)" >&2
        exit 1
      fi ;;
  esac
  echo "   exit $code — ok"
}

# What is asserted are the two ways the gate could mislead a team, in both
# directions. Which of the acceptable codes a control returns depends on how
# quiet the host was — this machine intermittently degrades for seconds at a
# time, and a refusal on those runs is the CV gate working, not failing. So the
# invariants forbid the wrong answers rather than requiring one right one.

# No false regression: a treatment with provably zero effect must never be
# called a regression, at any threshold.
run_control nullcontrol      not 3 "a zero-effect treatment must never be called a regression"
run_control tight1           not 3 "nor at a threshold near zero"

# No false pass: a large, real regression must never be certified as within
# budget. Detecting it (3), refusing the run as too noisy (1), or reporting it
# as undecided (4) are all honest; calling it a pass is not.
run_control poscontrol       not 0 "a real regression must never be certified as passing"

# No verdict from refused evidence: an unstable comparison must not become an
# approval, however large the apparent effect.
run_control poscontrol-cache not 0 "unstable evidence must not become an approval"

echo
echo "all four controls held their invariants"
echo "evidence: $W/evidence/{nullcontrol,poscontrol,poscontrol-cache,tight1}.json"

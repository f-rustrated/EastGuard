#!/usr/bin/env bash
#
# Stress-runs a single lib test N times to flush out flakiness, categorizing
# failures by panic line and saving each failing run's full log for inspection.
#
# Usage:
#   scripts/stress_test.sh [TEST] [N] [PER_RUN_TIMEOUT_SECS]
#
# Examples:
#   scripts/stress_test.sh                              # produce_then_fetch_hot, 100 runs
#   scripts/stress_test.sh produce_then_fetch_hot 200   # 200 runs
#   scripts/stress_test.sh e2e_swim_raft_cluster_lifecycle 50 90
#
# Env:
#   RUST_LOG   passed through to each run (default: off). e.g.
#              RUST_LOG=east_guard::data_plane::state=warn scripts/stress_test.sh
#
# Notes:
#   - Runs are sequential (no CPU contention → trustworthy rates). The turmoil
#     e2e tests are #[serial] anyway.
#   - Each run gets a wall-clock watchdog: the e2e helpers' `send_request` has no
#     client-side timeout, so a parked produce can hang a run forever. The
#     watchdog kills a stuck run and records it as TIMEOUT-HANG instead of
#     wedging the whole sweep.

set -u

TEST="${1:-produce_then_fetch_hot}"
N="${2:-100}"
PER_RUN_TIMEOUT="${3:-120}"   # seconds; a healthy run is well under this

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

OUTDIR="$(mktemp -d "${TMPDIR:-/tmp}/eg-stress-XXXXXX")"
SUMMARY="$OUTDIR/summary.txt"
: > "$SUMMARY"

echo "stress: test=$TEST runs=$N per_run_timeout=${PER_RUN_TIMEOUT}s"
echo "logs:   $OUTDIR"
echo "building once..."
if ! cargo test --lib "$TEST" --no-run >/dev/null 2>&1; then
  echo "BUILD FAILED"; exit 1
fi

pass=0; fail=0
for i in $(seq 1 "$N"); do
  log="$OUTDIR/run_$i.log"
  # Run with a wall-clock watchdog so a hung run can't wedge the sweep.
  RUST_LOG="${RUST_LOG:-}" cargo test --lib "$TEST" >"$log" 2>&1 &
  pid=$!
  killed=0
  for _ in $(seq 1 "$PER_RUN_TIMEOUT"); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null
    pkill -9 -f "deps/east_guard-" 2>/dev/null
    killed=1
  fi
  wait "$pid" 2>/dev/null

  if [ "$killed" = 1 ]; then
    fail=$((fail+1))
    echo "run=$i TIMEOUT-HANG" >> "$SUMMARY"
    echo "  run $i: TIMEOUT-HANG (kept $log)"
  elif grep -q "test result: FAILED" "$log"; then
    fail=$((fail+1))
    mode=$(grep -oE "client_protocol.rs:[0-9]+" "$log" | head -1)
    [ -z "$mode" ] && mode=$(grep -oE "phase[0-9]: [a-zA-Z0-9 -]+" "$log" | head -1)
    [ -z "$mode" ] && mode=$(grep -oE "did not converge on a single replacement leader" "$log" | head -1)
    [ -z "$mode" ] && mode=$(grep -oE "Ran for duration[^\"]*" "$log" | head -1 | sed 's/ .*/-duration/')
    seed=$(grep -oE "turmoil seed: [0-9]+" "$log" | head -1 | tr -dc '0-9')
    echo "run=$i ${mode:-unknown} seed=${seed:-?}" >> "$SUMMARY"
    echo "  run $i: FAIL ${mode:-unknown} seed=${seed:-?} (kept $log)"
  else
    pass=$((pass+1))
    rm -f "$log"   # keep only failing logs
  fi
done

echo
echo "==================== RESULT ===================="
echo "test=$TEST  pass=$pass  fail=$fail  /  $N"
if [ "$fail" -gt 0 ]; then
  echo "---- failure breakdown ----"
  sed -E 's/^run=[0-9]+ //; s/ seed=[0-9?]+$//' "$SUMMARY" | sort | uniq -c | sort -rn
  echo "---- failing logs kept in: $OUTDIR ----"
  echo "(panic line guide: 503=produce never acked, 543=fetch timeout, phaseN:*=leader_elects_after_kill phase, *-duration=sim timeout)"
else
  rm -rf "$OUTDIR"
fi
[ "$fail" -eq 0 ]

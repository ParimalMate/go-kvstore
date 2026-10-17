#!/usr/bin/env bash
# Runs every check and prints a PASS/FAIL/SKIP table.
#   scripts/verify-all.sh --quick   build, vet, gofmt, unit tests (-race), script unit tests, checkpoint
#   scripts/verify-all.sh           everything, including cluster scripts, chaos controls and 3 chaos seeds
# Per-check logs go to verify-logs/ (git-ignored). Exit code is non-zero if anything fails.
# A skipped check is reported, and the final line says so: it never claims "ALL CHECKS PASSED" when
# something did not run.
set -u
cd "$(dirname "$0")/.."
QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1
mkdir -p verify-logs
failed=0
skipped=0

run() { # run <name> <command...>
  local name="$1"; shift
  local start status
  start=$(date +%s)
  if "$@" > "verify-logs/${name}.log" 2>&1; then status=PASS; else status=FAIL; failed=1; fi
  printf '%-30s %-5s %5ss\n' "$name" "$status" "$(( $(date +%s) - start ))"
}

skip() { # skip <name> <reason>
  skipped=$((skipped + 1))
  printf '%-30s %-5s  (%s)\n' "$1" "SKIP" "$2"
}

echo "go version: $(go version)"
echo "python:     $(python3 --version)   (the scripts need Python 3.10 or newer)"
echo "started:    $(date -u +%FT%TZ)"
echo "------------------------------------------------"
run build          go build ./...
run vet            go vet ./...
run gofmt          bash -c 'test -z "$(gofmt -l .)"'
run go-test-race   go test -race -count=1 -timeout 15m ./...
run script-unit    bash -c 'cd scripts && python3 -m unittest -q $(ls test_*.py | sed "s/\.py$//")'
run checkpoint     bash checkpoint.sh

if [ "$QUICK" -eq 0 ]; then
  run quorum-cluster        python3 scripts/check-quorum-cluster.py
  run vectorclock-cluster   python3 scripts/check-vectorclock-cluster.py
  run gossip-cluster        python3 scripts/check-gossip-cluster.py
  run antientropy-cluster   python3 scripts/check-antientropy-cluster.py
  if [ -f scripts/chaos-cluster.py ]; then
    # The controls prove the harness can FAIL: each exits 0 only if the checker detected the planted problem.
    run chaos-control-no-repair   python3 scripts/chaos-cluster.py --control no-repair
    run chaos-control-mutate      python3 scripts/chaos-cluster.py --mutate-final --seed 5 --duration 10
    for seed in 1 2 3; do
      run "chaos-seed-$seed" python3 scripts/chaos-cluster.py --seed "$seed" --duration 45
    done
  else
    skip chaos-controls "scripts/chaos-cluster.py not found"
    skip chaos-seeds    "scripts/chaos-cluster.py not found"
  fi
fi

echo "------------------------------------------------"
if [ "$failed" -ne 0 ]; then
  echo "SOME CHECKS FAILED (see verify-logs/)"
elif [ "$QUICK" -eq 1 ]; then
  echo "QUICK CHECKS PASSED (cluster scripts and chaos were not run)"
elif [ "$skipped" -ne 0 ]; then
  echo "PASSED, BUT $skipped CHECK(S) WERE SKIPPED"
else
  echo "ALL CHECKS PASSED"
fi
exit "$failed"

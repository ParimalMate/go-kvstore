#!/usr/bin/env bash
# Runs every check and prints a PASS/FAIL table.
#   scripts/verify-all.sh --quick   build, vet, gofmt, unit tests (-race), checkpoint
#   scripts/verify-all.sh           everything, including cluster scripts and chaos seeds
# Per-check logs go to verify-logs/ (git-ignored). Exit code is non-zero if anything fails.
set -u
cd "$(dirname "$0")/.."
QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1
mkdir -p verify-logs
failed=0
skipped=0
rows=()

run() { # run <name> <command...>
  local name="$1"; shift
  local start status
  start=$(date +%s)
  if "$@" > "verify-logs/${name}.log" 2>&1; then status=PASS; else status=FAIL; failed=1; fi
  rows+=("$(printf '%-30s %-5s %5ss' "$name" "$status" "$(( $(date +%s) - start ))")")
  echo "${rows[${#rows[@]}-1]}"
}

echo "go version: $(go version)"
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
    for seed in 1 2 3; do
      run "chaos-seed-$seed" python3 scripts/chaos-cluster.py --seed "$seed" --duration 45
    done
  else
    skipped=$((skipped + 1))
    echo "chaos-seeds                    SKIP  (scripts/chaos-cluster.py not implemented)"
  fi
fi

echo "------------------------------------------------"
if [ "$failed" -ne 0 ]; then
  echo "SOME CHECKS FAILED (see verify-logs/; $skipped skipped)"
elif [ "$skipped" -gt 0 ]; then
  echo "PASSED ($skipped skipped)"
else
  echo "ALL CHECKS PASSED"
fi
exit "$failed"

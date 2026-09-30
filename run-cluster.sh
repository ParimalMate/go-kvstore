
#!/usr/bin/env bash

set -uo pipefail

BIN="./server"
PIDS=()

cleanup() {
    echo
    echo "Stopping cluster..."

    for pid in "${PIDS[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done

    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    echo "Cluster stopped."
}

trap cleanup EXIT
trap 'exit 0' INT TERM

echo "Building KV store..."
go build -o "$BIN" ./cmd/server || exit 1

echo "Starting node n1 on port 8081..."
"$BIN" \
    --id n1 \
    --port 8081 \
    --data n1.log \
    --peers localhost:8082,localhost:8083 \
    > n1.out 2>&1 &
PIDS+=("$!")

echo "Starting node n2 on port 8082..."
"$BIN" \
    --id n2 \
    --port 8082 \
    --data n2.log \
    --peers localhost:8081,localhost:8083 \
    > n2.out 2>&1 &
PIDS+=("$!")

echo "Starting node n3 on port 8083..."
"$BIN" \
    --id n3 \
    --port 8083 \
    --data n3.log \
    --peers localhost:8081,localhost:8082 \
    > n3.out 2>&1 &
PIDS+=("$!")

echo
echo "All three nodes launched."
echo "Node logs: n1.out, n2.out, n3.out"
echo "Press Ctrl+C to stop the cluster."

wait
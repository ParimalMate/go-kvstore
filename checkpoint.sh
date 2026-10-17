#!/usr/bin/env bash

set -uo pipefail

PORT_A=5011
PORT_B=5012
TMP_DIR="$(mktemp -d)"
BIN="$TMP_DIR/kvserver"
PID_A=""
PID_B=""

cleanup() {
    echo
    echo "Cleaning up test servers..."

    for pid in "$PID_A" "$PID_B"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done

    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1"
    echo
    echo "Server A log:"
    [[ -f "$TMP_DIR/server-a.log" ]] && cat "$TMP_DIR/server-a.log"
    echo
    echo "Server B log:"
    [[ -f "$TMP_DIR/server-b.log" ]] && cat "$TMP_DIR/server-b.log"
    exit 1
}

pass() {
    echo "PASS: $1"
}

start_server() {
    local id="$1"
    local port="$2"
    local wal="$3"
    local log="$4"

    "$BIN" --id "$id" --port "$port" --data "$wal" --w 1 --r 1 >"$log" 2>&1 &
    SERVER_PID=$!
}

wait_for_server() {
    local port="$1"
    local status

    for _ in {1..30}; do
        status=$(curl -s -o /dev/null -w "%{http_code}" \
            --max-time 1 \
            "http://localhost:$port/kv/checkpoint-probe" || true)

        if [[ "$status" == "404" || "$status" == "200" ]]; then
            return 0
        fi

        sleep 0.2
    done

    return 1
}

get_value() {
    curl -fsS --max-time 2 "http://localhost:$1/kv/$2"
}

put_value() {
    curl -s -o /dev/null -w "%{http_code}" \
        --max-time 2 \
        -X PUT "http://localhost:$1/kv/$2" \
        -H "Content-Type: text/plain" \
        -d "$3" || true
}

server_is_unreachable() {
    local port="$1"
    local status

    status=$(curl -s -o /dev/null -w "%{http_code}" \
        --max-time 1 \
        "http://localhost:$port/kv/name" || true)

    [[ "$status" == "000" ]]
}

echo "Building KV store..."
go build -o "$BIN" ./cmd/server || fail "Go build failed"

echo
echo "Starting Server A on port $PORT_A..."
start_server "n1" "$PORT_A" "$TMP_DIR/store-a.log" "$TMP_DIR/server-a.log"
PID_A="$SERVER_PID"
wait_for_server "$PORT_A" || fail "Server A did not start"

echo "Starting Server B on port $PORT_B..."
start_server "n2" "$PORT_B" "$TMP_DIR/store-b.log" "$TMP_DIR/server-b.log"
PID_B="$SERVER_PID"
wait_for_server "$PORT_B" || fail "Server B did not start"

pass "Checkpoint 1: Both servers started with separate WAL files"

echo
echo "Testing independent data..."

STATUS_A=$(put_value "$PORT_A" "name" "Parimal")
STATUS_B=$(put_value "$PORT_B" "name" "ServerB")

[[ "$STATUS_A" == "200" ]] || fail "Server A PUT returned HTTP $STATUS_A"
[[ "$STATUS_B" == "200" ]] || fail "Server B PUT returned HTTP $STATUS_B"

VALUE_A=$(get_value "$PORT_A" "name") || fail "Could not read from Server A"
VALUE_B=$(get_value "$PORT_B" "name") || fail "Could not read from Server B"

[[ "$VALUE_A" == "Parimal" ]] || fail "Unexpected value from Server A: $VALUE_A"
[[ "$VALUE_B" == "ServerB" ]] || fail "Unexpected value from Server B: $VALUE_B"

pass "Checkpoint 2: Both servers maintain independent data"

echo
echo "Restarting Server A..."

kill -TERM "$PID_A" || fail "Could not send SIGTERM to Server A"
wait "$PID_A" || true
PID_A=""

if ! server_is_unreachable "$PORT_A"; then
    fail "Server A is still responding after shutdown"
fi

start_server "n1" "$PORT_A" "$TMP_DIR/store-a.log" "$TMP_DIR/server-a.log"
PID_A="$SERVER_PID"
wait_for_server "$PORT_A" || fail "Server A did not restart"

VALUE_A=$(get_value "$PORT_A" "name") || fail "Could not read recovered value"
[[ "$VALUE_A" == "Parimal" ]] || fail "WAL recovery returned unexpected value: $VALUE_A"

VALUE_B=$(get_value "$PORT_B" "name") || fail "Server B stopped unexpectedly"
[[ "$VALUE_B" == "ServerB" ]] || fail "Server B data changed unexpectedly"

pass "Checkpoint 3: Server A recovered its data after restart"

echo
echo "Testing graceful shutdown of Server B..."

kill -TERM "$PID_B" || fail "Could not send SIGTERM to Server B"
wait "$PID_B" || true
PID_B=""

if ! server_is_unreachable "$PORT_B"; then
    fail "Server B is still responding after SIGTERM"
fi

pass "Checkpoint 4: Server B stopped responding after SIGTERM"

echo
echo "Verifying Server A is still operational..."

VALUE_A=$(get_value "$PORT_A" "name") || fail "Server A is not responding"
[[ "$VALUE_A" == "Parimal" ]] || fail "Server A data is incorrect"

pass "Checkpoint 5: Server A remains operational independently"

echo
echo "All automated checkpoints passed!"

#!/usr/bin/env bash
# Search-path smoke test (Stage 1 exit criterion): create/insert/search,
# restart the server, search again. Runs the real server + CLI binaries
# against a throwaway state directory.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build -p truthdb -p truthdb-cli

STATE_DIR=$(mktemp -d)
SERVER_PID=""
cleanup() {
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
    rm -rf "$STATE_DIR"
}
trap cleanup EXIT

start_server() {
    STATE_DIRECTORY="$STATE_DIR" ./target/debug/truthdb >>"$STATE_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if ./target/debug/truthdb-cli </dev/null >/dev/null 2>&1; then
            return
        fi
        sleep 0.2
    done
    echo "FAIL: server did not become ready" >&2
    cat "$STATE_DIR/server.log" >&2
    exit 1
}

stop_server() {
    kill -TERM "$SERVER_PID"
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
}

run_cli() {
    ./target/debug/truthdb-cli 2>/dev/null
}

expect() {
    local file=$1 pattern=$2
    if ! grep -qF "$pattern" "$file"; then
        echo "FAIL: expected '$pattern' in $file:" >&2
        cat "$file" >&2
        exit 1
    fi
}

start_server

printf '%s\n' \
    'create index products { "mappings": { "properties": { "name": { "type": "text" }, "category": { "type": "keyword" } } } }' \
    'insert document products { "name": "Red Running Shoes", "category": "shoes" }' \
    'insert document products { "name": "Blue Hiking Boots", "category": "boots" }' \
    'search products { "query": { "match": { "name": "running" } } }' \
    '\q' | run_cli >"$STATE_DIR/phase1.out"
expect "$STATE_DIR/phase1.out" '"acknowledged": true'
expect "$STATE_DIR/phase1.out" '"result": "created"'
expect "$STATE_DIR/phase1.out" '"Red Running Shoes"'
expect "$STATE_DIR/phase1.out" '"total": 1'

stop_server
start_server

printf '%s\n' \
    'search products { "query": { "bool": { "must": [ { "match": { "name": "running" } } ], "filter": [ { "term": { "category": "shoes" } } ] } } }' \
    '\q' | run_cli >"$STATE_DIR/phase2.out"
expect "$STATE_DIR/phase2.out" '"Red Running Shoes"'
expect "$STATE_DIR/phase2.out" '"total": 1'

echo "search smoke: OK"

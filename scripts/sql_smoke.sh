#!/usr/bin/env bash
# SQL front-door smoke (Stage 3 exit criterion): create/insert/select,
# restart the server, reselect — driven through the real server + CLI over
# the wire.
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

run_cli >"$STATE_DIR/phase1.out" <<'SQL'
CREATE TABLE products (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50), price FLOAT);
INSERT INTO products VALUES (1, 'Skor', 79.99), (2, 'Kangor', 129.5), (3, 'Sockar', NULL);
SELECT TOP 2 id, name, price FROM products WHERE price IS NOT NULL ORDER BY price DESC;
INSERT INTO products VALUES (1, 'dup', 0);
SELECT name FROM sys.tables;
\q
SQL
expect "$STATE_DIR/phase1.out" '(3 rows affected)'
expect "$STATE_DIR/phase1.out" 'Kangor'
expect "$STATE_DIR/phase1.out" '(2 rows affected)'
expect "$STATE_DIR/phase1.out" 'Msg 2627'
expect "$STATE_DIR/phase1.out" 'products'

stop_server
start_server

run_cli >"$STATE_DIR/phase2.out" <<'SQL'
SELECT id, name FROM products ORDER BY id;
\q
SQL
expect "$STATE_DIR/phase2.out" 'Skor'
expect "$STATE_DIR/phase2.out" 'Sockar'
expect "$STATE_DIR/phase2.out" '(3 rows affected)'

echo "sql smoke: OK"

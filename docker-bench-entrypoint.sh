#!/bin/sh
set -eu

cleanup() {
  status=$?
  if [ -n "${server_pid:-}" ]; then
    kill -TERM "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  exit "${status}"
}

trap cleanup EXIT INT TERM

mkdir -p "${STATE_DIRECTORY:-/data}"

truthdb &
server_pid=$!

ready=0
attempts=0
while [ "${attempts}" -lt 200 ]; do
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    wait "${server_pid}" || true
    echo "ERROR: truthdb exited before becoming ready" >&2
    exit 1
  fi

  if nc -z 127.0.0.1 9623 >/dev/null 2>&1; then
    ready=1
    break
  fi

  attempts=$((attempts + 1))
  sleep 0.1
done

if [ "${ready}" != "1" ]; then
  echo "ERROR: truthdb did not become ready on 127.0.0.1:9623" >&2
  exit 1
fi

truthdb-bench --host 127.0.0.1 --port 9623 "$@"

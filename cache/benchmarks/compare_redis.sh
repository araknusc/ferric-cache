#!/usr/bin/env bash
#
# Head-to-head throughput benchmark: ferric-cache vs Redis, using the same
# `redis-benchmark` client against both (ferric-cache is RESP-compatible).
#
# Requirements: redis-server, redis-benchmark, cargo. The comparison is only
# meaningful when both servers run on the SAME machine with nothing else busy.
#
# Usage:
#   ./benchmarks/compare_redis.sh                 # defaults below
#   REQUESTS=2000000 CLIENTS=50 PIPELINE=16 ./benchmarks/compare_redis.sh
#
set -euo pipefail

REQUESTS="${REQUESTS:-1000000}"   # total requests per test
CLIENTS="${CLIENTS:-50}"          # parallel connections
PIPELINE="${PIPELINE:-16}"        # pipelined commands per client (1 = no pipelining)
VALSIZE="${VALSIZE:-64}"          # value size in bytes
TESTS="${TESTS:-set,get,incr}"    # redis-benchmark command set

FERRIC_PORT=7777
REDIS_PORT=6379

here="$(cd "$(dirname "$0")" && pwd)"
crate_dir="$(dirname "$here")"

for bin in redis-server redis-benchmark cargo; do
  command -v "$bin" >/dev/null 2>&1 || { echo "ERROR: '$bin' not found in PATH."; exit 1; }
done

echo "Building ferric-cache (release)..."
( cd "$crate_dir" && cargo build --release --quiet )

ferric_pid=""
redis_pid=""
cleanup() {
  [ -n "$ferric_pid" ] && kill "$ferric_pid" 2>/dev/null || true
  [ -n "$redis_pid" ]  && kill "$redis_pid"  2>/dev/null || true
}
trap cleanup EXIT

echo "Starting ferric-cache on :$FERRIC_PORT (in-memory, no persistence)..."
( cd "$crate_dir" && ./target/release/ferric-cache --config benchmarks/bench_config.json --port "$FERRIC_PORT" ) >/tmp/ferric.log 2>&1 &
ferric_pid=$!

echo "Starting redis-server on :$REDIS_PORT (in-memory, save disabled)..."
redis-server --port "$REDIS_PORT" --save "" --appendonly no >/tmp/redis.log 2>&1 &
redis_pid=$!

sleep 1

run() {
  local name="$1" port="$2"
  echo
  echo "===================================================================="
  echo " $name  (port $port)"
  echo " requests=$REQUESTS clients=$CLIENTS pipeline=$PIPELINE valsize=$VALSIZE"
  echo "===================================================================="
  redis-benchmark -h 127.0.0.1 -p "$port" \
    -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$VALSIZE" \
    -t "$TESTS" --csv
}

run "ferric-cache" "$FERRIC_PORT"
run "Redis"        "$REDIS_PORT"

echo
echo "Done. Paste the two CSV blocks into benchmarks/README.md and record your"
echo "hardware (CPU model + core count) alongside them."

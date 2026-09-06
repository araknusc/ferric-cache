#!/usr/bin/env bash
#
# Docker variant of the head-to-head benchmark, matching how the numbers in
# README.md were produced on a Windows host: ferric-cache runs natively on the
# host, Redis runs in a container, and redis-benchmark is driven from a
# throwaway container reaching BOTH servers via host.docker.internal.
#
# NOTE the methodology caveat in README.md: reaching a native host process and a
# NAT'd container over the same name is not perfectly symmetric. This script is
# for reproducing the documented run, not for a definitive verdict — for that,
# run both servers natively on the same OS with a local client.
#
# Requirements: docker, cargo. Usage:
#   ./benchmarks/compare_redis_docker.sh
#   REQUESTS=500000 CLIENTS=50 PIPELINE=16 ./benchmarks/compare_redis_docker.sh
set -euo pipefail

REQUESTS="${REQUESTS:-500000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-16}"
VALSIZE="${VALSIZE:-64}"
TESTS="${TESTS:-set,get,incr}"
FERRIC_PORT="${FERRIC_PORT:-7788}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:latest}"

here="$(cd "$(dirname "$0")" && pwd)"
crate_dir="$(dirname "$here")"

command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
command -v cargo  >/dev/null || { echo "ERROR: cargo not found";  exit 1; }

echo "Building ferric-cache (release)..."
( cd "$crate_dir" && cargo build --release --quiet )

# In-memory config bound to 0.0.0.0 so the benchmark container can reach it.
cfg="$(mktemp)"
cat > "$cfg" <<JSON
{ "server": { "host": "0.0.0.0", "port": $FERRIC_PORT, "maxConnections": 20000 },
  "persistence": { "enabled": false }, "tls": { "enabled": false },
  "security": { "enabled": false } }
JSON

ferric_pid=""
redis_cid=""
cleanup() {
  [ -n "$ferric_pid" ] && kill "$ferric_pid" 2>/dev/null || true
  [ -n "$redis_cid" ]  && docker rm -f "$redis_cid" >/dev/null 2>&1 || true
  rm -f "$cfg"
}
trap cleanup EXIT

echo "Starting ferric-cache on host :$FERRIC_PORT ..."
( cd "$crate_dir" && ./target/release/ferric-cache --config "$cfg" --port "$FERRIC_PORT" ) >/tmp/ferric_bench.log 2>&1 &
ferric_pid=$!

echo "Starting Redis container on :6379 ..."
redis_cid="$(docker run -d -p 6379:6379 "$REDIS_IMAGE" redis-server --save "" --appendonly no)"
sleep 2

bench() {
  local name="$1" port="$2"
  echo
  echo "==== $name ($port)  n=$REQUESTS c=$CLIENTS P=$PIPELINE d=$VALSIZE ===="
  docker run --rm --add-host=host.docker.internal:host-gateway "$REDIS_IMAGE" \
    redis-benchmark -h host.docker.internal -p "$port" \
    -t "$TESTS" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$VALSIZE" -q
}

bench "ferric-cache" "$FERRIC_PORT"
bench "Redis"        6379

# Benchmarks

Two independent harnesses:

1. **Criterion micro-benchmarks** (`../benches/performance.rs`) — in-process,
   single-connection latency of GET/SET/DELETE, concurrent GET, and large
   values. Run with:

   ```bash
   cargo bench
   ```

   These measure steady-state op latency (the server is started in-process and
   a connection is reused across iterations). They are **not** a Redis
   comparison.

2. **Head-to-head vs Redis** (`compare_redis.sh`) — runs the same
   `redis-benchmark` client against both ferric-cache and a real `redis-server`
   on the same machine. Because ferric-cache is RESP-compatible, `redis-benchmark`
   drives it unchanged.

   ```bash
   ./benchmarks/compare_redis.sh
   # or tune:
   REQUESTS=2000000 CLIENTS=50 PIPELINE=16 VALSIZE=64 ./benchmarks/compare_redis.sh
   ```

   Requires `redis-server` and `redis-benchmark` in `PATH`.

## Methodology notes

- Both servers run **in-memory** (persistence disabled on ferric-cache; `save ""`
  and `appendonly no` on Redis) so the comparison is CPU/throughput, not disk.
- Run on an otherwise-idle machine. Record the **CPU model and core count** —
  ferric-cache shards across cores, so the multi-core story is the whole point;
  a single-core VM will not show it.
- Report both pipelined (`-P 16`) and non-pipelined (`-P 1`) numbers: pipelining
  changes the bottleneck from round-trips to raw processing.
- Redis is single-threaded for command execution; ferric-cache uses a 64-way
  sharded map across worker threads. Expect the gap to widen with more cores and
  more concurrent clients on disjoint keys, and to narrow for single-connection,
  non-pipelined workloads.

## Results

> Fill this in from your own run — do not ship numbers you have not reproduced.
> The previous README's "280K SET / 320K GET" figures were **targets from the
> planning docs, not measurements**, and have been removed.

Hardware: _CPU model, cores, RAM_ · OS: _..._ · Date: _..._ ·
ferric-cache _version/commit_ vs Redis _version_

| Workload (`-c 50 -P 16 -d 64`) | ferric-cache (req/s) | Redis (req/s) |
|--------------------------------|----------------------|---------------|
| SET                            | _tbd_                | _tbd_         |
| GET                            | _tbd_                | _tbd_         |
| INCR                           | _tbd_                | _tbd_         |

| Workload (`-c 50 -P 1 -d 64`)  | ferric-cache (req/s) | Redis (req/s) |
|--------------------------------|----------------------|---------------|
| SET                            | _tbd_                | _tbd_         |
| GET                            | _tbd_                | _tbd_         |

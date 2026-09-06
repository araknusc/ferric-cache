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

## Results (measured)

> These are real measurements, not targets. The previous README's "280K SET /
> 320K GET" figures were targets from the planning docs and have been removed.

**Setup.** AMD Ryzen 9 9950X (16C/32T), Windows 11. ferric-cache 0.1.0 (release
build) running **natively on the host**; Redis **8.2.1** running in Docker.
`redis-benchmark` 8.2.1 was driven from a throwaway Docker container, reaching
**both** servers via `host.docker.internal` (`-d 64`, in-memory: ferric
persistence disabled, Redis `--save "" --appendonly no`).

### Pipelined — `-c 50 -P 16 -d 64` (500K ops)

| Command | ferric-cache (req/s) | Redis (req/s) | ferric vs Redis |
|---------|---------------------:|--------------:|:---------------:|
| SET     | **452,080**          | 303,398       | +49%            |
| GET     | **474,383**          | 332,447       | +43%            |
| INCR    | **507,614**          | 326,158       | +56%            |

### Non-pipelined — `-c 50 -P 1 -d 64` (100K ops)

| Command | ferric-cache (req/s) | Redis (req/s) |
|---------|---------------------:|--------------:|
| SET     | **34,130**           | 22,237        |
| GET     | **33,807**           | 22,847        |
| INCR    | **34,746**           | 22,432        |

### High concurrency — `-c 500 -P 1 -d 64` (200K ops)

| Command | ferric-cache (req/s) | Redis (req/s) |
|---------|---------------------:|--------------:|
| SET     | **41,728** (p50 11.8ms) | 31,827 (p50 15.5ms) |
| GET     | **41,085** (p50 11.7ms) | 31,980 (p50 15.4ms) |

### Honest interpretation

- **ferric-cache leads all three regimes.** The **pipelined** result (+43–56%)
  is the most credible: with the network round-trip amortized across 16 commands,
  this is essentially a server-CPU comparison, and the multi-core-sharded design
  comes out ahead of Redis's single command thread on this 16-core box.
- The **non-pipelined** and **high-concurrency** results also favor ferric-cache,
  but they are round-trip-latency-bound and **confounded by a network asymmetry**:
  ferric-cache runs natively on the host (direct loopback) while Redis is reached
  through Docker's published-port NAT. That handicap falls hardest on the `-P 1`
  tests, so treat those rows as directional, not conclusive.
- **What made the pipelined win possible:** the server batches all replies from
  one read into a **single** socket write and sets `TCP_NODELAY`. An earlier
  version wrote and flushed once *per command*, which roughly halved pipelined
  throughput (ferric-cache SET was ~265K req/s and *lost* to Redis before that
  fix). Lesson: for a pipelined RESP server, reply batching dominates.
- **For a definitive number**, run both servers natively on the same OS (e.g.
  Linux) with the benchmark client local — that removes the Docker NAT variable
  entirely.

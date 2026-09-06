# ferric-cache

A multi-core-sharded, **RESP-compatible** cache server written in Rust — speaks
the Redis serialization protocol, so `redis-cli`, `redis-benchmark`, and standard
Redis client libraries work unmodified.

> **Status: late alpha.** The core is solid and well-tested (128 passing tests),
> but some distributed features (full replica resync, transitive gossip
> membership) are still in progress. Not yet recommended for production without
> review.

## Repository layout

The Cargo crate lives in [`cache/`](cache/) — all build/test/run commands run
from there. See [`cache/README.md`](cache/README.md) for full documentation.

```
ferric-cache/
├── LICENSE                 # Apache-2.0
├── CONTRIBUTING.md
└── cache/                  # the crate
    ├── src/                # server, storage, protocol, persistence, cluster, ...
    ├── benches/            # in-process Criterion micro-benchmarks
    ├── benchmarks/         # redis-benchmark head-to-head harness + results
    └── tests/
```

## What's inside

- **Sharded storage** — 64 independent `parking_lot::RwLock<HashMap>` shards; hot-path GET/SET take one shard lock, so disjoint-key work scales across cores.
- **Redis-shaped command surface** — strings, hashes, lists, sets, sorted sets, streams, pub/sub, `MULTI/EXEC/WATCH`, and Lua `EVAL`.
- **Persistence** — WAL + snapshots (`none`/`wal`/`snapshot`/`both`); snapshots capture every value type and WAL rotation keeps backup segments.
- **Replication & clustering** — master/replica fan-out and a consistent-hash ring with `-MOVED` redirects (see status note above).
- **Security** — optional auth with Argon2-hashed, config-provisioned users + ACLs, and optional TLS.

## Quick start

```bash
cd cache
cargo run --release                 # starts on 127.0.0.1:7777 (RESP)
redis-cli -p 7777 SET hello world
redis-cli -p 7777 GET hello
```

Building requires Rust 1.85+ and a C toolchain (Lua 5.4 is vendored for `EVAL`).

## Performance vs Redis

Real `redis-benchmark` numbers on an **AMD Ryzen 9 9950X (16C/32T)**, `-d 64`,
in-memory, vs **Redis 8.2.1**:

| Test | ferric-cache | Redis |
|------|-------------:|------:|
| Pipelined `-P16 -c50` — GET / SET | **474K / 452K req/s** | 332K / 303K req/s |
| Non-pipelined `-P1 -c50` — GET / SET | **33.8K / 34.1K req/s** | 22.8K / 22.2K req/s |
| High concurrency `-P1 -c500` — GET / SET | **41.1K / 41.7K req/s** | 32.0K / 31.8K req/s |

**Honest read:** ferric-cache leads all three regimes. The **pipelined** result
(~40–55% ahead) is the most credible — with the network round-trip amortized,
it's a genuine server-CPU comparison. The non-pipelined and high-concurrency
results also favor ferric-cache but are partly inflated by a network asymmetry
(ferric-cache runs natively on the host; Redis is reached through Docker NAT), so
treat those as directional. Full methodology, caveats, and reproduction scripts
are in [`cache/benchmarks/README.md`](cache/benchmarks/README.md).

## License

[Apache-2.0](LICENSE).

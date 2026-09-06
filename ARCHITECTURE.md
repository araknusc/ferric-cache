# Architecture notes

Orientation for contributors: where things live, how a request flows, and the conventions to follow when adding features.

## Repository Layout

The project root is **not** the Rust crate. It holds the license, contributing
guide, and a single Cargo crate at `cache/`. All build/test/run commands must be
issued from `cache/`.

```
ferric-cache/
├── LICENSE
├── CONTRIBUTING.md
└── cache/                            # The Rust crate — work happens here
```

## Common Commands

Run all of these from `cache/`:

```bash
# Build / run
cargo run --release                                    # Default config.json on port 7777
cargo run --release -- --port 8000                     # Override port
cargo run --release -- --config config.production.json # Use a specific config
cargo run --release -- --port 7001 --cluster-port 17001 --node-id node1                    # Seed cluster node
cargo run --release -- --port 7002 --cluster-port 17002 --node-id node2 --join 127.0.0.1:17001  # Join existing cluster

# Tests (integration tests bind real TCP ports — pick free ones if running multiple)
cargo test                          # All tests
cargo test --test integration       # Single integration test file
cargo test cluster                  # Pattern match by name
cargo test -- --nocapture           # See println! output (server prints a lot)
cargo test test_basic_set_get       # Run a single test by name

# Benchmarks (Criterion harness, defined in benches/performance.rs)
cargo bench

# Examples
cargo run --example test_replication --release
cargo run --example basic_usage --release

# TLS certs (only needed when tls.enabled = true in config)
./generate_certs.sh         # bash
./generate_certs.bat        # cmd.exe
```

The `CONFIG_PATH` env var overrides the `--config` flag. `cache/data/` holds runtime WAL/snapshot files and is not source — it is regenerated and may already contain stale state from prior runs. Wipe it (`rm cache/data/*.wal cache/data/*.snapshot`) when you need a clean slate to debug persistence.

## Architecture

### Top-level shape

A single `CacheServer` (`src/server.rs`) owns one `Arc<CacheStorage>` plus optional subsystems (persistence, clustering, TLS, replication) wired in via builder-style constructors:

- `CacheServer::new(addr)` — in-memory only
- `CacheServer::with_persistence(addr, persistence_config)` — adds WAL + snapshot
- `CacheServer::with_clustering(addr, cluster_addr, node_id, seed)` — adds gossip ring
- `CacheServer::with_clustering_and_persistence(...)` — both
- After construction, mutator methods layer on optional features: `with_tls(...)`, `as_master(port)`, `as_replica(master_addr)`

`main.rs` does the wiring in this order: load config → build server (clustering + persistence) → load_from_persistence (snapshot then WAL replay) → configure replication role → configure TLS → `run()`. **When adding a new optional subsystem, follow this same shape rather than threading another constructor variant**, because every existing constructor would have to grow another arg.

### Request lifecycle

Both plain TCP and TLS listeners run in `server::run()` and converge on the same code path:

1. `handle_connection` / `handle_tls_connection` reads bytes into a `BytesMut` buffer and holds a per-connection `ConnState { authed_user }`.
2. `protocol::parse_resp` parses one RESP value (incremental — returns an error to wait for more bytes).
3. `protocol::parse_command` translates the RESP `Array` into a `Command` enum variant.
4. `server::execute_command` runs four gates at the top before dispatch:
   - **Cluster routing** — if `ring` is set and the command's key hashes to a remote node, return `-MOVED <slot> <host:port>`.
   - **Auth gate** — if `auth` is set and the connection isn't authenticated, return `-NOAUTH` (PING and AUTH exempt).
   - **ACL gate** — if authenticated, run `auth.check_permission(user, command_name, key)`; deny with `-NOPERM`.
   - **Write log + replicate** — if `commands::is_write_command(&cmd)`, append to WAL and fan out to replicas before touching storage.
5. Then a single `match` dispatches to `CacheStorage` methods.

**Adding a new command** touches: `protocol.rs` (variant + parser), `storage.rs` (storage method if not already present), `commands.rs` (`is_write_command` + `apply_write_command` if it writes), `server.rs::execute_command` (dispatch arm + entries in `command_name` and `command_key`), tests. The `apply_write_command` helper is the **shared** apply path used by both WAL replay and replica side, so writes can never drift between persistence and replication.

### Storage model

`CacheStorage` (`src/storage.rs`) wraps a **`ShardedMap`** — 64 independent `parking_lot::RwLock<HashMap<Bytes, CacheValue>>` shards indexed by `xxh3(key) & 63`. Single-key ops route to one shard via `self.data.shard(key)`; global ops (KEYS, FLUSHALL, DBSIZE, cleanup_expired, sinter/sunion/sdiff) iterate `self.data.shards()`. Concurrent disjoint-key writes scale across shards.

`CacheValue` is an enum over the five Redis-style types (`src/data_structures/mod.rs`):

```
CacheValue::String(Bytes, Option<Instant>)  // value + expires_at
CacheValue::Hash(HashValue)
CacheValue::List(ListValue)
CacheValue::Set(SetValue)
CacheValue::SortedSet(SortedSetValue)
```

TTL is per-value via `is_expired()` polymorphism. Expired entries are **lazy-deleted on read** and also pruned by a background task that calls `storage.cleanup_expired()` every 60s (spawned in `server::run`). Wrong-type operations return `WRONGTYPE Operation against a key holding the wrong kind of value` — match that string when adding new typed commands. Use the per-value `expires_at` slot directly via `storage.set_expiry/persist/pttl_millis` for new TTL-touching commands.

Hot-path commands (GET/SET) take only **one** shard's lock for the duration of the operation. If you find yourself doing slow work under the write lock (I/O, network, large allocations), move it outside the guard — even with sharding, holding any shard's write lock for milliseconds blocks all keys hashing to it.

### Wire protocol

Two different protocols live in this codebase:

- **Client ↔ server**: RESP (Redis serialization protocol) — `src/protocol.rs`. The README mentions a "custom binary protocol" in places, but the actual implementation is RESP. The server is intentionally Redis-CLI compatible (see `tests/redis_compatibility_test.rs`).
- **Master ↔ replica**: bincode-serialized `ReplicationCommand` enum — `src/replication/protocol.rs`. Fire-and-forget; replicas reconnect on disconnect with a 5-second backoff (`server::run`).

### Persistence

`PersistenceMode` (`src/persistence/mod.rs`) selects: `None` | `WAL` | `Snapshot` | `Both`. On startup, `load_from_persistence` first loads the snapshot, then replays WAL entries on top — order matters because WAL entries are deltas after the last snapshot. WAL sync policies (`Always`, `EverySecond`, `Manual`) trade durability for throughput. Background sync and snapshot tasks are spawned in `server::run`.

The WAL logs every write command (strings, hashes, lists, sets, sorted sets, streams, and TTL changes) — see `is_write_command` in `commands.rs` and its regression test. Snapshots capture all value types via type-tagged reconstruction commands that go through `apply_write_command`.

### Clustering

`ConsistentHashRing` + `GossipProtocol` (`src/cluster/`) form the cluster membership layer. The seed node starts with itself in the ring; joining nodes contact a seed via `gossip.join_cluster(seed_addr)`. The ring is wired into the request path: when `ring` and `local_node_id` are present, `execute_command` checks `ring.node_for_key(key)` and returns `-MOVED <slot> <host:port>` for keys hashing to a remote node, where `slot = xxh3(key) % 16384` (note: not Redis CRC16). Smart clients follow the redirect. Slot migration / online resharding is still TODO.

### Replication

`ReplicationRole` (`src/replication/mod.rs`) is `Standalone | Master | Replica { master_addr }`. The master listens on a separate `replicationPort` for replicas; on each write, `MasterReplicator::replicate_write` fans out to all connected replicas via `mpsc` channels. Replicas run a reconnect loop (`server::run` spawns `replica_client.start_replication()` with 5s retry).

### Security

`AuthManager` and `ACLRule` (`src/security/`) are wired into the request path. When a `CacheServer` is built with `with_auth(Arc<AuthManager>)`, `execute_command` enforces three states: (1) unauthenticated connection issuing anything but PING/AUTH → `-NOAUTH`; (2) wrong AUTH password → `-WRONGPASS`; (3) authenticated user denied by `auth.check_permission` → `-NOPERM`. Per-connection state lives in a stack-local `ConnState { authed_user: Option<String> }` held by `handle_connection`/`handle_tls_connection`. Users are provisioned from the `security.users` section of the JSON config (see `config.secure.example.json`); passwords are stored as Argon2id hashes. There is no default account.

### TLS

`tls::create_tls_acceptor` builds a `tokio_rustls::TlsAcceptor` from PEM cert + key. When TLS is enabled, the server binds two listeners: plain TCP on `port` and TLS on `tlsPort` (defaults to `port + 1000`). Both run the same RESP/command loop.

## Configuration

Configs are JSON with **camelCase** field names (see `#[serde(rename_all = "camelCase")]` in `src/config.rs`). Multiple example configs live in `cache/`:

- `config.json` — default, persistence enabled
- `config.development.json` / `config.production.json` / `config.test.json` — environment variants
- `config-master.json` / `config-replica.json` — replication pair (use ports 7777 + 7778 with master replication on 8888)
- `tls_config.json` — TLS-enabled

When persistence is enabled in config, `to_persistence_config()` auto-creates the `dataDir`. Modes accepted by the JSON loader: `none`, `wal`, `snapshot`, `both` (anything else silently becomes `None` — note `config-master.json` says `"mode": "aof"` which is **not** a valid value and gets coerced to None).

## Conventions

- **Bytes everywhere on the hot path** — keys and values are `bytes::Bytes`, not `String` or `Vec<u8>`. Use `Bytes::copy_from_slice` only when you must own; otherwise pass `&[u8]`.
- **`parking_lot::RwLock`, not `std::sync::RwLock`** — already in dependencies; consistency matters because `parking_lot` guards are not async-aware (don't `.await` while holding one).
- **Async I/O on Tokio**, sync locking on `parking_lot`. `tokio::sync::Mutex` is reserved for cases where the lock crosses an `.await` (see `replica_client` field in `CacheServer`).
- **Errors are `Box<dyn std::error::Error>`** at module boundaries; internal modules (`persistence`, `replication`) define their own enums (`WALError`, etc.). Don't introduce `anyhow`/`thiserror` without a reason — current code is consistent without them.
- **Adding a Redis-style command**: variant in `Command` (`protocol.rs`), parse arm in `parse_command`, serialize arm in `serialize_command`, storage method in `storage.rs` (or the relevant `data_structures/*.rs`), dispatch arm in `server::execute_command`, optional client wrapper in `client.rs`, integration test in `tests/redis_compatibility_test.rs`.

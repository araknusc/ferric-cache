# ferric-cache

A multi-core-sharded, RESP-compatible cache server written in Rust.

> **Status: late alpha.** The core is solid and well-tested, but some
> distributed features (full replica resync, transitive gossip membership) are
> still in progress — see [`docs/`](../docs/) and the issue tracker. Not yet
> recommended for production without review.

## Features

### Core Performance
- **Designed for high multi-core throughput**: a 64-way sharded map on
  `parking_lot` locks lets disjoint-key operations scale across cores, where
  Redis executes commands on a single thread. See
  [`benchmarks/`](benchmarks/) for a reproducible `redis-benchmark` head-to-head
  harness — run it on your own hardware rather than trusting a number here.
- **RESP wire protocol**: speaks the Redis serialization protocol, so
  `redis-cli`, `redis-benchmark`, and standard Redis client libraries work
  unmodified.
- **Zero-copy on the hot path**: keys and values are `bytes::Bytes`.
- **Async I/O**: built on Tokio for high concurrency.

### Data Structures
- **Strings**: Basic key-value with TTL support
- **Hashes**: Field-value pairs (HGET, HSET, HDEL, HGETALL)
- **Lists**: Double-ended queues (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
- **Sets**: Unique members with operations (SADD, SREM, SINTER, SUNION)
- **Sorted Sets**: Score-based ordering (ZADD, ZREM, ZRANGE, ZRANK)

### Enterprise Features
- **Clustering**: Distributed cache with consistent hashing
- **Replication**: Master-replica replication for high availability
- **Persistence**: WAL and snapshot support for data durability
- **Authentication**: User/password with session management
- **ACL**: Fine-grained command and key pattern permissions
- **TLS Encryption**: Secure client connections with dual ports
- **Configuration**: JSON-based configuration system

## Prerequisites

- **Rust 1.75+** (`rustup` recommended).
- **A C toolchain** (`cc`/`gcc`/MSVC). `mlua` vendors and compiles Lua 5.4 from
  source for `EVAL`, so a working C compiler is required to build.
- Optional: `openssl` for `generate_certs.*`; `redis-server` + `redis-benchmark`
  for the comparison benchmarks.

All commands below run from the `cache/` directory.

## Quick Start

### Single Node

```bash
# Start with default settings
cargo run --release

# Start with custom port
cargo run --release -- --port 8000

# Start with persistence enabled
cargo run --release -- --config config.json

# Start with TLS enabled (requires certificates)
cargo run --release -- --config tls_config.json
```

### Cluster Mode

```bash
# Node 1 (seed node)
cargo run --release -- --port 7001 --cluster-port 17001 --node-id node1

# Node 2 (joins cluster)
cargo run --release -- --port 7002 --cluster-port 17002 --node-id node2 --join 127.0.0.1:17001

# Node 3 (joins cluster)
cargo run --release -- --port 7003 --cluster-port 17003 --node-id node3 --join 127.0.0.1:17001
```

### Replication Mode

```bash
# Master node (accepts writes and replicates to replicas)
cargo run --release -- --port 7777 --config config-master.json

# Replica node (receives replicated data from master)
cargo run --release -- --port 7778 --config config-replica.json
```

## Configuration

Create a `config.json` file:

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 7777,
    "tlsPort": 8777,
    "maxConnections": 10000
  },
  "persistence": {
    "enabled": true,
    "mode": "both",
    "walSyncPolicy": "everysecond",
    "snapshotIntervalSecs": 300,
    "maxWalSizeMb": 100,
    "dataDir": "./data",
    "walBackupCount": 1
  },
  "performance": {
    "cleanupIntervalSecs": 60,
    "maxMemoryMb": null,
    "workerThreads": null
  },
  "replication": {
    "role": "standalone",
    "replicationPort": null,
    "masterAddr": null
  },
  "security": {
    "enabled": false,
    "users": []
  },
  "tls": {
    "enabled": true,
    "certFile": "./certs/server.crt",
    "keyFile": "./certs/server.key",
    "caFile": null,
    "requireClientCert": false,
    "clusterTls": false
  }
}
```

### Authentication & ACL

Auth is **off by default** and there is **no hardcoded account**. To enable it,
add a `security` section with one or more users (passwords are hashed with
Argon2 at load time). See [`config.secure.example.json`](config.secure.example.json) for a full
example.

```json
"security": {
  "enabled": true,
  "users": [
    { "username": "admin", "password": "change-me", "isAdmin": true },
    {
      "username": "app",
      "password": "another-secret",
      "isAdmin": false,
      "rules": [
        { "permission": "readOnly", "keyPatterns": ["cache:*"] },
        { "permission": "custom", "commands": ["+SET", "+DEL"], "keyPatterns": ["cache:*"] }
      ]
    }
  ]
}
```

Clients authenticate with `AUTH <username> <password>`. A rule's `permission`
is one of `allowAll`, `readOnly`, `writeOnly`, `deny`, or `custom` (with an
explicit `commands` list of `+CMD`/`-CMD`). If `enabled` is true but no users are
configured, the server warns and every `AUTH` fails.

### Persistence Modes
- `"none"`: In-memory only (default for benchmarks)
- `"wal"`: Write-Ahead Log for crash recovery
- `"snapshot"`: Point-in-time snapshots
- `"both"`: WAL + Snapshots for maximum durability

### WAL Sync Policies
- `"always"`: Sync on every write (safest, slower)
- `"everysecond"`: Sync every second (balanced)
- `"manual"`: No automatic sync (fastest, less safe)

### Replication Configuration
- `"role"`: Node role - `"standalone"`, `"master"`, or `"replica"`
- `"replicationPort"`: Port for accepting replica connections (master only)
- `"masterAddr"`: Master server address in `host:port` format (replica only)

**Example Master Configuration:**
```json
{
  "replication": {
    "role": "master",
    "replicationPort": 8888
  }
}
```

**Example Replica Configuration:**
```json
{
  "replication": {
    "role": "replica",
    "masterAddr": "127.0.0.1:8888"
  }
}
```

### TLS Configuration
- `"enabled"`: Enable/disable TLS encryption
- `"certFile"`: Path to server certificate (PEM format)
- `"keyFile"`: Path to private key (PEM format)
- `"caFile"`: Path to CA certificate for client verification (optional)
- `"requireClientCert"`: Require client certificates for mutual TLS
- `"clusterTls"`: Enable TLS for cluster communication

The server supports dual ports when TLS is enabled:
- **Plain TCP**: Configured port (e.g., 7777)
- **TLS**: TLS port (e.g., 8777) or port+1000 if not specified

## Client Usage

### Plain TCP Connection
```rust
use ferric_cache::FerricClient;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FerricClient::connect("127.0.0.1:7777").await?;

    // String operations
    client.set("key", "value").await?;
    let value = client.get("key").await?;
    client.set_with_ttl("temp", "data", Duration::from_secs(60)).await?;
    client.delete("key").await?;

    Ok(())
}
```

### TLS Connection (when enabled)
```bash
# Connect to TLS port with tools like openssl s_client
openssl s_client -connect 127.0.0.1:8777

# Or use any Redis-compatible TLS client
```

## Wire Protocol (RESP)

The client ↔ server protocol is **RESP** (the Redis serialization protocol),
not a custom binary format. Any Redis client library, `redis-cli`, and
`redis-benchmark` work unmodified:

```bash
redis-cli -p 7777 SET foo bar
redis-cli -p 7777 GET foo
```

(The master ↔ replica channel is a separate, bincode-serialized internal
protocol — see below.)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                         │
├─────────────────────────────────────────────────────────────┤
│                    AUTHENTICATION & ACL                      │
├─────────────────────────────────────────────────────────────┤
│                      CLUSTER ROUTING                         │
│              (Consistent Hashing + Gossip)                   │
├─────────────────────────────────────────────────────────────┤
│                     COMMAND PROCESSOR                        │
│         (String/Hash/List/Set/SortedSet handlers)           │
├─────────────────────────────────────────────────────────────┤
│                      STORAGE ENGINE                          │
│              (Concurrent HashMap + TTL Manager)              │
├─────────────────────────────────────────────────────────────┤
│                       REPLICATION                            │
│                   (Master → Replicas)                       │
├─────────────────────────────────────────────────────────────┤
│                       PERSISTENCE                            │
│                    (WAL + Snapshots)                        │
└─────────────────────────────────────────────────────────────┘
```

### Replication Architecture

The cache server supports master-replica replication for high availability:

- **Master Node**: Accepts write operations and replicates them to all connected replicas
- **Replica Nodes**: Connect to master and receive replicated operations asynchronously
- **Automatic Reconnection**: Replicas automatically reconnect if connection is lost
- **Fire-and-Forget**: Replication is asynchronous for minimal write latency
- **Binary Protocol**: Uses efficient bincode serialization for replication commands

**Replication Flow:**
```
Client → Master (SET key value)
           ↓
      Write to storage
           ↓
      Replicate to all replicas (async)
           ↓
      Return OK to client
```

## Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test cluster         # Clustering tests
cargo test persistence     # Persistence tests
cargo test data_structures # Data structure tests
cargo test security       # Auth & ACL tests

# Test replication
cargo run --example test_replication --release

# Run benchmarks
cargo bench
```

## Performance

Real `redis-benchmark` head-to-head vs Redis 8.2.1 on an **AMD Ryzen 9 9950X
(16C/32T)**, `-d 64`, in-memory (see [`benchmarks/README.md`](benchmarks/README.md)
for full methodology and caveats):

| Test | ferric-cache | Redis |
|------|-------------:|------:|
| Pipelined `-P16 -c50` — GET | **474K req/s** | 332K req/s |
| Pipelined `-P16 -c50` — SET | **452K req/s** | 303K req/s |
| Non-pipelined `-P1 -c50` — GET | **33.8K req/s** | 22.8K req/s |
| High concurrency `-P1 -c500` — GET | **41.1K req/s** | 32.0K req/s |

**Honest read:** ferric-cache leads all three regimes. The **pipelined** result
(~40–55% ahead) is the most credible — with the network round-trip amortized it's
a genuine server-CPU comparison. The non-pipelined and high-concurrency results
also favor ferric-cache but are partly inflated by a network asymmetry
(ferric-cache runs natively on the host; Redis is reached through Docker NAT), so
treat those as directional. The earlier README's "280K/320K" figures were
*targets*, not measurements, and have been removed.

> The pipelined numbers are only this high because the server **batches all
> replies from one read into a single socket write** and sets `TCP_NODELAY`;
> writing/flushing per command (an earlier bug) cut pipelined throughput almost
> in half.

Reproduce it:

```bash
cargo bench                            # in-process Criterion micro-benchmarks
./benchmarks/compare_redis.sh          # native redis-server + redis-benchmark
./benchmarks/compare_redis_docker.sh   # Docker Redis (matches the numbers above)
```

## Development Status

### ✅ Completed Features

**Phase 1: Core Engine**
- [x] High-performance storage with RwLock
- [x] TTL support with automatic cleanup
- [x] Zero-copy binary protocol
- [x] Async TCP server with Tokio

**Phase 2: Persistence**
- [x] Write-Ahead Log (WAL)
- [x] Snapshot support
- [x] Automatic WAL rotation
- [x] Configurable sync policies

**Phase 3: Clustering**
- [x] Consistent hash ring
- [x] Gossip protocol for discovery
- [x] Node health monitoring
- [x] Automatic failover detection

**Phase 4: Data Structures**
- [x] Hash tables (Redis-compatible)
- [x] Lists with full operations
- [x] Sets with set operations
- [x] Sorted sets with scoring

**Phase 5: Security & Encryption**
- [x] User authentication
- [x] Session management
- [x] ACL with command permissions
- [x] Key pattern restrictions
- [x] TLS encryption with dual ports
- [x] Certificate-based security

**Week 9: High Availability - Replication**
- [x] Master-replica replication protocol
- [x] Asynchronous command replication
- [x] Automatic replica reconnection
- [x] Replication offset tracking
- [x] Configuration-based role assignment

### 🚧 Roadmap

**Phase 1: Production Clustering**
- [ ] **Client Routing**: MOVED/ASK redirects for proper request routing
- [ ] **Automatic Failover**: Promote replicas when masters fail
- [ ] **Smart Client**: Connection pooling with cached slot mappings

**Phase 2: Advanced Features**
- [ ] **Slot Migration**: Online resharding when adding/removing nodes
- [ ] **Consensus Protocol**: Raft/Paxos for split-brain prevention
- [ ] **Read Replicas**: Scale read operations independently
- [ ] **Cross-DC Replication**: Geographic distribution support

**Phase 3: Operations & Monitoring**
- [ ] **Redis Protocol**: Drop-in replacement compatibility
- [ ] **Prometheus Metrics**: Detailed performance monitoring
- [ ] **Health Dashboard**: Real-time cluster visualization
- [ ] **Kubernetes Operator**: Automated deployment and scaling

**Phase 4: Ecosystem**
- [ ] **Client Libraries**: Python, Go, Node.js, Java SDKs
- [ ] **Admin CLI**: Cluster management tool
- [ ] **Backup/Restore**: Point-in-time recovery
- [ ] **Change Data Capture**: Stream changes to other systems

## CLI Options

```
cache [OPTIONS]

Options:
  -p, --port <PORT>                   Cache server port [default: 7777]
  -c, --cluster-port <CLUSTER_PORT>   Cluster communication port
  -n, --node-id <NODE_ID>             Node ID for clustering
  -j, --join <JOIN>                   Seed node address to join
      --config <CONFIG>               Configuration file [default: config.json]
  -h, --help                          Print help
```

## Environment Variables

```bash
CONFIG_PATH=custom.json    # Override config file path
CACHE_ADDR=0.0.0.0:8000   # Override server address
```

## Production Deployment

For production use:

1. Use `config.production.json` with persistence enabled
2. Set appropriate memory limits in configuration
3. Enable authentication and configure ACL rules
4. Generate TLS certificates and enable encryption
5. Use systemd or Docker for process management
6. Monitor with health check endpoints

### TLS Certificate Setup

```bash
# Generate self-signed certificates for testing
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes

# For production, use certificates from a trusted CA
mkdir -p ./certs
cp your-server.crt ./certs/server.crt
cp your-server.key ./certs/server.key
chmod 600 ./certs/server.key
```

## License

Apache 2.0
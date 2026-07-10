# Ferric Cache MVP - Development Plan (KISS)

## Goal: Build the **fastest cache** with minimal features to validate performance claims

### Performance Target: **Beat Redis by 3x** (300K+ ops/sec vs Redis 100K)

---

## Phase 1: Core Cache Engine (Week 1-2)

### Features (KISS):
- **GET/SET/DELETE** only
- **In-memory storage** only
- **Single-threaded** event loop
- **No clustering** (single node)
- **No persistence** (pure memory)
- **Basic TTL** expiration

### Technology Stack:
```toml
# Cargo.toml - Minimal dependencies
[dependencies]
tokio = { version = "1.35", features = ["full"] }
parking_lot = "0.12"  # Faster RwLock than std
xxhash-rust = "0.8"   # Fast hashing
bytes = "1.5"         # Zero-copy byte handling
serde = { version = "1.0", features = ["derive"] }
```

### Core Architecture:
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   TCP Server    │───►│  Command Parser │───►│  Hash Storage   │
│  (Tokio + Mio)  │    │   (Zero-copy)   │    │ (RwLock + TTL)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Implementation Tasks:

#### Day 1-2: Project Setup
```bash
# Project structure
cache/
├── Cargo.toml
├── src/
│   ├── main.rs           # Server entry point
│   ├── storage.rs        # Hash map + TTL
│   ├── protocol.rs       # Binary protocol
│   ├── server.rs         # TCP server
│   └── commands.rs       # GET/SET/DELETE
├── benches/
│   └── performance.rs    # Benchmarks
├── tests/
│   └── integration.rs    # Basic tests
└── README.md
```

**Deliverable**: Rust project with basic structure

#### Day 3-4: Storage Engine
```rust
// storage.rs - Ultra-simple hash storage
use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use bytes::Bytes;

pub struct CacheStorage {
    data: RwLock<HashMap<Bytes, CacheEntry>>,
}

struct CacheEntry {
    value: Bytes,
    expires_at: Option<Instant>,
}

impl CacheStorage {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
    
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let guard = self.data.read();
        let entry = guard.get(key)?;
        
        // Check expiration
        if let Some(expires) = entry.expires_at {
            if Instant::now() > expires {
                return None;
            }
        }
        
        Some(entry.value.clone())
    }
    
    pub fn set(&self, key: Bytes, value: Bytes, ttl: Option<Duration>) -> bool {
        let expires_at = ttl.map(|d| Instant::now() + d);
        let entry = CacheEntry { value, expires_at };
        
        let mut guard = self.data.write();
        guard.insert(key, entry);
        true
    }
    
    pub fn delete(&self, key: &[u8]) -> bool {
        let mut guard = self.data.write();
        guard.remove(key).is_some()
    }
}
```

**Deliverable**: Working storage with GET/SET/DELETE + TTL

#### Day 5-6: Binary Protocol
```rust
// protocol.rs - Minimal binary protocol
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub enum Command {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes, ttl_secs: Option<u32> },
    Delete { key: Bytes },
}

#[derive(Debug)]
pub enum Response {
    Value(Bytes),
    Ok,
    NotFound,
    Error(String),
}

// Simple protocol: [CMD_TYPE:1][KEY_LEN:4][VALUE_LEN:4][TTL:4][KEY][VALUE]
pub fn parse_command(mut buf: Bytes) -> Result<Command, String> {
    if buf.len() < 13 { // Minimum: 1+4+4+4 bytes
        return Err("Invalid command length".to_string());
    }
    
    let cmd_type = buf.get_u8();
    let key_len = buf.get_u32() as usize;
    let value_len = buf.get_u32() as usize;
    let ttl_secs = buf.get_u32();
    
    if buf.len() < key_len + value_len {
        return Err("Buffer too short".to_string());
    }
    
    let key = buf.split_to(key_len);
    
    match cmd_type {
        1 => Ok(Command::Get { key }),
        2 => {
            let value = buf.split_to(value_len);
            let ttl = if ttl_secs > 0 { 
                Some(ttl_secs) 
            } else { 
                None 
            };
            Ok(Command::Set { key, value, ttl })
        },
        3 => Ok(Command::Delete { key }),
        _ => Err("Unknown command".to_string()),
    }
}

pub fn serialize_response(response: Response) -> Bytes {
    let mut buf = BytesMut::new();
    
    match response {
        Response::Value(value) => {
            buf.put_u8(1); // Value response
            buf.put_u32(value.len() as u32);
            buf.put(value);
        },
        Response::Ok => {
            buf.put_u8(2); // OK response
            buf.put_u32(0);
        },
        Response::NotFound => {
            buf.put_u8(3); // Not found
            buf.put_u32(0);
        },
        Response::Error(msg) => {
            buf.put_u8(4); // Error
            buf.put_u32(msg.len() as u32);
            buf.put(msg.as_bytes());
        },
    }
    
    buf.freeze()
}
```

**Deliverable**: Binary protocol for maximum performance

#### Day 7-8: TCP Server
```rust
// server.rs - Single-threaded TCP server
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

pub struct CacheServer {
    storage: Arc<CacheStorage>,
    addr: String,
}

impl CacheServer {
    pub fn new(addr: String) -> Self {
        Self {
            storage: Arc::new(CacheStorage::new()),
            addr,
        }
    }
    
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("Ferric Cache listening on {}", self.addr);
        
        loop {
            let (stream, _) = listener.accept().await?;
            let storage = Arc::clone(&self.storage);
            
            // Handle each connection
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, storage).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<CacheStorage>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = vec![0u8; 4096];
    
    loop {
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            break; // Connection closed
        }
        
        let command_buf = Bytes::copy_from_slice(&buffer[..bytes_read]);
        
        // Parse and execute command
        let response = match parse_command(command_buf) {
            Ok(Command::Get { key }) => {
                match storage.get(&key) {
                    Some(value) => Response::Value(value),
                    None => Response::NotFound,
                }
            },
            Ok(Command::Set { key, value, ttl }) => {
                let ttl_duration = ttl.map(|s| Duration::from_secs(s as u64));
                storage.set(key, value, ttl_duration);
                Response::Ok
            },
            Ok(Command::Delete { key }) => {
                if storage.delete(&key) {
                    Response::Ok
                } else {
                    Response::NotFound
                }
            },
            Err(e) => Response::Error(e),
        };
        
        // Send response
        let response_bytes = serialize_response(response);
        stream.write_all(&response_bytes).await?;
    }
    
    Ok(())
}
```

**Deliverable**: Working TCP server with command processing

#### Day 9-10: Basic Client + Benchmarks
```rust
// Simple Rust client for testing
pub struct FerricClient {
    stream: TcpStream,
}

impl FerricClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }
    
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Box<dyn std::error::Error>> {
        let cmd = Command::Get { key: Bytes::from(key) };
        self.send_command(cmd).await
    }
    
    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        let cmd = Command::Set { 
            key: Bytes::from(key), 
            value: Bytes::from(value),
            ttl: None 
        };
        self.send_command(cmd).await
    }
}
```

```rust
// benches/performance.rs - Benchmark vs Redis
use criterion::{criterion_group, criterion_main, Criterion};

fn benchmark_get_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    c.bench_function("ferric_get", |b| {
        b.to_async(&rt).iter(|| async {
            let mut client = FerricClient::connect("127.0.0.1:7777").await.unwrap();
            client.get("test_key").await.unwrap();
        });
    });
}

criterion_group!(benches, benchmark_get_operations);
criterion_main!(benches);
```

**Deliverable**: Basic client + performance benchmarks

---

## Phase 2: Performance Optimization (Week 3)

### Focus: **Squeeze every microsecond**

#### Week 3 Tasks:

**Day 11-12: Memory Optimization**
- Replace `HashMap` with `FxHashMap` (faster hash function)
- Use `Bytes` for zero-copy operations everywhere
- Memory pool for frequent allocations

**Day 13-14: Protocol Optimization**
- Implement connection pooling
- Batch operations support
- Pipeline multiple commands

**Day 15: Benchmarking**
- Load testing vs Redis
- Memory usage profiling
- Latency distribution analysis

### Performance Targets:
- **Throughput**: 300K+ GET ops/sec (vs Redis 100K)
- **Latency**: P99 < 1ms
- **Memory**: 50% less than Redis for same dataset

---

## Phase 3: Basic Redis Compatibility (Week 4)

### Add Essential Redis Commands:
- **String operations**: GET, SET, DEL, EXISTS, INCR, DECR
- **Hash operations**: HGET, HSET, HDEL, HGETALL
- **List operations**: LPUSH, RPUSH, LPOP, RPOP, LLEN
- **Info commands**: PING, INFO, KEYS (development only)

### Redis Protocol Support:
```rust
// Add RESP (Redis Serialization Protocol) parser
fn parse_resp_command(data: &[u8]) -> Result<Command, String> {
    // Parse Redis protocol for drop-in compatibility
    // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
}
```

---

## Success Metrics (Week 4 End):

### Performance Benchmarks:
- [ ] **300K+ ops/sec** (3x Redis performance)
- [ ] **P99 latency < 1ms**
- [ ] **Memory efficiency 50% better** than Redis
- [ ] **Zero crashes** in 24-hour load test

### Market Validation:
- [ ] **Drop-in Redis replacement** (basic commands)
- [ ] **Performance comparison** published
- [ ] **Docker image** available
- [ ] **Basic documentation** complete

---

## Development Guidelines (KISS):

### **DO:**
- Use `cargo fmt` and `cargo clippy` religiously
- Write benchmarks for every optimization
- Profile before optimizing (`perf`, `flamegraph`)
- Keep dependencies minimal
- Document performance claims with data

### **DON'T:**
- Add features without performance justification
- Optimize prematurely (measure first)
- Use complex algorithms without benchmarking
- Add dependencies without careful consideration
- Compromise performance for convenience

### **Performance-First Mindset:**
1. **Measure** current performance
2. **Identify** bottleneck
3. **Optimize** with data
4. **Benchmark** improvement
5. **Repeat**

---

## Week 4 Decision Point:

### If performance targets met:
**→ Continue with full product development**
- Clustering support
- Persistence options  
- Enterprise features
- Go-to-market strategy

### If performance targets missed:
**→ Deep optimization or pivot**
- Profile and optimize hot paths
- Consider different approach
- Evaluate market viability

---

## MVP Repository Structure:

```
cache/
├── Cargo.toml
├── README.md              # Performance claims + benchmarks
├── LICENSE               # Apache 2.0 for open source
├── src/
│   ├── main.rs           # Server binary
│   ├── lib.rs            # Library exports
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── memory.rs     # In-memory storage
│   │   └── ttl.rs        # TTL management
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── binary.rs     # Custom binary protocol
│   │   └── redis.rs      # Redis RESP protocol
│   ├── server/
│   │   ├── mod.rs
│   │   └── tcp.rs        # TCP server
│   └── client/
│       ├── mod.rs
│       └── rust.rs       # Rust client library
├── benches/
│   ├── vs_redis.rs       # Redis comparison
│   ├── throughput.rs     # Max ops/sec
│   └── latency.rs        # P99 latency
├── tests/
│   ├── integration.rs    # End-to-end tests
│   ├── compatibility.rs  # Redis compatibility
│   └── stress.rs         # Stress testing
├── examples/
│   ├── basic_usage.rs
│   └── performance_test.rs
├── docker/
│   └── Dockerfile
└── docs/
    ├── PERFORMANCE.md    # Benchmark results
    ├── PROTOCOL.md       # Protocol specification
    └── USAGE.md          # Getting started
```

This KISS approach gets you a **working, benchmarkable cache in 4 weeks** with clear validation of performance claims. No feature creep, just pure speed.
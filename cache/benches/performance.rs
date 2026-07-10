//! Criterion micro-benchmarks for ferric-cache.
//!
//! Unlike the original version, this harness:
//!   1. Starts an in-process server automatically (no external server needed).
//!   2. Reuses a single pooled connection per benchmark, so it measures
//!      steady-state op latency rather than TCP connect + handshake + op.
//!
//! These are single-process micro-benchmarks — for a head-to-head throughput
//! comparison against Redis, use the `redis-benchmark` harness under
//! `benchmarks/` (see `benchmarks/README.md`).

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use ferric_cache::{FerricClient, CacheServer};
use std::sync::Once;
use tokio::runtime::Runtime;

const BENCH_ADDR: &str = "127.0.0.1:7955";

/// Start the in-process server exactly once for the whole benchmark run.
fn ensure_server() {
    static START: Once = Once::new();
    START.call_once(|| {
        std::thread::spawn(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let server = CacheServer::new(BENCH_ADDR.to_string());
                let _ = server.run().await;
            });
        });
        std::thread::sleep(std::time::Duration::from_millis(300));
    });
}

async fn connect() -> FerricClient {
    for _ in 0..50 {
        if let Ok(c) = FerricClient::connect(BENCH_ADDR).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    FerricClient::connect(BENCH_ADDR).await.expect("connect to bench server")
}

fn benchmark_get_set_delete(c: &mut Criterion) {
    ensure_server();
    let rt = Runtime::new().unwrap();

    // One reused connection + preloaded data.
    let mut client = rt.block_on(async {
        let mut client = connect().await;
        for i in 0..1000 {
            client.set(&format!("bench_key_{}", i), &format!("bench_value_{}", i))
                .await
                .expect("preload set");
        }
        client
    });

    let mut group = c.benchmark_group("cache");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get", |b| {
        b.iter(|| rt.block_on(async {
            client.get("bench_key_42").await.expect("get");
        }));
    });

    group.bench_function("set", |b| {
        b.iter(|| rt.block_on(async {
            client.set("bench_key_new", "bench_value_new").await.expect("set");
        }));
    });

    group.bench_function("delete", |b| {
        b.iter(|| rt.block_on(async {
            // Re-create then delete so the op does real work each iteration.
            client.set("bench_del", "v").await.expect("set");
            client.delete("bench_del").await.expect("delete");
        }));
    });

    group.finish();
}

fn benchmark_concurrent_get(c: &mut Criterion) {
    ensure_server();
    let rt = Runtime::new().unwrap();

    // Preload once.
    rt.block_on(async {
        let mut client = connect().await;
        for i in 0..100 {
            client.set(&format!("concurrent_key_{}", i), "v").await.expect("preload");
        }
    });

    let mut group = c.benchmark_group("concurrent_get");
    for num_clients in [1usize, 10, 50, 100].iter() {
        group.throughput(Throughput::Elements(*num_clients as u64 * 10));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_clients),
            num_clients,
            |b, &num_clients| {
                b.iter(|| rt.block_on(async {
                    let handles: Vec<_> = (0..num_clients)
                        .map(|_| tokio::spawn(async move {
                            let mut client = connect().await;
                            for j in 0..10 {
                                client.get(&format!("concurrent_key_{}", j)).await.expect("get");
                            }
                        }))
                        .collect();
                    for h in handles {
                        h.await.expect("task");
                    }
                }));
            },
        );
    }
    group.finish();
}

fn benchmark_large_values(c: &mut Criterion) {
    ensure_server();
    let rt = Runtime::new().unwrap();
    let mut client = rt.block_on(connect());

    let mut group = c.benchmark_group("large_values");
    for size in [1024usize, 10240, 102400].iter() {
        let value = "x".repeat(*size);
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &value, |b, value| {
            b.iter(|| rt.block_on(async {
                client.set("large_value_key", value).await.expect("set");
            }));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_get_set_delete,
    benchmark_concurrent_get,
    benchmark_large_values
);
criterion_main!(benches);

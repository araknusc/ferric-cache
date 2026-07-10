use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use ferric_cache::FerricClient;
use tokio::runtime::Runtime;

fn benchmark_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Prepare test data
    rt.block_on(async {
        let mut client = FerricClient::connect("127.0.0.1:7000")
            .await
            .expect("Failed to connect to server. Make sure server is running on port 7000");

        for i in 0..100 {
            let key = format!("bench_key_{}", i);
            let value = format!("bench_value_{}", i);
            client.set(&key, &value).await.expect("Failed to set");
        }
    });

    let mut group = c.benchmark_group("cache_ops");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut client = FerricClient::connect("127.0.0.1:7000")
                    .await
                    .expect("Failed to connect");
                client.get("bench_key_42").await.expect("Failed to get");
            })
        });
    });

    group.bench_function("set", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut client = FerricClient::connect("127.0.0.1:7000")
                    .await
                    .expect("Failed to connect");
                client.set("bench_key_new", "bench_value_new")
                    .await
                    .expect("Failed to set");
            })
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_operations);
criterion_main!(benches);
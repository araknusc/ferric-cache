use ferric_cache::{FerricClient, CacheServer};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_basic_get_set() {
    start_test_server().await;
    sleep(Duration::from_millis(100)).await;

    let mut client = FerricClient::connect("127.0.0.1:7777")
        .await
        .expect("Failed to connect");

    // Test SET
    client.set("test_key", "test_value")
        .await
        .expect("Failed to set");

    // Test GET
    let value = client.get("test_key")
        .await
        .expect("Failed to get");

    assert_eq!(value, Some(bytes::Bytes::from("test_value")));

    // Test GET non-existent key
    let value = client.get("non_existent")
        .await
        .expect("Failed to get");

    assert_eq!(value, None);
}

#[tokio::test]
async fn test_delete() {
    start_test_server().await;
    sleep(Duration::from_millis(100)).await;

    let mut client = FerricClient::connect("127.0.0.1:7778")
        .await
        .expect("Failed to connect");

    // Set a key
    client.set("delete_test", "value")
        .await
        .expect("Failed to set");

    // Verify it exists
    let value = client.get("delete_test")
        .await
        .expect("Failed to get");
    assert!(value.is_some());

    // Delete it
    let deleted = client.delete("delete_test")
        .await
        .expect("Failed to delete");
    assert!(deleted);

    // Verify it's gone
    let value = client.get("delete_test")
        .await
        .expect("Failed to get");
    assert!(value.is_none());

    // Delete non-existent key
    let deleted = client.delete("non_existent")
        .await
        .expect("Failed to delete");
    assert!(!deleted);
}

#[tokio::test]
async fn test_ttl() {
    start_test_server().await;
    sleep(Duration::from_millis(100)).await;

    let mut client = FerricClient::connect("127.0.0.1:7779")
        .await
        .expect("Failed to connect");

    // Set with TTL
    client.set_with_ttl("ttl_test", "expiring_value", Some(Duration::from_secs(1)))
        .await
        .expect("Failed to set with TTL");

    // Should exist immediately
    let value = client.get("ttl_test")
        .await
        .expect("Failed to get");
    assert!(value.is_some());

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;

    // Should be expired
    let value = client.get("ttl_test")
        .await
        .expect("Failed to get");
    assert!(value.is_none());
}

#[tokio::test]
async fn test_concurrent_operations() {
    start_test_server().await;
    sleep(Duration::from_millis(100)).await;

    let handles: Vec<_> = (0..10)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = FerricClient::connect("127.0.0.1:7780")
                    .await
                    .expect("Failed to connect");

                let key = format!("concurrent_{}", i);
                let value = format!("value_{}", i);

                client.set(&key, &value)
                    .await
                    .expect("Failed to set");

                let retrieved = client.get(&key)
                    .await
                    .expect("Failed to get");

                assert_eq!(retrieved, Some(bytes::Bytes::from(value)));
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Task failed");
    }
}

async fn start_test_server() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let server = CacheServer::new("127.0.0.1:7777".to_string());
                let _ = server.run().await;
            });
        });

        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let server = CacheServer::new("127.0.0.1:7778".to_string());
                let _ = server.run().await;
            });
        });

        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let server = CacheServer::new("127.0.0.1:7779".to_string());
                let _ = server.run().await;
            });
        });

        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let server = CacheServer::new("127.0.0.1:7780".to_string());
                let _ = server.run().await;
            });
        });
    });
}
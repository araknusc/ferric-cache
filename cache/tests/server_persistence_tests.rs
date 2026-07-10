use ferric_cache::{CacheServer, PersistenceConfig, PersistenceMode, WALSyncPolicy, FerricClient};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::sleep;
use std::fs;

async fn create_test_server_config(test_name: &str) -> PersistenceConfig {
    let temp_dir = std::env::temp_dir();

    PersistenceConfig {
        mode: PersistenceMode::Both,
        wal_sync_policy: WALSyncPolicy::Always,
        snapshot_interval_secs: 0, // Disable automatic snapshots for tests
        max_wal_size_bytes: 1024 * 1024, // 1MB
        wal_path: format!("{}/server_test_{}.wal", temp_dir.display(), test_name),
        snapshot_path: format!("{}/server_test_{}.snapshot", temp_dir.display(), test_name),
        wal_backup_count: 1,
    }
}

async fn cleanup_server_test_files(config: &PersistenceConfig) {
    let _ = fs::remove_file(&config.wal_path);
    let _ = fs::remove_file(&config.snapshot_path);
    let _ = fs::remove_file(format!("{}.tmp", config.snapshot_path));
}

#[tokio::test]
async fn test_server_with_persistence() {
    let config = create_test_server_config("server_basic").await;
    cleanup_server_test_files(&config).await;

    let port = 8101;
    let addr = format!("127.0.0.1:{}", port);

    // Start server with persistence
    let server = CacheServer::with_persistence(addr.clone(), config.clone())
        .await
        .expect("Failed to create server with persistence");

    // Start server in background
    tokio::spawn(async move {
        server.run().await.expect("Server failed to run");
    });

    // Give server time to start
    sleep(Duration::from_millis(200)).await;

    // Connect client and perform operations
    let mut client = FerricClient::connect(&addr)
        .await
        .expect("Failed to connect to server");

    // Test SET operation
    client.set("test_key1", "test_value1")
        .await
        .expect("Failed to set key1");

    client.set("test_key2", "test_value2")
        .await
        .expect("Failed to set key2");

    // Test GET operation
    let value1 = client.get("test_key1")
        .await
        .expect("Failed to get key1");
    assert_eq!(value1, Some(Bytes::from("test_value1")));

    let value2 = client.get("test_key2")
        .await
        .expect("Failed to get key2");
    assert_eq!(value2, Some(Bytes::from("test_value2")));

    // Test DELETE operation
    let deleted = client.delete("test_key1")
        .await
        .expect("Failed to delete key1");
    assert!(deleted);

    // Verify key is deleted
    let value1_after_delete = client.get("test_key1")
        .await
        .expect("Failed to get deleted key");
    assert_eq!(value1_after_delete, None);

    // But key2 should still exist
    let value2_after_delete = client.get("test_key2")
        .await
        .expect("Failed to get key2 after delete");
    assert_eq!(value2_after_delete, Some(Bytes::from("test_value2")));

    cleanup_server_test_files(&config).await;
}

#[tokio::test]
async fn test_server_persistence_recovery() {
    let config = create_test_server_config("server_recovery").await;
    cleanup_server_test_files(&config).await;

    let port = 8102;
    let addr = format!("127.0.0.1:{}", port);

    // Phase 1: Start server and populate data
    {
        let server = CacheServer::with_persistence(addr.clone(), config.clone())
            .await
            .expect("Failed to create server with persistence");

        tokio::spawn(async move {
            server.run().await.expect("Server failed to run");
        });

        sleep(Duration::from_millis(200)).await;

        let mut client = FerricClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Add test data
        client.set("persistent_key1", "persistent_value1").await.expect("Failed to set");
        client.set("persistent_key2", "persistent_value2").await.expect("Failed to set");
        client.set("persistent_key3", "persistent_value3").await.expect("Failed to set");

        // Verify data exists
        assert_eq!(
            client.get("persistent_key1").await.expect("Failed to get"),
            Some(Bytes::from("persistent_value1"))
        );

        // Give time for WAL to sync
        sleep(Duration::from_millis(100)).await;
    }

    // Phase 2: Start new server instance and verify data recovery
    {
        let server = CacheServer::with_persistence(addr.clone(), config.clone())
            .await
            .expect("Failed to create recovery server");

        // Load data from persistence
        server.load_from_persistence()
            .await
            .expect("Failed to load from persistence");

        tokio::spawn(async move {
            server.run().await.expect("Recovery server failed to run");
        });

        sleep(Duration::from_millis(200)).await;

        let mut client = FerricClient::connect(&addr)
            .await
            .expect("Failed to connect to recovery server");

        // Verify all data was recovered
        assert_eq!(
            client.get("persistent_key1").await.expect("Failed to get key1"),
            Some(Bytes::from("persistent_value1"))
        );
        assert_eq!(
            client.get("persistent_key2").await.expect("Failed to get key2"),
            Some(Bytes::from("persistent_value2"))
        );
        assert_eq!(
            client.get("persistent_key3").await.expect("Failed to get key3"),
            Some(Bytes::from("persistent_value3"))
        );

        // Test that we can still perform operations after recovery
        client.set("new_key", "new_value").await.expect("Failed to set new key");
        assert_eq!(
            client.get("new_key").await.expect("Failed to get new key"),
            Some(Bytes::from("new_value"))
        );
    }

    cleanup_server_test_files(&config).await;
}

#[tokio::test]
async fn test_server_without_persistence() {
    let port = 8103;
    let addr = format!("127.0.0.1:{}", port);

    // Start server without persistence (original behavior)
    let server = CacheServer::new(addr.clone());

    tokio::spawn(async move {
        server.run().await.expect("Server failed to run");
    });

    sleep(Duration::from_millis(200)).await;

    let mut client = FerricClient::connect(&addr)
        .await
        .expect("Failed to connect to server");

    // Test that basic operations still work
    client.set("non_persistent_key", "non_persistent_value")
        .await
        .expect("Failed to set");

    let value = client.get("non_persistent_key")
        .await
        .expect("Failed to get");
    assert_eq!(value, Some(Bytes::from("non_persistent_value")));

    let deleted = client.delete("non_persistent_key")
        .await
        .expect("Failed to delete");
    assert!(deleted);

    let value_after_delete = client.get("non_persistent_key")
        .await
        .expect("Failed to get after delete");
    assert_eq!(value_after_delete, None);
}
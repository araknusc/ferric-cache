use ferric_cache::{WriteAheadLog, SnapshotManager, PersistenceConfig, PersistenceMode, WALSyncPolicy, CacheStorage};
use ferric_cache::protocol::Command;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use std::fs;

async fn create_test_config(test_name: &str) -> PersistenceConfig {
    // Use temp directory for tests
    let temp_dir = std::env::temp_dir();

    PersistenceConfig {
        mode: PersistenceMode::Both,
        wal_sync_policy: WALSyncPolicy::Always,
        snapshot_interval_secs: 0, // Disable automatic snapshots for tests
        max_wal_size_bytes: 1024 * 1024, // 1MB
        wal_path: format!("{}/test_{}.wal", temp_dir.display(), test_name),
        snapshot_path: format!("{}/test_{}.snapshot", temp_dir.display(), test_name),
        wal_backup_count: 1,
    }
}

async fn cleanup_test_files(config: &PersistenceConfig) {
    let _ = fs::remove_file(&config.wal_path);
    let _ = fs::remove_file(&config.snapshot_path);
    let _ = fs::remove_file(format!("{}.tmp", config.snapshot_path));
    // Rotated WAL backup segments (cache.wal.1, .2, ...)
    for n in 1..=12 {
        let _ = fs::remove_file(format!("{}.{}", config.wal_path, n));
    }

    // Clean up any rotated WAL files
    if let Ok(entries) = fs::read_dir(".") {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("test_") && (name.contains(".wal.") || name.contains(".snapshot.")) {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }
}

#[tokio::test]
async fn test_wal_basic_operations() {
    let config = create_test_config("wal_basic").await;
    cleanup_test_files(&config).await;

    let wal = WriteAheadLog::new(&config).await.expect("Failed to create WAL");

    // Test appending commands
    let set_cmd = Command::Set {
        key: Bytes::from("test_key"),
        value: Bytes::from("test_value"),
        ttl_secs: Some(3600),
    };

    let seq1 = wal.append_command(&set_cmd).await.expect("Failed to append SET");
    assert_eq!(seq1, 0);

    let get_cmd = Command::Get {
        key: Bytes::from("test_key"),
    };

    let seq2 = wal.append_command(&get_cmd).await.expect("Failed to append GET");
    assert_eq!(seq2, 1);

    let delete_cmd = Command::Delete {
        key: Bytes::from("test_key"),
    };

    let seq3 = wal.append_command(&delete_cmd).await.expect("Failed to append DELETE");
    assert_eq!(seq3, 2);

    // Ensure data is synced
    wal.sync().await.expect("Failed to sync WAL");

    cleanup_test_files(&config).await;
}

#[tokio::test]
async fn test_wal_replay() {
    let config = create_test_config("wal_replay").await;
    cleanup_test_files(&config).await;

    // Create and populate WAL
    {
        let wal = WriteAheadLog::new(&config).await.expect("Failed to create WAL");

        let commands = vec![
            Command::Set {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
                ttl_secs: None,
            },
            Command::Set {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
                ttl_secs: Some(3600),
            },
            Command::Delete {
                key: Bytes::from("key3"),
            },
        ];

        for cmd in commands {
            wal.append_command(&cmd).await.expect("Failed to append command");
        }
        wal.sync().await.expect("Failed to sync");
    }

    // Replay WAL
    let new_wal = WriteAheadLog::new(&config).await.expect("Failed to create new WAL");
    let mut replayed_commands = Vec::new();

    let entries_replayed = new_wal.replay(|entry| {
        replayed_commands.push(entry.command.clone());
        Ok(())
    }).await.expect("Failed to replay WAL");

    assert_eq!(entries_replayed, 3);
    assert_eq!(replayed_commands.len(), 3);

    // Verify commands
    match &replayed_commands[0] {
        Command::Set { key, value, ttl_secs } => {
            assert_eq!(key, &Bytes::from("key1"));
            assert_eq!(value, &Bytes::from("value1"));
            assert_eq!(*ttl_secs, None);
        },
        _ => panic!("Expected SET command"),
    }

    match &replayed_commands[1] {
        Command::Set { key, value, ttl_secs } => {
            assert_eq!(key, &Bytes::from("key2"));
            assert_eq!(value, &Bytes::from("value2"));
            assert_eq!(*ttl_secs, Some(3600));
        },
        _ => panic!("Expected SET command"),
    }

    match &replayed_commands[2] {
        Command::Delete { key } => {
            assert_eq!(key, &Bytes::from("key3"));
        },
        _ => panic!("Expected DELETE command"),
    }

    cleanup_test_files(&config).await;
}

#[tokio::test]
async fn test_wal_rotation() {
    let mut config = create_test_config("wal_rotation").await;
    config.max_wal_size_bytes = 1024; // Small size to trigger rotation
    cleanup_test_files(&config).await;

    let wal = WriteAheadLog::new(&config).await.expect("Failed to create WAL");

    // Add enough data to trigger rotation
    let large_value = "x".repeat(500);
    for i in 0..10 {
        let cmd = Command::Set {
            key: Bytes::from(format!("key_{}", i)),
            value: Bytes::from(large_value.clone()),
            ttl_secs: None,
        };
        wal.append_command(&cmd).await.expect("Failed to append command");
    }

    // Check if rotation is needed
    if wal.should_rotate() {
        wal.rotate().await.expect("Failed to rotate WAL");
    }

    // Verify we can still append after rotation
    let cmd = Command::Set {
        key: Bytes::from("after_rotation"),
        value: Bytes::from("test"),
        ttl_secs: None,
    };
    wal.append_command(&cmd).await.expect("Failed to append after rotation");

    cleanup_test_files(&config).await;
}

/// Regression test for the WAL-rotation data-loss bug: rotation used to
/// truncate the log with no backup, so in WAL-only mode every write before the
/// size limit was silently lost. Rotation must now preserve prior writes in a
/// backup segment, and replay must read both the segment and the active file.
#[tokio::test]
async fn test_wal_rotation_preserves_data() {
    let mut config = create_test_config("wal_rotation_preserve").await;
    config.max_wal_size_bytes = 1024; // small, to force several rotations
    config.wal_backup_count = 10; // retain enough segments to keep all writes
    cleanup_test_files(&config).await;

    {
        let wal = WriteAheadLog::new(&config).await.expect("create WAL");
        // Write enough large values that a rotation happens partway through.
        let big = "x".repeat(300);
        for i in 0..12 {
            wal.append_command(&Command::Set {
                key: Bytes::from(format!("key_{}", i)),
                value: Bytes::from(big.clone()),
                ttl_secs: None,
            }).await.expect("append");
        }
        wal.sync().await.expect("sync");
    }

    // Reopen and replay everything (segments + active). Every key must survive.
    let wal = WriteAheadLog::new(&config).await.expect("reopen WAL");
    let mut seen = std::collections::HashSet::new();
    wal.replay(|entry| {
        if let Command::Set { key, .. } = &entry.command {
            seen.insert(String::from_utf8_lossy(key).to_string());
        }
        Ok(())
    }).await.expect("replay");

    for i in 0..12 {
        assert!(seen.contains(&format!("key_{}", i)),
            "key_{} lost across rotation (seen {} keys)", i, seen.len());
    }

    cleanup_test_files(&config).await;
}

/// Regression test for the v1 snapshot data-loss bug: the old snapshot only
/// serialized `String` values, so hashes/lists/sets/sorted-sets/streams were
/// silently dropped — and in `Both` mode the post-snapshot WAL truncation then
/// destroyed the only durable copy. The v2 snapshot must round-trip every type.
#[tokio::test]
async fn test_snapshot_round_trips_all_value_types() {
    let config = create_test_config("snapshot_all_types").await;
    cleanup_test_files(&config).await;

    let storage = Arc::new(CacheStorage::new());
    let snapshot_manager = SnapshotManager::new(Arc::clone(&storage), config.clone());

    // One key of each type.
    storage.set(Bytes::from("str"), Bytes::from("hello"), None);
    storage.hset(Bytes::from("hash"), Bytes::from("f1"), Bytes::from("v1")).unwrap();
    storage.hset(Bytes::from("hash"), Bytes::from("f2"), Bytes::from("v2")).unwrap();
    storage.rpush(Bytes::from("list"), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
    storage.sadd(Bytes::from("set"), vec![Bytes::from("x"), Bytes::from("y")]).unwrap();
    storage.zadd(Bytes::from("zset"), vec![(1.0, Bytes::from("lo")), (2.0, Bytes::from("hi"))]).unwrap();
    storage.xadd(
        Bytes::from("stream"),
        Some(ferric_cache::data_structures::StreamId { ms: 5, seq: 0 }),
        vec![(Bytes::from("field"), Bytes::from("value"))],
    ).unwrap();

    snapshot_manager.save_snapshot().await.expect("Failed to save snapshot");

    // Reload into fresh storage — nothing carried over in memory.
    let restored = Arc::new(CacheStorage::new());
    let restore_mgr = SnapshotManager::new(Arc::clone(&restored), config.clone());
    let loaded = restore_mgr.load_snapshot().await.expect("Failed to load snapshot");
    assert_eq!(loaded, 6, "one record per key of each type");

    assert_eq!(restored.get(b"str"), Some(Bytes::from("hello")));
    assert_eq!(restored.hget(b"hash", b"f1"), Some(Bytes::from("v1")));
    assert_eq!(restored.hget(b"hash", b"f2"), Some(Bytes::from("v2")));
    assert_eq!(restored.lrange(b"list", 0, -1).unwrap(),
        vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
    assert!(restored.sismember(b"set", b"x").unwrap());
    assert!(restored.sismember(b"set", b"y").unwrap());
    assert_eq!(restored.zscore(b"zset", b"lo").unwrap(), Some(1.0));
    assert_eq!(restored.zscore(b"zset", b"hi").unwrap(), Some(2.0));
    assert_eq!(restored.xlen(b"stream").unwrap(), 1);

    cleanup_test_files(&config).await;
}

#[tokio::test]
async fn test_snapshot_basic_operations() {
    let config = create_test_config("snapshot_basic").await;
    cleanup_test_files(&config).await;

    let storage = Arc::new(CacheStorage::new());
    let snapshot_manager = SnapshotManager::new(Arc::clone(&storage), config.clone());

    // Populate storage with test data
    storage.set(
        Bytes::from("key1"),
        Bytes::from("value1"),
        None
    );
    storage.set(
        Bytes::from("key2"),
        Bytes::from("value2"),
        Some(Duration::from_secs(3600))
    );
    storage.set(
        Bytes::from("key3"),
        Bytes::from("value3"),
        None
    );

    // Create snapshot
    snapshot_manager.save_snapshot().await.expect("Failed to save snapshot");

    // Clear storage
    storage.delete(b"key1");
    storage.delete(b"key2");
    storage.delete(b"key3");

    // Verify storage is empty
    assert!(storage.get(b"key1").is_none());
    assert!(storage.get(b"key2").is_none());
    assert!(storage.get(b"key3").is_none());

    // Load snapshot
    let loaded_count = snapshot_manager.load_snapshot().await.expect("Failed to load snapshot");
    assert_eq!(loaded_count, 3);

    // Verify data is restored
    assert_eq!(storage.get(b"key1"), Some(Bytes::from("value1")));
    assert_eq!(storage.get(b"key2"), Some(Bytes::from("value2")));
    assert_eq!(storage.get(b"key3"), Some(Bytes::from("value3")));

    cleanup_test_files(&config).await;
}

#[tokio::test]
async fn test_snapshot_with_expiration() {
    let config = create_test_config("snapshot_expiration").await;
    cleanup_test_files(&config).await;

    let storage = Arc::new(CacheStorage::new());
    let snapshot_manager = SnapshotManager::new(Arc::clone(&storage), config.clone());

    // Add data with short expiration
    storage.set(
        Bytes::from("expiring_key"),
        Bytes::from("expiring_value"),
        Some(Duration::from_millis(100))
    );

    storage.set(
        Bytes::from("permanent_key"),
        Bytes::from("permanent_value"),
        None
    );

    // Wait for expiration
    sleep(Duration::from_millis(150)).await;

    // Create snapshot (should only include non-expired data)
    snapshot_manager.save_snapshot().await.expect("Failed to save snapshot");

    // Clear storage
    storage.delete(b"permanent_key");

    // Load snapshot
    let loaded_count = snapshot_manager.load_snapshot().await.expect("Failed to load snapshot");

    // Should only load the permanent key (expired key should be filtered out)
    assert_eq!(loaded_count, 1);
    assert_eq!(storage.get(b"permanent_key"), Some(Bytes::from("permanent_value")));
    assert!(storage.get(b"expiring_key").is_none());

    cleanup_test_files(&config).await;
}

#[tokio::test]
async fn test_persistence_integration() {
    let config = create_test_config("integration").await;
    cleanup_test_files(&config).await;

    let storage = Arc::new(CacheStorage::new());
    let wal = Arc::new(WriteAheadLog::new(&config).await.expect("Failed to create WAL"));
    let snapshot_manager = Arc::new(SnapshotManager::new(Arc::clone(&storage), config.clone()));

    // Simulate normal operations with WAL
    let commands = vec![
        Command::Set {
            key: Bytes::from("user:1"),
            value: Bytes::from("alice"),
            ttl_secs: None,
        },
        Command::Set {
            key: Bytes::from("user:2"),
            value: Bytes::from("bob"),
            ttl_secs: Some(3600),
        },
        Command::Set {
            key: Bytes::from("session:abc"),
            value: Bytes::from("active"),
            ttl_secs: Some(1800),
        },
    ];

    // Apply commands to both storage and WAL
    for cmd in &commands {
        wal.append_command(cmd).await.expect("Failed to write to WAL");

        match cmd {
            Command::Set { key, value, ttl_secs } => {
                let ttl = ttl_secs.map(|s| Duration::from_secs(s as u64));
                storage.set(key.clone(), value.clone(), ttl);
            },
            Command::Delete { key } => {
                storage.delete(key);
            },
            _ => {}
        }
    }

    // Create snapshot of current state
    snapshot_manager.save_snapshot().await.expect("Failed to save snapshot");

    // Simulate restart: clear storage and reload from persistence
    let new_storage = Arc::new(CacheStorage::new());
    let new_snapshot_manager = SnapshotManager::new(Arc::clone(&new_storage), config.clone());

    // Load from snapshot first
    let snapshot_count = new_snapshot_manager.load_snapshot().await.expect("Failed to load snapshot");
    assert_eq!(snapshot_count, 3);

    // Then replay any WAL entries after snapshot (in real implementation,
    // this would be WAL entries newer than snapshot timestamp)
    let new_wal = WriteAheadLog::new(&config).await.expect("Failed to create new WAL");
    let mut replay_count = 0;
    new_wal.replay(|entry| {
        match &entry.command {
            Command::Set { key, value, ttl_secs } => {
                let ttl = ttl_secs.map(|s| Duration::from_secs(s as u64));
                new_storage.set(key.clone(), value.clone(), ttl);
            },
            Command::Delete { key } => {
                new_storage.delete(key);
            },
            _ => {}
        }
        replay_count += 1;
        Ok(())
    }).await.expect("Failed to replay WAL");

    // Verify all data is restored correctly
    assert_eq!(new_storage.get(b"user:1"), Some(Bytes::from("alice")));
    assert_eq!(new_storage.get(b"user:2"), Some(Bytes::from("bob")));
    assert_eq!(new_storage.get(b"session:abc"), Some(Bytes::from("active")));

    cleanup_test_files(&config).await;
}

#[tokio::test]
async fn test_wal_corruption_recovery() {
    let config = create_test_config("wal_corruption").await;
    cleanup_test_files(&config).await;

    // Create WAL with valid entries
    {
        let wal = WriteAheadLog::new(&config).await.expect("Failed to create WAL");

        let cmd = Command::Set {
            key: Bytes::from("good_key"),
            value: Bytes::from("good_value"),
            ttl_secs: None,
        };
        wal.append_command(&cmd).await.expect("Failed to append command");
        wal.sync().await.expect("Failed to sync");
    }

    // Append corrupted data to WAL file
    {
        use std::fs::OpenOptions;
        use std::io::Write;

        let mut file = OpenOptions::new()
            .append(true)
            .open(&config.wal_path)
            .expect("Failed to open WAL file");

        // Write some garbage data
        file.write_all(b"CORRUPTED_DATA_HERE").expect("Failed to write corruption");
    }

    // Try to replay - should handle corruption gracefully
    let new_wal = WriteAheadLog::new(&config).await.expect("Failed to create new WAL");
    let mut valid_entries = 0;

    // This should not panic, but may log errors about corrupted entries
    let _result = new_wal.replay(|_entry| {
        valid_entries += 1;
        Ok(())
    }).await;

    // Should have replayed at least the valid entry before corruption
    assert!(valid_entries >= 1);

    cleanup_test_files(&config).await;
}

/// Regression: prior to the bincode-based WAL serialization fix, only Set
/// and Delete were WAL-serializable — HSET/LPUSH/SADD/ZADD writes either
/// failed to serialize or returned errors. This test writes one of each
/// non-string write through the actual WAL file, reopens it, replays into
/// fresh storage via the shared apply helper, and verifies all data types
/// survived a simulated restart.
#[tokio::test]
async fn test_wal_replay_covers_all_data_types() {
    use ferric_cache::commands::apply_write_command;

    let config = create_test_config("wal_all_types").await;
    cleanup_test_files(&config).await;

    let writes = vec![
        Command::Set {
            key: Bytes::from("str"),
            value: Bytes::from("v"),
            ttl_secs: None,
        },
        Command::HSet {
            key: Bytes::from("h"),
            field: Bytes::from("f"),
            value: Bytes::from("hv"),
        },
        Command::LPush {
            key: Bytes::from("l"),
            values: vec![Bytes::from("a"), Bytes::from("b")],
        },
        Command::SAdd {
            key: Bytes::from("s"),
            members: vec![Bytes::from("m1"), Bytes::from("m2")],
        },
        Command::ZAdd {
            key: Bytes::from("z"),
            members: vec![(1.5, Bytes::from("alpha"))],
        },
    ];

    {
        let wal = WriteAheadLog::new(&config).await.expect("create WAL");
        for cmd in &writes {
            wal.append_command(cmd).await.expect("append");
        }
        wal.sync().await.expect("sync");
    }

    let storage = Arc::new(CacheStorage::new());
    let storage_clone = Arc::clone(&storage);
    let new_wal = WriteAheadLog::new(&config).await.expect("reopen WAL");
    let replayed = new_wal.replay(move |entry| {
        apply_write_command(&entry.command, &storage_clone);
        Ok(())
    }).await.expect("replay");

    assert_eq!(replayed as usize, writes.len(), "all writes replayed");

    // Each data type survived the simulated restart
    assert_eq!(storage.get(b"str").as_deref(), Some(&b"v"[..]));
    assert_eq!(storage.hget(b"h", b"f").as_deref(), Some(&b"hv"[..]));
    assert_eq!(storage.llen(b"l").unwrap(), 2);
    assert!(storage.sismember(b"s", b"m1").unwrap());
    assert!(storage.sismember(b"s", b"m2").unwrap());
    assert_eq!(storage.zscore(b"z", b"alpha").unwrap(), Some(1.5));

    cleanup_test_files(&config).await;
}
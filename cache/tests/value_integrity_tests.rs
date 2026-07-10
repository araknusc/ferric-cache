use ferric_cache::FerricClient;
use std::time::Duration;
use tokio::time::sleep;
use bytes::Bytes;

/// Start a test server on a unique port for each test
async fn start_test_server_on_port(port: u16) {
    tokio::spawn(async move {
        let server = ferric_cache::CacheServer::new(format!("127.0.0.1:{}", port));
        let _ = server.run().await;
    });
    // Give server time to start
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_exact_value_retrieval() {
    start_test_server_on_port(19401).await;

    let mut client = FerricClient::connect("127.0.0.1:19401")
        .await
        .expect("Failed to connect");

    // Test simple string
    let test_cases = vec![
        ("simple", "Hello, World!"),
        ("empty", ""),
        ("single_char", "x"),
        ("numbers", "1234567890"),
        ("special_chars", "!@#$%^&*()_+-=[]{}|;':\",./<>?"),
    ];

    for (key, value) in test_cases {
        client.set(key, value).await.expect("Failed to set");
        let retrieved = client.get(key).await.expect("Failed to get");

        assert_eq!(
            retrieved,
            Some(Bytes::from(value)),
            "Value mismatch for key '{}': expected '{}', got '{:?}'",
            key, value, retrieved
        );
    }
}

#[tokio::test]
async fn test_binary_data_integrity() {
    start_test_server_on_port(19402).await;

    let mut client = FerricClient::connect("127.0.0.1:19402")
        .await
        .expect("Failed to connect");

    // Test binary data with all byte values
    let mut binary_data = Vec::new();
    for byte in 0u8..=255u8 {
        binary_data.push(byte);
    }
    let binary_str = String::from_utf8_lossy(&binary_data);

    client.set("binary_test", &binary_str)
        .await
        .expect("Failed to set binary data");

    let retrieved = client.get("binary_test")
        .await
        .expect("Failed to get binary data");

    assert_eq!(
        retrieved.as_ref().map(|b| b.as_ref()),
        Some(binary_str.as_bytes()),
        "Binary data integrity check failed"
    );
}

#[tokio::test]
async fn test_unicode_data_integrity() {
    start_test_server_on_port(19403).await;

    let mut client = FerricClient::connect("127.0.0.1:19403")
        .await
        .expect("Failed to connect");

    let unicode_tests = vec![
        ("emoji", "🚀🎉🌟💻🔥"),
        ("chinese", "你好世界"),
        ("japanese", "こんにちは世界"),
        ("arabic", "مرحبا بالعالم"),
        ("cyrillic", "Привет мир"),
        ("mixed", "Hello 世界 🌍 مرحبا こんにちは"),
    ];

    for (key, value) in unicode_tests {
        client.set(key, value).await.expect("Failed to set unicode");
        let retrieved = client.get(key).await.expect("Failed to get unicode");

        assert_eq!(
            retrieved,
            Some(Bytes::from(value)),
            "Unicode mismatch for key '{}': expected '{}', got '{:?}'",
            key, value, retrieved
        );
    }
}

#[tokio::test]
async fn test_large_value_integrity() {
    start_test_server_on_port(19404).await;

    let mut client = FerricClient::connect("127.0.0.1:19404")
        .await
        .expect("Failed to connect");

    // Test various sizes (keeping within reasonable limits)
    let sizes = vec![
        100,       // 100 bytes
        1_000,     // 1 KB
        10_000,    // 10 KB
        50_000,    // 50 KB
        100_000,   // 100 KB
    ];

    for size in sizes {
        let key = format!("large_{}", size);
        // Create a repeating pattern that we can verify
        let pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut value = String::new();
        while value.len() < size {
            value.push_str(pattern);
        }
        value.truncate(size);

        client.set(&key, &value).await.expect("Failed to set large value");
        let retrieved = client.get(&key).await.expect("Failed to get large value");

        assert_eq!(
            retrieved.as_ref().map(|b| b.len()),
            Some(size),
            "Size mismatch for {} bytes", size
        );

        assert_eq!(
            retrieved,
            Some(Bytes::from(value.clone())),
            "Large value mismatch for {} bytes", size
        );
    }
}

#[tokio::test]
async fn test_json_string_integrity() {
    start_test_server_on_port(19405).await;

    let mut client = FerricClient::connect("127.0.0.1:19405")
        .await
        .expect("Failed to connect");

    let json_tests = vec![
        ("simple_json", r#"{"name":"John","age":30}"#),
        ("nested_json", r#"{"user":{"name":"Alice","settings":{"theme":"dark","notifications":true}}}"#),
        ("array_json", r#"["apple","banana","cherry",123,true,null]"#),
        ("escaped_json", r#"{"message":"Hello \"World\"","path":"C:\\Users\\test"}"#),
        ("complex_json", r#"{"id":1,"products":[{"name":"Widget","price":19.99,"tags":["new","featured"]},{"name":"Gadget","price":29.99,"tags":["sale"]}],"metadata":{"created":"2024-01-01T00:00:00Z","version":"1.0.0"}}"#),
    ];

    for (key, json_value) in json_tests {
        client.set(key, json_value).await.expect("Failed to set JSON");
        let retrieved = client.get(key).await.expect("Failed to get JSON");

        assert_eq!(
            retrieved,
            Some(Bytes::from(json_value)),
            "JSON mismatch for key '{}': expected '{}', got '{:?}'",
            key, json_value, retrieved
        );
    }
}

#[tokio::test]
async fn test_whitespace_preservation() {
    start_test_server_on_port(19406).await;

    let mut client = FerricClient::connect("127.0.0.1:19406")
        .await
        .expect("Failed to connect");

    let whitespace_tests = vec![
        ("spaces", "  leading and trailing spaces  "),
        ("tabs", "\t\ttabs\t\there\t\t"),
        ("newlines", "line1\nline2\r\nline3"),
        ("mixed", " \t mixed \n whitespace \r\n test \t "),
    ];

    for (key, value) in whitespace_tests {
        client.set(key, value).await.expect("Failed to set whitespace");
        let retrieved = client.get(key).await.expect("Failed to get whitespace");

        assert_eq!(
            retrieved,
            Some(Bytes::from(value)),
            "Whitespace not preserved for key '{}': expected '{:?}', got '{:?}'",
            key, value, retrieved
        );
    }
}

#[tokio::test]
async fn test_consecutive_updates() {
    start_test_server_on_port(19407).await;

    let mut client = FerricClient::connect("127.0.0.1:19407")
        .await
        .expect("Failed to connect");

    let key = "update_test";
    let values = vec![
        "initial_value",
        "second_value",
        "much_longer_third_value_that_exceeds_the_previous_ones",
        "short",
        "",
        "final_value_with_special_chars!@#$%",
    ];

    for (i, value) in values.iter().enumerate() {
        client.set(key, value).await.expect("Failed to update");
        let retrieved = client.get(key).await.expect("Failed to get after update");

        assert_eq!(
            retrieved,
            Some(Bytes::from(*value)),
            "Update {} failed: expected '{}', got '{:?}'",
            i + 1, value, retrieved
        );
    }
}

#[tokio::test]
async fn test_null_bytes_in_value() {
    start_test_server_on_port(19408).await;

    let mut client = FerricClient::connect("127.0.0.1:19408")
        .await
        .expect("Failed to connect");

    // Create string with null bytes
    let value_with_nulls = "before\0middle\0after";

    client.set("null_test", value_with_nulls)
        .await
        .expect("Failed to set value with null bytes");

    let retrieved = client.get("null_test")
        .await
        .expect("Failed to get value with null bytes");

    assert_eq!(
        retrieved,
        Some(Bytes::from(value_with_nulls)),
        "Null bytes not handled correctly"
    );
}

#[tokio::test]
async fn test_key_value_isolation() {
    start_test_server_on_port(19409).await;

    let mut client = FerricClient::connect("127.0.0.1:19409")
        .await
        .expect("Failed to connect");

    // Set multiple key-value pairs
    let pairs = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("similar_key1", "different_value1"),
        ("key", "short_key_value"),
    ];

    // Set all pairs
    for (key, value) in &pairs {
        client.set(key, value).await.expect("Failed to set");
    }

    // Verify each pair independently
    for (key, expected_value) in &pairs {
        let retrieved = client.get(key).await.expect("Failed to get");

        assert_eq!(
            retrieved,
            Some(Bytes::from(*expected_value)),
            "Key isolation failed for '{}': expected '{}', got '{:?}'",
            key, expected_value, retrieved
        );
    }
}

#[tokio::test]
async fn test_value_integrity_after_expiration() {
    start_test_server_on_port(19410).await;

    let mut client = FerricClient::connect("127.0.0.1:19410")
        .await
        .expect("Failed to connect");

    let key = "ttl_integrity";
    let value = "This value should expire but be intact until then";

    // Set with 2 second TTL
    client.set_with_ttl(key, value, Some(Duration::from_secs(2)))
        .await
        .expect("Failed to set with TTL");

    // Check immediately - should be intact
    let retrieved = client.get(key).await.expect("Failed to get");
    assert_eq!(retrieved, Some(Bytes::from(value)));

    // Check after 1 second - should still be intact
    sleep(Duration::from_secs(1)).await;
    let retrieved = client.get(key).await.expect("Failed to get");
    assert_eq!(retrieved, Some(Bytes::from(value)));

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;
    let retrieved = client.get(key).await.expect("Failed to get");
    assert_eq!(retrieved, None);

    // Set the same key again with new value
    let new_value = "New value after expiration";
    client.set(key, new_value).await.expect("Failed to set");
    let retrieved = client.get(key).await.expect("Failed to get");
    assert_eq!(retrieved, Some(Bytes::from(new_value)));
}

#[tokio::test]
async fn test_concurrent_different_values() {
    start_test_server_on_port(19411).await;
    sleep(Duration::from_millis(100)).await;

    let handles: Vec<_> = (0..20)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = FerricClient::connect("127.0.0.1:19411")
                    .await
                    .expect("Failed to connect");

                let key = format!("concurrent_key_{}", i);
                let value = format!("Unique value for thread {} with some random text: {}",
                    i, "x".repeat(i * 10));

                client.set(&key, &value).await.expect("Failed to set");
                let retrieved = client.get(&key).await.expect("Failed to get");

                assert_eq!(
                    retrieved,
                    Some(Bytes::from(value.clone())),
                    "Concurrent value mismatch for key '{}'", key
                );
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Task failed");
    }
}
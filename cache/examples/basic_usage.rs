use ferric_cache::FerricClient;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("ferric-cache - Basic Usage Example");
    println!("=====================================\n");

    // Connect to the cache server
    let mut client = FerricClient::connect("127.0.0.1:7777").await?;
    println!("Connected to ferric-cache server");

    // Basic SET operation
    println!("\n1. Setting key 'user:1' = 'John Doe'");
    client.set("user:1", "John Doe").await?;
    println!("   ✓ Set successful");

    // Basic GET operation
    println!("\n2. Getting key 'user:1'");
    match client.get("user:1").await? {
        Some(value) => {
            let value_str = String::from_utf8_lossy(&value);
            println!("   ✓ Value: {}", value_str);
        }
        None => println!("   ✗ Key not found"),
    }

    // SET with TTL
    println!("\n3. Setting key 'session:xyz' with 3 second TTL");
    client
        .set_with_ttl("session:xyz", "active", Some(Duration::from_secs(3)))
        .await?;
    println!("   ✓ Set with TTL successful");

    // Get before expiration
    println!("\n4. Getting 'session:xyz' immediately");
    match client.get("session:xyz").await? {
        Some(value) => {
            let value_str = String::from_utf8_lossy(&value);
            println!("   ✓ Value: {}", value_str);
        }
        None => println!("   ✗ Key not found"),
    }

    // Wait for expiration
    println!("\n5. Waiting 4 seconds for TTL expiration...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    println!("   Getting 'session:xyz' after expiration");
    match client.get("session:xyz").await? {
        Some(_) => println!("   ✗ Key still exists (unexpected)"),
        None => println!("   ✓ Key expired as expected"),
    }

    // DELETE operation
    println!("\n6. Deleting key 'user:1'");
    let deleted = client.delete("user:1").await?;
    if deleted {
        println!("   ✓ Key deleted successfully");
    } else {
        println!("   ✗ Key not found");
    }

    // Verify deletion
    println!("\n7. Verifying deletion of 'user:1'");
    match client.get("user:1").await? {
        Some(_) => println!("   ✗ Key still exists"),
        None => println!("   ✓ Key successfully deleted"),
    }

    // Performance test
    println!("\n8. Performance test - 1000 operations");
    let start = std::time::Instant::now();

    for i in 0..1000 {
        let key = format!("perf_key_{}", i);
        let value = format!("perf_value_{}", i);
        client.set(&key, &value).await?;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = 1000.0 / elapsed.as_secs_f64();
    println!("   ✓ Completed 1000 SET operations in {:?}", elapsed);
    println!("   ✓ Throughput: {:.0} ops/sec", ops_per_sec);

    println!("\n✅ All tests completed successfully!");

    Ok(())
}
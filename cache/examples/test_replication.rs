use ferric_cache::FerricClient;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Master-Replica Replication");
    println!("{}", "=".repeat(50));

    // Connect to master and replica
    let mut master = FerricClient::connect("127.0.0.1:7777").await?;
    let mut replica = FerricClient::connect("127.0.0.1:7778").await?;

    // Test 1: Write to master
    println!("\n1. Writing data to MASTER (port 7777)...");
    master.set("testkey", "testvalue").await?;
    println!("   Response: OK");

    // Wait for replication
    println!("\n2. Waiting 1 second for replication...");
    sleep(Duration::from_secs(1)).await;

    // Test 2: Read from master
    println!("\n3. Reading from MASTER (port 7777)...");
    if let Some(value) = master.get("testkey").await? {
        println!("   Response: {}", String::from_utf8_lossy(&value));
    } else {
        println!("   Response: Key not found");
    }

    // Test 3: Read from replica
    println!("\n4. Reading from REPLICA (port 7778)...");
    if let Some(value) = replica.get("testkey").await? {
        println!("   Response: {}", String::from_utf8_lossy(&value));
    } else {
        println!("   Response: Key not found");
    }

    // Test 4: Write another key to master
    println!("\n5. Writing another key to MASTER...");
    master.set("anotherkey", "anothervalue").await?;
    println!("   Response: OK");

    sleep(Duration::from_secs(1)).await;

    // Test 5: Read from replica
    println!("\n6. Reading second key from REPLICA...");
    if let Some(value) = replica.get("anotherkey").await? {
        println!("   Response: {}", String::from_utf8_lossy(&value));
    } else {
        println!("   Response: Key not found");
    }

    // Test 6: Delete from master
    println!("\n7. Deleting key from MASTER...");
    let deleted = master.delete("testkey").await?;
    println!("   Response: Deleted = {}", deleted);

    sleep(Duration::from_secs(1)).await;

    // Test 7: Verify deletion replicated
    println!("\n8. Verifying deletion on REPLICA...");
    if let Some(value) = replica.get("testkey").await? {
        println!("   Response: {}", String::from_utf8_lossy(&value));
        println!("   ERROR: Key should have been deleted!");
    } else {
        println!("   Response: Key not found (correctly deleted)");
    }

    println!("\n{}", "=".repeat(50));
    println!("Replication test complete!");

    Ok(())
}

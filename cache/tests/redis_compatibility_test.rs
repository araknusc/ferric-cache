use std::net::TcpStream;
use std::io::{Write, Read};

/// Dedicated port for this suite's self-started server (previously these tests
/// silently required an external server on 7777 and always failed under
/// `cargo test`).
const TEST_ADDR: &str = "127.0.0.1:7901";

/// Start an in-process server once for the whole suite and return a connection.
fn connect() -> TcpStream {
    use std::sync::Once;
    static START: Once = Once::new();
    START.call_once(|| {
        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let server = ferric_cache::CacheServer::new(TEST_ADDR.to_string());
                let _ = server.run().await;
            });
        });
        // Give the listener a moment to bind before the first connect.
        std::thread::sleep(std::time::Duration::from_millis(200));
    });

    // Retry briefly in case the server is still binding.
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(TEST_ADDR) {
            return s;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    TcpStream::connect(TEST_ADDR).expect("Failed to connect to test server")
}

fn send_resp_command(stream: &mut TcpStream, command: &str) -> String {
    // Send command
    stream.write_all(command.as_bytes()).unwrap();
    stream.flush().unwrap();

    // Read response
    let mut buffer = [0u8; 4096];
    let n = stream.read(&mut buffer).unwrap();
    String::from_utf8_lossy(&buffer[..n]).to_string()
}

#[test]
fn test_redis_ping() {
    let mut stream = connect();
    let response = send_resp_command(&mut stream, "*1\r\n$4\r\nPING\r\n");
    assert!(response.contains("PONG"));
    println!("✓ PING test passed: {}", response);
}

#[test]
fn test_redis_set_get() {
    let mut stream = connect();

    // SET command
    let set_cmd = "*3\r\n$3\r\nSET\r\n$8\r\ntestkey1\r\n$9\r\ntestvalue\r\n";
    let response = send_resp_command(&mut stream, set_cmd);
    assert!(response.contains("OK"));
    println!("✓ SET test passed: {}", response);

    // GET command
    let get_cmd = "*2\r\n$3\r\nGET\r\n$8\r\ntestkey1\r\n";
    let response = send_resp_command(&mut stream, get_cmd);
    assert!(response.contains("testvalue"));
    println!("✓ GET test passed: {}", response);
}

#[test]
fn test_redis_incr_decr() {
    let mut stream = connect();

    // SET counter to 10
    let set_cmd = "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n10\r\n";
    send_resp_command(&mut stream, set_cmd);

    // INCR counter
    let incr_cmd = "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
    let response = send_resp_command(&mut stream, incr_cmd);
    assert!(response.contains(":11"));
    println!("✓ INCR test passed: {}", response);

    // DECR counter
    let decr_cmd = "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n";
    let response = send_resp_command(&mut stream, decr_cmd);
    assert!(response.contains(":10"));
    println!("✓ DECR test passed: {}", response);
}

#[test]
fn test_redis_hash_operations() {
    let mut stream = connect();

    // HSET user:1 name "Alice"
    let hset_cmd = "*4\r\n$4\r\nHSET\r\n$6\r\nuser:1\r\n$4\r\nname\r\n$5\r\nAlice\r\n";
    let response = send_resp_command(&mut stream, hset_cmd);
    println!("✓ HSET test: {}", response);

    // HGET user:1 name
    let hget_cmd = "*3\r\n$4\r\nHGET\r\n$6\r\nuser:1\r\n$4\r\nname\r\n";
    let response = send_resp_command(&mut stream, hget_cmd);
    assert!(response.contains("Alice"));
    println!("✓ HGET test passed: {}", response);

    // HSET user:1 age "30"
    let hset_cmd2 = "*4\r\n$4\r\nHSET\r\n$6\r\nuser:1\r\n$3\r\nage\r\n$2\r\n30\r\n";
    send_resp_command(&mut stream, hset_cmd2);

    // HGETALL user:1
    let hgetall_cmd = "*2\r\n$7\r\nHGETALL\r\n$6\r\nuser:1\r\n";
    let response = send_resp_command(&mut stream, hgetall_cmd);
    assert!(response.contains("name") && response.contains("Alice"));
    println!("✓ HGETALL test passed: {}", response);
}

#[test]
fn test_redis_list_operations() {
    let mut stream = connect();

    // LPUSH mylist "item1" "item2"
    let lpush_cmd = "*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nitem1\r\n$5\r\nitem2\r\n";
    let response = send_resp_command(&mut stream, lpush_cmd);
    println!("✓ LPUSH test: {}", response);

    // LLEN mylist
    let llen_cmd = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let response = send_resp_command(&mut stream, llen_cmd);
    assert!(response.contains(":2"));
    println!("✓ LLEN test passed: {}", response);

    // LRANGE mylist 0 -1
    let lrange_cmd = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let response = send_resp_command(&mut stream, lrange_cmd);
    assert!(response.contains("item"));
    println!("✓ LRANGE test passed: {}", response);
}

#[test]
fn test_redis_set_operations() {
    let mut stream = connect();

    // SADD myset "a" "b" "c"
    let sadd_cmd = "*5\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let response = send_resp_command(&mut stream, sadd_cmd);
    println!("✓ SADD test: {}", response);

    // SCARD myset
    let scard_cmd = "*2\r\n$5\r\nSCARD\r\n$5\r\nmyset\r\n";
    let response = send_resp_command(&mut stream, scard_cmd);
    assert!(response.contains(":3"));
    println!("✓ SCARD test passed: {}", response);

    // SMEMBERS myset
    let smembers_cmd = "*2\r\n$8\r\nSMEMBERS\r\n$5\r\nmyset\r\n";
    let response = send_resp_command(&mut stream, smembers_cmd);
    println!("✓ SMEMBERS test passed: {}", response);
}

#[test]
fn test_redis_sorted_set_operations() {
    let mut stream = connect();

    // ZADD leaderboard 100 "player1" 200 "player2"
    let zadd_cmd = "*6\r\n$4\r\nZADD\r\n$11\r\nleaderboard\r\n$3\r\n100\r\n$7\r\nplayer1\r\n$3\r\n200\r\n$7\r\nplayer2\r\n";
    let response = send_resp_command(&mut stream, zadd_cmd);
    println!("✓ ZADD test: {}", response);

    // ZCARD leaderboard
    let zcard_cmd = "*2\r\n$5\r\nZCARD\r\n$11\r\nleaderboard\r\n";
    let response = send_resp_command(&mut stream, zcard_cmd);
    assert!(response.contains(":2"));
    println!("✓ ZCARD test passed: {}", response);

    // ZRANGE leaderboard 0 -1
    let zrange_cmd = "*4\r\n$6\r\nZRANGE\r\n$11\r\nleaderboard\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let response = send_resp_command(&mut stream, zrange_cmd);
    assert!(response.contains("player"));
    println!("✓ ZRANGE test passed: {}", response);
}

#[test]
fn test_redis_server_commands() {
    let mut stream = connect();

    // ECHO "hello"
    let echo_cmd = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    let response = send_resp_command(&mut stream, echo_cmd);
    assert!(response.contains("hello"));
    println!("✓ ECHO test passed: {}", response);

    // DBSIZE
    let dbsize_cmd = "*1\r\n$6\r\nDBSIZE\r\n";
    let response = send_resp_command(&mut stream, dbsize_cmd);
    assert!(response.contains(":"));
    println!("✓ DBSIZE test passed: {}", response);

    // EXISTS testkey1
    let exists_cmd = "*2\r\n$6\r\nEXISTS\r\n$8\r\ntestkey1\r\n";
    let response = send_resp_command(&mut stream, exists_cmd);
    println!("✓ EXISTS test passed: {}", response);
}

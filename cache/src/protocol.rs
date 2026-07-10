use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor};

/// RESP (REdis Serialization Protocol) data types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(String),
    /// Error: -Error message\r\n
    Error(String),
    /// Integer: :1000\r\n
    Integer(i64),
    /// Bulk string: $6\r\nfoobar\r\n (or $-1\r\n for null)
    BulkString(Option<Bytes>),
    /// Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Array(Vec<RespValue>),
}

/// Redis commands
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    // String commands
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes, ttl_secs: Option<u32> },
    Delete { key: Bytes },
    Incr { key: Bytes },
    Decr { key: Bytes },
    Append { key: Bytes, value: Bytes },
    Strlen { key: Bytes },

    // Hash commands
    HGet { key: Bytes, field: Bytes },
    HSet { key: Bytes, field: Bytes, value: Bytes },
    HDel { key: Bytes, fields: Vec<Bytes> },
    HGetAll { key: Bytes },
    HKeys { key: Bytes },
    HVals { key: Bytes },
    HLen { key: Bytes },
    HExists { key: Bytes, field: Bytes },

    // List commands
    LPush { key: Bytes, values: Vec<Bytes> },
    RPush { key: Bytes, values: Vec<Bytes> },
    LPop { key: Bytes },
    RPop { key: Bytes },
    LRange { key: Bytes, start: i64, stop: i64 },
    LLen { key: Bytes },
    LIndex { key: Bytes, index: i64 },

    // Set commands
    SAdd { key: Bytes, members: Vec<Bytes> },
    SRem { key: Bytes, members: Vec<Bytes> },
    SMembers { key: Bytes },
    SIsMember { key: Bytes, member: Bytes },
    SCard { key: Bytes },
    SInter { keys: Vec<Bytes> },
    SUnion { keys: Vec<Bytes> },
    SDiff { keys: Vec<Bytes> },

    // Sorted Set commands
    ZAdd { key: Bytes, members: Vec<(f64, Bytes)> },
    ZRem { key: Bytes, members: Vec<Bytes> },
    ZRange { key: Bytes, start: i64, stop: i64, with_scores: bool },
    ZRank { key: Bytes, member: Bytes },
    ZScore { key: Bytes, member: Bytes },
    ZCard { key: Bytes },
    ZCount { key: Bytes, min: f64, max: f64 },

    // Server commands
    Ping,
    Echo { message: Bytes },
    Exists { keys: Vec<Bytes> },
    Keys { pattern: String },
    FlushAll,
    DbSize,

    // Key TTL commands. Storage already supports per-value expiry on every
    // data type; these expose it at the protocol level (Redis parity).
    Expire { key: Bytes, seconds: i64 },
    PExpire { key: Bytes, millis: i64 },
    ExpireAt { key: Bytes, unix_secs: i64 },
    Ttl { key: Bytes },
    PTtl { key: Bytes },
    Persist { key: Bytes },

    /// AUTH password   (Redis legacy form, single-arg)
    /// AUTH user pass  (Redis 6+ form, two-arg)
    /// Connection-scoped: success attaches the user to the connection state.
    Auth { username: Option<Bytes>, password: Bytes },

    // ===== P3 Bucket 2: bulk Redis-parity commands =====
    // String
    SetNx { key: Bytes, value: Bytes },
    SetEx { key: Bytes, seconds: u32, value: Bytes },
    GetSet { key: Bytes, value: Bytes },
    MGet { keys: Vec<Bytes> },
    MSet { pairs: Vec<(Bytes, Bytes)> },
    GetRange { key: Bytes, start: i64, end: i64 },
    IncrBy { key: Bytes, delta: i64 },
    DecrBy { key: Bytes, delta: i64 },
    IncrByFloat { key: Bytes, delta: f64 },
    // Hash
    HMGet { key: Bytes, fields: Vec<Bytes> },
    HMSet { key: Bytes, pairs: Vec<(Bytes, Bytes)> },
    HSetNx { key: Bytes, field: Bytes, value: Bytes },
    HIncrBy { key: Bytes, field: Bytes, delta: i64 },
    // List
    LSet { key: Bytes, index: i64, value: Bytes },
    LRem { key: Bytes, count: i64, value: Bytes },
    LTrim { key: Bytes, start: i64, stop: i64 },
    RPopLPush { source: Bytes, destination: Bytes },
    // Set
    SPop { key: Bytes, count: Option<usize> },
    SRandMember { key: Bytes, count: Option<i64> },
    SMove { source: Bytes, destination: Bytes, member: Bytes },
    // Sorted set
    ZIncrBy { key: Bytes, delta: f64, member: Bytes },
    ZRangeByScore { key: Bytes, min: f64, max: f64, with_scores: bool },
    ZRevRange { key: Bytes, start: i64, stop: i64, with_scores: bool },
    ZPopMin { key: Bytes, count: usize },
    ZPopMax { key: Bytes, count: usize },
    // Server / key admin
    Type { key: Bytes },
    Rename { key: Bytes, new_key: Bytes },
    RandomKey,

    // P4.1 Pub/Sub
    Publish { channel: Bytes, payload: Bytes },
    Subscribe { channels: Vec<Bytes> },
    Unsubscribe { channels: Vec<Bytes> }, // empty = unsubscribe all current

    // P4.2 Transactions
    Multi,
    Exec,
    Discard,
    Watch { keys: Vec<Bytes> },
    Unwatch,

    // P4.3 Streams. Wire format uses the textual id form (`<ms>-<seq>` or `*`).
    XAdd { key: Bytes, id: Option<String>, fields: Vec<(Bytes, Bytes)> },
    XLen { key: Bytes },
    XRange { key: Bytes, start: String, end: String, count: Option<usize> },
    XRead { count: Option<usize>, keys: Vec<Bytes>, ids: Vec<String> },

    // P4.4 Lua scripting. EVAL stores by SHA1 and runs; EVALSHA looks up.
    LuaEval { script: String, keys: Vec<Bytes>, argv: Vec<Bytes> },
    LuaEvalSha { sha1: String, keys: Vec<Bytes>, argv: Vec<Bytes> },
}

/// Response type (compatible with old code)
#[derive(Debug)]
pub enum Response {
    Value(Bytes),
    Ok,
    NotFound,
    Error(String),
    Integer(i64),
    Array(Vec<Bytes>),
    Null,
    /// Pre-serialized RESP frames written verbatim. Used by SUBSCRIBE /
    /// UNSUBSCRIBE which produce one frame per channel — the Redis protocol
    /// has no single representation for that, so we build the bytes inline.
    /// Also used by EXEC which returns a heterogeneous array of replies.
    Raw(Bytes),
}

impl Response {
    pub fn to_resp(&self) -> RespValue {
        match self {
            Response::Value(bytes) => RespValue::BulkString(Some(bytes.clone())),
            Response::Ok => RespValue::SimpleString("OK".to_string()),
            Response::NotFound => RespValue::BulkString(None),
            Response::Error(msg) => RespValue::Error(msg.clone()),
            Response::Integer(n) => RespValue::Integer(*n),
            Response::Array(items) => RespValue::Array(
                items.iter()
                    .map(|b| RespValue::BulkString(Some(b.clone())))
                    .collect()
            ),
            Response::Null => RespValue::BulkString(None),
            // Raw is handled specially by serialize_response — to_resp is
            // a fallback that wraps the bytes as a simple string.
            Response::Raw(b) => RespValue::BulkString(Some(b.clone())),
        }
    }
}

/// Parse RESP data from buffer
pub fn parse_resp(buf: &mut Cursor<&[u8]>) -> Result<RespValue, String> {
    if !buf.has_remaining() {
        return Err("Empty buffer".to_string());
    }

    let marker = buf.get_u8();

    match marker {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => Err(format!("Unknown RESP type marker: {}", marker as char)),
    }
}

fn parse_simple_string(buf: &mut Cursor<&[u8]>) -> Result<RespValue, String> {
    let line = read_line(buf)?;
    Ok(RespValue::SimpleString(line))
}

fn parse_error(buf: &mut Cursor<&[u8]>) -> Result<RespValue, String> {
    let line = read_line(buf)?;
    Ok(RespValue::Error(line))
}

fn parse_integer(buf: &mut Cursor<&[u8]>) -> Result<RespValue, String> {
    let line = read_line(buf)?;
    let num = line.parse::<i64>()
        .map_err(|_| format!("Invalid integer: {}", line))?;
    Ok(RespValue::Integer(num))
}

fn parse_bulk_string(buf: &mut Cursor<&[u8]>) -> Result<RespValue, String> {
    let line = read_line(buf)?;
    let len = line.parse::<i64>()
        .map_err(|_| format!("Invalid bulk string length: {}", line))?;

    if len == -1 {
        return Ok(RespValue::BulkString(None));
    }

    if len < 0 {
        return Err(format!("Invalid bulk string length: {}", len));
    }

    let len = len as usize;
    if buf.remaining() < len + 2 {
        return Err("Incomplete bulk string".to_string());
    }

    let mut data = vec![0u8; len];
    buf.copy_to_slice(&mut data);

    // Consume \r\n
    if buf.remaining() < 2 {
        return Err("Missing CRLF after bulk string".to_string());
    }
    let cr = buf.get_u8();
    let lf = buf.get_u8();
    if cr != b'\r' || lf != b'\n' {
        return Err("Invalid CRLF after bulk string".to_string());
    }

    Ok(RespValue::BulkString(Some(Bytes::from(data))))
}

fn parse_array(buf: &mut Cursor<&[u8]>) -> Result<RespValue, String> {
    let line = read_line(buf)?;
    let count = line.parse::<i64>()
        .map_err(|_| format!("Invalid array length: {}", line))?;

    if count < 0 {
        return Ok(RespValue::Array(vec![]));
    }

    let count = count as usize;
    let mut array = Vec::with_capacity(count);

    for _ in 0..count {
        array.push(parse_resp(buf)?);
    }

    Ok(RespValue::Array(array))
}

fn read_line(buf: &mut Cursor<&[u8]>) -> Result<String, String> {
    let start = buf.position() as usize;
    let slice = &buf.get_ref()[start..];

    // Find \r\n
    for (i, window) in slice.windows(2).enumerate() {
        if window == b"\r\n" {
            let line_bytes = &slice[..i];
            let line = String::from_utf8_lossy(line_bytes).to_string();
            buf.set_position((start + i + 2) as u64);
            return Ok(line);
        }
    }

    Err("Incomplete line".to_string())
}

/// Serialize RESP value to bytes
pub fn serialize_resp(value: &RespValue) -> Bytes {
    let mut buf = BytesMut::new();
    write_resp(&mut buf, value);
    buf.freeze()
}

fn write_resp(buf: &mut BytesMut, value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put(s.as_bytes());
            buf.put(&b"\r\n"[..]);
        }
        RespValue::Error(s) => {
            buf.put_u8(b'-');
            buf.put(s.as_bytes());
            buf.put(&b"\r\n"[..]);
        }
        RespValue::Integer(n) => {
            buf.put_u8(b':');
            buf.put(n.to_string().as_bytes());
            buf.put(&b"\r\n"[..]);
        }
        RespValue::BulkString(Some(bytes)) => {
            buf.put_u8(b'$');
            buf.put(bytes.len().to_string().as_bytes());
            buf.put(&b"\r\n"[..]);
            buf.put(bytes.as_ref());
            buf.put(&b"\r\n"[..]);
        }
        RespValue::BulkString(None) => {
            buf.put(&b"$-1\r\n"[..]);
        }
        RespValue::Array(items) => {
            buf.put_u8(b'*');
            buf.put(items.len().to_string().as_bytes());
            buf.put(&b"\r\n"[..]);
            for item in items {
                write_resp(buf, item);
            }
        }
    }
}

/// Parse Redis command from RESP array
pub fn parse_command(resp: RespValue) -> Result<Command, String> {
    let array = match resp {
        RespValue::Array(arr) => arr,
        _ => return Err("Command must be an array".to_string()),
    };

    if array.is_empty() {
        return Err("Empty command".to_string());
    }

    let cmd_bytes = extract_bulk_string(&array[0])?;
    let cmd_name = String::from_utf8_lossy(&cmd_bytes).to_ascii_uppercase();

    match cmd_name.as_str() {
        "GET" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Get { key: extract_bulk_string(&array[1])? })
        }
        "SET" => {
            if array.len() < 3 {
                return Err("SET requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let value = extract_bulk_string(&array[2])?;

            // Parse optional EX/PX for TTL
            let mut ttl_secs = None;
            let mut i = 3;
            while i < array.len() {
                let opt_bytes = extract_bulk_string(&array[i])?;
                let opt = String::from_utf8_lossy(&opt_bytes).to_ascii_uppercase();
                match opt.as_str() {
                    "EX" => {
                        i += 1;
                        if i >= array.len() {
                            return Err("EX requires a value".to_string());
                        }
                        let secs = extract_integer(&array[i])? as u32;
                        ttl_secs = Some(secs);
                    }
                    "PX" => {
                        i += 1;
                        if i >= array.len() {
                            return Err("PX requires a value".to_string());
                        }
                        let millis = extract_integer(&array[i])? as u32;
                        ttl_secs = Some(millis / 1000);
                    }
                    _ => {}
                }
                i += 1;
            }

            Ok(Command::Set { key, value, ttl_secs })
        }
        "DEL" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Delete { key: extract_bulk_string(&array[1])? })
        }
        "INCR" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Incr { key: extract_bulk_string(&array[1])? })
        }
        "DECR" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Decr { key: extract_bulk_string(&array[1])? })
        }
        "APPEND" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::Append {
                key: extract_bulk_string(&array[1])?,
                value: extract_bulk_string(&array[2])?,
            })
        }
        "STRLEN" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Strlen { key: extract_bulk_string(&array[1])? })
        }

        // Hash commands
        "HGET" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::HGet {
                key: extract_bulk_string(&array[1])?,
                field: extract_bulk_string(&array[2])?,
            })
        }
        "HSET" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::HSet {
                key: extract_bulk_string(&array[1])?,
                field: extract_bulk_string(&array[2])?,
                value: extract_bulk_string(&array[3])?,
            })
        }
        "HDEL" => {
            if array.len() < 3 {
                return Err("HDEL requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let fields = array[2..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::HDel { key, fields })
        }
        "HGETALL" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::HGetAll { key: extract_bulk_string(&array[1])? })
        }
        "HKEYS" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::HKeys { key: extract_bulk_string(&array[1])? })
        }
        "HVALS" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::HVals { key: extract_bulk_string(&array[1])? })
        }
        "HLEN" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::HLen { key: extract_bulk_string(&array[1])? })
        }
        "HEXISTS" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::HExists {
                key: extract_bulk_string(&array[1])?,
                field: extract_bulk_string(&array[2])?,
            })
        }

        // List commands
        "LPUSH" => {
            if array.len() < 3 {
                return Err("LPUSH requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let values = array[2..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::LPush { key, values })
        }
        "RPUSH" => {
            if array.len() < 3 {
                return Err("RPUSH requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let values = array[2..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::RPush { key, values })
        }
        "LPOP" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::LPop { key: extract_bulk_string(&array[1])? })
        }
        "RPOP" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::RPop { key: extract_bulk_string(&array[1])? })
        }
        "LRANGE" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::LRange {
                key: extract_bulk_string(&array[1])?,
                start: extract_integer(&array[2])?,
                stop: extract_integer(&array[3])?,
            })
        }
        "LLEN" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::LLen { key: extract_bulk_string(&array[1])? })
        }
        "LINDEX" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::LIndex {
                key: extract_bulk_string(&array[1])?,
                index: extract_integer(&array[2])?,
            })
        }

        // Set commands
        "SADD" => {
            if array.len() < 3 {
                return Err("SADD requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let members = array[2..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::SAdd { key, members })
        }
        "SREM" => {
            if array.len() < 3 {
                return Err("SREM requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let members = array[2..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::SRem { key, members })
        }
        "SMEMBERS" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::SMembers { key: extract_bulk_string(&array[1])? })
        }
        "SISMEMBER" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::SIsMember {
                key: extract_bulk_string(&array[1])?,
                member: extract_bulk_string(&array[2])?,
            })
        }
        "SCARD" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::SCard { key: extract_bulk_string(&array[1])? })
        }
        "SINTER" => {
            if array.len() < 2 {
                return Err("SINTER requires at least 1 key".to_string());
            }
            let keys = array[1..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::SInter { keys })
        }
        "SUNION" => {
            if array.len() < 2 {
                return Err("SUNION requires at least 1 key".to_string());
            }
            let keys = array[1..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::SUnion { keys })
        }
        "SDIFF" => {
            if array.len() < 2 {
                return Err("SDIFF requires at least 1 key".to_string());
            }
            let keys = array[1..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::SDiff { keys })
        }

        // Sorted Set commands
        "ZADD" => {
            if array.len() < 4 || (array.len() - 2) % 2 != 0 {
                return Err("ZADD requires key and score-member pairs".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let mut members = Vec::new();
            for i in (2..array.len()).step_by(2) {
                let score = extract_float(&array[i])?;
                let member = extract_bulk_string(&array[i + 1])?;
                members.push((score, member));
            }
            Ok(Command::ZAdd { key, members })
        }
        "ZREM" => {
            if array.len() < 3 {
                return Err("ZREM requires at least 2 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let members = array[2..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::ZRem { key, members })
        }
        "ZRANGE" => {
            if array.len() < 4 {
                return Err("ZRANGE requires at least 3 arguments".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let start = extract_integer(&array[2])?;
            let stop = extract_integer(&array[3])?;
            let with_scores = if array.len() > 4 {
                let opt_bytes = extract_bulk_string(&array[4])?;
                String::from_utf8_lossy(&opt_bytes).to_ascii_uppercase() == "WITHSCORES"
            } else {
                false
            };
            Ok(Command::ZRange { key, start, stop, with_scores })
        }
        "ZRANK" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::ZRank {
                key: extract_bulk_string(&array[1])?,
                member: extract_bulk_string(&array[2])?,
            })
        }
        "ZSCORE" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::ZScore {
                key: extract_bulk_string(&array[1])?,
                member: extract_bulk_string(&array[2])?,
            })
        }
        "ZCARD" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::ZCard { key: extract_bulk_string(&array[1])? })
        }
        "ZCOUNT" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::ZCount {
                key: extract_bulk_string(&array[1])?,
                min: extract_float(&array[2])?,
                max: extract_float(&array[3])?,
            })
        }

        // Server commands
        "PING" => Ok(Command::Ping),
        "ECHO" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Echo { message: extract_bulk_string(&array[1])? })
        }
        "EXISTS" => {
            if array.len() < 2 {
                return Err("EXISTS requires at least 1 key".to_string());
            }
            let keys = array[1..].iter()
                .map(extract_bulk_string)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Exists { keys })
        }
        "KEYS" => {
            ensure_arg_count(&array, 2)?;
            let pattern = String::from_utf8_lossy(&extract_bulk_string(&array[1])?).to_string();
            Ok(Command::Keys { pattern })
        }
        "FLUSHALL" => Ok(Command::FlushAll),
        "DBSIZE" => Ok(Command::DbSize),

        // TTL commands
        "EXPIRE" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::Expire {
                key: extract_bulk_string(&array[1])?,
                seconds: extract_integer(&array[2])?,
            })
        }
        "PEXPIRE" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::PExpire {
                key: extract_bulk_string(&array[1])?,
                millis: extract_integer(&array[2])?,
            })
        }
        "EXPIREAT" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::ExpireAt {
                key: extract_bulk_string(&array[1])?,
                unix_secs: extract_integer(&array[2])?,
            })
        }
        "TTL" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Ttl { key: extract_bulk_string(&array[1])? })
        }
        "PTTL" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::PTtl { key: extract_bulk_string(&array[1])? })
        }
        "PERSIST" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Persist { key: extract_bulk_string(&array[1])? })
        }

        "AUTH" => match array.len() {
            2 => Ok(Command::Auth {
                username: None,
                password: extract_bulk_string(&array[1])?,
            }),
            3 => Ok(Command::Auth {
                username: Some(extract_bulk_string(&array[1])?),
                password: extract_bulk_string(&array[2])?,
            }),
            _ => Err("AUTH requires 1 or 2 arguments".to_string()),
        },

        // ===== P3 Bucket 2 =====
        "SETNX" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::SetNx {
                key: extract_bulk_string(&array[1])?,
                value: extract_bulk_string(&array[2])?,
            })
        }
        "SETEX" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::SetEx {
                key: extract_bulk_string(&array[1])?,
                seconds: extract_integer(&array[2])? as u32,
                value: extract_bulk_string(&array[3])?,
            })
        }
        "GETSET" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::GetSet {
                key: extract_bulk_string(&array[1])?,
                value: extract_bulk_string(&array[2])?,
            })
        }
        "MGET" => {
            if array.len() < 2 { return Err("MGET requires at least 1 key".to_string()); }
            let keys = array[1..].iter().map(extract_bulk_string).collect::<Result<Vec<_>, _>>()?;
            Ok(Command::MGet { keys })
        }
        "MSET" => {
            if array.len() < 3 || (array.len() - 1) % 2 != 0 {
                return Err("MSET requires non-empty key/value pairs".to_string());
            }
            let mut pairs = Vec::with_capacity((array.len() - 1) / 2);
            let mut i = 1;
            while i + 1 < array.len() {
                pairs.push((
                    extract_bulk_string(&array[i])?,
                    extract_bulk_string(&array[i + 1])?,
                ));
                i += 2;
            }
            Ok(Command::MSet { pairs })
        }
        "GETRANGE" | "SUBSTR" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::GetRange {
                key: extract_bulk_string(&array[1])?,
                start: extract_integer(&array[2])?,
                end: extract_integer(&array[3])?,
            })
        }
        "INCRBY" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::IncrBy {
                key: extract_bulk_string(&array[1])?,
                delta: extract_integer(&array[2])?,
            })
        }
        "DECRBY" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::DecrBy {
                key: extract_bulk_string(&array[1])?,
                delta: extract_integer(&array[2])?,
            })
        }
        "INCRBYFLOAT" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::IncrByFloat {
                key: extract_bulk_string(&array[1])?,
                delta: extract_float(&array[2])?,
            })
        }
        "HMGET" => {
            if array.len() < 3 { return Err("HMGET requires key + at least 1 field".to_string()); }
            let key = extract_bulk_string(&array[1])?;
            let fields = array[2..].iter().map(extract_bulk_string).collect::<Result<Vec<_>, _>>()?;
            Ok(Command::HMGet { key, fields })
        }
        "HMSET" => {
            if array.len() < 4 || (array.len() - 2) % 2 != 0 {
                return Err("HMSET requires key + non-empty field/value pairs".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let mut pairs = Vec::new();
            let mut i = 2;
            while i + 1 < array.len() {
                pairs.push((
                    extract_bulk_string(&array[i])?,
                    extract_bulk_string(&array[i + 1])?,
                ));
                i += 2;
            }
            Ok(Command::HMSet { key, pairs })
        }
        "HSETNX" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::HSetNx {
                key: extract_bulk_string(&array[1])?,
                field: extract_bulk_string(&array[2])?,
                value: extract_bulk_string(&array[3])?,
            })
        }
        "HINCRBY" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::HIncrBy {
                key: extract_bulk_string(&array[1])?,
                field: extract_bulk_string(&array[2])?,
                delta: extract_integer(&array[3])?,
            })
        }
        "LSET" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::LSet {
                key: extract_bulk_string(&array[1])?,
                index: extract_integer(&array[2])?,
                value: extract_bulk_string(&array[3])?,
            })
        }
        "LREM" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::LRem {
                key: extract_bulk_string(&array[1])?,
                count: extract_integer(&array[2])?,
                value: extract_bulk_string(&array[3])?,
            })
        }
        "LTRIM" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::LTrim {
                key: extract_bulk_string(&array[1])?,
                start: extract_integer(&array[2])?,
                stop: extract_integer(&array[3])?,
            })
        }
        "RPOPLPUSH" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::RPopLPush {
                source: extract_bulk_string(&array[1])?,
                destination: extract_bulk_string(&array[2])?,
            })
        }
        "SPOP" => {
            if array.len() < 2 || array.len() > 3 {
                return Err("SPOP requires 1 or 2 arguments".to_string());
            }
            let count = if array.len() == 3 {
                Some(extract_integer(&array[2])?.max(0) as usize)
            } else { None };
            Ok(Command::SPop { key: extract_bulk_string(&array[1])?, count })
        }
        "SRANDMEMBER" => {
            if array.len() < 2 || array.len() > 3 {
                return Err("SRANDMEMBER requires 1 or 2 arguments".to_string());
            }
            let count = if array.len() == 3 {
                Some(extract_integer(&array[2])?)
            } else { None };
            Ok(Command::SRandMember { key: extract_bulk_string(&array[1])?, count })
        }
        "SMOVE" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::SMove {
                source: extract_bulk_string(&array[1])?,
                destination: extract_bulk_string(&array[2])?,
                member: extract_bulk_string(&array[3])?,
            })
        }
        "ZINCRBY" => {
            ensure_arg_count(&array, 4)?;
            Ok(Command::ZIncrBy {
                key: extract_bulk_string(&array[1])?,
                delta: extract_float(&array[2])?,
                member: extract_bulk_string(&array[3])?,
            })
        }
        "ZRANGEBYSCORE" => {
            if array.len() < 4 { return Err("ZRANGEBYSCORE requires key min max".to_string()); }
            let with_scores = array.len() >= 5
                && String::from_utf8_lossy(&extract_bulk_string(&array[4])?).eq_ignore_ascii_case("WITHSCORES");
            Ok(Command::ZRangeByScore {
                key: extract_bulk_string(&array[1])?,
                min: extract_float(&array[2])?,
                max: extract_float(&array[3])?,
                with_scores,
            })
        }
        "ZREVRANGE" => {
            if array.len() < 4 { return Err("ZREVRANGE requires key start stop".to_string()); }
            let with_scores = array.len() >= 5
                && String::from_utf8_lossy(&extract_bulk_string(&array[4])?).eq_ignore_ascii_case("WITHSCORES");
            Ok(Command::ZRevRange {
                key: extract_bulk_string(&array[1])?,
                start: extract_integer(&array[2])?,
                stop: extract_integer(&array[3])?,
                with_scores,
            })
        }
        "ZPOPMIN" => {
            if array.len() < 2 || array.len() > 3 {
                return Err("ZPOPMIN requires 1 or 2 arguments".to_string());
            }
            let count = if array.len() == 3 {
                extract_integer(&array[2])?.max(0) as usize
            } else { 1 };
            Ok(Command::ZPopMin { key: extract_bulk_string(&array[1])?, count })
        }
        "ZPOPMAX" => {
            if array.len() < 2 || array.len() > 3 {
                return Err("ZPOPMAX requires 1 or 2 arguments".to_string());
            }
            let count = if array.len() == 3 {
                extract_integer(&array[2])?.max(0) as usize
            } else { 1 };
            Ok(Command::ZPopMax { key: extract_bulk_string(&array[1])?, count })
        }
        "TYPE" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::Type { key: extract_bulk_string(&array[1])? })
        }
        "RENAME" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::Rename {
                key: extract_bulk_string(&array[1])?,
                new_key: extract_bulk_string(&array[2])?,
            })
        }
        "RANDOMKEY" => {
            ensure_arg_count(&array, 1)?;
            Ok(Command::RandomKey)
        }

        // ===== P4.1 Pub/Sub =====
        "PUBLISH" => {
            ensure_arg_count(&array, 3)?;
            Ok(Command::Publish {
                channel: extract_bulk_string(&array[1])?,
                payload: extract_bulk_string(&array[2])?,
            })
        }
        "SUBSCRIBE" => {
            if array.len() < 2 { return Err("SUBSCRIBE requires at least 1 channel".to_string()); }
            let channels = array[1..].iter().map(extract_bulk_string).collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Subscribe { channels })
        }
        "UNSUBSCRIBE" => {
            // No args = unsubscribe from everything currently subscribed.
            let channels = if array.len() == 1 {
                Vec::new()
            } else {
                array[1..].iter().map(extract_bulk_string).collect::<Result<Vec<_>, _>>()?
            };
            Ok(Command::Unsubscribe { channels })
        }

        // ===== P4.2 Transactions =====
        "MULTI" => { ensure_arg_count(&array, 1)?; Ok(Command::Multi) }
        "EXEC" => { ensure_arg_count(&array, 1)?; Ok(Command::Exec) }
        "DISCARD" => { ensure_arg_count(&array, 1)?; Ok(Command::Discard) }
        "WATCH" => {
            if array.len() < 2 { return Err("WATCH requires at least 1 key".to_string()); }
            let keys = array[1..].iter().map(extract_bulk_string).collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Watch { keys })
        }
        "UNWATCH" => { ensure_arg_count(&array, 1)?; Ok(Command::Unwatch) }

        // ===== P4.3 Streams =====
        "XADD" => {
            // XADD key id|* field value [field value...]
            if array.len() < 5 || (array.len() - 3) % 2 != 0 {
                return Err("XADD requires key, id, and at least one field/value pair".to_string());
            }
            let key = extract_bulk_string(&array[1])?;
            let id_bytes = extract_bulk_string(&array[2])?;
            let id_str = String::from_utf8_lossy(&id_bytes).to_string();
            let id = if id_str == "*" { None } else { Some(id_str) };
            let mut fields = Vec::new();
            let mut i = 3;
            while i + 1 < array.len() {
                fields.push((extract_bulk_string(&array[i])?, extract_bulk_string(&array[i + 1])?));
                i += 2;
            }
            Ok(Command::XAdd { key, id, fields })
        }
        "XLEN" => {
            ensure_arg_count(&array, 2)?;
            Ok(Command::XLen { key: extract_bulk_string(&array[1])? })
        }
        "XRANGE" => {
            // XRANGE key start end [COUNT n]
            if array.len() < 4 || array.len() == 5 || array.len() > 6 {
                return Err("XRANGE requires key, start, end, [COUNT n]".to_string());
            }
            let count = if array.len() == 6 {
                let kw = extract_bulk_string(&array[4])?;
                if !String::from_utf8_lossy(&kw).eq_ignore_ascii_case("COUNT") {
                    return Err("XRANGE: expected COUNT keyword".to_string());
                }
                Some(extract_integer(&array[5])?.max(0) as usize)
            } else { None };
            Ok(Command::XRange {
                key: extract_bulk_string(&array[1])?,
                start: String::from_utf8_lossy(&extract_bulk_string(&array[2])?).to_string(),
                end: String::from_utf8_lossy(&extract_bulk_string(&array[3])?).to_string(),
                count,
            })
        }
        "XREAD" => {
            // XREAD [COUNT n] [BLOCK ms] STREAMS key [key...] id [id...]
            // We parse but do NOT honor BLOCK (returns immediately).
            let mut idx = 1usize;
            let mut count: Option<usize> = None;
            // Optional COUNT n
            if array.len() > idx + 1 {
                let kw = extract_bulk_string(&array[idx])?;
                if String::from_utf8_lossy(&kw).eq_ignore_ascii_case("COUNT") {
                    idx += 1;
                    count = Some(extract_integer(&array[idx])?.max(0) as usize);
                    idx += 1;
                }
            }
            // Optional BLOCK ms (consumed and ignored)
            if array.len() > idx + 1 {
                let kw = extract_bulk_string(&array[idx])?;
                if String::from_utf8_lossy(&kw).eq_ignore_ascii_case("BLOCK") {
                    idx += 2;
                }
            }
            // STREAMS keyword
            if array.len() <= idx {
                return Err("XREAD requires STREAMS keyword".to_string());
            }
            let kw = extract_bulk_string(&array[idx])?;
            if !String::from_utf8_lossy(&kw).eq_ignore_ascii_case("STREAMS") {
                return Err("XREAD: expected STREAMS".to_string());
            }
            idx += 1;
            // Half of remaining is keys, half is ids.
            let remaining = array.len() - idx;
            if remaining == 0 || remaining % 2 != 0 {
                return Err("XREAD: STREAMS must be followed by an even number of args".to_string());
            }
            let n = remaining / 2;
            let mut keys = Vec::with_capacity(n);
            let mut ids = Vec::with_capacity(n);
            for i in 0..n {
                keys.push(extract_bulk_string(&array[idx + i])?);
            }
            for i in 0..n {
                let b = extract_bulk_string(&array[idx + n + i])?;
                ids.push(String::from_utf8_lossy(&b).to_string());
            }
            Ok(Command::XRead { count, keys, ids })
        }

        // ===== P4.4 Lua scripting =====
        "EVAL" | "EVALSHA" => {
            if array.len() < 3 {
                return Err(format!("{} requires script + numkeys", cmd_name));
            }
            let script_or_sha = extract_bulk_string(&array[1])?;
            let numkeys = extract_integer(&array[2])?.max(0) as usize;
            if array.len() < 3 + numkeys {
                return Err(format!("{}: not enough keys for numkeys={}", cmd_name, numkeys));
            }
            let mut keys = Vec::with_capacity(numkeys);
            for i in 0..numkeys {
                keys.push(extract_bulk_string(&array[3 + i])?);
            }
            let argv: Vec<Bytes> = array[3 + numkeys..].iter()
                .map(extract_bulk_string).collect::<Result<Vec<_>, _>>()?;
            if cmd_name == "EVAL" {
                let script = String::from_utf8_lossy(&script_or_sha).to_string();
                Ok(Command::LuaEval { script, keys, argv })
            } else {
                let sha1 = String::from_utf8_lossy(&script_or_sha).to_string();
                Ok(Command::LuaEvalSha { sha1, keys, argv })
            }
        }

        _ => Err(format!("Unknown command: {}", cmd_name)),
    }
}

fn extract_bulk_string(value: &RespValue) -> Result<Bytes, String> {
    match value {
        RespValue::BulkString(Some(bytes)) => Ok(bytes.clone()),
        RespValue::BulkString(None) => Err("Null bulk string".to_string()),
        RespValue::SimpleString(s) => Ok(Bytes::from(s.clone())),
        _ => Err(format!("Expected bulk string, got {:?}", value)),
    }
}

fn extract_integer(value: &RespValue) -> Result<i64, String> {
    match value {
        RespValue::Integer(n) => Ok(*n),
        RespValue::BulkString(Some(bytes)) => {
            let s = String::from_utf8_lossy(bytes);
            s.parse::<i64>()
                .map_err(|_| format!("Invalid integer: {}", s))
        }
        _ => Err(format!("Expected integer, got {:?}", value)),
    }
}

fn extract_float(value: &RespValue) -> Result<f64, String> {
    match value {
        RespValue::BulkString(Some(bytes)) => {
            let s = String::from_utf8_lossy(bytes);
            s.parse::<f64>()
                .map_err(|_| format!("Invalid float: {}", s))
        }
        RespValue::Integer(n) => Ok(*n as f64),
        _ => Err(format!("Expected float, got {:?}", value)),
    }
}

fn ensure_arg_count(array: &[RespValue], expected: usize) -> Result<(), String> {
    if array.len() != expected {
        Err(format!("Expected {} arguments, got {}", expected, array.len()))
    } else {
        Ok(())
    }
}

/// Serialize response to RESP format
pub fn serialize_response(response: Response) -> Bytes {
    if let Response::Raw(frames) = &response {
        // Pre-serialized RESP frames — write verbatim.
        return frames.clone();
    }
    serialize_resp(&response.to_resp())
}

/// Legacy function for compatibility - serialize command (used in client)
pub fn serialize_command(command: Command) -> Bytes {
    let resp = command_to_resp(&command);
    serialize_resp(&resp)
}

fn command_to_resp(command: &Command) -> RespValue {
    match command {
        Command::Get { key } => {
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("GET"))),
                RespValue::BulkString(Some(key.clone())),
            ])
        }
        Command::Set { key, value, ttl_secs } => {
            let mut arr = vec![
                RespValue::BulkString(Some(Bytes::from("SET"))),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(value.clone())),
            ];
            if let Some(ttl) = ttl_secs {
                arr.push(RespValue::BulkString(Some(Bytes::from("EX"))));
                arr.push(RespValue::BulkString(Some(Bytes::from(ttl.to_string()))));
            }
            RespValue::Array(arr)
        }
        Command::Delete { key } => {
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("DEL"))),
                RespValue::BulkString(Some(key.clone())),
            ])
        }
        _ => RespValue::Error("Command serialization not implemented".to_string()),
    }
}

/// Parse response from RESP (used in client)
pub fn parse_response(buf: Bytes) -> Result<Response, String> {
    let mut cursor = Cursor::new(buf.as_ref());
    let resp = parse_resp(&mut cursor)?;

    Ok(match resp {
        RespValue::SimpleString(s) if s == "OK" => Response::Ok,
        RespValue::SimpleString(_) | RespValue::BulkString(Some(_)) => {
            if let RespValue::BulkString(Some(bytes)) = resp {
                Response::Value(bytes)
            } else {
                Response::Error("Unexpected response type".to_string())
            }
        }
        RespValue::BulkString(None) => Response::NotFound,
        RespValue::Error(e) => Response::Error(e),
        RespValue::Integer(n) => Response::Integer(n),
        RespValue::Array(items) => {
            let bytes_items = items.into_iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => Some(b),
                    _ => None,
                })
                .collect();
            Response::Array(bytes_items)
        }
    })
}

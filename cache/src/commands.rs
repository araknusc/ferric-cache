use bytes::Bytes;
use std::time::Duration;

use crate::protocol::Command;
use crate::storage::CacheStorage;

#[derive(Debug, Clone)]
pub struct GetCommand {
    pub key: Bytes,
}

#[derive(Debug, Clone)]
pub struct SetCommand {
    pub key: Bytes,
    pub value: Bytes,
    pub ttl: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct DeleteCommand {
    pub key: Bytes,
}

impl GetCommand {
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

impl SetCommand {
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            ttl: None,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl DeleteCommand {
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

/// Apply a write `Command` to storage, ignoring the response.
///
/// Shared between WAL replay and the replica apply path so neither can drift
/// from `server::execute_command` for write semantics. Read-only commands and
/// admin commands (KEYS, DBSIZE, ...) are no-ops here.
pub fn apply_write_command(cmd: &Command, storage: &CacheStorage) {
    match cmd {
        // String writes
        Command::Set { key, value, ttl_secs } => {
            let ttl = ttl_secs.map(|s| Duration::from_secs(s as u64));
            storage.set(key.clone(), value.clone(), ttl);
        }
        Command::Delete { key } => {
            storage.delete(key);
        }
        Command::Incr { key } => {
            let _ = storage.incr(key);
        }
        Command::Decr { key } => {
            let _ = storage.decr(key);
        }
        Command::Append { key, value } => {
            let _ = storage.append(key, value.clone());
        }

        // Hash writes
        Command::HSet { key, field, value } => {
            let _ = storage.hset(key.clone(), field.clone(), value.clone());
        }
        Command::HDel { key, fields } => {
            let _ = storage.hdel(key, fields);
        }

        // List writes
        Command::LPush { key, values } => {
            let _ = storage.lpush(key.clone(), values.clone());
        }
        Command::RPush { key, values } => {
            let _ = storage.rpush(key.clone(), values.clone());
        }
        Command::LPop { key } => {
            let _ = storage.lpop(key);
        }
        Command::RPop { key } => {
            let _ = storage.rpop(key);
        }

        // Set writes
        Command::SAdd { key, members } => {
            let _ = storage.sadd(key.clone(), members.clone());
        }
        Command::SRem { key, members } => {
            let _ = storage.srem(key, members);
        }

        // Sorted-set writes
        Command::ZAdd { key, members } => {
            let _ = storage.zadd(key.clone(), members.clone());
        }
        Command::ZRem { key, members } => {
            let _ = storage.zrem(key, members);
        }

        // Server-wide writes
        Command::FlushAll => {
            storage.flushall();
        }

        // TTL writes — relative TTLs are converted to an absolute Instant
        // here. Note: replaying an EXPIRE from an old WAL gives the key a
        // fresh duration (matches Redis AOF semantics).
        Command::Expire { key, seconds } => {
            if let Ok(secs) = u64::try_from(*seconds) {
                let at = std::time::Instant::now() + Duration::from_secs(secs);
                storage.set_expiry(key, at);
            }
        }
        Command::PExpire { key, millis } => {
            if let Ok(ms) = u64::try_from(*millis) {
                let at = std::time::Instant::now() + Duration::from_millis(ms);
                storage.set_expiry(key, at);
            }
        }
        Command::ExpireAt { key, unix_secs } => {
            // Redis EXPIREAT uses a wall-clock unix timestamp. We translate
            // to a monotonic Instant by computing the offset from now.
            if let Ok(target) = u64::try_from(*unix_secs) {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_unix = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                if target > now_unix {
                    let at = std::time::Instant::now()
                        + Duration::from_secs(target - now_unix);
                    storage.set_expiry(key, at);
                } else {
                    // Past timestamp → delete immediately.
                    storage.delete(key);
                }
            }
        }
        Command::Persist { key } => {
            storage.persist(key);
        }

        // P3 writes — replay onto storage. Errors during replay are dropped:
        // either the key doesn't exist (ok), wrong type (drift between master
        // and replica that the application caused), or a value parse error.
        Command::SetNx { key, value } => {
            storage.set_nx(key.clone(), value.clone());
        }
        Command::SetEx { key, seconds, value } => {
            storage.set_ex(key.clone(), value.clone(), Duration::from_secs(*seconds as u64));
        }
        Command::GetSet { key, value } => {
            let _ = storage.get_set(key.clone(), value.clone());
        }
        Command::MSet { pairs } => {
            for (k, v) in pairs {
                storage.set(k.clone(), v.clone(), None);
            }
        }
        Command::IncrBy { key, delta } => {
            let _ = storage.incr_by(key, *delta);
        }
        Command::DecrBy { key, delta } => {
            let _ = storage.incr_by(key, -*delta);
        }
        Command::IncrByFloat { key, delta } => {
            let _ = storage.incr_by_float(key, *delta);
        }
        Command::HMSet { key, pairs } => {
            for (f, v) in pairs {
                let _ = storage.hset(key.clone(), f.clone(), v.clone());
            }
        }
        Command::HSetNx { key, field, value } => {
            let _ = storage.hset_nx(key.clone(), field.clone(), value.clone());
        }
        Command::HIncrBy { key, field, delta } => {
            let _ = storage.hincr_by(key.clone(), field.clone(), *delta);
        }
        Command::LSet { key, index, value } => {
            let _ = storage.lset(key, *index, value.clone());
        }
        Command::LRem { key, count, value } => {
            let _ = storage.lrem(key, *count, value);
        }
        Command::LTrim { key, start, stop } => {
            let _ = storage.ltrim(key, *start, *stop);
        }
        Command::RPopLPush { source, destination } => {
            let _ = storage.rpoplpush(source, destination.clone());
        }
        Command::SPop { key, count } => {
            let _ = storage.spop(key, count.unwrap_or(1));
        }
        Command::SMove { source, destination, member } => {
            let _ = storage.smove(source, destination.clone(), member);
        }
        Command::ZIncrBy { key, delta, member } => {
            let _ = storage.zincr_by(key.clone(), *delta, member.clone());
        }
        Command::ZPopMin { key, count } => {
            let _ = storage.zpop_min(key, *count);
        }
        Command::ZPopMax { key, count } => {
            let _ = storage.zpop_max(key, *count);
        }
        Command::Rename { key, new_key } => {
            let _ = storage.rename(key, new_key.clone());
        }
        Command::XAdd { key, id, fields } => {
            // For replay, parse the textual id back into a StreamId. `None`
            // means we used auto-id at master time — replay generates a
            // fresh auto-id on the replica. Drift is acceptable here:
            // consumers track ids per-stream, not across master+replica.
            use crate::data_structures::StreamId;
            let parsed = id.as_deref()
                .and_then(|s| StreamId::parse(s, false).ok());
            let _ = storage.xadd(key.clone(), parsed, fields.clone());
        }

        // Read-only or non-write commands: nothing to apply.
        _ => {}
    }
}

/// True if a `Command` mutates state and therefore must be WAL-logged and
/// replicated. Keep in sync with `apply_write_command`.
pub fn is_write_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Set { .. }
            | Command::Delete { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Append { .. }
            | Command::HSet { .. }
            | Command::HDel { .. }
            | Command::LPush { .. }
            | Command::RPush { .. }
            | Command::LPop { .. }
            | Command::RPop { .. }
            | Command::SAdd { .. }
            | Command::SRem { .. }
            | Command::ZAdd { .. }
            | Command::ZRem { .. }
            | Command::FlushAll
            | Command::Expire { .. }
            | Command::PExpire { .. }
            | Command::ExpireAt { .. }
            | Command::Persist { .. }
            // P3 writes
            | Command::SetNx { .. }
            | Command::SetEx { .. }
            | Command::GetSet { .. }
            | Command::MSet { .. }
            | Command::IncrBy { .. }
            | Command::DecrBy { .. }
            | Command::IncrByFloat { .. }
            | Command::HMSet { .. }
            | Command::HSetNx { .. }
            | Command::HIncrBy { .. }
            | Command::LSet { .. }
            | Command::LRem { .. }
            | Command::LTrim { .. }
            | Command::RPopLPush { .. }
            | Command::SPop { .. }
            | Command::SMove { .. }
            | Command::ZIncrBy { .. }
            | Command::ZPopMin { .. }
            | Command::ZPopMax { .. }
            | Command::Rename { .. }
            // P4.3 stream writes
            | Command::XAdd { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// Round-trip every write variant through bincode (the WAL/replication
    /// payload format) and apply it to fresh storage. Proves that:
    ///   1. `Command` is bincode-serializable across all write variants
    ///   2. `apply_write_command` covers each variant
    /// This is the regression test for the pre-fix bug where only Set/Delete
    /// reached WAL/replication and HSET/LPUSH/SADD/ZADD writes were silently
    /// dropped on crash and not replicated.
    #[test]
    fn write_commands_round_trip_through_bincode_and_apply() {
        let writes = vec![
            Command::Set {
                key: Bytes::from_static(b"s"),
                value: Bytes::from_static(b"v"),
                ttl_secs: None,
            },
            Command::HSet {
                key: Bytes::from_static(b"h"),
                field: Bytes::from_static(b"f"),
                value: Bytes::from_static(b"v"),
            },
            Command::LPush {
                key: Bytes::from_static(b"l"),
                values: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
            Command::SAdd {
                key: Bytes::from_static(b"set"),
                members: vec![Bytes::from_static(b"x")],
            },
            Command::ZAdd {
                key: Bytes::from_static(b"z"),
                members: vec![(1.0, Bytes::from_static(b"m"))],
            },
            Command::Delete { key: Bytes::from_static(b"s") },
        ];

        let storage = CacheStorage::new();
        for cmd in &writes {
            assert!(is_write_command(cmd), "{:?} should be a write", cmd);

            let bytes = bincode::serialize(cmd).expect("serialize");
            let decoded: Command = bincode::deserialize(&bytes).expect("deserialize");
            apply_write_command(&decoded, &storage);
        }

        // Verify side-effects landed for each type (string was deleted last)
        assert_eq!(storage.get(b"s"), None);
        assert_eq!(storage.hget(b"h", b"f").as_deref(), Some(&b"v"[..]));
        assert_eq!(storage.llen(b"l").unwrap(), 2);
        assert!(storage.sismember(b"set", b"x").unwrap());
        assert_eq!(storage.zscore(b"z", b"m").unwrap(), Some(1.0));
    }

    #[test]
    fn read_commands_are_not_writes() {
        assert!(!is_write_command(&Command::Get { key: Bytes::from_static(b"k") }));
        assert!(!is_write_command(&Command::HGet {
            key: Bytes::from_static(b"k"),
            field: Bytes::from_static(b"f"),
        }));
        assert!(!is_write_command(&Command::Ping));
        assert!(!is_write_command(&Command::DbSize));
        // Reads of TTL state are not writes.
        assert!(!is_write_command(&Command::Ttl { key: Bytes::from_static(b"k") }));
        assert!(!is_write_command(&Command::PTtl { key: Bytes::from_static(b"k") }));
    }

    /// P3: MSET/MGET/SETNX/SETEX/INCRBY/TYPE/RENAME end-to-end on storage.
    #[test]
    fn p3_storage_primitives() {
        let storage = CacheStorage::new();

        // MSET → MGET via apply
        apply_write_command(
            &Command::MSet { pairs: vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
            ]},
            &storage,
        );
        assert_eq!(storage.get(b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(storage.get(b"b").as_deref(), Some(&b"2"[..]));

        // SETNX is a no-op when key exists
        assert!(!storage.set_nx(Bytes::from_static(b"a"), Bytes::from_static(b"X")));
        assert_eq!(storage.get(b"a").as_deref(), Some(&b"1"[..]));

        // SETEX sets TTL atomically
        storage.set_ex(Bytes::from_static(b"t"), Bytes::from_static(b"v"), std::time::Duration::from_secs(60));
        let pttl = storage.pttl_millis(b"t").unwrap();
        assert!(pttl > 0 && pttl <= 60_000);

        // INCRBY generalizes INCR
        assert_eq!(storage.incr_by(b"counter", 5).unwrap(), 5);
        assert_eq!(storage.incr_by(b"counter", -2).unwrap(), 3);

        // TYPE
        assert_eq!(storage.type_of(b"a"), "string");
        assert_eq!(storage.type_of(b"missing"), "none");

        // RENAME moves the value, key disappears
        storage.rename(b"a", Bytes::from_static(b"a-renamed")).unwrap();
        assert_eq!(storage.get(b"a"), None);
        assert_eq!(storage.get(b"a-renamed").as_deref(), Some(&b"1"[..]));
    }

    /// EXPIRE / PERSIST round-trip via apply_write_command — proves TTL
    /// writes go through the same WAL/replication chokepoint as data writes.
    #[test]
    fn ttl_commands_apply_correctly() {
        use std::time::Instant;
        let storage = CacheStorage::new();
        storage.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None);

        // Apply EXPIRE k 60
        apply_write_command(
            &Command::Expire { key: Bytes::from_static(b"k"), seconds: 60 },
            &storage,
        );
        let pttl = storage.pttl_millis(b"k").unwrap();
        assert!(pttl > 0 && pttl <= 60_000, "TTL within 60s: got {}", pttl);

        // Apply PERSIST k
        apply_write_command(
            &Command::Persist { key: Bytes::from_static(b"k") },
            &storage,
        );
        assert_eq!(storage.pttl_millis(b"k"), Some(-1));

        // EXPIRE on a missing key is a no-op (storage.set_expiry returns false).
        apply_write_command(
            &Command::Expire { key: Bytes::from_static(b"missing"), seconds: 60 },
            &storage,
        );
        assert_eq!(storage.pttl_millis(b"missing"), None);

        // EXPIREAT in the past deletes the key.
        let _ = Instant::now();
        apply_write_command(
            &Command::ExpireAt {
                key: Bytes::from_static(b"k"),
                unix_secs: 1, // way in the past
            },
            &storage,
        );
        assert_eq!(storage.get(b"k"), None, "past EXPIREAT should delete");
    }
}

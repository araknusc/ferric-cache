use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use bytes::Bytes;
use xxhash_rust::xxh3;

use crate::data_structures::{CacheValue, HashValue, ListValue, SetValue, SortedSetValue, StreamValue, StreamId};

/// Number of shards. Power of two so we can use a bitmask instead of `%`.
/// 64 keeps per-shard tables small (~1/64th of working set) while limiting
/// memory overhead to one HashMap header + RwLock per shard. Empirical
/// sweet spot for similar caches (DashMap defaults to 4×ncpu).
const SHARD_COUNT: usize = 64;
const SHARD_MASK: u64 = (SHARD_COUNT as u64) - 1;

/// Sharded key→value map. Single-key operations route to one shard via
/// `xxh3(key) & mask`; global ops iterate all shards. Replaces a single
/// coarse `RwLock<HashMap>` so write contention scales out across shards.
pub struct ShardedMap {
    shards: Vec<RwLock<HashMap<Bytes, CacheValue>>>,
}

impl ShardedMap {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }

    /// Pick the shard that owns `key`. All single-key reads/writes go through
    /// this; multiple keys hashing to the same shard share a lock (rare).
    #[inline]
    pub fn shard(&self, key: &[u8]) -> &RwLock<HashMap<Bytes, CacheValue>> {
        let idx = (xxh3::xxh3_64(key) & SHARD_MASK) as usize;
        &self.shards[idx]
    }

    /// Iterate over every shard. Used by global ops (KEYS, FLUSHALL, DBSIZE,
    /// SINTER/SUNION/SDIFF, cleanup_expired). Each callback locks its shard
    /// independently so concurrent single-key ops on other shards stay live.
    #[inline]
    pub fn shards(&self) -> &[RwLock<HashMap<Bytes, CacheValue>>] {
        &self.shards
    }
}

pub struct CacheStorage {
    data: ShardedMap,
}

impl CacheStorage {
    pub fn new() -> Self {
        Self {
            data: ShardedMap::new(),
        }
    }

    // ============ String Operations ============

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        let entry = guard.get(&key_bytes)?;

        // Check expiration and type
        if entry.is_expired() {
            return None;
        }

        match entry {
            CacheValue::String(value, _) => Some(value.clone()),
            _ => None, // Wrong type
        }
    }

    pub fn set(&self, key: Bytes, value: Bytes, ttl: Option<Duration>) -> bool {
        let expires_at = ttl.map(|d| Instant::now() + d);
        let entry = CacheValue::String(value, expires_at);

        let mut guard = self.data.shard(&key).write();
        guard.insert(key, entry);
        true
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        guard.remove(&key_bytes).is_some()
    }

    pub fn incr(&self, key: &[u8]) -> Result<i64, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        let value = match guard.get_mut(&key_bytes) {
            Some(CacheValue::String(bytes, _)) => {
                let current = String::from_utf8_lossy(bytes)
                    .parse::<i64>()
                    .map_err(|_| "Value is not an integer")?;
                let new_value = current + 1;
                *bytes = Bytes::from(new_value.to_string());
                new_value
            }
            Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                // Key doesn't exist, create it with value 1
                guard.insert(key_bytes, CacheValue::String(Bytes::from("1"), None));
                1
            }
        };

        Ok(value)
    }

    pub fn decr(&self, key: &[u8]) -> Result<i64, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        let value = match guard.get_mut(&key_bytes) {
            Some(CacheValue::String(bytes, _)) => {
                let current = String::from_utf8_lossy(bytes)
                    .parse::<i64>()
                    .map_err(|_| "Value is not an integer")?;
                let new_value = current - 1;
                *bytes = Bytes::from(new_value.to_string());
                new_value
            }
            Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                // Key doesn't exist, create it with value -1
                guard.insert(key_bytes, CacheValue::String(Bytes::from("-1"), None));
                -1
            }
        };

        Ok(value)
    }

    pub fn append(&self, key: &[u8], value: Bytes) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        let new_len = match guard.get_mut(&key_bytes) {
            Some(CacheValue::String(existing, _)) => {
                let mut combined = existing.to_vec();
                combined.extend_from_slice(&value);
                let new_len = combined.len();
                *existing = Bytes::from(combined);
                new_len
            }
            Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let len = value.len();
                guard.insert(key_bytes, CacheValue::String(value, None));
                len
            }
        };

        Ok(new_len)
    }

    pub fn strlen(&self, key: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::String(bytes, _)) => Ok(bytes.len()),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    // ============ Hash Operations ============

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Option<Bytes> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Hash(hash)) if !hash.is_expired() => hash.get_field(field),
            _ => None,
        }
    }

    pub fn hset(&self, key: Bytes, field: Bytes, value: Bytes) -> Result<bool, String> {
        let mut guard = self.data.shard(&key).write();

        match guard.get_mut(&key) {
            Some(CacheValue::Hash(hash)) => {
                let is_new = hash.set_field(field, value).is_none();
                Ok(is_new)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let mut hash = HashValue::new();
                hash.set_field(field, value);
                guard.insert(key, CacheValue::Hash(hash));
                Ok(true)
            }
        }
    }

    pub fn hdel(&self, key: &[u8], fields: &[Bytes]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        match guard.get_mut(&key_bytes) {
            Some(CacheValue::Hash(hash)) => {
                let mut count = 0;
                for field in fields {
                    if hash.delete_field(field) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Hash(hash)) if !hash.is_expired() => Ok(hash.get_all()),
            Some(CacheValue::Hash(_)) => Ok(vec![]),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn hkeys(&self, key: &[u8]) -> Result<Vec<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Hash(hash)) if !hash.is_expired() => {
                Ok(hash.get_all().into_iter().map(|(k, _)| k).collect())
            }
            Some(CacheValue::Hash(_)) => Ok(vec![]),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn hvals(&self, key: &[u8]) -> Result<Vec<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Hash(hash)) if !hash.is_expired() => {
                Ok(hash.get_all().into_iter().map(|(_, v)| v).collect())
            }
            Some(CacheValue::Hash(_)) => Ok(vec![]),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn hlen(&self, key: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Hash(hash)) if !hash.is_expired() => Ok(hash.len()),
            Some(CacheValue::Hash(_)) => Ok(0),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Hash(hash)) if !hash.is_expired() => {
                Ok(hash.get_field(field).is_some())
            }
            Some(CacheValue::Hash(_)) => Ok(false),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(false),
        }
    }

    // ============ List Operations ============

    pub fn lpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<usize, String> {
        let mut guard = self.data.shard(&key).write();

        match guard.get_mut(&key) {
            Some(CacheValue::List(list)) => {
                for value in values {
                    list.push_left(value);
                }
                Ok(list.len())
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let mut list = ListValue::new();
                for value in values {
                    list.push_left(value);
                }
                let len = list.len();
                guard.insert(key, CacheValue::List(list));
                Ok(len)
            }
        }
    }

    pub fn rpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<usize, String> {
        let mut guard = self.data.shard(&key).write();

        match guard.get_mut(&key) {
            Some(CacheValue::List(list)) => {
                for value in values {
                    list.push_right(value);
                }
                Ok(list.len())
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let mut list = ListValue::new();
                for value in values {
                    list.push_right(value);
                }
                let len = list.len();
                guard.insert(key, CacheValue::List(list));
                Ok(len)
            }
        }
    }

    pub fn lpop(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        match guard.get_mut(&key_bytes) {
            Some(CacheValue::List(list)) => Ok(list.pop_left()),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(None),
        }
    }

    pub fn rpop(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        match guard.get_mut(&key_bytes) {
            Some(CacheValue::List(list)) => Ok(list.pop_right()),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(None),
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::List(list)) if !list.is_expired() => Ok(list.get_range(start, stop)),
            Some(CacheValue::List(_)) => Ok(vec![]),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn llen(&self, key: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::List(list)) if !list.is_expired() => Ok(list.len()),
            Some(CacheValue::List(_)) => Ok(0),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::List(list)) if !list.is_expired() => {
                let range = list.get_range(index, index);
                Ok(range.into_iter().next())
            }
            Some(CacheValue::List(_)) => Ok(None),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(None),
        }
    }

    // ============ Set Operations ============

    pub fn sadd(&self, key: Bytes, members: Vec<Bytes>) -> Result<usize, String> {
        let mut guard = self.data.shard(&key).write();

        match guard.get_mut(&key) {
            Some(CacheValue::Set(set)) => {
                let mut count = 0;
                for member in members {
                    if set.add(member) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let mut set = SetValue::new();
                let mut count = 0;
                for member in members {
                    if set.add(member) {
                        count += 1;
                    }
                }
                guard.insert(key, CacheValue::Set(set));
                Ok(count)
            }
        }
    }

    pub fn srem(&self, key: &[u8], members: &[Bytes]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        match guard.get_mut(&key_bytes) {
            Some(CacheValue::Set(set)) => {
                let mut count = 0;
                for member in members {
                    if set.remove(member) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn smembers(&self, key: &[u8]) -> Result<Vec<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Set(set)) if !set.is_expired() => Ok(set.get_all()),
            Some(CacheValue::Set(_)) => Ok(vec![]),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Set(set)) if !set.is_expired() => Ok(set.contains(member)),
            Some(CacheValue::Set(_)) => Ok(false),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(false),
        }
    }

    pub fn scard(&self, key: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::Set(set)) if !set.is_expired() => Ok(set.len()),
            Some(CacheValue::Set(_)) => Ok(0),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    /// Snapshot a Set under its shard's read lock. Returns:
    ///   Ok(Some(set))  → live set, cloned out
    ///   Ok(None)       → key missing or set expired
    ///   Err(...)       → wrong type at this key
    /// Used by sinter/sunion/sdiff so they don't hold cross-shard locks.
    fn snapshot_set(&self, key: &Bytes) -> Result<Option<SetValue>, String> {
        let guard = self.data.shard(key).read();
        match guard.get(key) {
            Some(CacheValue::Set(set)) if !set.is_expired() => Ok(Some(set.clone())),
            Some(CacheValue::Set(_)) => Ok(None),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(None),
        }
    }

    pub fn sinter(&self, keys: &[Bytes]) -> Result<Vec<Bytes>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut result: Option<SetValue> = None;

        for key in keys {
            match self.snapshot_set(key)? {
                Some(set) => match result {
                    None => result = Some(set),
                    Some(ref res) => {
                        let mut new_set = SetValue::new();
                        for m in res.intersection(&set) {
                            new_set.add(m);
                        }
                        result = Some(new_set);
                    }
                },
                // Intersection with empty set is empty.
                None => return Ok(vec![]),
            }
        }

        Ok(result.map(|s| s.get_all()).unwrap_or_default())
    }

    pub fn sunion(&self, keys: &[Bytes]) -> Result<Vec<Bytes>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut result = SetValue::new();
        for key in keys {
            if let Some(set) = self.snapshot_set(key)? {
                for member in set.get_all() {
                    result.add(member);
                }
            }
        }
        Ok(result.get_all())
    }

    pub fn sdiff(&self, keys: &[Bytes]) -> Result<Vec<Bytes>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut result = match self.snapshot_set(&keys[0])? {
            Some(set) => set,
            None => return Ok(vec![]),
        };

        for key in &keys[1..] {
            if let Some(set) = self.snapshot_set(key)? {
                let mut new_set = SetValue::new();
                for m in result.difference(&set) {
                    new_set.add(m);
                }
                result = new_set;
            }
        }

        Ok(result.get_all())
    }

    // ============ Sorted Set Operations ============

    pub fn zadd(&self, key: Bytes, members: Vec<(f64, Bytes)>) -> Result<usize, String> {
        let mut guard = self.data.shard(&key).write();

        match guard.get_mut(&key) {
            Some(CacheValue::SortedSet(zset)) => {
                let mut count = 0;
                for (score, member) in members {
                    zset.add(score, member);
                    count += 1;
                }
                Ok(count)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let mut zset = SortedSetValue::new();
                let count = members.len();
                for (score, member) in members {
                    zset.add(score, member);
                }
                guard.insert(key, CacheValue::SortedSet(zset));
                Ok(count)
            }
        }
    }

    pub fn zrem(&self, key: &[u8], members: &[Bytes]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();

        match guard.get_mut(&key_bytes) {
            Some(CacheValue::SortedSet(zset)) => {
                let mut count = 0;
                for member in members {
                    if zset.remove(member) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn zrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool) -> Result<Vec<(Bytes, Option<f64>)>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::SortedSet(zset)) if !zset.is_expired() => {
                let len = zset.len() as i64;
                let start_idx = if start < 0 { (len + start).max(0) as usize } else { start as usize };
                let stop_idx = if stop < 0 { (len + stop).max(0) as usize } else { stop as usize };
                Ok(zset.get_range(start_idx, stop_idx, with_scores))
            }
            Some(CacheValue::SortedSet(_)) => Ok(vec![]),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn zrank(&self, key: &[u8], member: &[u8]) -> Result<Option<usize>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::SortedSet(zset)) if !zset.is_expired() => Ok(zset.get_rank(member)),
            Some(CacheValue::SortedSet(_)) => Ok(None),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(None),
        }
    }

    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::SortedSet(zset)) if !zset.is_expired() => Ok(zset.get_score(member)),
            Some(CacheValue::SortedSet(_)) => Ok(None),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(None),
        }
    }

    pub fn zcard(&self, key: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::SortedSet(zset)) if !zset.is_expired() => Ok(zset.len()),
            Some(CacheValue::SortedSet(_)) => Ok(0),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();

        match guard.get(&key_bytes) {
            Some(CacheValue::SortedSet(zset)) if !zset.is_expired() => {
                Ok(zset.get_range_by_score(min, max, false).len())
            }
            Some(CacheValue::SortedSet(_)) => Ok(0),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    // ============ Server Operations ============

    pub fn exists(&self, keys: &[Bytes]) -> usize {
        // Each key may live on a different shard; lock per key briefly.
        keys.iter()
            .filter(|key| {
                let guard = self.data.shard(key).read();
                guard.get(*key).map(|v| !v.is_expired()).unwrap_or(false)
            })
            .count()
    }

    pub fn keys(&self, pattern: &str) -> Vec<Bytes> {
        // Iterate across all shards. Each shard locks independently so
        // concurrent single-key writes on other shards stay live.
        let mut out = Vec::new();
        for shard in self.data.shards() {
            let guard = shard.read();
            if pattern == "*" {
                for (k, v) in guard.iter() {
                    if !v.is_expired() {
                        out.push(k.clone());
                    }
                }
            } else {
                for (k, v) in guard.iter() {
                    let key_str = String::from_utf8_lossy(k);
                    if !v.is_expired() && Self::matches_pattern_static(&key_str, pattern) {
                        out.push(k.clone());
                    }
                }
            }
        }
        out
    }

    /// Simplistic glob matcher preserved from the original single-lock
    /// implementation: treats `*` as a wildcard by stripping it and doing
    /// a substring match. Good enough for common KEYS use; not a full glob.
    fn matches_pattern_static(key: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        key.contains(&pattern.replace('*', ""))
    }

    pub fn flushall(&self) {
        for shard in self.data.shards() {
            shard.write().clear();
        }
    }

    pub fn dbsize(&self) -> usize {
        self.data.shards().iter()
            .map(|shard| {
                let guard = shard.read();
                guard.values().filter(|v| !v.is_expired()).count()
            })
            .sum()
    }

    // ============ TTL Operations ============

    /// Set absolute expiry on an existing key. Returns true if the key
    /// existed (and TTL was applied), false otherwise — matching Redis
    /// EXPIRE semantics (1 / 0).
    pub fn set_expiry(&self, key: &[u8], expires_at: Instant) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(value) if !value.is_expired() => {
                *expires_at_mut(value) = Some(expires_at);
                true
            }
            _ => false,
        }
    }

    /// Milliseconds until expiration. Returns:
    ///   None     → key does not exist (caller emits -2)
    ///   Some(-1) → key exists but has no TTL
    ///   Some(n)  → milliseconds remaining (n >= 0)
    pub fn pttl_millis(&self, key: &[u8]) -> Option<i64> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        let value = guard.get(&key_bytes)?;
        if value.is_expired() {
            return None;
        }
        match expires_at_ref(value) {
            None => Some(-1),
            Some(at) => {
                let now = Instant::now();
                if *at <= now {
                    Some(0)
                } else {
                    Some(at.saturating_duration_since(now).as_millis() as i64)
                }
            }
        }
    }

    /// Clear TTL on a key. Returns true if the key existed AND had a TTL,
    /// matching Redis PERSIST semantics.
    pub fn persist(&self, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(value) if !value.is_expired() => {
                let slot = expires_at_mut(value);
                if slot.is_some() {
                    *slot = None;
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn cleanup_expired(&self) {
        for shard in self.data.shards() {
            let mut guard = shard.write();
            guard.retain(|_, entry| !entry.is_expired());
        }
    }

    pub async fn get_all_entries(&self) -> Result<Vec<(Bytes, CacheValue)>, Box<dyn std::error::Error + Send + Sync>> {
        let mut entries = Vec::new();
        for shard in self.data.shards() {
            let guard = shard.read();
            for (k, v) in guard.iter() {
                entries.push((k.clone(), v.clone()));
            }
        }
        Ok(entries)
    }
}

// ===== P3 Bucket 2: extended Redis-parity primitives =====
impl CacheStorage {
    /// Set if not exists (Redis SETNX). Returns true if the key was set.
    pub fn set_nx(&self, key: Bytes, value: Bytes) -> bool {
        let mut guard = self.data.shard(&key).write();
        match guard.get(&key) {
            Some(v) if !v.is_expired() => false,
            _ => {
                guard.insert(key, CacheValue::String(value, None));
                true
            }
        }
    }

    /// SET with TTL atomically (Redis SETEX).
    pub fn set_ex(&self, key: Bytes, value: Bytes, ttl: Duration) {
        let expires_at = Some(Instant::now() + ttl);
        let mut guard = self.data.shard(&key).write();
        guard.insert(key, CacheValue::String(value, expires_at));
    }

    /// GET old value, then SET new (Redis GETSET).
    pub fn get_set(&self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, String> {
        let mut guard = self.data.shard(&key).write();
        let old = match guard.get(&key) {
            Some(CacheValue::String(b, _)) => Some(b.clone()),
            Some(v) if !v.is_expired() => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            _ => None,
        };
        guard.insert(key, CacheValue::String(value, None));
        Ok(old)
    }

    /// Generalized integer increment (INCRBY / DECRBY).
    pub fn incr_by(&self, key: &[u8], delta: i64) -> Result<i64, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::String(bytes, _)) => {
                let current = String::from_utf8_lossy(bytes)
                    .parse::<i64>()
                    .map_err(|_| "Value is not an integer")?;
                let new_value = current.checked_add(delta)
                    .ok_or("ERR increment or decrement would overflow")?;
                *bytes = Bytes::from(new_value.to_string());
                Ok(new_value)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                guard.insert(key_bytes, CacheValue::String(Bytes::from(delta.to_string()), None));
                Ok(delta)
            }
        }
    }

    /// Float increment (INCRBYFLOAT). Stores the result as a string.
    pub fn incr_by_float(&self, key: &[u8], delta: f64) -> Result<f64, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::String(bytes, _)) => {
                let current = String::from_utf8_lossy(bytes)
                    .parse::<f64>()
                    .map_err(|_| "Value is not a valid float")?;
                let new_value = current + delta;
                if !new_value.is_finite() {
                    return Err("ERR increment would produce NaN or Infinity".to_string());
                }
                *bytes = Bytes::from(new_value.to_string());
                Ok(new_value)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                guard.insert(key_bytes, CacheValue::String(Bytes::from(delta.to_string()), None));
                Ok(delta)
            }
        }
    }

    /// Substring (GETRANGE / SUBSTR). Negative indices count from the end.
    pub fn get_range(&self, key: &[u8], start: i64, end: i64) -> Option<Bytes> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        let bytes = match guard.get(&key_bytes)? {
            CacheValue::String(b, _) => b.clone(),
            _ => return None,
        };
        let len = bytes.len() as i64;
        if len == 0 { return Some(Bytes::new()); }
        let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let e = if end < 0 { (len + end).max(0) } else { end.min(len - 1) } as usize;
        if s > e { return Some(Bytes::new()); }
        Some(bytes.slice(s..=e.min(bytes.len() - 1)))
    }

    /// Set hash field only if absent (HSETNX). Returns true if set.
    pub fn hset_nx(&self, key: Bytes, field: Bytes, value: Bytes) -> Result<bool, String> {
        let mut guard = self.data.shard(&key).write();
        match guard.get_mut(&key) {
            Some(CacheValue::Hash(h)) if !h.is_expired() => {
                if h.get_field(&field).is_some() { return Ok(false); }
                h.set_field(field, value);
                Ok(true)
            }
            Some(CacheValue::Hash(_)) | None => {
                let mut h = HashValue::new();
                h.set_field(field, value);
                guard.insert(key, CacheValue::Hash(h));
                Ok(true)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    /// HINCRBY: integer increment of a hash field.
    pub fn hincr_by(&self, key: Bytes, field: Bytes, delta: i64) -> Result<i64, String> {
        let mut guard = self.data.shard(&key).write();
        match guard.get_mut(&key) {
            Some(CacheValue::Hash(h)) if !h.is_expired() => {
                let current = match h.get_field(&field) {
                    Some(b) => String::from_utf8_lossy(&b).parse::<i64>()
                        .map_err(|_| "Value is not an integer")?,
                    None => 0,
                };
                let new_value = current.checked_add(delta)
                    .ok_or("ERR increment would overflow")?;
                h.set_field(field, Bytes::from(new_value.to_string()));
                Ok(new_value)
            }
            Some(CacheValue::Hash(_)) | None => {
                let mut h = HashValue::new();
                h.set_field(field, Bytes::from(delta.to_string()));
                guard.insert(key, CacheValue::Hash(h));
                Ok(delta)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    /// LSET: set element at index. Errors if index out of range.
    pub fn lset(&self, key: &[u8], index: i64, value: Bytes) -> Result<(), String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::List(l)) if !l.is_expired() => {
                let len = l.items.len() as i64;
                let real = if index < 0 { len + index } else { index };
                if real < 0 || real >= len {
                    return Err("ERR index out of range".to_string());
                }
                l.items[real as usize] = value;
                Ok(())
            }
            Some(CacheValue::List(_)) | None => Err("ERR no such key".to_string()),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    /// LREM: remove `count` occurrences of `value`. Returns # removed.
    /// count > 0: head→tail, count < 0: tail→head, count == 0: all.
    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::List(l)) if !l.is_expired() => {
                let mut removed = 0usize;
                if count >= 0 {
                    let limit = if count == 0 { usize::MAX } else { count as usize };
                    l.items.retain(|v| {
                        if removed < limit && v.as_ref() == value {
                            removed += 1;
                            false
                        } else { true }
                    });
                } else {
                    let limit = (-count) as usize;
                    let mut keep = Vec::with_capacity(l.items.len());
                    for v in l.items.iter().rev() {
                        if removed < limit && v.as_ref() == value {
                            removed += 1;
                        } else {
                            keep.push(v.clone());
                        }
                    }
                    keep.reverse();
                    l.items = keep.into();
                }
                Ok(removed)
            }
            _ => Ok(0),
        }
    }

    /// LTRIM: keep only elements within [start, stop].
    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<(), String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::List(l)) if !l.is_expired() => {
                let len = l.items.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                let e = if stop < 0 { (len + stop).max(-1) + 1 } else { (stop + 1).min(len) } as usize;
                if s >= e {
                    l.items.clear();
                } else {
                    l.items.drain(..s);
                    l.items.drain((e - s)..);
                }
                Ok(())
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(()),
        }
    }

    /// RPOPLPUSH: pop from source tail, push to destination head atomically.
    pub fn rpoplpush(&self, source: &[u8], destination: Bytes) -> Result<Option<Bytes>, String> {
        // Take both shards in deterministic order to avoid theoretical deadlock
        // (matters less for read-heavy ops, but cheap insurance).
        let src = Bytes::copy_from_slice(source);
        let mut sg = self.data.shard(&src).write();
        let popped = match sg.get_mut(&src) {
            Some(CacheValue::List(l)) if !l.is_expired() => l.items.pop_back(),
            Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => None,
        };
        drop(sg); // release source lock before locking destination
        if let Some(ref v) = popped {
            let mut dg = self.data.shard(&destination).write();
            match dg.get_mut(&destination) {
                Some(CacheValue::List(l)) if !l.is_expired() => l.items.push_front(v.clone()),
                Some(CacheValue::List(_)) | None => {
                    let mut new_list = ListValue::new();
                    new_list.items.push_front(v.clone());
                    dg.insert(destination, CacheValue::List(new_list));
                }
                Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
        Ok(popped)
    }

    /// SPOP: remove and return one (or `count`) random members.
    pub fn spop(&self, key: &[u8], count: usize) -> Result<Vec<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::Set(s)) if !s.is_expired() => {
                let mut out = Vec::with_capacity(count);
                let all = s.get_all();
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                let chosen: Vec<&Bytes> = all.choose_multiple(&mut rng, count).collect();
                for member in chosen {
                    s.remove(member);
                    out.push(member.clone());
                }
                Ok(out)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    /// SRANDMEMBER: return random members without removing them.
    /// Negative `count` allows duplicates.
    pub fn srand_member(&self, key: &[u8], count: i64) -> Result<Vec<Bytes>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        match guard.get(&key_bytes) {
            Some(CacheValue::Set(s)) if !s.is_expired() => {
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                let all = s.get_all();
                if count >= 0 {
                    Ok(all.choose_multiple(&mut rng, count as usize).cloned().collect())
                } else {
                    let n = (-count) as usize;
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        if let Some(v) = all.choose(&mut rng) {
                            out.push(v.clone());
                        }
                    }
                    Ok(out)
                }
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    /// SMOVE: atomically move a member from source set to destination set.
    pub fn smove(&self, source: &[u8], destination: Bytes, member: &[u8]) -> Result<bool, String> {
        let src = Bytes::copy_from_slice(source);
        let mut sg = self.data.shard(&src).write();
        let removed = match sg.get_mut(&src) {
            Some(CacheValue::Set(s)) if !s.is_expired() => s.remove(member),
            Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => false,
        };
        drop(sg);
        if removed {
            let mut dg = self.data.shard(&destination).write();
            match dg.get_mut(&destination) {
                Some(CacheValue::Set(s)) if !s.is_expired() => { s.add(Bytes::copy_from_slice(member)); }
                Some(CacheValue::Set(_)) | None => {
                    let mut new_set = SetValue::new();
                    new_set.add(Bytes::copy_from_slice(member));
                    dg.insert(destination, CacheValue::Set(new_set));
                }
                Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
        Ok(removed)
    }

    /// ZINCRBY: increment a sorted-set member's score.
    pub fn zincr_by(&self, key: Bytes, delta: f64, member: Bytes) -> Result<f64, String> {
        let mut guard = self.data.shard(&key).write();
        match guard.get_mut(&key) {
            Some(CacheValue::SortedSet(z)) if !z.is_expired() => {
                let new_score = z.get_score(&member).unwrap_or(0.0) + delta;
                z.add(new_score, member);
                Ok(new_score)
            }
            Some(CacheValue::SortedSet(_)) | None => {
                let mut z = SortedSetValue::new();
                z.add(delta, member);
                guard.insert(key, CacheValue::SortedSet(z));
                Ok(delta)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    /// ZRANGEBYSCORE: members with score in [min, max].
    pub fn zrange_by_score(&self, key: &[u8], min: f64, max: f64, with_scores: bool)
        -> Result<Vec<(Bytes, Option<f64>)>, String>
    {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        match guard.get(&key_bytes) {
            Some(CacheValue::SortedSet(z)) if !z.is_expired() => {
                Ok(z.get_range_by_score(min, max, with_scores))
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    /// ZREVRANGE: like ZRANGE but reversed.
    pub fn zrevrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool)
        -> Result<Vec<(Bytes, Option<f64>)>, String>
    {
        let mut all = self.zrange(key, 0, -1, with_scores)?;
        all.reverse();
        // After reversal, slice [start..stop] inclusive (Redis semantics).
        let len = all.len() as i64;
        let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let e = if stop < 0 { (len + stop).max(-1) + 1 } else { (stop + 1).min(len) } as usize;
        if s >= e { return Ok(vec![]); }
        Ok(all[s..e].to_vec())
    }

    /// ZPOPMIN: remove and return up to `count` lowest-scoring members.
    pub fn zpop_min(&self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::SortedSet(z)) if !z.is_expired() => Ok(z.pop_min(count)),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    /// ZPOPMAX: remove and return up to `count` highest-scoring members.
    pub fn zpop_max(&self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut guard = self.data.shard(&key_bytes).write();
        match guard.get_mut(&key_bytes) {
            Some(CacheValue::SortedSet(z)) if !z.is_expired() => Ok(z.pop_max(count)),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    /// TYPE: return Redis type name ("string"|"hash"|"list"|"set"|"zset"|"none").
    pub fn type_of(&self, key: &[u8]) -> &'static str {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        match guard.get(&key_bytes) {
            Some(v) if !v.is_expired() => match v {
                CacheValue::String(_, _) => "string",
                CacheValue::Hash(_) => "hash",
                CacheValue::List(_) => "list",
                CacheValue::Set(_) => "set",
                CacheValue::SortedSet(_) => "zset",
                CacheValue::Stream(_) => "stream",
            },
            _ => "none",
        }
    }

    /// RENAME: move value at `key` to `new_key`. Errors if key is missing.
    pub fn rename(&self, key: &[u8], new_key: Bytes) -> Result<(), String> {
        let key_bytes = Bytes::copy_from_slice(key);
        // Source first, then destination (separate shards likely).
        let value = {
            let mut sg = self.data.shard(&key_bytes).write();
            match sg.remove(&key_bytes) {
                Some(v) if !v.is_expired() => v,
                _ => return Err("ERR no such key".to_string()),
            }
        };
        let mut dg = self.data.shard(&new_key).write();
        dg.insert(new_key, value);
        Ok(())
    }

    // ===== P4.3 Streams =====

    /// XADD: append `fields` to the stream at `key`. `requested_id` is None
    /// for `*` (auto-generate). Creates the stream if it doesn't exist.
    pub fn xadd(&self, key: Bytes, requested_id: Option<StreamId>,
                fields: Vec<(Bytes, Bytes)>) -> Result<StreamId, String>
    {
        let mut guard = self.data.shard(&key).write();
        match guard.get_mut(&key) {
            Some(CacheValue::Stream(s)) if !s.is_expired() => s.add(requested_id, fields),
            Some(CacheValue::Stream(_)) | None => {
                let mut s = StreamValue::new();
                let id = s.add(requested_id, fields)?;
                guard.insert(key, CacheValue::Stream(s));
                Ok(id)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn xlen(&self, key: &[u8]) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        match guard.get(&key_bytes) {
            Some(CacheValue::Stream(s)) if !s.is_expired() => Ok(s.len()),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    pub fn xrange(&self, key: &[u8], start: StreamId, end: StreamId, count: Option<usize>)
        -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, String>
    {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        match guard.get(&key_bytes) {
            Some(CacheValue::Stream(s)) if !s.is_expired() => Ok(s.range(start, end, count)),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    pub fn xread(&self, key: &[u8], cursor: StreamId, count: Option<usize>)
        -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, String>
    {
        let key_bytes = Bytes::copy_from_slice(key);
        let guard = self.data.shard(&key_bytes).read();
        match guard.get(&key_bytes) {
            Some(CacheValue::Stream(s)) if !s.is_expired() => Ok(s.read_after(cursor, count)),
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
        }
    }

    /// RANDOMKEY: a random non-expired key from any shard, or None.
    pub fn random_key(&self) -> Option<Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        // Scan shards in random order; first non-empty shard supplies a key.
        let mut shard_indices: Vec<usize> = (0..self.data.shards().len()).collect();
        use rand::seq::SliceRandom;
        shard_indices.shuffle(&mut rng);
        for i in shard_indices {
            let guard = self.data.shards()[i].read();
            if let Some((k, _)) = guard.iter()
                .filter(|(_, v)| !v.is_expired())
                .choose(&mut rng)
            {
                return Some(k.clone());
            }
        }
        None
    }
}

/// Polymorphic accessors for the per-value `expires_at` slot. Each data type
/// carries it on the respective struct (String inlines it in the tuple).
fn expires_at_mut(value: &mut CacheValue) -> &mut Option<Instant> {
    match value {
        CacheValue::String(_, slot) => slot,
        CacheValue::Hash(h) => &mut h.expires_at,
        CacheValue::List(l) => &mut l.expires_at,
        CacheValue::Set(s) => &mut s.expires_at,
        CacheValue::SortedSet(z) => &mut z.expires_at,
        CacheValue::Stream(s) => &mut s.expires_at,
    }
}

fn expires_at_ref(value: &CacheValue) -> &Option<Instant> {
    match value {
        CacheValue::String(_, slot) => slot,
        CacheValue::Hash(h) => &h.expires_at,
        CacheValue::List(l) => &l.expires_at,
        CacheValue::Set(s) => &s.expires_at,
        CacheValue::SortedSet(z) => &z.expires_at,
        CacheValue::Stream(s) => &s.expires_at,
    }
}

#[cfg(test)]
mod sharding_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    /// Keys distribute across shards (not all in shard 0).
    #[test]
    fn keys_distribute_across_shards() {
        let storage = CacheStorage::new();
        for i in 0..1000u32 {
            let k = Bytes::from(format!("k{}", i));
            storage.set(k, Bytes::from_static(b"v"), None);
        }
        // dbsize must equal what we wrote (correctness across shards).
        assert_eq!(storage.dbsize(), 1000);

        // Across 64 shards, 1000 unique keys must touch many shards.
        let nonempty = storage.data.shards().iter()
            .filter(|s| !s.read().is_empty())
            .count();
        assert!(nonempty >= 32, "expected ≥32 shards used, got {}", nonempty);
    }

    /// Concurrent writes to disjoint keys land correctly under the new
    /// per-shard locking. With one big lock this would still work but slowly;
    /// this test mainly proves correctness of the dispatch.
    #[test]
    fn concurrent_writes_land_correctly() {
        let storage = Arc::new(CacheStorage::new());
        let threads: Vec<_> = (0..8u32).map(|t| {
            let s = Arc::clone(&storage);
            thread::spawn(move || {
                for i in 0..1000u32 {
                    let key = Bytes::from(format!("t{}-k{}", t, i));
                    s.set(key, Bytes::from_static(b"v"), None);
                }
            })
        }).collect();
        for h in threads { h.join().unwrap(); }

        assert_eq!(storage.dbsize(), 8 * 1000);
        assert_eq!(storage.get(b"t3-k500").as_deref(), Some(&b"v"[..]));
    }

    /// FLUSHALL clears every shard.
    #[test]
    fn flushall_clears_all_shards() {
        let storage = CacheStorage::new();
        for i in 0..100u32 {
            storage.set(Bytes::from(format!("k{}", i)), Bytes::from_static(b"v"), None);
        }
        assert_eq!(storage.dbsize(), 100);
        storage.flushall();
        assert_eq!(storage.dbsize(), 0);
    }

    /// Multi-key set ops still produce correct results across shards.
    #[test]
    fn sinter_across_shards() {
        let storage = CacheStorage::new();
        storage.sadd(
            Bytes::from_static(b"A"),
            vec![Bytes::from_static(b"x"), Bytes::from_static(b"y"), Bytes::from_static(b"z")],
        ).unwrap();
        storage.sadd(
            Bytes::from_static(b"B"),
            vec![Bytes::from_static(b"y"), Bytes::from_static(b"z"), Bytes::from_static(b"w")],
        ).unwrap();
        let mut got = storage.sinter(&[
            Bytes::from_static(b"A"),
            Bytes::from_static(b"B"),
        ]).unwrap();
        got.sort();
        assert_eq!(got, vec![Bytes::from_static(b"y"), Bytes::from_static(b"z")]);
    }
}

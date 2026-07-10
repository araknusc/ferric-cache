//! Redis-style optimistic-concurrency transactions.
//!
//! - MULTI / EXEC / DISCARD: per-connection command queue. Commands issued
//!   between MULTI and EXEC are not executed immediately — they're queued
//!   and reply `QUEUED`. EXEC runs them atomically.
//! - WATCH / UNWATCH: optimistic concurrency. WATCH captures a per-shard
//!   version stamp at the time of WATCH. EXEC aborts (returns nil) if any
//!   watched key's shard has been written to since.
//!
//! False-abort caveat: we use **per-shard** versioning, not per-key. If two
//! keys hash to the same shard, a write to one will abort a transaction
//! watching the other. With 64 shards and well-distributed keys this is
//! rare; clients retry on abort. Per-key versioning would require a parallel
//! HashMap and a write-path mutation — deliberately deferred.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;

use crate::protocol::Command;

/// Bumped on every write to a shard. WATCH captures the value at watch time;
/// EXEC compares — divergence aborts the transaction. One of these per
/// shard, parallel to `ShardedMap::shards`.
pub struct ShardVersions {
    versions: Vec<AtomicU64>,
}

impl ShardVersions {
    pub fn new(shard_count: usize) -> Self {
        let mut versions = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            versions.push(AtomicU64::new(0));
        }
        Self { versions }
    }

    #[inline]
    pub fn read(&self, shard_idx: usize) -> u64 {
        self.versions[shard_idx].load(Ordering::Acquire)
    }

    /// Called by the write path right before/after a shard mutation lands.
    /// Using a single fetch_add gives a per-shard monotonic counter.
    #[inline]
    pub fn bump(&self, shard_idx: usize) {
        self.versions[shard_idx].fetch_add(1, Ordering::Release);
    }
}

/// Per-connection transaction state. Lives inside `ConnState`.
pub struct TxState {
    /// `Some(queue)` between MULTI and EXEC/DISCARD. Commands accumulate
    /// here; the connection responds `QUEUED` instead of executing.
    pub queue: Option<Vec<Command>>,
    /// True once a queued command failed to parse — EXEC must then abort.
    pub queue_dirty: bool,
    /// shard_idx → version observed at WATCH time. EXEC aborts if any of
    /// these no longer matches the current ShardVersions.
    pub watched: HashMap<usize, u64>,
}

impl TxState {
    pub fn new() -> Self {
        Self { queue: None, queue_dirty: false, watched: HashMap::new() }
    }

    pub fn in_multi(&self) -> bool {
        self.queue.is_some()
    }

    pub fn start(&mut self) {
        self.queue = Some(Vec::new());
        self.queue_dirty = false;
    }

    pub fn enqueue(&mut self, cmd: Command) {
        if let Some(q) = &mut self.queue {
            q.push(cmd);
        }
    }

    pub fn discard(&mut self) {
        self.queue = None;
        self.queue_dirty = false;
        self.watched.clear();
    }

    pub fn take_queue(&mut self) -> Option<Vec<Command>> {
        self.queue.take()
    }

    /// Capture current versions for the shards owning `keys`. Subsequent
    /// EXEC verifies they haven't changed.
    pub fn watch(&mut self, shard_idx: usize, version: u64) {
        // Only store the *first* observed version per shard — re-watching
        // the same shard after a write would mask real conflicts.
        self.watched.entry(shard_idx).or_insert(version);
    }

    pub fn unwatch(&mut self) {
        self.watched.clear();
    }

    /// True if any watched shard has been written to since WATCH.
    pub fn any_watched_changed(&self, versions: &ShardVersions) -> bool {
        self.watched.iter().any(|(idx, v)| versions.read(*idx) != *v)
    }
}

impl Default for TxState {
    fn default() -> Self { Self::new() }
}

/// Compute the shard index a key hashes to. Mirrors `ShardedMap::shard`'s
/// bitmask logic but without taking the lock — used for WATCH.
pub fn shard_index_for(key: &[u8], shard_count: usize) -> usize {
    use xxhash_rust::xxh3;
    let mask = (shard_count as u64) - 1;
    (xxh3::xxh3_64(key) & mask) as usize
}

/// Convenience: a `ShardVersions` clone-able across the server.
pub type SharedVersions = Arc<ShardVersions>;

/// Commands that are NOT queued by MULTI — they execute immediately even
/// inside a transaction. Mirrors Redis.
pub fn is_tx_control(cmd: &Command) -> bool {
    matches!(cmd, Command::Multi | Command::Exec | Command::Discard
                | Command::Watch { .. } | Command::Unwatch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_then_unrelated_write_does_not_abort() {
        let v = ShardVersions::new(4);
        let mut tx = TxState::new();
        tx.watch(0, v.read(0));
        // Bumping a different shard is fine.
        v.bump(2);
        assert!(!tx.any_watched_changed(&v));
    }

    #[test]
    fn watch_then_write_to_same_shard_aborts() {
        let v = ShardVersions::new(4);
        let mut tx = TxState::new();
        tx.watch(1, v.read(1));
        v.bump(1);
        assert!(tx.any_watched_changed(&v));
    }

    #[test]
    fn rewatching_same_shard_keeps_first_version() {
        let v = ShardVersions::new(4);
        let mut tx = TxState::new();
        tx.watch(0, v.read(0));
        v.bump(0);
        // A second WATCH on a key in shard 0 must NOT mask the conflict.
        tx.watch(0, v.read(0));
        assert!(tx.any_watched_changed(&v),
            "rewatching after a write must still abort");
    }
}

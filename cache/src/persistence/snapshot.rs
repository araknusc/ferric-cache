use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::time::interval;
use bytes::Bytes;
use serde::{Serialize, Deserialize};

use crate::storage::CacheStorage;
use crate::protocol::Command;
use crate::commands::apply_write_command;
use super::PersistenceConfig;

/// Snapshot file magic + version. Bumped from the original string-only v1
/// format ("CACHESNP"): the v2 format captures *every* value type by storing
/// the reconstruction `Command`s for each key, applied on load through the same
/// `apply_write_command` path used by WAL replay and replication — so a
/// snapshot can never silently drop hashes/lists/sets/sorted-sets/streams.
const MAGIC_V2: &[u8; 8] = b"CACHESN2";
const MAGIC_V1: &[u8; 8] = b"CACHESNP";
const VERSION: u32 = 2;

#[derive(Debug)]
pub enum SnapshotError {
    IoError(std::io::Error),
    SerializationError(String),
    CorruptedSnapshot(String),
}

impl From<std::io::Error> for SnapshotError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "Snapshot IO error: {}", err),
            Self::SerializationError(msg) => write!(f, "Snapshot serialization error: {}", msg),
            Self::CorruptedSnapshot(msg) => write!(f, "Corrupted snapshot: {}", msg),
        }
    }
}

impl std::error::Error for SnapshotError {}

/// One key's worth of snapshot state: the commands that rebuild its value plus
/// an optional absolute expiry (unix epoch millis). Keeping the expiry separate
/// (rather than as a TTL on the reconstruction command) preserves the absolute
/// deadline across a restart instead of resetting it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotRecord {
    key: Bytes,
    commands: Vec<Command>,
    expire_unix_ms: Option<u64>,
}

pub struct SnapshotManager {
    storage: Arc<CacheStorage>,
    config: PersistenceConfig,
    wal: Option<Arc<crate::persistence::WriteAheadLog>>,
}

impl SnapshotManager {
    pub fn new(storage: Arc<CacheStorage>, config: PersistenceConfig) -> Self {
        Self {
            storage,
            config,
            wal: None,
        }
    }

    pub fn with_wal(storage: Arc<CacheStorage>, config: PersistenceConfig, wal: Arc<crate::persistence::WriteAheadLog>) -> Self {
        Self {
            storage,
            config,
            wal: Some(wal),
        }
    }

    pub async fn save_snapshot(&self) -> Result<(), SnapshotError> {
        self.save_snapshot_to_path(&self.config.snapshot_path).await?;

        // Truncating the WAL after a snapshot is only safe now that the
        // snapshot captures every value type. Before v2 this step destroyed
        // the only durable copy of hashes/lists/sets/zsets/streams.
        if let Some(wal) = &self.wal {
            if let Err(e) = wal.truncate_after_snapshot().await {
                eprintln!("Failed to truncate WAL after snapshot: {}", e);
                // Don't fail the snapshot operation if WAL truncation fails
            }
        }

        Ok(())
    }

    pub async fn save_snapshot_to_path(&self, path: &str) -> Result<(), SnapshotError> {
        let records = self.create_snapshot().await?;

        let blob = bincode::serialize(&records)
            .map_err(|e| SnapshotError::SerializationError(e.to_string()))?;
        let checksum = crc32fast::hash(&blob);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        // Write to a temp file, then atomically rename into place.
        let temp_path = format!("{}.tmp", path);
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)?;
            let mut writer = BufWriter::new(file);

            // Header: [MAGIC:8][VERSION:4][TIMESTAMP:8][RECORD_COUNT:8][BLOB_LEN:8][CHECKSUM:4]
            writer.write_all(MAGIC_V2)?;
            writer.write_all(&VERSION.to_be_bytes())?;
            writer.write_all(&timestamp.to_be_bytes())?;
            writer.write_all(&(records.len() as u64).to_be_bytes())?;
            writer.write_all(&(blob.len() as u64).to_be_bytes())?;
            writer.write_all(&checksum.to_be_bytes())?;
            writer.write_all(&blob)?;
            writer.flush()?;
        }

        std::fs::rename(temp_path, path)?;
        Ok(())
    }

    pub async fn load_snapshot(&self) -> Result<u64, SnapshotError> {
        self.load_snapshot_from_path(&self.config.snapshot_path).await
    }

    pub async fn load_snapshot_from_path(&self, path: &str) -> Result<u64, SnapshotError> {
        if !Path::new(path).exists() {
            return Ok(0);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic == MAGIC_V1 {
            eprintln!(
                "Warning: snapshot at {} uses the obsolete v1 (string-only) format and \
                 will be ignored; it will be replaced on the next snapshot.",
                path
            );
            return Ok(0);
        }
        if &magic != MAGIC_V2 {
            return Err(SnapshotError::CorruptedSnapshot("Invalid magic number".to_string()));
        }

        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];

        reader.read_exact(&mut u32_buf)?;
        let version = u32::from_be_bytes(u32_buf);
        if version != VERSION {
            return Err(SnapshotError::CorruptedSnapshot(format!("Unsupported version: {}", version)));
        }

        reader.read_exact(&mut u64_buf)?; // timestamp (informational)
        reader.read_exact(&mut u64_buf)?; // record count (informational)
        reader.read_exact(&mut u64_buf)?;
        let blob_len = u64::from_be_bytes(u64_buf) as usize;
        reader.read_exact(&mut u32_buf)?;
        let expected_checksum = u32::from_be_bytes(u32_buf);

        let mut blob = vec![0u8; blob_len];
        reader.read_exact(&mut blob)?;

        if crc32fast::hash(&blob) != expected_checksum {
            return Err(SnapshotError::CorruptedSnapshot("Blob checksum mismatch".to_string()));
        }

        let records: Vec<SnapshotRecord> = bincode::deserialize(&blob)
            .map_err(|e| SnapshotError::CorruptedSnapshot(e.to_string()))?;

        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let mut loaded_count = 0u64;
        for record in records {
            // Skip keys whose absolute expiry has already passed.
            if let Some(expire_ms) = record.expire_unix_ms {
                if expire_ms <= now_unix_ms {
                    continue;
                }
            }

            for cmd in &record.commands {
                apply_write_command(cmd, &self.storage);
            }

            if let Some(expire_ms) = record.expire_unix_ms {
                let remaining = Duration::from_millis(expire_ms - now_unix_ms);
                self.storage.set_expiry(&record.key, Instant::now() + remaining);
            }

            loaded_count += 1;
        }

        Ok(loaded_count)
    }

    async fn create_snapshot(&self) -> Result<Vec<SnapshotRecord>, SnapshotError> {
        let entries = self.storage.get_all_entries().await
            .map_err(|e| SnapshotError::SerializationError(e.to_string()))?;

        let now_instant = Instant::now();
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        // Convert a monotonic `Instant` expiry to an absolute unix-ms deadline.
        let to_unix_ms = |expires_at: Instant| -> u64 {
            let remaining = expires_at.saturating_duration_since(now_instant);
            now_unix_ms + remaining.as_millis() as u64
        };

        let records = entries.into_iter()
            .filter_map(|(key, value)| {
                Self::record_for(&key, &value, now_instant, &to_unix_ms)
            })
            .collect();

        Ok(records)
    }

    /// Build the reconstruction record for one key. Returns `None` for already
    /// expired or empty values (nothing to persist).
    fn record_for(
        key: &Bytes,
        value: &crate::data_structures::CacheValue,
        now: Instant,
        to_unix_ms: &impl Fn(Instant) -> u64,
    ) -> Option<SnapshotRecord> {
        use crate::data_structures::CacheValue;

        let (commands, expires_at): (Vec<Command>, Option<Instant>) = match value {
            CacheValue::String(v, expires_at) => {
                if matches!(expires_at, Some(e) if *e <= now) {
                    return None;
                }
                (
                    vec![Command::Set { key: key.clone(), value: v.clone(), ttl_secs: None }],
                    *expires_at,
                )
            }
            CacheValue::Hash(h) => {
                if h.is_expired() {
                    return None;
                }
                let cmds = h.get_all().into_iter()
                    .map(|(field, value)| Command::HSet { key: key.clone(), field, value })
                    .collect::<Vec<_>>();
                (cmds, h.expires_at)
            }
            CacheValue::List(l) => {
                if l.is_expired() {
                    return None;
                }
                let values: Vec<Bytes> = l.items.iter().cloned().collect();
                if values.is_empty() {
                    return None;
                }
                (vec![Command::RPush { key: key.clone(), values }], l.expires_at)
            }
            CacheValue::Set(s) => {
                if s.is_expired() {
                    return None;
                }
                let members = s.get_all();
                if members.is_empty() {
                    return None;
                }
                (vec![Command::SAdd { key: key.clone(), members }], s.expires_at)
            }
            CacheValue::SortedSet(ss) => {
                if ss.is_expired() {
                    return None;
                }
                let members: Vec<(f64, Bytes)> = ss
                    .get_range(0, usize::MAX, true)
                    .into_iter()
                    .filter_map(|(member, score)| score.map(|s| (s, member)))
                    .collect();
                if members.is_empty() {
                    return None;
                }
                (vec![Command::ZAdd { key: key.clone(), members }], ss.expires_at)
            }
            CacheValue::Stream(st) => {
                if st.is_expired() {
                    return None;
                }
                let cmds = st.entries.iter()
                    .map(|(id, fields)| Command::XAdd {
                        key: key.clone(),
                        id: Some(id.to_string()),
                        fields: fields.clone(),
                    })
                    .collect::<Vec<_>>();
                if cmds.is_empty() {
                    return None;
                }
                (cmds, st.expires_at)
            }
        };

        if commands.is_empty() {
            return None;
        }

        Some(SnapshotRecord {
            key: key.clone(),
            commands,
            expire_unix_ms: expires_at.map(to_unix_ms),
        })
    }

    pub async fn start_background_snapshots(manager: Arc<Self>) {
        let interval_secs = manager.config.snapshot_interval_secs;
        if interval_secs > 0 {
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(interval_secs));
                loop {
                    interval.tick().await;
                    if let Err(e) = manager.save_snapshot().await {
                        eprintln!("Background snapshot failed: {}", e);
                    } else {
                        println!("Background snapshot completed successfully");
                    }
                }
            });
        }
    }
}

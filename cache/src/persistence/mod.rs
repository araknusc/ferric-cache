pub mod wal;
pub mod snapshot;

pub use wal::{WriteAheadLog, WALEntry, WALError};
pub use snapshot::{SnapshotManager, SnapshotError};

#[derive(Debug, Clone)]
pub enum PersistenceMode {
    None,
    WAL,
    Snapshot,
    Both,
}

impl Default for PersistenceMode {
    fn default() -> Self {
        Self::None
    }
}

impl std::str::FromStr for PersistenceMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "wal" => Ok(Self::WAL),
            "snapshot" => Ok(Self::Snapshot),
            "both" => Ok(Self::Both),
            _ => Err(format!("Invalid persistence mode: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WALSyncPolicy {
    Always,
    EverySecond,
    Manual,
}

impl Default for WALSyncPolicy {
    fn default() -> Self {
        Self::EverySecond
    }
}

impl std::str::FromStr for WALSyncPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "always" => Ok(Self::Always),
            "every_sec" | "every_second" => Ok(Self::EverySecond),
            "manual" => Ok(Self::Manual),
            _ => Err(format!("Invalid WAL sync policy: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub mode: PersistenceMode,
    pub wal_sync_policy: WALSyncPolicy,
    pub snapshot_interval_secs: u64,
    pub max_wal_size_bytes: u64,
    pub wal_path: String,
    pub snapshot_path: String,
    /// Number of rotated WAL backup segments to retain. The WAL always keeps at
    /// least one segment on rotation (even if this is 0) so that hitting the
    /// size limit can never silently discard the only copy of unsnapshotted data.
    pub wal_backup_count: u32,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            mode: PersistenceMode::None,
            wal_sync_policy: WALSyncPolicy::EverySecond,
            snapshot_interval_secs: 300,
            max_wal_size_bytes: 100 * 1024 * 1024, // 100MB
            wal_path: "cache.wal".to_string(),
            snapshot_path: "cache.snapshot".to_string(),
            wal_backup_count: 1,
        }
    }
}
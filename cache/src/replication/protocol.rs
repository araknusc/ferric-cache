use serde::{Deserialize, Serialize};

use crate::protocol::Command;

/// Master ↔ replica wire protocol (bincode framed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationCommand {
    /// Replica connects to master
    Connect { replica_id: String },

    /// Request full synchronization
    Sync { offset: u64 },

    /// Replicate a write command — carries the full `Command` so any write
    /// variant (string/hash/list/set/sortedset) is faithfully applied on the
    /// replica.
    Write { command: Command },

    /// Heartbeat
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationResponse {
    Connected { master_id: String, offset: u64 },
    SyncComplete { offset: u64 },
    Ack { offset: u64 },
    Pong,
    Error { message: String },
}

impl ReplicationCommand {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        bincode::deserialize(data).map_err(|e| e.to_string())
    }
}

impl ReplicationResponse {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        bincode::deserialize(data).map_err(|e| e.to_string())
    }
}

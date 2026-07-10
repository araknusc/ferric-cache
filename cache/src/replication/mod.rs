mod master;
mod replica;
mod protocol;

pub use master::MasterReplicator;
pub use replica::ReplicaClient;
pub use protocol::{ReplicationCommand, ReplicationResponse};

/// Role of this cache node in replication
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Master,
    Replica { master_addr: String },
    Standalone,
}

/// Replication status information
#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    pub role: ReplicationRole,
    pub connected_replicas: usize,
    pub replication_offset: u64,
}

impl ReplicationInfo {
    pub fn new(role: ReplicationRole) -> Self {
        Self {
            role,
            connected_replicas: 0,
            replication_offset: 0,
        }
    }
}

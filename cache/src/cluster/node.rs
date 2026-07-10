use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

pub type NodeId = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Suspicious,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub cluster_addr: SocketAddr,
    pub status: NodeStatus,
    pub last_seen: Instant,
    pub last_seen_timestamp: u64, // For serialization
    pub version: u64,
}

// Custom serialization for Node
impl Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Node", 6)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("addr", &self.addr)?;
        state.serialize_field("cluster_addr", &self.cluster_addr)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("last_seen_timestamp", &self.last_seen_timestamp)?;
        state.serialize_field("version", &self.version)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct NodeData {
            id: NodeId,
            addr: SocketAddr,
            cluster_addr: SocketAddr,
            status: NodeStatus,
            last_seen_timestamp: u64,
            version: u64,
        }

        let data = NodeData::deserialize(deserializer)?;
        Ok(Node {
            id: data.id,
            addr: data.addr,
            cluster_addr: data.cluster_addr,
            status: data.status,
            last_seen: Instant::now(), // Will be updated based on timestamp
            last_seen_timestamp: data.last_seen_timestamp,
            version: data.version,
        })
    }
}

impl Node {
    pub fn new(id: String, addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        let now = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id,
            addr,
            cluster_addr,
            status: NodeStatus::Active,
            last_seen: now,
            last_seen_timestamp: timestamp,
            version: 0,
        }
    }

    pub fn is_alive(&self) -> bool {
        self.status == NodeStatus::Active
    }

    pub fn update_last_seen(&mut self) {
        self.last_seen = Instant::now();
        self.last_seen_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.status = NodeStatus::Active;
    }

    pub fn mark_suspicious(&mut self) {
        self.status = NodeStatus::Suspicious;
    }

    pub fn mark_failed(&mut self) {
        self.status = NodeStatus::Failed;
    }

    pub fn time_since_last_seen(&self) -> Duration {
        Instant::now() - self.last_seen
    }
}
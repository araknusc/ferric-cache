use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use parking_lot::RwLock;
use xxhash_rust::xxh3;

use super::node::{Node, NodeId};

const DEFAULT_VIRTUAL_NODES: usize = 150;

pub struct ConsistentHashRing {
    ring: Arc<RwLock<BTreeMap<u64, NodeId>>>,
    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,
    virtual_nodes: usize,
}

impl ConsistentHashRing {
    pub fn new() -> Self {
        Self::with_virtual_nodes(DEFAULT_VIRTUAL_NODES)
    }

    pub fn with_virtual_nodes(virtual_nodes: usize) -> Self {
        Self {
            ring: Arc::new(RwLock::new(BTreeMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            virtual_nodes,
        }
    }

    pub fn add_node(&self, node: Node) {
        let node_id = node.id.clone();

        // Add to nodes map
        self.nodes.write().insert(node_id.clone(), node);

        // Add virtual nodes to ring
        let mut ring = self.ring.write();
        for i in 0..self.virtual_nodes {
            let virtual_key = format!("{}:{}", node_id, i);
            let hash = xxh3::xxh3_64(virtual_key.as_bytes());
            ring.insert(hash, node_id.clone());
        }
    }

    pub fn remove_node(&self, node_id: &str) {
        // Remove from nodes map
        self.nodes.write().remove(node_id);

        // Remove virtual nodes from ring
        let mut ring = self.ring.write();
        ring.retain(|_, id| id != node_id);
    }

    pub fn get_node(&self, key: &[u8]) -> Option<NodeId> {
        let hash = xxh3::xxh3_64(key);
        let ring = self.ring.read();

        if ring.is_empty() {
            return None;
        }

        // Find the first node with hash >= key hash
        let node_id = ring
            .range(hash..)
            .next()
            .or_else(|| ring.iter().next())
            .map(|(_, id)| id.clone());

        node_id
    }

    pub fn get_nodes_for_replication(&self, key: &[u8], count: usize) -> Vec<NodeId> {
        let hash = xxh3::xxh3_64(key);
        let ring = self.ring.read();

        if ring.is_empty() {
            return Vec::new();
        }

        let mut nodes = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Start from the primary node
        let iter = ring.range(hash..).chain(ring.iter());

        for (_, node_id) in iter {
            if seen.insert(node_id.clone()) {
                nodes.push(node_id.clone());
                if nodes.len() >= count {
                    break;
                }
            }
        }

        nodes
    }

    pub fn get_all_nodes(&self) -> Vec<Node> {
        self.nodes.read().values().cloned().collect()
    }

    pub fn get_node_by_id(&self, node_id: &str) -> Option<Node> {
        self.nodes.read().get(node_id).cloned()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Resolve the `Node` responsible for `key`. Combines `get_node` (id by
    /// hash) with `get_node_by_id` (full node record).
    pub fn node_for_key(&self, key: &[u8]) -> Option<Node> {
        let id = self.get_node(key)?;
        self.get_node_by_id(&id)
    }
}

/// 14-bit slot derived from `key`. Used in `-MOVED <slot> <host:port>`
/// responses so smart clients can update their slot map. Note: Redis uses
/// CRC16(key) % 16384 — we use xxh3 % 16384 for consistency with this
/// codebase's distribution function. Slot numbers are therefore stable but
/// won't match Redis's; client correctness still holds because clients
/// always honor the host:port in the MOVED reply.
pub fn slot_for_key(key: &[u8]) -> u16 {
    (xxh3::xxh3_64(key) % 16384) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_consistent_hash_ring() {
        let ring = ConsistentHashRing::new();

        // Add nodes
        let node1 = Node::new(
            "node1".to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 17001),
        );
        let node2 = Node::new(
            "node2".to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 17002),
        );

        ring.add_node(node1);
        ring.add_node(node2);

        // Test key distribution
        let key1 = b"test_key_1";
        let key2 = b"test_key_2";

        let node_for_key1 = ring.get_node(key1);
        let node_for_key2 = ring.get_node(key2);

        assert!(node_for_key1.is_some());
        assert!(node_for_key2.is_some());

        // Keys should consistently map to the same node
        assert_eq!(ring.get_node(key1), node_for_key1);
        assert_eq!(ring.get_node(key2), node_for_key2);
    }

    #[test]
    fn test_node_removal() {
        let ring = ConsistentHashRing::new();

        let node = Node::new(
            "node1".to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 17001),
        );

        ring.add_node(node);
        assert_eq!(ring.node_count(), 1);

        ring.remove_node("node1");
        assert_eq!(ring.node_count(), 0);
        assert!(ring.get_node(b"test").is_none());
    }

    #[test]
    fn test_replication_nodes() {
        let ring = ConsistentHashRing::new();

        for i in 1..=3 {
            let node = Node::new(
                format!("node{}", i),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000 + i),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 17000 + i),
            );
            ring.add_node(node);
        }

        let replicas = ring.get_nodes_for_replication(b"test_key", 2);
        assert_eq!(replicas.len(), 2);

        // Should return different nodes
        assert_ne!(replicas[0], replicas[1]);
    }
}
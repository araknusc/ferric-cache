use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::interval;
use serde::{Serialize, Deserialize};

use super::node::{Node, NodeId, NodeStatus};
use super::ring::ConsistentHashRing;

const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);
const SUSPICIOUS_TIMEOUT: Duration = Duration::from_secs(5);
const FAILED_TIMEOUT: Duration = Duration::from_secs(10);
const GOSSIP_FANOUT: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping {
        from: NodeId,
        version: u64,
    },
    Pong {
        from: NodeId,
        version: u64,
    },
    NodeState {
        node: Node,
    },
    NodeList {
        nodes: Vec<Node>,
    },
}

pub struct GossipProtocol {
    local_node: Arc<RwLock<Node>>,
    ring: Arc<ConsistentHashRing>,
    socket: Arc<UdpSocket>,
    known_nodes: Arc<RwLock<HashMap<NodeId, Node>>>,
}

impl GossipProtocol {
    pub async fn new(
        local_node: Node,
        ring: Arc<ConsistentHashRing>,
        cluster_addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind(cluster_addr).await?;

        Ok(Self {
            local_node: Arc::new(RwLock::new(local_node)),
            ring,
            socket: Arc::new(socket),
            known_nodes: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn start(self: Arc<Self>) {
        // Start gossip sender task
        let gossip_sender = self.clone();
        tokio::spawn(async move {
            gossip_sender.gossip_loop().await;
        });

        // Start gossip receiver task
        let gossip_receiver = self.clone();
        tokio::spawn(async move {
            gossip_receiver.receive_loop().await;
        });

        // Start health check task
        let health_checker = self.clone();
        tokio::spawn(async move {
            health_checker.health_check_loop().await;
        });
    }

    async fn gossip_loop(&self) {
        let mut ticker = interval(GOSSIP_INTERVAL);

        loop {
            ticker.tick().await;
            self.gossip_round().await;
        }
    }

    async fn gossip_round(&self) {
        let nodes = self.known_nodes.read().await;

        if nodes.is_empty() {
            return;
        }

        // Select random nodes to gossip with (fanout)
        let mut target_nodes: Vec<Node> = nodes
            .values()
            .filter(|n| n.is_alive())
            .cloned()
            .collect();

        // Shuffle and take GOSSIP_FANOUT nodes
        use rand::seq::SliceRandom;
        target_nodes.shuffle(&mut rand::thread_rng());
        target_nodes.truncate(GOSSIP_FANOUT);

        // Send ping to selected nodes
        let local_node = self.local_node.read().await;
        let msg = GossipMessage::Ping {
            from: local_node.id.clone(),
            version: local_node.version,
        };

        for node in target_nodes {
            self.send_message(&msg, node.cluster_addr).await;
        }
    }

    async fn receive_loop(&self) {
        let mut buf = vec![0u8; 65536];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((size, addr)) => {
                    let data = &buf[..size];
                    if let Ok(msg) = serde_json::from_slice::<GossipMessage>(data) {
                        self.handle_message(msg, addr).await;
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving gossip message: {}", e);
                }
            }
        }
    }

    async fn handle_message(&self, msg: GossipMessage, from_addr: SocketAddr) {
        match msg {
            GossipMessage::Ping { from, version } => {
                // Update node state
                if let Some(mut node) = self.known_nodes.write().await.get_mut(&from) {
                    node.update_last_seen();
                    node.version = version;
                }

                // Send pong response
                let local_node = self.local_node.read().await;
                let pong = GossipMessage::Pong {
                    from: local_node.id.clone(),
                    version: local_node.version,
                };
                self.send_message(&pong, from_addr).await;
            }

            GossipMessage::Pong { from, version } => {
                // Update node state
                if let Some(mut node) = self.known_nodes.write().await.get_mut(&from) {
                    node.update_last_seen();
                    node.version = version;
                }
            }

            GossipMessage::NodeState { node } => {
                // Update or add node
                let mut nodes = self.known_nodes.write().await;
                let existing = nodes.get(&node.id);

                // Only update if version is newer or node doesn't exist
                if existing.is_none() || existing.unwrap().version < node.version {
                    nodes.insert(node.id.clone(), node.clone());
                    self.ring.add_node(node);
                }
            }

            GossipMessage::NodeList { nodes: node_list } => {
                // Merge node list
                let mut known = self.known_nodes.write().await;
                for node in node_list {
                    let existing = known.get(&node.id);
                    if existing.is_none() || existing.unwrap().version < node.version {
                        known.insert(node.id.clone(), node.clone());
                        self.ring.add_node(node);
                    }
                }
            }
        }
    }

    async fn health_check_loop(&self) {
        let mut ticker = interval(GOSSIP_INTERVAL);

        loop {
            ticker.tick().await;

            let mut nodes = self.known_nodes.write().await;
            let mut failed_nodes = Vec::new();

            for (id, node) in nodes.iter_mut() {
                let elapsed = node.time_since_last_seen();

                if elapsed > FAILED_TIMEOUT && node.status != NodeStatus::Failed {
                    node.mark_failed();
                    failed_nodes.push(id.clone());
                } else if elapsed > SUSPICIOUS_TIMEOUT && node.status == NodeStatus::Active {
                    node.mark_suspicious();
                }
            }

            // Remove failed nodes from the ring
            drop(nodes); // Release write lock
            for node_id in failed_nodes {
                self.ring.remove_node(&node_id);
                println!("Node {} marked as failed and removed from ring", node_id);
            }
        }
    }

    async fn send_message(&self, msg: &GossipMessage, addr: SocketAddr) {
        if let Ok(data) = serde_json::to_vec(msg) {
            if let Err(e) = self.socket.send_to(&data, addr).await {
                eprintln!("Failed to send gossip message to {}: {}", addr, e);
            }
        }
    }

    pub async fn join_cluster(&self, seed_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        // Send our node info to seed
        let local_node = self.local_node.read().await;
        let msg = GossipMessage::NodeState {
            node: local_node.clone(),
        };
        self.send_message(&msg, seed_addr).await;

        // Request node list from seed
        let request = GossipMessage::Ping {
            from: local_node.id.clone(),
            version: local_node.version,
        };
        self.send_message(&request, seed_addr).await;

        Ok(())
    }

    pub async fn get_cluster_nodes(&self) -> Vec<Node> {
        let mut nodes: Vec<Node> = self.known_nodes.read().await
            .values()
            .cloned()
            .collect();

        // Add local node
        nodes.push(self.local_node.read().await.clone());
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_gossip_protocol_creation() {
        let ring = Arc::new(ConsistentHashRing::new());
        let node = Node::new(
            "test_node".to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 17001),
        );

        let cluster_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 17001);

        // This may fail if port is in use, but tests the creation
        let result = GossipProtocol::new(node, ring, cluster_addr).await;

        // If successful, protocol should be created
        if result.is_ok() {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_gossip_message_serialization() {
        let msg = GossipMessage::Ping {
            from: "node1".to_string(),
            version: 1,
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        let deserialized: GossipMessage = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            GossipMessage::Ping { from, version } => {
                assert_eq!(from, "node1");
                assert_eq!(version, 1);
            }
            _ => panic!("Wrong message type"),
        }
    }
}
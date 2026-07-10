use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};

use crate::protocol::Command;
use super::protocol::{ReplicationCommand, ReplicationResponse};

/// Master node that replicates data to replicas
pub struct MasterReplicator {
    replicas: Arc<RwLock<HashMap<String, ReplicaConnection>>>,
    replication_offset: Arc<RwLock<u64>>,
    master_id: String,
}

struct ReplicaConnection {
    id: String,
    tx: mpsc::Sender<ReplicationCommand>,
}

impl MasterReplicator {
    pub fn new(master_id: String) -> Self {
        Self {
            replicas: Arc::new(RwLock::new(HashMap::new())),
            replication_offset: Arc::new(RwLock::new(0)),
            master_id,
        }
    }

    /// Start listening for replica connections
    pub async fn start_replication_listener(&self, addr: String) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&addr).await?;
        println!("Replication listener started on {}", addr);

        let replicas = self.replicas.clone();
        let replication_offset = self.replication_offset.clone();
        let master_id = self.master_id.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        println!("Replica connected from: {}", addr);
                        let replicas = replicas.clone();
                        let replication_offset = replication_offset.clone();
                        let master_id = master_id.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_replica_connection(
                                stream,
                                replicas,
                                replication_offset,
                                master_id,
                            )
                            .await
                            {
                                eprintln!("Replica connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept replica connection: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_replica_connection(
        mut stream: TcpStream,
        replicas: Arc<RwLock<HashMap<String, ReplicaConnection>>>,
        replication_offset: Arc<RwLock<u64>>,
        master_id: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Read connect command
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut cmd_buf = vec![0u8; len];
        stream.read_exact(&mut cmd_buf).await?;

        let cmd = ReplicationCommand::deserialize(&cmd_buf)?;

        let replica_id = match cmd {
            ReplicationCommand::Connect { replica_id } => {
                // Send connected response
                let offset = *replication_offset.read().await;
                let response = ReplicationResponse::Connected {
                    master_id: master_id.clone(),
                    offset,
                };
                let response_data = response.serialize();
                stream.write_u32(response_data.len() as u32).await?;
                stream.write_all(&response_data).await?;
                stream.flush().await?;
                replica_id
            }
            _ => return Err("Expected Connect command".into()),
        };

        // Create channel for sending replication commands
        let (tx, mut rx) = mpsc::channel::<ReplicationCommand>(1000);

        // Store replica connection
        replicas.write().await.insert(
            replica_id.clone(),
            ReplicaConnection {
                id: replica_id.clone(),
                tx,
            },
        );

        println!("Replica {} registered", replica_id);

        // Send replication commands to replica
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                let cmd_data = cmd.serialize();
                if stream.write_u32(cmd_data.len() as u32).await.is_err() {
                    break;
                }
                if stream.write_all(&cmd_data).await.is_err() {
                    break;
                }
                if stream.flush().await.is_err() {
                    break;
                }
            }
        });

        Ok(())
    }

    /// Replicate a write command to all replicas. Carries the full `Command`
    /// so any write variant (string/hash/list/set/sortedset) is faithfully
    /// applied on the replica.
    pub async fn replicate_write(&self, command: Command) {
        let cmd = ReplicationCommand::Write { command };

        let mut offset = self.replication_offset.write().await;
        *offset += 1;

        let replicas = self.replicas.read().await;
        for replica in replicas.values() {
            let _ = replica.tx.send(cmd.clone()).await;
        }
    }

    /// Get number of connected replicas
    pub async fn replica_count(&self) -> usize {
        self.replicas.read().await.len()
    }

    /// Get current replication offset
    pub async fn get_offset(&self) -> u64 {
        *self.replication_offset.read().await
    }
}

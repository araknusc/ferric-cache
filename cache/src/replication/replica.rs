use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

use crate::commands::apply_write_command;
use crate::storage::CacheStorage;
use super::protocol::{ReplicationCommand, ReplicationResponse};

/// Replica client that connects to master and receives replication
pub struct ReplicaClient {
    replica_id: String,
    master_addr: String,
    storage: Arc<CacheStorage>,
    replication_offset: u64,
}

impl ReplicaClient {
    pub fn new(replica_id: String, master_addr: String, storage: Arc<CacheStorage>) -> Self {
        Self {
            replica_id,
            master_addr,
            storage,
            replication_offset: 0,
        }
    }

    /// Start replication from master
    pub async fn start_replication(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Connecting to master at {}...", self.master_addr);

        loop {
            let should_retry = match self.connect_and_sync().await {
                Ok(_) => {
                    println!("Replication stopped");
                    false
                }
                Err(e) => {
                    eprintln!("Replication error: {}. Retrying in 5s...", e);
                    true
                }
            };

            if !should_retry {
                break;
            }
            time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }

    async fn connect_and_sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Connect to master
        let mut stream = TcpStream::connect(&self.master_addr).await?;
        println!("Connected to master");

        // Send connect command
        let connect_cmd = ReplicationCommand::Connect {
            replica_id: self.replica_id.clone(),
        };
        let cmd_data = connect_cmd.serialize();
        stream.write_u32(cmd_data.len() as u32).await?;
        stream.write_all(&cmd_data).await?;
        stream.flush().await?;

        // Read connected response
        let len = stream.read_u32().await? as usize;
        let mut response_buf = vec![0u8; len];
        stream.read_exact(&mut response_buf).await?;

        let response = ReplicationResponse::deserialize(&response_buf)?;
        match response {
            ReplicationResponse::Connected { master_id, offset } => {
                println!("Connected to master: {} at offset {}", master_id, offset);
                self.replication_offset = offset;
            }
            ReplicationResponse::Error { message } => {
                return Err(format!("Master error: {}", message).into());
            }
            _ => {
                return Err("Unexpected response from master".into());
            }
        }

        // Receive and apply replication commands
        loop {
            let len = stream.read_u32().await? as usize;
            let mut cmd_buf = vec![0u8; len];
            stream.read_exact(&mut cmd_buf).await?;

            let cmd = ReplicationCommand::deserialize(&cmd_buf)?;
            self.apply_command(cmd).await?;
        }
    }

    async fn apply_command(&mut self, cmd: ReplicationCommand) -> Result<(), Box<dyn std::error::Error>> {
        match cmd {
            ReplicationCommand::Write { command } => {
                apply_write_command(&command, &self.storage);
                self.replication_offset += 1;
            }
            ReplicationCommand::Ping => {}
            _ => {
                eprintln!("Unexpected command on replica: {:?}", cmd);
            }
        }
        Ok(())
    }

    /// Get current replication offset
    pub fn get_offset(&self) -> u64 {
        self.replication_offset
    }
}

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncRead, AsyncWrite};
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use bytes::{Bytes, BytesMut, Buf};
use tokio_rustls::TlsAcceptor;
use std::io::Cursor;

use crate::storage::CacheStorage;
use crate::protocol::{parse_resp, parse_command, serialize_response, Command, Response};
use crate::persistence::{WriteAheadLog, SnapshotManager, PersistenceConfig, PersistenceMode};
use crate::cluster::{ConsistentHashRing, GossipProtocol, Node, slot_for_key};
use crate::replication::{MasterReplicator, ReplicaClient, ReplicationRole};
use crate::commands::is_write_command;
use crate::security::AuthManager;
use crate::pubsub::{PubSub, SubscriberState, PubSubMessage};
use crate::transactions::{TxState, ShardVersions, shard_index_for, is_tx_control};
use crate::scripting::{LuaEngine, AclCheck, allow_all_acl};

const SHARD_COUNT: usize = 64;

/// Per-connection state. Kept on the stack of each connection task; never
/// shared across connections.
struct ConnState {
    /// `Some(username)` once an AUTH succeeds on this connection.
    authed_user: Option<String>,
    /// Pub/Sub subscription state for this connection.
    sub: SubscriberState,
    /// Transaction queue + watched shards.
    tx: TxState,
}

impl ConnState {
    fn new() -> Self {
        Self {
            authed_user: None,
            sub: SubscriberState::new(),
            tx: TxState::new(),
        }
    }
}

pub struct CacheServer {
    storage: Arc<CacheStorage>,
    addr: String,
    persistence_config: Option<PersistenceConfig>,
    wal: Option<Arc<WriteAheadLog>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    // Clustering
    node_id: Option<String>,
    ring: Option<Arc<ConsistentHashRing>>,
    gossip: Option<Arc<GossipProtocol>>,
    // TLS
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    tls_addr: Option<String>,
    // Replication
    replication_role: ReplicationRole,
    master_replicator: Option<Arc<MasterReplicator>>,
    replica_client: Option<Arc<tokio::sync::Mutex<ReplicaClient>>>,
    // Security: when Some, every connection must AUTH before issuing
    // non-auth commands (PING is exempt to mirror Redis behavior).
    auth: Option<Arc<AuthManager>>,
    // Pub/Sub registry (always-on, cheap empty HashMap by default).
    pubsub: Arc<PubSub>,
    // Per-shard write counters used by WATCH to detect concurrent mutations.
    versions: Arc<ShardVersions>,
    // Lua scripting engine (always-on; cheap empty HashMap by default).
    lua: Arc<LuaEngine>,
}

impl CacheServer {
    pub fn new(addr: String) -> Self {
        Self {
            storage: Arc::new(CacheStorage::new()),
            addr,
            persistence_config: None,
            wal: None,
            snapshot_manager: None,
            node_id: None,
            ring: None,
            gossip: None,
            tls_acceptor: None,
            tls_addr: None,
            replication_role: ReplicationRole::Standalone,
            master_replicator: None,
            replica_client: None,
            auth: None,
            pubsub: Arc::new(PubSub::new()),
            versions: Arc::new(ShardVersions::new(SHARD_COUNT)),
            lua: Arc::new(LuaEngine::new()),
        }
    }

    pub fn with_tls(&mut self, tls_acceptor: TlsAcceptor, tls_addr: String) {
        self.tls_acceptor = Some(Arc::new(tls_acceptor));
        self.tls_addr = Some(tls_addr);
    }

    /// Enable authentication. Once attached, every connection must run AUTH
    /// before issuing non-AUTH commands (PING is exempt). ACL rules on the
    /// authenticated user gate per-command access.
    pub fn with_auth(&mut self, auth: Arc<AuthManager>) {
        self.auth = Some(auth);
    }

    pub async fn with_persistence(addr: String, config: PersistenceConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = Arc::new(CacheStorage::new());

        let (wal, snapshot_manager) = match &config.mode {
            PersistenceMode::None => (None, None),
            PersistenceMode::WAL => {
                let wal = Arc::new(WriteAheadLog::new(&config).await?);
                (Some(wal), None)
            },
            PersistenceMode::Snapshot => {
                let snapshot_manager = Arc::new(SnapshotManager::new(Arc::clone(&storage), config.clone()));
                (None, Some(snapshot_manager))
            },
            PersistenceMode::Both => {
                let wal = Arc::new(WriteAheadLog::new(&config).await?);
                let snapshot_manager = Arc::new(SnapshotManager::with_wal(Arc::clone(&storage), config.clone(), Arc::clone(&wal)));
                (Some(wal), Some(snapshot_manager))
            }
        };

        Ok(Self {
            storage,
            addr,
            persistence_config: Some(config),
            wal,
            snapshot_manager,
            node_id: None,
            ring: None,
            gossip: None,
            tls_acceptor: None,
            tls_addr: None,
            replication_role: ReplicationRole::Standalone,
            master_replicator: None,
            replica_client: None,
            auth: None,
            pubsub: Arc::new(PubSub::new()),
            versions: Arc::new(ShardVersions::new(SHARD_COUNT)),
            lua: Arc::new(LuaEngine::new()),
        })
    }

    pub async fn with_clustering(
        addr: String,
        cluster_addr: String,
        node_id: String,
        seed_node: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = Arc::new(CacheStorage::new());
        let ring = Arc::new(ConsistentHashRing::new());

        // Parse addresses
        let cache_addr: SocketAddr = addr.parse()?;
        let cluster_socket_addr: SocketAddr = cluster_addr.parse()?;

        // Create local node
        let local_node = Node::new(node_id.clone(), cache_addr, cluster_socket_addr);

        // Add local node to ring
        ring.add_node(local_node.clone());

        // Create gossip protocol
        let gossip = Arc::new(GossipProtocol::new(
            local_node,
            ring.clone(),
            cluster_socket_addr,
        ).await?);

        // Join cluster if seed node provided
        if let Some(seed) = seed_node {
            let seed_addr: SocketAddr = seed.parse()?;
            gossip.join_cluster(seed_addr).await?;
        }

        Ok(Self {
            storage,
            addr,
            persistence_config: None,
            wal: None,
            snapshot_manager: None,
            node_id: Some(node_id),
            ring: Some(ring),
            gossip: Some(gossip),
            tls_acceptor: None,
            tls_addr: None,
            replication_role: ReplicationRole::Standalone,
            master_replicator: None,
            replica_client: None,
            auth: None,
            pubsub: Arc::new(PubSub::new()),
            versions: Arc::new(ShardVersions::new(SHARD_COUNT)),
            lua: Arc::new(LuaEngine::new()),
        })
    }

    pub async fn with_clustering_and_persistence(
        addr: String,
        cluster_addr: String,
        node_id: String,
        seed_node: Option<String>,
        config: PersistenceConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = Arc::new(CacheStorage::new());
        let ring = Arc::new(ConsistentHashRing::new());

        // Parse addresses
        let cache_addr: SocketAddr = addr.parse()?;
        let cluster_socket_addr: SocketAddr = cluster_addr.parse()?;

        // Create local node
        let local_node = Node::new(node_id.clone(), cache_addr, cluster_socket_addr);

        // Add local node to ring
        ring.add_node(local_node.clone());

        // Create gossip protocol
        let gossip = Arc::new(GossipProtocol::new(
            local_node,
            ring.clone(),
            cluster_socket_addr,
        ).await?);

        // Join cluster if seed node provided
        if let Some(seed) = seed_node {
            let seed_addr: SocketAddr = seed.parse()?;
            gossip.join_cluster(seed_addr).await?;
        }

        // Setup persistence
        let (wal, snapshot_manager) = match &config.mode {
            PersistenceMode::None => (None, None),
            PersistenceMode::WAL => {
                let wal = Arc::new(WriteAheadLog::new(&config).await?);
                (Some(wal), None)
            },
            PersistenceMode::Snapshot => {
                let snapshot_manager = Arc::new(SnapshotManager::new(Arc::clone(&storage), config.clone()));
                (None, Some(snapshot_manager))
            },
            PersistenceMode::Both => {
                let wal = Arc::new(WriteAheadLog::new(&config).await?);
                let snapshot_manager = Arc::new(SnapshotManager::with_wal(Arc::clone(&storage), config.clone(), Arc::clone(&wal)));
                (Some(wal), Some(snapshot_manager))
            }
        };

        Ok(Self {
            storage,
            addr,
            persistence_config: Some(config),
            wal,
            snapshot_manager,
            node_id: Some(node_id),
            ring: Some(ring),
            gossip: Some(gossip),
            tls_acceptor: None,
            tls_addr: None,
            replication_role: ReplicationRole::Standalone,
            master_replicator: None,
            replica_client: None,
            auth: None,
            pubsub: Arc::new(PubSub::new()),
            versions: Arc::new(ShardVersions::new(SHARD_COUNT)),
            lua: Arc::new(LuaEngine::new()),
        })
    }

    /// Configure this server as a replication master
    pub async fn as_master(&mut self, replication_port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let node_id = self.node_id.clone().unwrap_or_else(|| "master".to_string());
        let master_replicator = MasterReplicator::new(node_id);

        // Start replication listener
        let addr = format!("0.0.0.0:{}", replication_port);
        master_replicator.start_replication_listener(addr).await?;

        self.replication_role = ReplicationRole::Master;
        self.master_replicator = Some(Arc::new(master_replicator));

        println!("Server configured as replication master on port {}", replication_port);
        Ok(())
    }

    /// Configure this server as a replica
    pub async fn as_replica(&mut self, master_addr: String) -> Result<(), Box<dyn std::error::Error>> {
        let replica_id = self.node_id.clone().unwrap_or_else(|| format!("replica-{}", rand::random::<u32>()));
        let replica_client = ReplicaClient::new(
            replica_id,
            master_addr.clone(),
            Arc::clone(&self.storage),
        );

        self.replication_role = ReplicationRole::Replica { master_addr: master_addr.clone() };
        self.replica_client = Some(Arc::new(tokio::sync::Mutex::new(replica_client)));

        println!("Server configured as replica, connecting to master at {}", master_addr);
        Ok(())
    }

    pub async fn load_from_persistence(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Load from snapshot first if available
        if let Some(snapshot_manager) = &self.snapshot_manager {
            let loaded_count = snapshot_manager.load_snapshot().await?;
            if loaded_count > 0 {
                println!("Loaded {} entries from snapshot", loaded_count);
            }
        }

        // Then replay WAL entries on top, using the same write-apply helper
        // the replica uses — so persistence and replication can never drift.
        if let Some(wal) = &self.wal {
            let storage = Arc::clone(&self.storage);
            let replayed_count = wal.replay(move |entry| {
                crate::commands::apply_write_command(&entry.command, &storage);
                Ok(())
            }).await?;

            if replayed_count > 0 {
                println!("Replayed {} WAL entries", replayed_count);
            }
        }

        Ok(())
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start replication if configured as replica
        if let Some(replica_client) = &self.replica_client {
            let replica = Arc::clone(replica_client);
            tokio::spawn(async move {
                loop {
                    {
                        let mut client = replica.lock().await;
                        if let Err(e) = client.start_replication().await {
                            eprintln!("Replication error: {}. Retrying in 5s...", e);
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }

        // Start clustering if configured
        if let Some(gossip) = &self.gossip {
            println!("Starting cluster gossip protocol...");
            Arc::clone(gossip).start().await;
        }

        // Start background persistence tasks
        if let Some(wal) = &self.wal {
            WriteAheadLog::start_sync_task(Arc::clone(wal)).await;
        }

        if let Some(snapshot_manager) = &self.snapshot_manager {
            SnapshotManager::start_background_snapshots(Arc::clone(snapshot_manager)).await;
        }

        // Spawn background task for periodic cleanup
        let storage = Arc::clone(&self.storage);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                storage.cleanup_expired();
            }
        });

        // Start plain TCP listener
        let tcp_listener = TcpListener::bind(&self.addr).await?;
        println!("ferric-cache server listening on {} (RESP protocol)", self.addr);

        let storage_tcp = Arc::clone(&self.storage);
        let wal_tcp = self.wal.clone();
        let replicator_tcp = self.master_replicator.clone();
        let auth_tcp = self.auth.clone();
        let ring_tcp = self.ring.clone();
        let node_id_tcp = self.node_id.clone();
        let pubsub_tcp = Arc::clone(&self.pubsub);
        let versions_tcp = Arc::clone(&self.versions);
        let lua_tcp = Arc::clone(&self.lua);

        tokio::spawn(async move {
            loop {
                match tcp_listener.accept().await {
                    Ok((stream, addr)) => {
                        let storage = Arc::clone(&storage_tcp);
                        let wal = wal_tcp.clone();
                        let replicator = replicator_tcp.clone();
                        let auth = auth_tcp.clone();
                        let ring = ring_tcp.clone();
                        let node_id = node_id_tcp.clone();
                        let pubsub = Arc::clone(&pubsub_tcp);
                        let versions = Arc::clone(&versions_tcp);
                        let lua = Arc::clone(&lua_tcp);

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, storage, wal, replicator, auth, ring, node_id, pubsub, versions, lua).await {
                                eprintln!("Connection error from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => eprintln!("Failed to accept connection: {}", e),
                }
            }
        });

        // Start TLS listener if configured
        if let Some(tls_acceptor) = &self.tls_acceptor {
            if let Some(tls_addr) = &self.tls_addr {
                let tls_listener = TcpListener::bind(tls_addr).await?;
                println!("ferric-cache server listening on {} (TLS + RESP)", tls_addr);

                let storage_tls = Arc::clone(&self.storage);
                let wal_tls = self.wal.clone();
                let replicator_tls = self.master_replicator.clone();
                let auth_tls = self.auth.clone();
                let ring_tls = self.ring.clone();
                let node_id_tls = self.node_id.clone();
                let pubsub_tls = Arc::clone(&self.pubsub);
                let versions_tls = Arc::clone(&self.versions);
                let lua_tls = Arc::clone(&self.lua);
                let acceptor = Arc::clone(tls_acceptor);

                tokio::spawn(async move {
                    loop {
                        match tls_listener.accept().await {
                            Ok((stream, addr)) => {
                                let storage = Arc::clone(&storage_tls);
                                let wal = wal_tls.clone();
                                let replicator = replicator_tls.clone();
                                let auth = auth_tls.clone();
                                let ring = ring_tls.clone();
                                let node_id = node_id_tls.clone();
                                let pubsub = Arc::clone(&pubsub_tls);
                                let versions = Arc::clone(&versions_tls);
                                let lua = Arc::clone(&lua_tls);
                                let acceptor = Arc::clone(&acceptor);

                                tokio::spawn(async move {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            if let Err(e) = handle_tls_connection(tls_stream, storage, wal, replicator, auth, ring, node_id, pubsub, versions, lua).await {
                                                eprintln!("TLS connection error from {}: {}", addr, e);
                                            }
                                        }
                                        Err(e) => eprintln!("TLS handshake failed from {}: {}", addr, e),
                                    }
                                });
                            }
                            Err(e) => eprintln!("Failed to accept TLS connection: {}", e),
                        }
                    }
                });
            }
        }

        // Keep main task running
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<CacheStorage>,
    wal: Option<Arc<WriteAheadLog>>,
    replicator: Option<Arc<MasterReplicator>>,
    auth: Option<Arc<AuthManager>>,
    ring: Option<Arc<ConsistentHashRing>>,
    local_node_id: Option<String>,
    pubsub: Arc<PubSub>,
    versions: Arc<ShardVersions>,
    lua: Arc<LuaEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = BytesMut::with_capacity(8192);
    let mut conn = ConnState::new();
    // The pubsub receiver is owned out here (separate from `conn`) so the
    // select! loop can borrow it independently from the rest of `conn`.
    let mut subs_rx: Option<tokio::sync::mpsc::UnboundedReceiver<PubSubMessage>> = None;

    loop {
        tokio::select! {
            biased;
            // Pub/Sub mailbox → socket. The pending() arm keeps the branch
            // alive but never resolves while the connection has no
            // subscriptions; once SUBSCRIBE runs, subs_rx becomes Some and
            // real messages start flowing.
            msg = async {
                match subs_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending::<Option<PubSubMessage>>().await,
                }
            } => {
                if let Some(msg) = msg {
                    let frame = format_pubsub_message(&msg);
                    stream.write_all(&frame).await?;
                    stream.flush().await?;
                }
                // None means all senders dropped — loop will get pending()
                // until a new subscription happens.
            }
            // Socket → command pipeline.
            n = stream.read_buf(&mut buffer) => {
                let n = n?;
                if n == 0 { return Ok(()); }

                // Try to parse RESP commands from buffer
                while !buffer.is_empty() {
                    let mut cursor = Cursor::new(&buffer[..]);

                    match parse_resp(&mut cursor) {
                        Ok(resp_value) => {
                            let consumed = cursor.position() as usize;

                            let response = match parse_command(resp_value) {
                                Ok(cmd) => execute_command(cmd, &storage, &wal, &replicator, &auth, &mut conn, &ring, local_node_id.as_deref(), &pubsub, &versions, &lua).await,
                                Err(e) => Response::Error(format!("ERR {}", e)),
                            };

                            let response_bytes = serialize_response(response);
                            stream.write_all(&response_bytes).await?;
                            stream.flush().await?;
                            buffer.advance(consumed);

                            // After SUBSCRIBE creates the per-connection mailbox,
                            // hoist the receiver out of conn so the select! arm
                            // above can borrow it independently of `conn`.
                            if subs_rx.is_none() && conn.sub.rx.is_some() {
                                subs_rx = conn.sub.rx.take();
                            }
                        }
                        Err(_) => break, // incomplete; await more bytes
                    }
                }

                if buffer.len() > 1024 * 1024 {
                    return Err("Buffer overflow".into());
                }
            }
        }
    }
}

async fn handle_tls_connection<S>(
    mut stream: S,
    storage: Arc<CacheStorage>,
    wal: Option<Arc<WriteAheadLog>>,
    replicator: Option<Arc<MasterReplicator>>,
    auth: Option<Arc<AuthManager>>,
    ring: Option<Arc<ConsistentHashRing>>,
    local_node_id: Option<String>,
    pubsub: Arc<PubSub>,
    versions: Arc<ShardVersions>,
    lua: Arc<LuaEngine>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffer = BytesMut::with_capacity(8192);
    let mut conn = ConnState::new();
    let mut subs_rx: Option<tokio::sync::mpsc::UnboundedReceiver<PubSubMessage>> = None;

    loop {
        tokio::select! {
            biased;
            msg = async {
                match subs_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending::<Option<PubSubMessage>>().await,
                }
            } => {
                if let Some(msg) = msg {
                    let frame = format_pubsub_message(&msg);
                    stream.write_all(&frame).await?;
                    stream.flush().await?;
                }
            }
            n = stream.read_buf(&mut buffer) => {
                let n = n?;
                if n == 0 { return Ok(()); }

                while !buffer.is_empty() {
                    let mut cursor = Cursor::new(&buffer[..]);

                    match parse_resp(&mut cursor) {
                        Ok(resp_value) => {
                            let consumed = cursor.position() as usize;
                            let response = match parse_command(resp_value) {
                                Ok(cmd) => execute_command(cmd, &storage, &wal, &replicator, &auth, &mut conn, &ring, local_node_id.as_deref(), &pubsub, &versions, &lua).await,
                                Err(e) => Response::Error(format!("ERR {}", e)),
                            };
                            let response_bytes = serialize_response(response);
                            stream.write_all(&response_bytes).await?;
                            stream.flush().await?;
                            buffer.advance(consumed);

                            if subs_rx.is_none() && conn.sub.rx.is_some() {
                                subs_rx = conn.sub.rx.take();
                            }
                        }
                        Err(_) => break,
                    }
                }

                if buffer.len() > 1024 * 1024 {
                    return Err("Buffer overflow".into());
                }
            }
        }
    }
}

async fn execute_command(
    cmd: Command,
    storage: &Arc<CacheStorage>,
    wal: &Option<Arc<WriteAheadLog>>,
    replicator: &Option<Arc<MasterReplicator>>,
    auth: &Option<Arc<AuthManager>>,
    conn: &mut ConnState,
    ring: &Option<Arc<ConsistentHashRing>>,
    local_node_id: Option<&str>,
    pubsub: &Arc<PubSub>,
    versions: &Arc<ShardVersions>,
    lua: &Arc<LuaEngine>,
) -> Response {
    // ---- Transaction queueing gate ----
    // Inside MULTI, every non-control command is queued and replies QUEUED.
    // Control commands (EXEC/DISCARD/WATCH/UNWATCH/MULTI) execute normally.
    if conn.tx.in_multi() && !is_tx_control(&cmd) {
        conn.tx.enqueue(cmd);
        return Response::Raw(Bytes::from_static(b"+QUEUED\r\n"));
    }

    // ---- Cluster routing gate ----
    // If clustering is enabled and this command targets a single key whose
    // hash maps to a different node, return -MOVED so smart clients (e.g.
    // `redis-cli -c`, lettuce, jedis) can follow the redirect.
    if let (Some(ring), Some(local_id)) = (ring, local_node_id) {
        if let Some(key) = command_key(&cmd) {
            if let Some(target) = ring.node_for_key(key) {
                if target.id != local_id {
                    return Response::Error(format!(
                        "MOVED {} {}",
                        slot_for_key(key),
                        target.addr
                    ));
                }
            }
        }
    }

    // ---- Auth gate ----
    // AUTH itself is the one command an unauthenticated connection is
    // allowed to issue (alongside PING, which is a connectivity probe and
    // safe to expose). Everything else gets NOAUTH until the connection
    // has a user attached.
    if let Some(am) = auth {
        if am.is_auth_required() && conn.authed_user.is_none() {
            match &cmd {
                Command::Auth { .. } | Command::Ping => {}
                _ => {
                    return Response::Error(
                        "NOAUTH Authentication required.".to_string(),
                    );
                }
            }
        }

        // Once authenticated, run the per-command ACL check. Skip for AUTH
        // (it's the gate itself) and PING (matches Redis).
        if let Some(user) = &conn.authed_user {
            if !matches!(&cmd, Command::Auth { .. } | Command::Ping) {
                let cname = command_name(&cmd);
                let key_str = command_key(&cmd).map(|b| String::from_utf8_lossy(b).to_string());
                if !am.check_permission(user, cname, key_str.as_deref()) {
                    return Response::Error(format!(
                        "NOPERM this user has no permissions to run the '{}' command",
                        cname.to_lowercase()
                    ));
                }
            }
        }
    }

    // Single chokepoint: every write command is WAL-logged, replicated, and
    // its target shard's version counter is bumped before storage is
    // touched. Adding a new write command means adding it to
    // `commands::is_write_command` — no per-arm wiring needed.
    if is_write_command(&cmd) {
        if let Some(wal) = wal {
            let _ = wal.append_command(&cmd).await;
        }
        if let Some(rep) = replicator {
            rep.replicate_write(cmd.clone()).await;
        }
        // Bump the version of the target shard so any in-flight WATCH
        // observes the conflict at EXEC time. Multi-key writes (MSET) bump
        // every affected shard.
        bump_versions_for(&cmd, versions);
    }

    match cmd {
        // String commands
        Command::Get { key } => {
            match storage.get(&key) {
                Some(value) => Response::Value(value),
                None => Response::NotFound,
            }
        }
        Command::Set { key, value, ttl_secs } => {
            let ttl = ttl_secs.map(|s| Duration::from_secs(s as u64));
            storage.set(key, value, ttl);
            Response::Ok
        }
        Command::Delete { key } => {
            if storage.delete(&key) {
                Response::Ok
            } else {
                Response::Integer(0)
            }
        }
        Command::Incr { key } => {
            match storage.incr(&key) {
                Ok(value) => Response::Integer(value),
                Err(e) => Response::Error(e),
            }
        }
        Command::Decr { key } => {
            match storage.decr(&key) {
                Ok(value) => Response::Integer(value),
                Err(e) => Response::Error(e),
            }
        }
        Command::Append { key, value } => {
            match storage.append(&key, value) {
                Ok(len) => Response::Integer(len as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::Strlen { key } => {
            match storage.strlen(&key) {
                Ok(len) => Response::Integer(len as i64),
                Err(e) => Response::Error(e),
            }
        }

        // Hash commands
        Command::HGet { key, field } => {
            match storage.hget(&key, &field) {
                Some(value) => Response::Value(value),
                None => Response::NotFound,
            }
        }
        Command::HSet { key, field, value } => {
            match storage.hset(key, field, value) {
                Ok(_) => Response::Integer(1),
                Err(e) => Response::Error(e),
            }
        }
        Command::HDel { key, fields } => {
            match storage.hdel(&key, &fields) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::HGetAll { key } => {
            match storage.hgetall(&key) {
                Ok(pairs) => {
                    let mut result = Vec::new();
                    for (k, v) in pairs {
                        result.push(k);
                        result.push(v);
                    }
                    Response::Array(result)
                }
                Err(e) => Response::Error(e),
            }
        }
        Command::HKeys { key } => {
            match storage.hkeys(&key) {
                Ok(keys) => Response::Array(keys),
                Err(e) => Response::Error(e),
            }
        }
        Command::HVals { key } => {
            match storage.hvals(&key) {
                Ok(vals) => Response::Array(vals),
                Err(e) => Response::Error(e),
            }
        }
        Command::HLen { key } => {
            match storage.hlen(&key) {
                Ok(len) => Response::Integer(len as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::HExists { key, field } => {
            match storage.hexists(&key, &field) {
                Ok(exists) => Response::Integer(if exists { 1 } else { 0 }),
                Err(e) => Response::Error(e),
            }
        }

        // List commands
        Command::LPush { key, values } => {
            match storage.lpush(key, values) {
                Ok(len) => Response::Integer(len as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::RPush { key, values } => {
            match storage.rpush(key, values) {
                Ok(len) => Response::Integer(len as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::LPop { key } => {
            match storage.lpop(&key) {
                Ok(Some(value)) => Response::Value(value),
                Ok(None) => Response::NotFound,
                Err(e) => Response::Error(e),
            }
        }
        Command::RPop { key } => {
            match storage.rpop(&key) {
                Ok(Some(value)) => Response::Value(value),
                Ok(None) => Response::NotFound,
                Err(e) => Response::Error(e),
            }
        }
        Command::LRange { key, start, stop } => {
            match storage.lrange(&key, start, stop) {
                Ok(values) => Response::Array(values),
                Err(e) => Response::Error(e),
            }
        }
        Command::LLen { key } => {
            match storage.llen(&key) {
                Ok(len) => Response::Integer(len as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::LIndex { key, index } => {
            match storage.lindex(&key, index) {
                Ok(Some(value)) => Response::Value(value),
                Ok(None) => Response::NotFound,
                Err(e) => Response::Error(e),
            }
        }

        // Set commands
        Command::SAdd { key, members } => {
            match storage.sadd(key, members) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::SRem { key, members } => {
            match storage.srem(&key, &members) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::SMembers { key } => {
            match storage.smembers(&key) {
                Ok(members) => Response::Array(members),
                Err(e) => Response::Error(e),
            }
        }
        Command::SIsMember { key, member } => {
            match storage.sismember(&key, &member) {
                Ok(is_member) => Response::Integer(if is_member { 1 } else { 0 }),
                Err(e) => Response::Error(e),
            }
        }
        Command::SCard { key } => {
            match storage.scard(&key) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::SInter { keys } => {
            match storage.sinter(&keys) {
                Ok(members) => Response::Array(members),
                Err(e) => Response::Error(e),
            }
        }
        Command::SUnion { keys } => {
            match storage.sunion(&keys) {
                Ok(members) => Response::Array(members),
                Err(e) => Response::Error(e),
            }
        }
        Command::SDiff { keys } => {
            match storage.sdiff(&keys) {
                Ok(members) => Response::Array(members),
                Err(e) => Response::Error(e),
            }
        }

        // Sorted Set commands
        Command::ZAdd { key, members } => {
            match storage.zadd(key, members) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::ZRem { key, members } => {
            match storage.zrem(&key, &members) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::ZRange { key, start, stop, with_scores } => {
            match storage.zrange(&key, start, stop, with_scores) {
                Ok(results) => {
                    let mut response = Vec::new();
                    for (member, score_opt) in results {
                        response.push(member);
                        if let Some(score) = score_opt {
                            response.push(Bytes::from(score.to_string()));
                        }
                    }
                    Response::Array(response)
                }
                Err(e) => Response::Error(e),
            }
        }
        Command::ZRank { key, member } => {
            match storage.zrank(&key, &member) {
                Ok(Some(rank)) => Response::Integer(rank as i64),
                Ok(None) => Response::NotFound,
                Err(e) => Response::Error(e),
            }
        }
        Command::ZScore { key, member } => {
            match storage.zscore(&key, &member) {
                Ok(Some(score)) => Response::Value(Bytes::from(score.to_string())),
                Ok(None) => Response::NotFound,
                Err(e) => Response::Error(e),
            }
        }
        Command::ZCard { key } => {
            match storage.zcard(&key) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }
        Command::ZCount { key, min, max } => {
            match storage.zcount(&key, min, max) {
                Ok(count) => Response::Integer(count as i64),
                Err(e) => Response::Error(e),
            }
        }

        // Server commands
        Command::Ping => Response::Value(Bytes::from("PONG")),
        Command::Echo { message } => Response::Value(message),
        Command::Exists { keys } => {
            let count = storage.exists(&keys);
            Response::Integer(count as i64)
        }
        Command::Keys { pattern } => {
            let keys = storage.keys(&pattern);
            Response::Array(keys)
        }
        Command::FlushAll => {
            storage.flushall();
            Response::Ok
        }
        Command::DbSize => {
            let size = storage.dbsize();
            Response::Integer(size as i64)
        }

        // TTL commands. Writes (Expire/PExpire/ExpireAt/Persist) have already
        // been WAL-logged and replicated up top via is_write_command, so the
        // dispatch arms here only compute the response.
        Command::Expire { key, seconds } => {
            match u64::try_from(seconds) {
                Ok(secs) => {
                    let at = std::time::Instant::now() + Duration::from_secs(secs);
                    if storage.set_expiry(&key, at) {
                        Response::Integer(1)
                    } else {
                        Response::Integer(0)
                    }
                }
                Err(_) => Response::Integer(0),
            }
        }
        Command::PExpire { key, millis } => {
            match u64::try_from(millis) {
                Ok(ms) => {
                    let at = std::time::Instant::now() + Duration::from_millis(ms);
                    if storage.set_expiry(&key, at) {
                        Response::Integer(1)
                    } else {
                        Response::Integer(0)
                    }
                }
                Err(_) => Response::Integer(0),
            }
        }
        Command::ExpireAt { key, unix_secs } => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            if unix_secs <= now_unix {
                // Past timestamp → key dies immediately.
                if storage.delete(&key) {
                    Response::Integer(1)
                } else {
                    Response::Integer(0)
                }
            } else {
                let delta = (unix_secs - now_unix) as u64;
                let at = std::time::Instant::now() + Duration::from_secs(delta);
                if storage.set_expiry(&key, at) {
                    Response::Integer(1)
                } else {
                    Response::Integer(0)
                }
            }
        }
        Command::Persist { key } => {
            if storage.persist(&key) {
                Response::Integer(1)
            } else {
                Response::Integer(0)
            }
        }
        Command::Ttl { key } => {
            // -2 if missing, -1 if no TTL, else seconds (rounded down).
            match storage.pttl_millis(&key) {
                None => Response::Integer(-2),
                Some(-1) => Response::Integer(-1),
                Some(ms) => Response::Integer(ms / 1000),
            }
        }
        Command::PTtl { key } => {
            match storage.pttl_millis(&key) {
                None => Response::Integer(-2),
                Some(n) => Response::Integer(n),
            }
        }

        // ===== P3 Bucket 2 dispatch =====
        Command::SetNx { key, value } => {
            if storage.set_nx(key, value) { Response::Integer(1) } else { Response::Integer(0) }
        }
        Command::SetEx { key, seconds, value } => {
            storage.set_ex(key, value, Duration::from_secs(seconds as u64));
            Response::Ok
        }
        Command::GetSet { key, value } => match storage.get_set(key, value) {
            Ok(Some(old)) => Response::Value(old),
            Ok(None) => Response::NotFound,
            Err(e) => Response::Error(e),
        },
        Command::MGet { keys } => {
            // Each key may live on a different shard; locks are taken
            // briefly per-key inside `storage.get`.
            let mut out = Vec::with_capacity(keys.len());
            for k in &keys {
                match storage.get(k) {
                    Some(v) => out.push(v),
                    None => out.push(Bytes::new()), // TODO: nil placeholder
                }
            }
            Response::Array(out)
        }
        Command::MSet { pairs } => {
            for (k, v) in pairs {
                storage.set(k, v, None);
            }
            Response::Ok
        }
        Command::GetRange { key, start, end } => match storage.get_range(&key, start, end) {
            Some(b) => Response::Value(b),
            None => Response::Value(Bytes::new()),
        },
        Command::IncrBy { key, delta } => match storage.incr_by(&key, delta) {
            Ok(n) => Response::Integer(n),
            Err(e) => Response::Error(e),
        },
        Command::DecrBy { key, delta } => match storage.incr_by(&key, -delta) {
            Ok(n) => Response::Integer(n),
            Err(e) => Response::Error(e),
        },
        Command::IncrByFloat { key, delta } => match storage.incr_by_float(&key, delta) {
            Ok(f) => Response::Value(Bytes::from(f.to_string())),
            Err(e) => Response::Error(e),
        },
        Command::HMGet { key, fields } => {
            let mut out = Vec::with_capacity(fields.len());
            for f in &fields {
                out.push(storage.hget(&key, f).unwrap_or_else(Bytes::new));
            }
            Response::Array(out)
        }
        Command::HMSet { key, pairs } => {
            for (f, v) in pairs {
                if let Err(e) = storage.hset(key.clone(), f, v) {
                    return Response::Error(e);
                }
            }
            Response::Ok
        }
        Command::HSetNx { key, field, value } => match storage.hset_nx(key, field, value) {
            Ok(true) => Response::Integer(1),
            Ok(false) => Response::Integer(0),
            Err(e) => Response::Error(e),
        },
        Command::HIncrBy { key, field, delta } => match storage.hincr_by(key, field, delta) {
            Ok(n) => Response::Integer(n),
            Err(e) => Response::Error(e),
        },
        Command::LSet { key, index, value } => match storage.lset(&key, index, value) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        Command::LRem { key, count, value } => match storage.lrem(&key, count, &value) {
            Ok(n) => Response::Integer(n as i64),
            Err(e) => Response::Error(e),
        },
        Command::LTrim { key, start, stop } => match storage.ltrim(&key, start, stop) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        Command::RPopLPush { source, destination } => match storage.rpoplpush(&source, destination) {
            Ok(Some(v)) => Response::Value(v),
            Ok(None) => Response::NotFound,
            Err(e) => Response::Error(e),
        },
        Command::SPop { key, count } => match storage.spop(&key, count.unwrap_or(1)) {
            Ok(members) => {
                if count.is_none() {
                    members.into_iter().next().map_or(Response::NotFound, Response::Value)
                } else {
                    Response::Array(members)
                }
            }
            Err(e) => Response::Error(e),
        },
        Command::SRandMember { key, count } => match count {
            Some(n) => match storage.srand_member(&key, n) {
                Ok(members) => Response::Array(members),
                Err(e) => Response::Error(e),
            },
            None => match storage.srand_member(&key, 1) {
                Ok(mut members) => members.pop().map_or(Response::NotFound, Response::Value),
                Err(e) => Response::Error(e),
            },
        },
        Command::SMove { source, destination, member } => match storage.smove(&source, destination, &member) {
            Ok(true) => Response::Integer(1),
            Ok(false) => Response::Integer(0),
            Err(e) => Response::Error(e),
        },
        Command::ZIncrBy { key, delta, member } => match storage.zincr_by(key, delta, member) {
            Ok(score) => Response::Value(Bytes::from(score.to_string())),
            Err(e) => Response::Error(e),
        },
        Command::ZRangeByScore { key, min, max, with_scores } => match storage.zrange_by_score(&key, min, max, with_scores) {
            Ok(results) => {
                let mut out = Vec::new();
                for (m, s) in results {
                    out.push(m);
                    if let Some(sc) = s { out.push(Bytes::from(sc.to_string())); }
                }
                Response::Array(out)
            }
            Err(e) => Response::Error(e),
        },
        Command::ZRevRange { key, start, stop, with_scores } => match storage.zrevrange(&key, start, stop, with_scores) {
            Ok(results) => {
                let mut out = Vec::new();
                for (m, s) in results {
                    out.push(m);
                    if let Some(sc) = s { out.push(Bytes::from(sc.to_string())); }
                }
                Response::Array(out)
            }
            Err(e) => Response::Error(e),
        },
        Command::ZPopMin { key, count } => match storage.zpop_min(&key, count) {
            Ok(results) => {
                let mut out = Vec::new();
                for (m, s) in results {
                    out.push(m);
                    out.push(Bytes::from(s.to_string()));
                }
                Response::Array(out)
            }
            Err(e) => Response::Error(e),
        },
        Command::ZPopMax { key, count } => match storage.zpop_max(&key, count) {
            Ok(results) => {
                let mut out = Vec::new();
                for (m, s) in results {
                    out.push(m);
                    out.push(Bytes::from(s.to_string()));
                }
                Response::Array(out)
            }
            Err(e) => Response::Error(e),
        },
        Command::Type { key } => Response::Value(Bytes::from_static(storage.type_of(&key).as_bytes())),
        Command::Rename { key, new_key } => match storage.rename(&key, new_key) {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error(e),
        },
        Command::RandomKey => match storage.random_key() {
            Some(k) => Response::Value(k),
            None => Response::NotFound,
        },

        // ===== P4.4 Lua scripting dispatch =====
        Command::LuaEval { script, keys, argv } => {
            let acl = build_script_acl(auth, conn);
            let (resp, script_writes) = lua.run(&script, keys, argv, storage, acl);
            persist_script_writes(&script_writes, wal, replicator, versions).await;
            resp
        }
        Command::LuaEvalSha { sha1, keys, argv } => {
            let acl = build_script_acl(auth, conn);
            let (resp, script_writes) = lua.run_sha(&sha1, keys, argv, storage, acl);
            persist_script_writes(&script_writes, wal, replicator, versions).await;
            resp
        }

        // ===== P4.3 Streams dispatch =====
        Command::XAdd { key, id, fields } => {
            use crate::data_structures::StreamId;
            let parsed = match id.as_deref() {
                Some(s) => match StreamId::parse(s, false) {
                    Ok(id) => Some(id),
                    Err(e) => return Response::Error(e),
                },
                None => None,
            };
            match storage.xadd(key, parsed, fields) {
                Ok(id) => Response::Value(Bytes::from(id.to_string())),
                Err(e) => Response::Error(e),
            }
        }
        Command::XLen { key } => match storage.xlen(&key) {
            Ok(n) => Response::Integer(n as i64),
            Err(e) => Response::Error(e),
        },
        Command::XRange { key, start, end, count } => {
            use crate::data_structures::StreamId;
            let s = match StreamId::parse(&start, false) {
                Ok(id) => id, Err(e) => return Response::Error(e),
            };
            let e = match StreamId::parse(&end, true) {
                Ok(id) => id, Err(err) => return Response::Error(err),
            };
            match storage.xrange(&key, s, e, count) {
                Ok(entries) => Response::Raw(format_xentries(&entries)),
                Err(err) => Response::Error(err),
            }
        }
        Command::XRead { count, keys, ids } => {
            if keys.len() != ids.len() {
                return Response::Error("ERR XREAD streams/ids length mismatch".to_string());
            }
            use crate::data_structures::StreamId;
            // Per-stream results: only include streams that yielded ≥1 entry.
            let mut per_stream: Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)> = Vec::new();
            for (k, id_str) in keys.iter().zip(ids.iter()) {
                let cursor = match StreamId::parse(id_str, false) {
                    Ok(c) => c, Err(e) => return Response::Error(e),
                };
                match storage.xread(k, cursor, count) {
                    Ok(entries) if !entries.is_empty() => {
                        per_stream.push((k.clone(), entries));
                    }
                    Ok(_) => {}
                    Err(e) => return Response::Error(e),
                }
            }
            if per_stream.is_empty() {
                // Redis returns nil array when no streams have data.
                return Response::Raw(Bytes::from_static(b"*-1\r\n"));
            }
            Response::Raw(format_xread(&per_stream))
        }

        // ===== P4.1 Pub/Sub dispatch =====
        Command::Publish { channel, payload } => {
            let n = pubsub.publish(channel, payload);
            Response::Integer(n as i64)
        }
        Command::Subscribe { channels } => {
            let mut out = BytesMut::new();
            for ch in channels {
                let tx = conn.sub.ensure_sender();
                pubsub.subscribe(ch.clone(), tx);
                if !conn.sub.channels.contains(&ch) {
                    conn.sub.channels.push(ch.clone());
                }
                // RESP frame: *3\r\n$9\r\nsubscribe\r\n$<n>\r\n<channel>\r\n:<count>\r\n
                out.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n$");
                out.extend_from_slice(ch.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n:");
                out.extend_from_slice(conn.sub.channels.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Response::Raw(out.freeze())
        }
        Command::Unsubscribe { channels } => {
            let to_remove: Vec<Bytes> = if channels.is_empty() {
                conn.sub.channels.clone()
            } else {
                channels
            };
            let mut out = BytesMut::new();
            for ch in to_remove {
                conn.sub.channels.retain(|c| c != &ch);
                pubsub.prune_closed(&ch);
                out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                out.extend_from_slice(ch.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n:");
                out.extend_from_slice(conn.sub.channels.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Response::Raw(out.freeze())
        }

        // ===== P4.2 Transaction dispatch =====
        Command::Multi => {
            if conn.tx.in_multi() {
                Response::Error("ERR MULTI calls can not be nested".to_string())
            } else {
                conn.tx.start();
                Response::Ok
            }
        }
        Command::Discard => {
            if !conn.tx.in_multi() {
                Response::Error("ERR DISCARD without MULTI".to_string())
            } else {
                conn.tx.discard();
                Response::Ok
            }
        }
        Command::Watch { keys } => {
            if conn.tx.in_multi() {
                return Response::Error("ERR WATCH inside MULTI is not allowed".to_string());
            }
            for k in keys {
                let idx = shard_index_for(&k, SHARD_COUNT);
                let v = versions.read(idx);
                conn.tx.watch(idx, v);
            }
            Response::Ok
        }
        Command::Unwatch => {
            conn.tx.unwatch();
            Response::Ok
        }
        Command::Exec => {
            let queue = match conn.tx.take_queue() {
                Some(q) => q,
                None => return Response::Error("ERR EXEC without MULTI".to_string()),
            };
            // Optimistic-concurrency check.
            if conn.tx.any_watched_changed(versions) {
                conn.tx.unwatch();
                // Redis sends `*-1\r\n` — null array — to signal abort.
                return Response::Raw(Bytes::from_static(b"*-1\r\n"));
            }
            conn.tx.unwatch();
            // Run each queued command and concatenate their serialized
            // replies into a single RESP array of length `queue.len()`.
            let mut out = BytesMut::new();
            out.extend_from_slice(b"*");
            out.extend_from_slice(queue.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for c in queue {
                // Recurse — but bypass the MULTI gate since tx.queue is now
                // None (we took it out). This means writes inside EXEC still
                // hit WAL/replication/version-bump.
                let r = Box::pin(execute_command(
                    c, storage, wal, replicator, auth, conn, ring, local_node_id,
                    pubsub, versions, lua,
                )).await;
                out.extend_from_slice(&serialize_response(r));
            }
            Response::Raw(out.freeze())
        }

        Command::Auth { username, password } => {
            // Either AUTH <pass> (default user) or AUTH <user> <pass>.
            let user = match &username {
                Some(u) => String::from_utf8_lossy(u).to_string(),
                None => "default".to_string(),
            };
            let pass = String::from_utf8_lossy(&password).to_string();
            match auth {
                Some(am) => {
                    if am.authenticate(&user, &pass).is_some() {
                        conn.authed_user = Some(user);
                        Response::Ok
                    } else {
                        Response::Error(
                            "WRONGPASS invalid username-password pair or user is disabled.".to_string(),
                        )
                    }
                }
                // No auth configured — Redis returns this exact message.
                None => Response::Error(
                    "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?".to_string(),
                ),
            }
        }
    }
}

/// Short Redis-style command name (uppercase) for ACL/permission checks.
fn command_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Get { .. } => "GET",
        Command::Set { .. } => "SET",
        Command::Delete { .. } => "DEL",
        Command::Incr { .. } => "INCR",
        Command::Decr { .. } => "DECR",
        Command::Append { .. } => "APPEND",
        Command::Strlen { .. } => "STRLEN",
        Command::HGet { .. } => "HGET",
        Command::HSet { .. } => "HSET",
        Command::HDel { .. } => "HDEL",
        Command::HGetAll { .. } => "HGETALL",
        Command::HKeys { .. } => "HKEYS",
        Command::HVals { .. } => "HVALS",
        Command::HLen { .. } => "HLEN",
        Command::HExists { .. } => "HEXISTS",
        Command::LPush { .. } => "LPUSH",
        Command::RPush { .. } => "RPUSH",
        Command::LPop { .. } => "LPOP",
        Command::RPop { .. } => "RPOP",
        Command::LRange { .. } => "LRANGE",
        Command::LLen { .. } => "LLEN",
        Command::LIndex { .. } => "LINDEX",
        Command::SAdd { .. } => "SADD",
        Command::SRem { .. } => "SREM",
        Command::SMembers { .. } => "SMEMBERS",
        Command::SIsMember { .. } => "SISMEMBER",
        Command::SCard { .. } => "SCARD",
        Command::SInter { .. } => "SINTER",
        Command::SUnion { .. } => "SUNION",
        Command::SDiff { .. } => "SDIFF",
        Command::ZAdd { .. } => "ZADD",
        Command::ZRem { .. } => "ZREM",
        Command::ZRange { .. } => "ZRANGE",
        Command::ZRank { .. } => "ZRANK",
        Command::ZScore { .. } => "ZSCORE",
        Command::ZCard { .. } => "ZCARD",
        Command::ZCount { .. } => "ZCOUNT",
        Command::Ping => "PING",
        Command::Echo { .. } => "ECHO",
        Command::Exists { .. } => "EXISTS",
        Command::Keys { .. } => "KEYS",
        Command::FlushAll => "FLUSHALL",
        Command::DbSize => "DBSIZE",
        Command::Expire { .. } => "EXPIRE",
        Command::PExpire { .. } => "PEXPIRE",
        Command::ExpireAt { .. } => "EXPIREAT",
        Command::Ttl { .. } => "TTL",
        Command::PTtl { .. } => "PTTL",
        Command::Persist { .. } => "PERSIST",
        Command::Auth { .. } => "AUTH",
        // P3
        Command::SetNx { .. } => "SETNX",
        Command::SetEx { .. } => "SETEX",
        Command::GetSet { .. } => "GETSET",
        Command::MGet { .. } => "MGET",
        Command::MSet { .. } => "MSET",
        Command::GetRange { .. } => "GETRANGE",
        Command::IncrBy { .. } => "INCRBY",
        Command::DecrBy { .. } => "DECRBY",
        Command::IncrByFloat { .. } => "INCRBYFLOAT",
        Command::HMGet { .. } => "HMGET",
        Command::HMSet { .. } => "HMSET",
        Command::HSetNx { .. } => "HSETNX",
        Command::HIncrBy { .. } => "HINCRBY",
        Command::LSet { .. } => "LSET",
        Command::LRem { .. } => "LREM",
        Command::LTrim { .. } => "LTRIM",
        Command::RPopLPush { .. } => "RPOPLPUSH",
        Command::SPop { .. } => "SPOP",
        Command::SRandMember { .. } => "SRANDMEMBER",
        Command::SMove { .. } => "SMOVE",
        Command::ZIncrBy { .. } => "ZINCRBY",
        Command::ZRangeByScore { .. } => "ZRANGEBYSCORE",
        Command::ZRevRange { .. } => "ZREVRANGE",
        Command::ZPopMin { .. } => "ZPOPMIN",
        Command::ZPopMax { .. } => "ZPOPMAX",
        Command::Type { .. } => "TYPE",
        Command::Rename { .. } => "RENAME",
        Command::RandomKey => "RANDOMKEY",
        // P4.1
        Command::Publish { .. } => "PUBLISH",
        Command::Subscribe { .. } => "SUBSCRIBE",
        Command::Unsubscribe { .. } => "UNSUBSCRIBE",
        // P4.2
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Watch { .. } => "WATCH",
        Command::Unwatch => "UNWATCH",
        // P4.3
        Command::XAdd { .. } => "XADD",
        Command::XLen { .. } => "XLEN",
        Command::XRange { .. } => "XRANGE",
        Command::XRead { .. } => "XREAD",
        // P4.4
        Command::LuaEval { .. } => "EVAL",
        Command::LuaEvalSha { .. } => "EVALSHA",
    }
}

/// Bump shard version counters affected by a write. For multi-shard writes
/// (MSET, RENAME crossing shards) this touches more than one. WATCH compares
/// these counters at EXEC time.
/// Build the per-command ACL checker handed to the Lua bridge, so writes
/// performed by `redis.call` are subject to the same permissions as the
/// authenticated user's direct commands. Returns allow-all when auth is off or
/// the connection is unauthenticated (in which case the NOAUTH gate already ran).
fn build_script_acl(auth: &Option<Arc<AuthManager>>, conn: &ConnState) -> AclCheck {
    match (auth, &conn.authed_user) {
        (Some(am), Some(user)) if am.is_auth_required() => {
            let am = Arc::clone(am);
            let user = user.clone();
            Arc::new(move |cmd: &str, key: Option<&[u8]>| {
                let key_str = key.map(|k| String::from_utf8_lossy(k).to_string());
                am.check_permission(&user, cmd, key_str.as_deref())
            })
        }
        _ => allow_all_acl(),
    }
}

/// Route writes performed inside a Lua script through the same WAL +
/// replication + version-bump chokepoint as ordinary writes. The script has
/// already applied them to storage, so we only log/replicate here.
async fn persist_script_writes(
    script_writes: &[Command],
    wal: &Option<Arc<WriteAheadLog>>,
    replicator: &Option<Arc<MasterReplicator>>,
    versions: &Arc<ShardVersions>,
) {
    for w in script_writes {
        if let Some(wal) = wal {
            let _ = wal.append_command(w).await;
        }
        if let Some(rep) = replicator {
            rep.replicate_write(w.clone()).await;
        }
        bump_versions_for(w, versions);
    }
}

fn bump_versions_for(cmd: &Command, versions: &Arc<ShardVersions>) {
    match cmd {
        Command::MSet { pairs } => {
            for (k, _) in pairs {
                versions.bump(shard_index_for(k, SHARD_COUNT));
            }
        }
        Command::RPopLPush { source, destination } => {
            versions.bump(shard_index_for(source, SHARD_COUNT));
            versions.bump(shard_index_for(destination, SHARD_COUNT));
        }
        Command::SMove { source, destination, .. } => {
            versions.bump(shard_index_for(source, SHARD_COUNT));
            versions.bump(shard_index_for(destination, SHARD_COUNT));
        }
        Command::Rename { key, new_key } => {
            versions.bump(shard_index_for(key, SHARD_COUNT));
            versions.bump(shard_index_for(new_key, SHARD_COUNT));
        }
        Command::FlushAll => {
            // Flush invalidates every WATCH.
            for i in 0..SHARD_COUNT {
                versions.bump(i);
            }
        }
        _ => {
            if let Some(k) = command_key(cmd) {
                versions.bump(shard_index_for(k, SHARD_COUNT));
            }
        }
    }
}

/// Serialize XRANGE-style stream entries: array of [id, [field, value, ...]]
/// pairs. Each entry is a 2-element RESP array.
fn format_xentries(entries: &[(crate::data_structures::StreamId, Vec<(Bytes, Bytes)>)]) -> Bytes {
    let mut out = BytesMut::new();
    out.extend_from_slice(b"*");
    out.extend_from_slice(entries.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for (id, fields) in entries {
        out.extend_from_slice(b"*2\r\n");
        let id_str = id.to_string();
        out.extend_from_slice(b"$");
        out.extend_from_slice(id_str.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(id_str.as_bytes());
        out.extend_from_slice(b"\r\n");
        // Inner field/value array — flat: [f1, v1, f2, v2, ...]
        out.extend_from_slice(b"*");
        out.extend_from_slice((fields.len() * 2).to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        for (f, v) in fields {
            for s in &[f, v] {
                out.extend_from_slice(b"$");
                out.extend_from_slice(s.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
        }
    }
    out.freeze()
}

/// Serialize XREAD reply: array of [stream_name, [entries]] pairs.
fn format_xread(per_stream: &[(Bytes, Vec<(crate::data_structures::StreamId, Vec<(Bytes, Bytes)>)>)]) -> Bytes {
    let mut out = BytesMut::new();
    out.extend_from_slice(b"*");
    out.extend_from_slice(per_stream.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for (name, entries) in per_stream {
        out.extend_from_slice(b"*2\r\n");
        out.extend_from_slice(b"$");
        out.extend_from_slice(name.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(name);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&format_xentries(entries));
    }
    out.freeze()
}

/// Serialize a Pub/Sub delivery as the RESP array
/// `*3\r\n$7\r\nmessage\r\n$<n>\r\n<channel>\r\n$<m>\r\n<payload>\r\n`.
fn format_pubsub_message(msg: &PubSubMessage) -> Bytes {
    let mut out = BytesMut::new();
    out.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n$");
    out.extend_from_slice(msg.channel.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&msg.channel);
    out.extend_from_slice(b"\r\n$");
    out.extend_from_slice(msg.payload.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&msg.payload);
    out.extend_from_slice(b"\r\n");
    out.freeze()
}

/// Primary key for ACL key-pattern matching. None for commands that don't
/// target a single key (FLUSHALL, KEYS, multi-key ops like SINTER).
fn command_key(cmd: &Command) -> Option<&[u8]> {
    match cmd {
        Command::Get { key }
        | Command::Set { key, .. }
        | Command::Delete { key }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::Append { key, .. }
        | Command::Strlen { key }
        | Command::HGet { key, .. }
        | Command::HSet { key, .. }
        | Command::HDel { key, .. }
        | Command::HGetAll { key }
        | Command::HKeys { key }
        | Command::HVals { key }
        | Command::HLen { key }
        | Command::HExists { key, .. }
        | Command::LPush { key, .. }
        | Command::RPush { key, .. }
        | Command::LPop { key }
        | Command::RPop { key }
        | Command::LRange { key, .. }
        | Command::LLen { key }
        | Command::LIndex { key, .. }
        | Command::SAdd { key, .. }
        | Command::SRem { key, .. }
        | Command::SMembers { key }
        | Command::SIsMember { key, .. }
        | Command::SCard { key }
        | Command::ZAdd { key, .. }
        | Command::ZRem { key, .. }
        | Command::ZRange { key, .. }
        | Command::ZRank { key, .. }
        | Command::ZScore { key, .. }
        | Command::ZCard { key }
        | Command::ZCount { key, .. }
        | Command::Expire { key, .. }
        | Command::PExpire { key, .. }
        | Command::ExpireAt { key, .. }
        | Command::Ttl { key }
        | Command::PTtl { key }
        | Command::Persist { key } => Some(key),

        // P3 single-key commands
        Command::SetNx { key, .. }
        | Command::SetEx { key, .. }
        | Command::GetSet { key, .. }
        | Command::GetRange { key, .. }
        | Command::IncrBy { key, .. }
        | Command::DecrBy { key, .. }
        | Command::IncrByFloat { key, .. }
        | Command::HMGet { key, .. }
        | Command::HMSet { key, .. }
        | Command::HSetNx { key, .. }
        | Command::HIncrBy { key, .. }
        | Command::LSet { key, .. }
        | Command::LRem { key, .. }
        | Command::LTrim { key, .. }
        | Command::SPop { key, .. }
        | Command::SRandMember { key, .. }
        | Command::ZIncrBy { key, .. }
        | Command::ZRangeByScore { key, .. }
        | Command::ZRevRange { key, .. }
        | Command::ZPopMin { key, .. }
        | Command::ZPopMax { key, .. }
        | Command::Type { key } => Some(key),

        // P3 commands using `source` as their primary key for ACL purposes.
        Command::RPopLPush { source: key, .. }
        | Command::SMove { source: key, .. }
        | Command::Rename { key, .. } => Some(key),

        // P3 multi-key/server-wide
        Command::MGet { .. }
        | Command::MSet { .. }
        | Command::RandomKey => None,

        // P4: Pub/Sub channels and transaction control don't take a key for
        // ACL-pattern purposes. PUBLISH could be gated on channel name; we
        // skip that for now.
        Command::Publish { .. }
        | Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Watch { .. }
        | Command::Unwatch => None,

        // P4.3 streams: single-key access.
        Command::XAdd { key, .. }
        | Command::XLen { key }
        | Command::XRange { key, .. } => Some(key),
        Command::XRead { .. } => None, // XREAD takes multiple streams

        // P4.4: scripts can target many keys; ACL can't be enforced at the
        // top level. Bridge calls inside the script run un-gated.
        Command::LuaEval { .. }
        | Command::LuaEvalSha { .. } => None,

        // No single key (multi-key, server-wide, or auth/echo/ping).
        Command::Exists { .. }
        | Command::Keys { .. }
        | Command::FlushAll
        | Command::DbSize
        | Command::Ping
        | Command::Echo { .. }
        | Command::SInter { .. }
        | Command::SUnion { .. }
        | Command::SDiff { .. }
        | Command::Auth { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Auth gate: NOAUTH → AUTH → OK transitions on the connection.
    #[tokio::test]
    async fn auth_gate_blocks_then_admits() {
        let storage = Arc::new(CacheStorage::new());
        let wal: Option<Arc<WriteAheadLog>> = None;
        let rep: Option<Arc<MasterReplicator>> = None;
        let auth = Some(Arc::new(AuthManager::with_users(
            true,
            vec![crate::security::User::new("admin".to_string(), "admin123", true)],
        )));
        let mut conn = ConnState::new();
        let pubsub = Arc::new(PubSub::new());
        let versions = Arc::new(ShardVersions::new(SHARD_COUNT));
        let lua = Arc::new(LuaEngine::new());

        // Unauthenticated SET → NOAUTH.
        let r = execute_command(
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                ttl_secs: None,
            },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Error(msg) => assert!(msg.starts_with("NOAUTH"), "got {}", msg),
            other => panic!("expected NOAUTH, got {:?}", other),
        }

        // PING is exempt.
        let r = execute_command(
            Command::Ping, &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Value(_)));

        // Wrong password → WRONGPASS, conn stays unauthenticated.
        let r = execute_command(
            Command::Auth {
                username: Some(Bytes::from_static(b"admin")),
                password: Bytes::from_static(b"wrong"),
            },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Error(msg) => assert!(msg.starts_with("WRONGPASS"), "got {}", msg),
            other => panic!("expected WRONGPASS, got {:?}", other),
        }
        assert!(conn.authed_user.is_none());

        // Right password → OK; subsequent SET goes through.
        let r = execute_command(
            Command::Auth {
                username: Some(Bytes::from_static(b"admin")),
                password: Bytes::from_static(b"admin123"),
            },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Ok));
        assert_eq!(conn.authed_user.as_deref(), Some("admin"));

        let r = execute_command(
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                ttl_secs: None,
            },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Ok));
        assert_eq!(storage.get(b"k").as_deref(), Some(&b"v"[..]));
    }

    /// Cluster routing: a key hashing to a remote node returns -MOVED with
    /// the target's address; a key hashing to the local node serves locally.
    #[tokio::test]
    async fn moved_redirect_for_remote_keys() {
        use crate::cluster::Node;
        use std::net::SocketAddr;

        let storage = Arc::new(CacheStorage::new());
        let wal: Option<Arc<WriteAheadLog>> = None;
        let rep: Option<Arc<MasterReplicator>> = None;
        let auth: Option<Arc<AuthManager>> = None;
        let mut conn = ConnState::new();
        let pubsub = Arc::new(PubSub::new());
        let versions = Arc::new(ShardVersions::new(SHARD_COUNT));
        let lua = Arc::new(LuaEngine::new());

        let ring = Arc::new(ConsistentHashRing::new());
        let local = Node::new(
            "local".to_string(),
            "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:17001".parse::<SocketAddr>().unwrap(),
        );
        let remote = Node::new(
            "remote".to_string(),
            "127.0.0.1:7002".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:17002".parse::<SocketAddr>().unwrap(),
        );
        ring.add_node(local.clone());
        ring.add_node(remote.clone());
        let ring_opt = Some(ring.clone());

        // Find one key that hashes to local and one that hashes to remote.
        // With 150 vnodes per node, both classes exist with high probability;
        // probe a small range until we have one of each.
        let mut local_key = None;
        let mut remote_key = None;
        for i in 0..200u32 {
            let k = format!("probe-{}", i);
            match ring.node_for_key(k.as_bytes()).map(|n| n.id) {
                Some(id) if id == "local" && local_key.is_none() => {
                    local_key = Some(k);
                }
                Some(id) if id == "remote" && remote_key.is_none() => {
                    remote_key = Some(k);
                }
                _ => {}
            }
            if local_key.is_some() && remote_key.is_some() { break; }
        }
        let local_key = local_key.expect("found local-owned key");
        let remote_key = remote_key.expect("found remote-owned key");

        // Local key → served locally as Ok.
        let r = execute_command(
            Command::Set {
                key: Bytes::from(local_key.clone()),
                value: Bytes::from_static(b"v"),
                ttl_secs: None,
            },
            &storage, &wal, &rep, &auth, &mut conn, &ring_opt, Some("local"), &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Ok), "local SET should succeed: {:?}", r);

        // Remote key → MOVED.
        let r = execute_command(
            Command::Get { key: Bytes::from(remote_key) },
            &storage, &wal, &rep, &auth, &mut conn, &ring_opt, Some("local"), &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Error(msg) => {
                assert!(msg.starts_with("MOVED "), "expected MOVED, got {}", msg);
                assert!(msg.contains("127.0.0.1:7002"), "should point at remote: {}", msg);
            }
            other => panic!("expected MOVED error, got {:?}", other),
        }

        // Server-wide command (PING) is exempt — no key, no redirect.
        let r = execute_command(
            Command::Ping,
            &storage, &wal, &rep, &auth, &mut conn, &ring_opt, Some("local"), &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Value(_)));
    }

    /// PUBLISH delivers to a registered subscriber's mailbox.
    #[tokio::test]
    async fn pubsub_publish_to_subscribed_connection() {
        let storage = Arc::new(CacheStorage::new());
        let wal: Option<Arc<WriteAheadLog>> = None;
        let rep: Option<Arc<MasterReplicator>> = None;
        let auth: Option<Arc<AuthManager>> = None;
        let mut conn = ConnState::new();
        let pubsub = Arc::new(PubSub::new());
        let versions = Arc::new(ShardVersions::new(SHARD_COUNT));
        let lua = Arc::new(LuaEngine::new());

        let r = execute_command(
            Command::Subscribe { channels: vec![Bytes::from_static(b"news")] },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Raw(b) => {
                let s = String::from_utf8_lossy(&b);
                assert!(s.contains("subscribe"), "should be subscribe frame: {}", s);
            }
            other => panic!("expected Raw, got {:?}", other),
        }
        assert_eq!(conn.sub.channels.len(), 1);
        let mut rx = conn.sub.rx.take().expect("rx created on first subscribe");

        let mut conn2 = ConnState::new();
        let r = execute_command(
            Command::Publish {
                channel: Bytes::from_static(b"news"),
                payload: Bytes::from_static(b"hello"),
            },
            &storage, &wal, &rep, &auth, &mut conn2, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Integer(1)), "1 subscriber should receive: {:?}", r);

        let msg = rx.recv().await.expect("subscriber gets message");
        assert_eq!(msg.channel.as_ref(), b"news");
        assert_eq!(msg.payload.as_ref(), b"hello");
    }

    /// MULTI → SET → SET → EXEC commits both writes atomically.
    #[tokio::test]
    async fn transaction_multi_then_exec_commits_writes() {
        let storage = Arc::new(CacheStorage::new());
        let wal: Option<Arc<WriteAheadLog>> = None;
        let rep: Option<Arc<MasterReplicator>> = None;
        let auth: Option<Arc<AuthManager>> = None;
        let mut conn = ConnState::new();
        let pubsub = Arc::new(PubSub::new());
        let versions = Arc::new(ShardVersions::new(SHARD_COUNT));
        let lua = Arc::new(LuaEngine::new());

        // MULTI begins the queue.
        let r = execute_command(
            Command::Multi,
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Ok));
        assert!(conn.tx.in_multi());

        // Queued writes return +QUEUED, not +OK.
        let r = execute_command(
            Command::Set { key: Bytes::from_static(b"a"), value: Bytes::from_static(b"1"), ttl_secs: None },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Raw(b) => assert_eq!(b.as_ref(), b"+QUEUED\r\n"),
            other => panic!("expected QUEUED, got {:?}", other),
        }

        let r = execute_command(
            Command::Set { key: Bytes::from_static(b"b"), value: Bytes::from_static(b"2"), ttl_secs: None },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Raw(_)));

        // Storage hasn't seen them yet.
        assert_eq!(storage.get(b"a"), None);

        // EXEC runs them.
        let r = execute_command(
            Command::Exec,
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Raw(b) => {
                let s = String::from_utf8_lossy(&b);
                assert!(s.starts_with("*2\r\n"), "expected 2-element array: {}", s);
            }
            other => panic!("expected Raw array, got {:?}", other),
        }
        assert_eq!(storage.get(b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(storage.get(b"b").as_deref(), Some(&b"2"[..]));
        assert!(!conn.tx.in_multi());
    }

    /// WATCH + concurrent write → EXEC aborts (returns nil array).
    #[tokio::test]
    async fn transaction_watch_aborts_on_conflict() {
        let storage = Arc::new(CacheStorage::new());
        let wal: Option<Arc<WriteAheadLog>> = None;
        let rep: Option<Arc<MasterReplicator>> = None;
        let auth: Option<Arc<AuthManager>> = None;
        let mut conn_a = ConnState::new();
        let mut conn_b = ConnState::new();
        let pubsub = Arc::new(PubSub::new());
        let versions = Arc::new(ShardVersions::new(SHARD_COUNT));
        let lua = Arc::new(LuaEngine::new());

        // A: WATCH x; MULTI; SET x = "A"
        execute_command(
            Command::Watch { keys: vec![Bytes::from_static(b"x")] },
            &storage, &wal, &rep, &auth, &mut conn_a, &None, None, &pubsub, &versions, &lua,
        ).await;
        execute_command(
            Command::Multi,
            &storage, &wal, &rep, &auth, &mut conn_a, &None, None, &pubsub, &versions, &lua,
        ).await;
        execute_command(
            Command::Set { key: Bytes::from_static(b"x"), value: Bytes::from_static(b"A"), ttl_secs: None },
            &storage, &wal, &rep, &auth, &mut conn_a, &None, None, &pubsub, &versions, &lua,
        ).await;

        // B: SET x = "B"  (lands and bumps shard version)
        execute_command(
            Command::Set { key: Bytes::from_static(b"x"), value: Bytes::from_static(b"B"), ttl_secs: None },
            &storage, &wal, &rep, &auth, &mut conn_b, &None, None, &pubsub, &versions, &lua,
        ).await;

        // A: EXEC — should abort with nil array.
        let r = execute_command(
            Command::Exec,
            &storage, &wal, &rep, &auth, &mut conn_a, &None, None, &pubsub, &versions, &lua,
        ).await;
        match r {
            Response::Raw(b) => assert_eq!(b.as_ref(), b"*-1\r\n", "expected nil-array abort"),
            other => panic!("expected Raw nil-array, got {:?}", other),
        }
        // B's value survived; A's was aborted.
        assert_eq!(storage.get(b"x").as_deref(), Some(&b"B"[..]));
    }

    /// Without an AuthManager attached, no gating happens — preserves
    /// backward compatibility with existing in-memory test setups.
    #[tokio::test]
    async fn no_auth_manager_means_no_gate() {
        let storage = Arc::new(CacheStorage::new());
        let wal: Option<Arc<WriteAheadLog>> = None;
        let rep: Option<Arc<MasterReplicator>> = None;
        let auth: Option<Arc<AuthManager>> = None;
        let mut conn = ConnState::new();
        let pubsub = Arc::new(PubSub::new());
        let versions = Arc::new(ShardVersions::new(SHARD_COUNT));
        let lua = Arc::new(LuaEngine::new());

        let r = execute_command(
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                ttl_secs: None,
            },
            &storage, &wal, &rep, &auth, &mut conn, &None, None, &pubsub, &versions, &lua,
        ).await;
        assert!(matches!(r, Response::Ok));
    }
}


mod storage;
mod protocol;
mod server;
mod commands;
mod client;
mod persistence;
mod config;
mod cluster;
mod data_structures;
mod security;
mod tls;
mod replication;
mod pubsub;
mod transactions;
mod scripting;

use server::CacheServer;
use config::AppConfig;
use clap::Parser;
use tls::create_tls_acceptor;

#[derive(Parser, Debug)]
#[command(name = "cache")]
#[command(about = "High-performance cache server", long_about = None)]
struct Args {
    /// Cache server port
    #[arg(short, long, default_value = "7777")]
    port: u16,

    /// Cluster communication port
    #[arg(short = 'c', long)]
    cluster_port: Option<u16>,

    /// Node ID for clustering
    #[arg(short = 'n', long)]
    node_id: Option<String>,

    /// Seed node address to join cluster (host:port)
    #[arg(short = 'j', long)]
    join: Option<String>,

    /// Configuration file path
    #[arg(long, default_value = "config.json")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load configuration from file or use defaults
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| args.config);
    let config = AppConfig::load(&config_path);

    // Build server address
    let addr = format!("{}:{}", config.server.host, args.port);

    // Determine if clustering is enabled
    let cluster_config = if let Some(cluster_port) = args.cluster_port {
        Some((
            cluster_port,
            args.node_id.unwrap_or_else(|| format!("node-{}", args.port)),
            args.join,
        ))
    } else {
        None
    };

    println!("Starting Cache Server v0.1.0");
    println!("Configuration: {}", config_path);
    println!("Address: {}", addr);
    println!("Persistence: {}", if config.persistence.enabled { "Enabled" } else { "Disabled" });

    if let Some((cluster_port, ref node_id, ref join)) = cluster_config {
        println!("Cluster Mode: Enabled");
        println!("Node ID: {}", node_id);
        println!("Cluster Port: {}", cluster_port);
        if let Some(ref seed) = join {
            println!("Joining cluster via: {}", seed);
        }
    }

    println!("================================================");

    // Create server with clustering and persistence support
    let mut server = if let Some((cluster_port, node_id, join)) = cluster_config {
        // Create clustered server
        let cluster_addr = format!("{}:{}", config.server.host, cluster_port);

        if let Some(persistence_config) = config.to_persistence_config() {
            println!("Persistence Mode: {}", config.persistence.mode);
            println!("Data Directory: {}", config.persistence.data_dir);

            let mut server = CacheServer::with_clustering_and_persistence(
                addr,
                cluster_addr,
                node_id,
                join,
                persistence_config
            ).await?;

            // Load existing data if available
            if let Err(e) = server.load_from_persistence().await {
                eprintln!("Warning: Could not load persistence data: {}", e);
            }

            server
        } else {
            println!("Running clustered in-memory mode");
            CacheServer::with_clustering(addr, cluster_addr, node_id, join).await?
        }
    } else {
        // Non-clustered mode
        if let Some(persistence_config) = config.to_persistence_config() {
            println!("Persistence Mode: {}", config.persistence.mode);
            println!("Data Directory: {}", config.persistence.data_dir);

            let mut server = CacheServer::with_persistence(addr, persistence_config).await?;

            // Load existing data if available
            if let Err(e) = server.load_from_persistence().await {
                eprintln!("Warning: Could not load persistence data: {}", e);
            }

            server
        } else {
            println!("Running in-memory only (no persistence)");
            CacheServer::new(addr)
        }
    };

    // Configure replication based on role
    match config.replication.role.as_str() {
        "master" => {
            if let Some(repl_port) = config.replication.replication_port {
                println!("Replication: Master mode on port {}", repl_port);
                if let Err(e) = server.as_master(repl_port).await {
                    eprintln!("Failed to configure as master: {}", e);
                }
            } else {
                eprintln!("Warning: Replication role is 'master' but replicationPort not specified");
            }
        },
        "replica" => {
            if let Some(master_addr) = &config.replication.master_addr {
                println!("Replication: Replica mode, master at {}", master_addr);
                if let Err(e) = server.as_replica(master_addr.clone()).await {
                    eprintln!("Failed to configure as replica: {}", e);
                }
            } else {
                eprintln!("Warning: Replication role is 'replica' but masterAddr not specified");
            }
        },
        _ => {
            println!("Replication: Standalone mode");
        }
    }

    // Configure TLS if enabled
    if config.tls.enabled {
        println!("TLS: Enabled");
        match create_tls_acceptor(&config.tls) {
            Ok(tls_acceptor) => {
                let tls_port = config.server.tls_port.unwrap_or(args.port + 1000);
                let tls_addr = format!("{}:{}", config.server.host, tls_port);
                println!("TLS Port: {}", tls_port);
                println!("TLS Address: {}", tls_addr);
                server.with_tls(tls_acceptor, tls_addr);
            },
            Err(e) => {
                eprintln!("Failed to configure TLS: {}", e);
                eprintln!("TLS will be disabled");
            }
        }
    } else {
        println!("TLS: Disabled");
    }

    // Configure authentication if enabled in config
    match config.to_auth_manager() {
        Some(auth) => {
            let n = auth.user_count();
            if n == 0 {
                eprintln!(
                    "WARNING: security.enabled is true but no users are configured — \
                     every AUTH will fail and the server is effectively locked. \
                     Add users under \"security\": {{ \"users\": [...] }}."
                );
            }
            println!("Authentication: Enabled ({} user(s))", n);
            server.with_auth(auth);
        }
        None => {
            println!("Authentication: Disabled");
        }
    }

    println!("================================================");
    println!("Server started successfully!");

    server.run().await
}

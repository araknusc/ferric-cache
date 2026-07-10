use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use crate::persistence::{PersistenceMode, WALSyncPolicy};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    #[serde(default = "default_server")]
    pub server: ServerConfig,

    #[serde(default)]
    pub persistence: PersistenceConfigJson,

    #[serde(default)]
    pub performance: PerformanceConfig,

    #[serde(default)]
    pub tls: TlsConfig,

    #[serde(default)]
    pub replication: ReplicationConfig,

    #[serde(default)]
    pub security: SecurityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default)]
    pub tls_port: Option<u16>,

    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceConfigJson {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default = "default_mode")]
    pub mode: String,

    #[serde(default = "default_wal_sync_policy")]
    pub wal_sync_policy: String,

    #[serde(default = "default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,

    #[serde(default = "default_max_wal_size_mb")]
    pub max_wal_size_mb: u64,

    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    #[serde(default = "default_wal_backup_count")]
    pub wal_backup_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceConfig {
    #[serde(default = "default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: Option<u64>,

    #[serde(default = "default_worker_threads")]
    pub worker_threads: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub cert_file: Option<String>,

    #[serde(default)]
    pub key_file: Option<String>,

    #[serde(default)]
    pub ca_file: Option<String>,

    #[serde(default)]
    pub require_client_cert: bool,

    #[serde(default)]
    pub cluster_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationConfig {
    /// Role: "master", "replica", or "standalone"
    #[serde(default = "default_replication_role")]
    pub role: String,

    /// Replication port for master (accepts replica connections)
    #[serde(default)]
    pub replication_port: Option<u16>,

    /// Master address for replica (format: "host:port")
    #[serde(default)]
    pub master_addr: Option<String>,
}

/// Authentication + ACL configuration. Auth is enforced only when
/// `enabled` is true AND at least one user is provisioned. There is no
/// hardcoded default account: an enabled-but-empty user list locks everyone
/// out (and the server warns at startup).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SecurityConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub users: Vec<UserConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserConfig {
    pub username: String,
    /// Plaintext password from config; hashed with Argon2 at load time and
    /// never stored in plaintext beyond the config file.
    pub password: String,
    #[serde(default)]
    pub is_admin: bool,
    #[serde(default)]
    pub rules: Vec<AclRuleConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AclRuleConfig {
    /// One of: "allowAll", "readOnly", "writeOnly", "deny", or "custom".
    /// With "custom", `commands` lists `+CMD` / `-CMD` entries.
    #[serde(default = "default_acl_permission")]
    pub permission: String,

    /// Explicit `+GET` / `-SET` entries, used when `permission == "custom"`.
    #[serde(default)]
    pub commands: Vec<String>,

    #[serde(default = "default_key_patterns")]
    pub key_patterns: Vec<String>,
}

impl AclRuleConfig {
    fn to_acl_rule(&self) -> crate::security::ACLRule {
        use crate::security::{ACLRule, ACLPermission};

        let permissions = match self.permission.to_ascii_lowercase().as_str() {
            "allowall" | "all" => vec![ACLPermission::AllCommands],
            "readonly" | "read" => vec![ACLPermission::ReadCommands],
            "writeonly" | "write" => vec![ACLPermission::WriteCommands],
            "deny" | "none" => vec![ACLPermission::Deny],
            // "custom" (or anything else): derive from the explicit command list.
            _ => self
                .commands
                .iter()
                .map(|c| ACLPermission::SpecificCommand(c.clone()))
                .collect(),
        };

        ACLRule {
            permissions,
            key_patterns: self.key_patterns.clone(),
        }
    }
}

// Default value functions
fn default_server() -> ServerConfig {
    ServerConfig {
        host: default_host(),
        port: default_port(),
        tls_port: None,
        max_connections: default_max_connections(),
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    7777
}

fn default_max_connections() -> usize {
    10000
}

fn default_enabled() -> bool {
    false
}

fn default_mode() -> String {
    "both".to_string()
}

fn default_wal_sync_policy() -> String {
    "everysecond".to_string()
}

fn default_snapshot_interval_secs() -> u64 {
    300 // 5 minutes
}

fn default_max_wal_size_mb() -> u64 {
    100
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_cleanup_interval_secs() -> u64 {
    60
}

fn default_max_memory_mb() -> Option<u64> {
    None
}

fn default_worker_threads() -> Option<usize> {
    None
}

fn default_wal_backup_count() -> u32 {
    // Retain one rotated segment by default so a size-triggered rotation never
    // discards the only durable copy of unsnapshotted writes. Increase for a
    // longer WAL-only retention window; use `both` mode for full durability.
    1
}

fn default_replication_role() -> String {
    "standalone".to_string()
}

fn default_acl_permission() -> String {
    "custom".to_string()
}

fn default_key_patterns() -> Vec<String> {
    vec!["*".to_string()]
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: default_server(),
            persistence: PersistenceConfigJson::default(),
            performance: PerformanceConfig::default(),
            tls: TlsConfig::default(),
            replication: ReplicationConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        default_server()
    }
}

impl Default for PersistenceConfigJson {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            mode: default_mode(),
            wal_sync_policy: default_wal_sync_policy(),
            snapshot_interval_secs: default_snapshot_interval_secs(),
            max_wal_size_mb: default_max_wal_size_mb(),
            data_dir: default_data_dir(),
            wal_backup_count: default_wal_backup_count(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            cleanup_interval_secs: default_cleanup_interval_secs(),
            max_memory_mb: default_max_memory_mb(),
            worker_threads: default_worker_threads(),
        }
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            require_client_cert: false,
            cluster_tls: false,
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: default_replication_role(),
            replication_port: None,
            master_addr: None,
        }
    }
}

impl AppConfig {
    /// Load configuration from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = fs::read_to_string(path)?;
        let config: AppConfig = serde_json::from_str(&config_str)?;
        Ok(config)
    }

    /// Load configuration from file or use defaults if file doesn't exist
    pub fn load<P: AsRef<Path>>(path: P) -> Self {
        match Self::from_file(path) {
            Ok(config) => {
                println!("Configuration loaded from file");
                config
            },
            Err(e) => {
                println!("Using default configuration ({})", e);
                Self::default()
            }
        }
    }

    /// Save current configuration to a file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(self)?;
        fs::write(path, json)?;
        Ok(())
    }

    /// Get the server address as a string
    pub fn server_address(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    /// Build an `AuthManager` from the security config, or `None` if auth is
    /// disabled. Passwords are Argon2-hashed here. Returns an empty manager
    /// (which denies all AUTH) if enabled with no users — callers should warn.
    pub fn to_auth_manager(&self) -> Option<std::sync::Arc<crate::security::AuthManager>> {
        use crate::security::{AuthManager, User};

        if !self.security.enabled {
            return None;
        }

        let users = self
            .security
            .users
            .iter()
            .map(|uc| {
                let mut user = User::new(uc.username.clone(), &uc.password, uc.is_admin);
                for rule in &uc.rules {
                    user.add_acl_rule(rule.to_acl_rule());
                }
                user
            })
            .collect();

        Some(std::sync::Arc::new(AuthManager::with_users(true, users)))
    }

    /// Convert to persistence config for the persistence module
    pub fn to_persistence_config(&self) -> Option<crate::persistence::PersistenceConfig> {
        if !self.persistence.enabled {
            return None;
        }

        // Create data directory if it doesn't exist
        if !Path::new(&self.persistence.data_dir).exists() {
            let _ = fs::create_dir_all(&self.persistence.data_dir);
        }

        let mode = match self.persistence.mode.to_lowercase().as_str() {
            "wal" => PersistenceMode::WAL,
            "snapshot" => PersistenceMode::Snapshot,
            "both" => PersistenceMode::Both,
            _ => PersistenceMode::None,
        };

        let wal_sync_policy = match self.persistence.wal_sync_policy.to_lowercase().as_str() {
            "always" => WALSyncPolicy::Always,
            "everysecond" | "every_second" => WALSyncPolicy::EverySecond,
            "manual" => WALSyncPolicy::Manual,
            _ => WALSyncPolicy::EverySecond,
        };

        Some(crate::persistence::PersistenceConfig {
            mode,
            wal_sync_policy,
            snapshot_interval_secs: self.persistence.snapshot_interval_secs,
            max_wal_size_bytes: self.persistence.max_wal_size_mb * 1024 * 1024,
            wal_path: format!("{}/cache.wal", self.persistence.data_dir),
            snapshot_path: format!("{}/cache.snapshot", self.persistence.data_dir),
            wal_backup_count: self.persistence.wal_backup_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 7777);
        assert!(!config.persistence.enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = AppConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: AppConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.server.port, config.server.port);
    }
}
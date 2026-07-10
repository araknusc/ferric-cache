use serde::{Serialize, Deserialize};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ACLPermission {
    AllCommands,           // +@all
    ReadCommands,          // +@read
    WriteCommands,         // +@write
    SpecificCommand(String), // +GET, -SET
    Deny,                  // -@all
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ACLRule {
    pub permissions: Vec<ACLPermission>,
    pub key_patterns: Vec<String>, // Patterns like "user:*", "cache:*"
}

impl ACLRule {
    pub fn new() -> Self {
        Self {
            permissions: Vec::new(),
            key_patterns: vec!["*".to_string()], // Default: all keys
        }
    }

    pub fn allow_all() -> Self {
        Self {
            permissions: vec![ACLPermission::AllCommands],
            key_patterns: vec!["*".to_string()],
        }
    }

    pub fn read_only() -> Self {
        Self {
            permissions: vec![ACLPermission::ReadCommands],
            key_patterns: vec!["*".to_string()],
        }
    }

    pub fn write_only() -> Self {
        Self {
            permissions: vec![ACLPermission::WriteCommands],
            key_patterns: vec!["*".to_string()],
        }
    }

    pub fn with_key_pattern(mut self, pattern: String) -> Self {
        self.key_patterns = vec![pattern];
        self
    }

    pub fn with_key_patterns(mut self, patterns: Vec<String>) -> Self {
        self.key_patterns = patterns;
        self
    }

    pub fn allows_command(&self, command: &str, key: Option<&str>) -> bool {
        // Check if command is allowed
        let command_allowed = self.check_command_permission(command);

        if !command_allowed {
            return false;
        }

        // Check key pattern if key is provided
        if let Some(key) = key {
            self.check_key_pattern(key)
        } else {
            true // Commands without keys are allowed if command permission passes
        }
    }

    fn check_command_permission(&self, command: &str) -> bool {
        let command_upper = command.to_uppercase();

        for permission in &self.permissions {
            match permission {
                ACLPermission::AllCommands => return true,
                ACLPermission::Deny => return false,
                ACLPermission::ReadCommands => {
                    if Self::is_read_command(&command_upper) {
                        return true;
                    }
                },
                ACLPermission::WriteCommands => {
                    if Self::is_write_command(&command_upper) {
                        return true;
                    }
                },
                ACLPermission::SpecificCommand(cmd) => {
                    if cmd.starts_with('+') && cmd[1..].to_uppercase() == command_upper {
                        return true;
                    } else if cmd.starts_with('-') && cmd[1..].to_uppercase() == command_upper {
                        return false;
                    }
                },
            }
        }

        false
    }

    fn check_key_pattern(&self, key: &str) -> bool {
        for pattern in &self.key_patterns {
            if Self::pattern_matches(pattern, key) {
                return true;
            }
        }
        false
    }

    fn pattern_matches(pattern: &str, key: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        // Simple glob pattern matching
        if pattern.ends_with('*') {
            let prefix = &pattern[..pattern.len() - 1];
            return key.starts_with(prefix);
        }

        pattern == key
    }

    fn is_read_command(command: &str) -> bool {
        const READ_COMMANDS: &[&str] = &[
            "GET", "MGET", "EXISTS", "TTL", "PTTL", "TYPE",
            "HGET", "HMGET", "HGETALL", "HLEN", "HEXISTS",
            "LLEN", "LINDEX", "LRANGE",
            "SCARD", "SISMEMBER", "SMEMBERS",
            "ZCARD", "ZCOUNT", "ZRANGE", "ZRANK", "ZSCORE",
        ];
        READ_COMMANDS.contains(&command)
    }

    fn is_write_command(command: &str) -> bool {
        const WRITE_COMMANDS: &[&str] = &[
            "SET", "SETEX", "SETNX", "DEL", "EXPIRE", "PERSIST",
            "HSET", "HDEL", "HINCRBY",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LREM",
            "SADD", "SREM", "SPOP",
            "ZADD", "ZREM", "ZINCRBY",
        ];
        WRITE_COMMANDS.contains(&command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_permissions() {
        let rule = ACLRule::allow_all();
        assert!(rule.allows_command("GET", Some("key1")));
        assert!(rule.allows_command("SET", Some("key1")));

        let read_rule = ACLRule::read_only();
        assert!(read_rule.allows_command("GET", Some("key1")));
        assert!(!read_rule.allows_command("SET", Some("key1")));

        let write_rule = ACLRule::write_only();
        assert!(!write_rule.allows_command("GET", Some("key1")));
        assert!(write_rule.allows_command("SET", Some("key1")));
    }

    #[test]
    fn test_key_patterns() {
        let rule = ACLRule::read_only()
            .with_key_pattern("user:*".to_string());

        assert!(rule.allows_command("GET", Some("user:123")));
        assert!(!rule.allows_command("GET", Some("admin:456")));

        // Test wildcard
        let wildcard_rule = ACLRule::read_only()
            .with_key_pattern("*".to_string());
        assert!(wildcard_rule.allows_command("GET", Some("anything")));
    }
}
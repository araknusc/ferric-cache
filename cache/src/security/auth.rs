use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

use super::acl::ACLRule;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub acl_rules: Vec<ACLRule>,
    pub is_admin: bool,
}

impl User {
    pub fn new(username: String, password: &str, is_admin: bool) -> Self {
        Self {
            username,
            password_hash: Self::hash_password(password),
            acl_rules: Vec::new(),
            is_admin,
        }
    }

    pub fn verify_password(&self, password: &str) -> bool {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        use argon2::Argon2;
        match PasswordHash::new(&self.password_hash) {
            Ok(parsed) => Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
            Err(_) => false,
        }
    }

    /// Hash a password with Argon2id and a fresh random 16-byte salt. The
    /// returned PHC string (`$argon2id$v=19$...`) embeds the salt and
    /// parameters, so it is self-describing and each call produces a distinct
    /// hash even for identical passwords.
    fn hash_password(password: &str) -> String {
        use argon2::password_hash::{PasswordHasher, SaltString};
        use argon2::Argon2;
        use rand::RngCore;

        let mut salt_bytes = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut salt_bytes);
        let salt = SaltString::encode_b64(&salt_bytes)
            .expect("16 bytes is a valid salt length");
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .expect("Argon2 hashing of a valid password does not fail")
            .to_string()
    }

    pub fn add_acl_rule(&mut self, rule: ACLRule) {
        self.acl_rules.push(rule);
    }

    pub fn can_execute_command(&self, command: &str, key: Option<&str>) -> bool {
        if self.is_admin {
            return true;
        }

        for rule in &self.acl_rules {
            if rule.allows_command(command, key) {
                return true;
            }
        }

        false
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    pub session_id: String,
    pub username: String,
    pub created_at: std::time::Instant,
    pub last_activity: std::time::Instant,
}

impl Session {
    pub fn new(username: String) -> Self {
        let session_id = format!("sess_{:x}", rand::random::<u64>());
        let now = std::time::Instant::now();

        Self {
            session_id,
            username,
            created_at: now,
            last_activity: now,
        }
    }

    pub fn is_expired(&self, timeout: std::time::Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    pub fn update_activity(&mut self) {
        self.last_activity = std::time::Instant::now();
    }
}

pub struct AuthManager {
    users: Arc<RwLock<HashMap<String, User>>>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    require_auth: bool,
}

impl AuthManager {
    /// Create an empty AuthManager. No users are seeded — provision them from
    /// config via [`AuthManager::with_users`] or [`AuthManager::add_user`].
    /// (There is deliberately no hardcoded default account.)
    pub fn new(require_auth: bool) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            require_auth,
        }
    }

    /// Create an AuthManager seeded with the given users (from configuration).
    pub fn with_users(require_auth: bool, users: Vec<User>) -> Self {
        let map = users
            .into_iter()
            .map(|u| (u.username.clone(), u))
            .collect();
        Self {
            users: Arc::new(RwLock::new(map)),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            require_auth,
        }
    }

    /// Number of provisioned users. Used by startup to warn on an empty,
    /// auth-enabled configuration (which would lock everyone out).
    pub fn user_count(&self) -> usize {
        self.users.read().len()
    }

    pub fn add_user(&self, user: User) {
        self.users.write().insert(user.username.clone(), user);
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Option<String> {
        let users = self.users.read();

        if let Some(user) = users.get(username) {
            if user.verify_password(password) {
                let session = Session::new(username.to_string());
                let session_id = session.session_id.clone();
                self.sessions.write().insert(session_id.clone(), session);
                return Some(session_id);
            }
        }

        None
    }

    pub fn verify_session(&self, session_id: &str) -> Option<String> {
        let mut sessions = self.sessions.write();

        if let Some(session) = sessions.get_mut(session_id) {
            if !session.is_expired(std::time::Duration::from_secs(3600)) {
                session.update_activity();
                return Some(session.username.clone());
            } else {
                sessions.remove(session_id);
            }
        }

        None
    }

    pub fn logout(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    pub fn check_permission(&self, username: &str, command: &str, key: Option<&str>) -> bool {
        if !self.require_auth {
            return true;
        }

        let users = self.users.read();

        if let Some(user) = users.get(username) {
            user.can_execute_command(command, key)
        } else {
            false
        }
    }

    pub fn is_auth_required(&self) -> bool {
        self.require_auth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_authentication() {
        let user = User::new("test".to_string(), "password123", false);
        assert!(user.verify_password("password123"));
        assert!(!user.verify_password("wrongpass"));
    }

    #[test]
    fn test_auth_manager() {
        // Users are provisioned explicitly — there is no hardcoded default.
        let auth_mgr = AuthManager::with_users(
            true,
            vec![User::new("admin".to_string(), "admin123", true)],
        );

        // Test admin authentication
        let session = auth_mgr.authenticate("admin", "admin123");
        assert!(session.is_some());

        // Test wrong password
        let session = auth_mgr.authenticate("admin", "wrongpass");
        assert!(session.is_none());

        // Test session verification
        let session_id = auth_mgr.authenticate("admin", "admin123").unwrap();
        let username = auth_mgr.verify_session(&session_id);
        assert_eq!(username, Some("admin".to_string()));

        // Test logout
        auth_mgr.logout(&session_id);
        assert!(auth_mgr.verify_session(&session_id).is_none());
    }
}
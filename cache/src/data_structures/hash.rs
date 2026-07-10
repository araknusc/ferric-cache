use std::collections::HashMap;
use std::time::Instant;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct HashValue {
    pub fields: HashMap<Bytes, Bytes>,
    pub expires_at: Option<Instant>,
}

impl HashValue {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            expires_at: None,
        }
    }

    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            fields: HashMap::new(),
            expires_at: Some(Instant::now() + ttl),
        }
    }

    pub fn set_field(&mut self, field: Bytes, value: Bytes) -> Option<Bytes> {
        self.fields.insert(field, value)
    }

    pub fn get_field(&self, field: &[u8]) -> Option<Bytes> {
        self.fields.get(field).cloned()
    }

    pub fn delete_field(&mut self, field: &[u8]) -> bool {
        self.fields.remove(field).is_some()
    }

    pub fn get_all(&self) -> Vec<(Bytes, Bytes)> {
        self.fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |expires| Instant::now() > expires)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_operations() {
        let mut hash = HashValue::new();

        // Test set and get
        hash.set_field(Bytes::from("field1"), Bytes::from("value1"));
        assert_eq!(hash.get_field(b"field1"), Some(Bytes::from("value1")));
        assert_eq!(hash.get_field(b"field2"), None);

        // Test delete
        assert!(hash.delete_field(b"field1"));
        assert!(!hash.delete_field(b"field2"));
        assert_eq!(hash.get_field(b"field1"), None);

        // Test len
        hash.set_field(Bytes::from("a"), Bytes::from("1"));
        hash.set_field(Bytes::from("b"), Bytes::from("2"));
        assert_eq!(hash.len(), 2);
    }
}
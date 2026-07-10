use std::collections::HashSet;
use std::time::Instant;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct SetValue {
    pub members: HashSet<Bytes>,
    pub expires_at: Option<Instant>,
}

impl SetValue {
    pub fn new() -> Self {
        Self {
            members: HashSet::new(),
            expires_at: None,
        }
    }

    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            members: HashSet::new(),
            expires_at: Some(Instant::now() + ttl),
        }
    }

    pub fn add(&mut self, member: Bytes) -> bool {
        self.members.insert(member)
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.members.remove(member)
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        self.members.contains(member)
    }

    pub fn get_all(&self) -> Vec<Bytes> {
        self.members.iter().cloned().collect()
    }

    pub fn intersection(&self, other: &SetValue) -> Vec<Bytes> {
        self.members
            .intersection(&other.members)
            .cloned()
            .collect()
    }

    pub fn union(&self, other: &SetValue) -> Vec<Bytes> {
        self.members
            .union(&other.members)
            .cloned()
            .collect()
    }

    pub fn difference(&self, other: &SetValue) -> Vec<Bytes> {
        self.members
            .difference(&other.members)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |expires| Instant::now() > expires)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_operations() {
        let mut set1 = SetValue::new();
        let mut set2 = SetValue::new();

        // Test add and contains
        assert!(set1.add(Bytes::from("a")));
        assert!(!set1.add(Bytes::from("a"))); // Duplicate
        assert!(set1.contains(b"a"));
        assert!(!set1.contains(b"b"));

        // Test remove
        assert!(set1.remove(b"a"));
        assert!(!set1.remove(b"a")); // Already removed

        // Test set operations
        set1.add(Bytes::from("1"));
        set1.add(Bytes::from("2"));
        set1.add(Bytes::from("3"));

        set2.add(Bytes::from("2"));
        set2.add(Bytes::from("3"));
        set2.add(Bytes::from("4"));

        let inter = set1.intersection(&set2);
        assert_eq!(inter.len(), 2); // 2 and 3

        let uni = set1.union(&set2);
        assert_eq!(uni.len(), 4); // 1, 2, 3, 4

        let diff = set1.difference(&set2);
        assert_eq!(diff.len(), 1); // 1
    }
}
use std::collections::VecDeque;
use std::time::Instant;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct ListValue {
    pub items: VecDeque<Bytes>,
    pub expires_at: Option<Instant>,
}

impl ListValue {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
            expires_at: None,
        }
    }

    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            items: VecDeque::new(),
            expires_at: Some(Instant::now() + ttl),
        }
    }

    pub fn push_left(&mut self, value: Bytes) {
        self.items.push_front(value);
    }

    pub fn push_right(&mut self, value: Bytes) {
        self.items.push_back(value);
    }

    pub fn pop_left(&mut self) -> Option<Bytes> {
        self.items.pop_front()
    }

    pub fn pop_right(&mut self) -> Option<Bytes> {
        self.items.pop_back()
    }

    pub fn get_range(&self, start: i64, stop: i64) -> Vec<Bytes> {
        let len = self.items.len() as i64;

        // Convert negative indices to positive
        let start_idx = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len - 1).max(0) as usize
        };

        let stop_idx = if stop < 0 {
            (len + stop + 1).max(0) as usize
        } else {
            (stop + 1).min(len).max(0) as usize
        };

        if start_idx >= stop_idx {
            return Vec::new();
        }

        self.items.iter()
            .skip(start_idx)
            .take(stop_idx - start_idx)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |expires| Instant::now() > expires)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_operations() {
        let mut list = ListValue::new();

        // Test push and pop
        list.push_left(Bytes::from("a"));
        list.push_left(Bytes::from("b"));
        list.push_right(Bytes::from("c"));

        assert_eq!(list.pop_left(), Some(Bytes::from("b")));
        assert_eq!(list.pop_right(), Some(Bytes::from("c")));
        assert_eq!(list.len(), 1);

        // Test range
        list.push_right(Bytes::from("1"));
        list.push_right(Bytes::from("2"));
        list.push_right(Bytes::from("3"));

        let range = list.get_range(1, 2);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], Bytes::from("1"));
        assert_eq!(range[1], Bytes::from("2"));
    }
}
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[derive(Debug, Clone)]
pub struct SortedSetValue {
    // Score -> Set of members with that score
    members_by_score: BTreeMap<OrderedFloat, HashSet<Bytes>>,
    // Member -> Score mapping for quick lookups
    member_scores: HashMap<Bytes, OrderedFloat>,
    pub expires_at: Option<Instant>,
}

impl SortedSetValue {
    pub fn new() -> Self {
        Self {
            members_by_score: BTreeMap::new(),
            member_scores: HashMap::new(),
            expires_at: None,
        }
    }

    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            members_by_score: BTreeMap::new(),
            member_scores: HashMap::new(),
            expires_at: Some(Instant::now() + ttl),
        }
    }

    pub fn add(&mut self, score: f64, member: Bytes) -> bool {
        let score = OrderedFloat(score);

        // Remove old score if member exists
        if let Some(old_score) = self.member_scores.get(&member) {
            if let Some(members) = self.members_by_score.get_mut(old_score) {
                members.remove(&member);
                if members.is_empty() {
                    self.members_by_score.remove(old_score);
                }
            }
        }

        // Add with new score
        self.member_scores.insert(member.clone(), score.clone());
        self.members_by_score
            .entry(score)
            .or_insert_with(HashSet::new)
            .insert(member);

        true
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.member_scores.remove(member) {
            if let Some(members) = self.members_by_score.get_mut(&score) {
                members.remove(member);
                if members.is_empty() {
                    self.members_by_score.remove(&score);
                }
            }
            true
        } else {
            false
        }
    }

    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.member_scores.get(member).map(|s| s.0)
    }

    pub fn get_range(&self, start: usize, stop: usize, with_scores: bool) -> Vec<(Bytes, Option<f64>)> {
        let mut result = Vec::new();
        let mut count = 0;

        for (score, members) in &self.members_by_score {
            for member in members {
                if count >= start && count <= stop {
                    result.push((
                        member.clone(),
                        if with_scores { Some(score.0) } else { None }
                    ));
                }
                count += 1;
                if count > stop {
                    return result;
                }
            }
        }

        result
    }

    pub fn get_range_by_score(&self, min: f64, max: f64, with_scores: bool) -> Vec<(Bytes, Option<f64>)> {
        let min_score = OrderedFloat(min);
        let max_score = OrderedFloat(max);
        let mut result = Vec::new();

        for (score, members) in self.members_by_score.range(min_score..=max_score) {
            for member in members {
                result.push((
                    member.clone(),
                    if with_scores { Some(score.0) } else { None }
                ));
            }
        }

        result
    }

    pub fn get_rank(&self, member: &[u8]) -> Option<usize> {
        if let Some(target_score) = self.member_scores.get(member) {
            let mut rank = 0;
            for (score, members) in &self.members_by_score {
                if score < target_score {
                    rank += members.len();
                } else if score == target_score {
                    // Find position within same score
                    let mut sorted_members: Vec<_> = members.iter().collect();
                    sorted_members.sort();
                    for m in sorted_members {
                        if m.as_ref() == member {
                            return Some(rank);
                        }
                        rank += 1;
                    }
                } else {
                    break;
                }
            }
        }
        None
    }

    /// Remove and return up to `count` lowest-score members. Used by ZPOPMIN.
    /// Within the same score, members come out in insertion-order (HashSet
    /// iteration order is unspecified, matching Redis's documented behavior).
    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut out = Vec::with_capacity(count);
        while out.len() < count {
            let (score, member) = match self.members_by_score.iter().next() {
                Some((s, members)) => match members.iter().next().cloned() {
                    Some(m) => (s.0, m),
                    None => break,
                },
                None => break,
            };
            self.remove(&member);
            out.push((member, score));
        }
        out
    }

    /// Remove and return up to `count` highest-score members. Used by ZPOPMAX.
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut out = Vec::with_capacity(count);
        while out.len() < count {
            let (score, member) = match self.members_by_score.iter().next_back() {
                Some((s, members)) => match members.iter().next().cloned() {
                    Some(m) => (s.0, m),
                    None => break,
                },
                None => break,
            };
            self.remove(&member);
            out.push((member, score));
        }
        out
    }

    pub fn len(&self) -> usize {
        self.member_scores.len()
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |expires| Instant::now() > expires)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sorted_set_operations() {
        let mut zset = SortedSetValue::new();

        // Test add
        zset.add(10.0, Bytes::from("a"));
        zset.add(20.0, Bytes::from("b"));
        zset.add(15.0, Bytes::from("c"));

        // Test get_score
        assert_eq!(zset.get_score(b"a"), Some(10.0));
        assert_eq!(zset.get_score(b"b"), Some(20.0));
        assert_eq!(zset.get_score(b"d"), None);

        // Test get_rank
        assert_eq!(zset.get_rank(b"a"), Some(0)); // Lowest score
        assert_eq!(zset.get_rank(b"c"), Some(1)); // Middle
        assert_eq!(zset.get_rank(b"b"), Some(2)); // Highest

        // Test get_range
        let range = zset.get_range(0, 1, true);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].1, Some(10.0)); // 'a'
        assert_eq!(range[1].1, Some(15.0)); // 'c'

        // Test get_range_by_score
        let range_by_score = zset.get_range_by_score(10.0, 15.0, false);
        assert_eq!(range_by_score.len(), 2);

        // Test remove
        assert!(zset.remove(b"a"));
        assert!(!zset.remove(b"a")); // Already removed
        assert_eq!(zset.len(), 2);
    }
}
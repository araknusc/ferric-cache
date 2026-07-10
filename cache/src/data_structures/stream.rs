//! Redis-style Streams: append-only log keyed by sortable IDs.
//!
//! First-pass scope: XADD / XLEN / XRANGE / XREAD on simple ids.
//! - Auto-id `*` generates `<millis>-<seq>` where seq increments on
//!   collision within the same millisecond.
//! - No blocking reads (XREAD ... BLOCK ms is parsed but ignored — returns
//!   immediately with whatever's available).
//! - No consumer groups (XREADGROUP / XACK / XGROUP / XPENDING / XCLAIM).
//! - No XTRIM / XDEL / XREVRANGE / XINFO.

use std::collections::BTreeMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use bytes::Bytes;

/// Stream entry id. `(millis, seq)`. Lexicographic on the tuple gives the
/// correct total order over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    /// Parse `<ms>-<seq>` or `<ms>` (seq defaults to 0 on input, MAX for
    /// upper bounds — caller passes `is_upper` accordingly).
    pub fn parse(s: &str, is_upper: bool) -> Result<Self, String> {
        match s {
            "-" => return Ok(StreamId::MIN),
            "+" => return Ok(StreamId::MAX),
            "$" => return Ok(StreamId::MAX), // XREAD: only entries newer than this
            _ => {}
        }
        if let Some((ms_s, seq_s)) = s.split_once('-') {
            let ms = ms_s.parse::<u64>().map_err(|_| format!("invalid stream id: {}", s))?;
            let seq = seq_s.parse::<u64>().map_err(|_| format!("invalid stream id: {}", s))?;
            Ok(StreamId { ms, seq })
        } else {
            let ms = s.parse::<u64>().map_err(|_| format!("invalid stream id: {}", s))?;
            Ok(StreamId { ms, seq: if is_upper { u64::MAX } else { 0 } })
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamValue {
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub expires_at: Option<Instant>,
}

impl StreamValue {
    pub fn new() -> Self {
        Self { entries: BTreeMap::new(), last_id: StreamId::MIN, expires_at: None }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |e| Instant::now() > e)
    }

    pub fn len(&self) -> usize { self.entries.len() }

    /// Append `fields` under either an explicit id or an auto-generated one
    /// (`requested == None`). Returns the final id used, or an error if the
    /// caller-supplied id is not strictly greater than `last_id`.
    pub fn add(&mut self, requested: Option<StreamId>, fields: Vec<(Bytes, Bytes)>)
        -> Result<StreamId, String>
    {
        let id = match requested {
            Some(id) => {
                if id <= self.last_id {
                    return Err(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                    );
                }
                id
            }
            None => {
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                if now_ms > self.last_id.ms {
                    StreamId { ms: now_ms, seq: 0 }
                } else {
                    StreamId { ms: self.last_id.ms, seq: self.last_id.seq + 1 }
                }
            }
        };
        self.entries.insert(id, fields);
        self.last_id = id;
        Ok(id)
    }

    /// Inclusive range [start, end]. Optional limit caps the number of
    /// returned entries. Used by XRANGE.
    pub fn range(&self, start: StreamId, end: StreamId, limit: Option<usize>)
        -> Vec<(StreamId, Vec<(Bytes, Bytes)>)>
    {
        let mut out = Vec::new();
        for (id, fields) in self.entries.range(start..=end) {
            out.push((*id, fields.clone()));
            if let Some(n) = limit {
                if out.len() >= n { break; }
            }
        }
        out
    }

    /// All entries strictly newer than `cursor`. Used by XREAD.
    pub fn read_after(&self, cursor: StreamId, limit: Option<usize>)
        -> Vec<(StreamId, Vec<(Bytes, Bytes)>)>
    {
        let mut out = Vec::new();
        // BTreeMap range is half-open at the upper bound but inclusive at
        // lower; we want strictly-greater so use a manual filter.
        for (id, fields) in self.entries.range(cursor..) {
            if *id <= cursor { continue; }
            out.push((*id, fields.clone()));
            if let Some(n) = limit {
                if out.len() >= n { break; }
            }
        }
        out
    }
}

impl Default for StreamValue {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

    #[test]
    fn auto_id_monotonic() {
        let mut s = StreamValue::new();
        let id1 = s.add(None, vec![(b("k"), b("v"))]).unwrap();
        let id2 = s.add(None, vec![(b("k"), b("v"))]).unwrap();
        assert!(id2 > id1, "{:?} > {:?}", id2, id1);
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn explicit_id_must_increase() {
        let mut s = StreamValue::new();
        s.add(Some(StreamId { ms: 5, seq: 0 }), vec![]).unwrap();
        let err = s.add(Some(StreamId { ms: 5, seq: 0 }), vec![]).unwrap_err();
        assert!(err.contains("equal or smaller"));
    }

    #[test]
    fn range_inclusive_with_limit() {
        let mut s = StreamValue::new();
        for i in 1..=5 {
            s.add(Some(StreamId { ms: i, seq: 0 }), vec![(b("i"), b(&i.to_string()))]).unwrap();
        }
        let r = s.range(StreamId { ms: 2, seq: 0 }, StreamId { ms: 4, seq: 0 }, None);
        assert_eq!(r.len(), 3);
        let r = s.range(StreamId::MIN, StreamId::MAX, Some(2));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn read_after_is_strict() {
        let mut s = StreamValue::new();
        let id = s.add(Some(StreamId { ms: 1, seq: 0 }), vec![]).unwrap();
        s.add(Some(StreamId { ms: 2, seq: 0 }), vec![]).unwrap();
        // Reading after the first entry must NOT include it.
        let r = s.read_after(id, None);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, StreamId { ms: 2, seq: 0 });
    }

    #[test]
    fn parse_id_forms() {
        assert_eq!(StreamId::parse("-", false).unwrap(), StreamId::MIN);
        assert_eq!(StreamId::parse("+", true).unwrap(), StreamId::MAX);
        assert_eq!(StreamId::parse("100-3", false).unwrap(), StreamId { ms: 100, seq: 3 });
        assert_eq!(StreamId::parse("100", false).unwrap(), StreamId { ms: 100, seq: 0 });
        assert_eq!(StreamId::parse("100", true).unwrap(),  StreamId { ms: 100, seq: u64::MAX });
    }
}

pub mod hash;
pub mod list;
pub mod set;
pub mod sorted_set;
pub mod stream;

pub use hash::HashValue;
pub use list::ListValue;
pub use set::SetValue;
pub use sorted_set::SortedSetValue;
pub use stream::{StreamValue, StreamId};

use bytes::Bytes;
use std::time::Instant;

#[derive(Debug, Clone)]
pub enum CacheValue {
    String(Bytes, Option<Instant>),  // value, expires_at
    Hash(HashValue),
    List(ListValue),
    Set(SetValue),
    SortedSet(SortedSetValue),
    Stream(StreamValue),
}

impl CacheValue {
    pub fn is_expired(&self) -> bool {
        match self {
            CacheValue::String(_, Some(expires_at)) => Instant::now() > *expires_at,
            CacheValue::Hash(h) => h.is_expired(),
            CacheValue::List(l) => l.is_expired(),
            CacheValue::Set(s) => s.is_expired(),
            CacheValue::SortedSet(ss) => ss.is_expired(),
            CacheValue::Stream(s) => s.is_expired(),
            _ => false,
        }
    }
}
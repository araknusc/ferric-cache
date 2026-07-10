//! Pub/Sub for the cache server.
//!
//! Each server holds one `PubSub` registry keyed by channel name. A client
//! that runs SUBSCRIBE registers an `mpsc::UnboundedSender` per channel; the
//! corresponding receiver lives on the connection's `tokio::select!` loop
//! (`server::handle_connection`) so incoming messages get pushed to the
//! socket alongside synchronous command/response traffic.
//!
//! First-pass scope (matches the user-facing Bucket-3 brief):
//! - PUBLISH / SUBSCRIBE / UNSUBSCRIBE on exact channel names
//! - Pattern subscribe (PSUBSCRIBE) is **not implemented**; clients that try
//!   it get a normal `Unknown command` error from the parser.

use std::collections::HashMap;
use std::sync::Arc;
use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// One published message that the connection task formats and writes back
/// to the subscriber as a `*3\r\n$7\r\nmessage\r\n$<n>\r\n<channel>\r\n$<m>\r\n<payload>\r\n`
/// RESP frame.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel: Bytes,
    pub payload: Bytes,
}

/// Server-wide pub/sub registry. One instance lives in `CacheServer`. Cheap
/// to construct (empty HashMap), so always-on rather than opt-in.
pub struct PubSub {
    /// channel → list of subscriber senders. A subscriber may appear in many
    /// channels' lists; identity is kept implicit via the `Arc`-based sender
    /// the connection holds. Unsubscribe drops the sender, which closes the
    /// receiver and is detected the next time PUBLISH iterates.
    channels: RwLock<HashMap<Bytes, Vec<UnboundedSender<PubSubMessage>>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self { channels: RwLock::new(HashMap::new()) }
    }

    /// Register `tx` as a subscriber to `channel`. The connection task
    /// retains the matching `UnboundedReceiver`; its select! loop pulls
    /// messages and writes them to the socket.
    pub fn subscribe(&self, channel: Bytes, tx: UnboundedSender<PubSubMessage>) {
        self.channels.write().entry(channel).or_default().push(tx);
    }

    /// Drop subscriptions for `channel` whose sender is closed (best-effort
    /// cleanup; called from PUBLISH and UNSUBSCRIBE paths).
    pub fn prune_closed(&self, channel: &Bytes) {
        let mut g = self.channels.write();
        if let Some(list) = g.get_mut(channel) {
            list.retain(|tx| !tx.is_closed());
            if list.is_empty() {
                g.remove(channel);
            }
        }
    }

    /// Fan out `payload` to every active subscriber of `channel`. Returns
    /// the number of clients the message reached. Closed senders are skipped
    /// (and pruned lazily via `prune_closed`).
    pub fn publish(&self, channel: Bytes, payload: Bytes) -> usize {
        let g = self.channels.read();
        let Some(list) = g.get(&channel) else { return 0 };
        let msg = PubSubMessage { channel, payload };
        let mut delivered = 0usize;
        for tx in list {
            if tx.send(msg.clone()).is_ok() {
                delivered += 1;
            }
        }
        delivered
    }
}

impl Default for PubSub {
    fn default() -> Self { Self::new() }
}

/// Per-connection subscription state. Owned by the connection task's stack
/// (in `ConnState`); the (tx, rx) pair is created lazily on first SUBSCRIBE
/// and reused for the connection's lifetime. `tx` is cloned each time the
/// connection registers with a new channel; `rx` is what the connection's
/// `tokio::select!` loop reads to push messages to the socket.
pub struct SubscriberState {
    pub tx: Option<UnboundedSender<PubSubMessage>>,
    pub rx: Option<UnboundedReceiver<PubSubMessage>>,
    pub channels: Vec<Bytes>,
}

impl SubscriberState {
    pub fn new() -> Self {
        Self { tx: None, rx: None, channels: Vec::new() }
    }

    /// First call creates (tx, rx). Subsequent calls return clones of the
    /// existing tx. The rx stays inside `SubscriberState` until the
    /// connection task takes ownership.
    pub fn ensure_sender(&mut self) -> UnboundedSender<PubSubMessage> {
        if let Some(ref tx) = self.tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.tx = Some(tx.clone());
        self.rx = Some(rx);
        tx
    }

    /// True once at least one SUBSCRIBE has succeeded — meaning the
    /// connection task should switch to a select! loop.
    pub fn is_active(&self) -> bool {
        !self.channels.is_empty()
    }
}

impl Default for SubscriberState {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_reaches_subscribers() {
        let ps = Arc::new(PubSub::new());
        let (tx, mut rx) = mpsc::unbounded_channel::<PubSubMessage>();
        ps.subscribe(Bytes::from_static(b"news"), tx);

        let count = ps.publish(Bytes::from_static(b"news"), Bytes::from_static(b"hi"));
        assert_eq!(count, 1);

        let msg = rx.recv().await.expect("message received");
        assert_eq!(msg.channel.as_ref(), b"news");
        assert_eq!(msg.payload.as_ref(), b"hi");
    }

    #[test]
    fn publish_to_empty_channel_returns_zero() {
        let ps = PubSub::new();
        assert_eq!(ps.publish(Bytes::from_static(b"empty"), Bytes::from_static(b"x")), 0);
    }

    #[tokio::test]
    async fn closed_subscriber_is_skipped() {
        let ps = PubSub::new();
        let (tx, rx) = mpsc::unbounded_channel::<PubSubMessage>();
        ps.subscribe(Bytes::from_static(b"c"), tx);
        drop(rx); // closes the channel
        // Publish still returns 0 since the only sender is closed.
        let count = ps.publish(Bytes::from_static(b"c"), Bytes::from_static(b"x"));
        assert_eq!(count, 0);
    }
}

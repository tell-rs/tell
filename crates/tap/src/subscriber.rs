//! Subscriber management for tap connections
//!
//! Each connected client gets a `Subscriber` instance that tracks:
//! - Unique ID for the connection
//! - Filter criteria (workspace, source, type)
//! - Rate limiting state (sample rate, max batches/sec)
//! - Channel sender for async batch delivery
//!
//! The `SubscriberManager` handles registration, removal, and fan-out.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use tokio::sync::mpsc;

use tell_protocol::Batch;

use crate::SubscribeRequest;
use crate::error::{Result, TapError};
use crate::filter::TapFilter;

/// Counter for generating unique subscriber IDs
static SUBSCRIBER_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Maximum number of concurrent subscribers
const MAX_SUBSCRIBERS: usize = 100;

/// Channel buffer size for subscriber batches
const CHANNEL_BUFFER_SIZE: usize = 256;

/// A single tap subscriber (connected client)
#[derive(Debug)]
pub struct Subscriber {
    /// Unique identifier
    id: u64,
    /// Filter criteria
    filter: TapFilter,
    /// Channel sender for batch delivery
    sender: mpsc::Sender<Arc<Batch>>,
    /// Sample rate (0.0 - 1.0, None = send all)
    sample_rate: Option<f32>,
    /// Max batches per second (None = unlimited)
    max_batches_per_sec: Option<u32>,
    /// Counter for rate limiting
    batches_this_second: AtomicU64,
}

impl Subscriber {
    /// Create a new subscriber
    pub fn new(
        filter: TapFilter,
        sender: mpsc::Sender<Arc<Batch>>,
        sample_rate: Option<f32>,
        max_batches_per_sec: Option<u32>,
    ) -> Self {
        Self {
            id: SUBSCRIBER_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            filter,
            sender,
            sample_rate,
            max_batches_per_sec,
            batches_this_second: AtomicU64::new(0),
        }
    }

    /// Get the subscriber ID
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the filter
    #[inline]
    pub fn filter(&self) -> &TapFilter {
        &self.filter
    }

    /// Check if a batch matches this subscriber's filter
    #[inline]
    pub fn matches(&self, batch: &Batch) -> bool {
        self.filter.matches(batch)
    }

    /// Check if we should send based on sampling
    #[inline]
    pub fn should_sample(&self) -> bool {
        match self.sample_rate {
            None => true,
            Some(rate) if rate >= 1.0 => true,
            Some(rate) if rate <= 0.0 => false,
            Some(rate) => rand::random::<f32>() < rate,
        }
    }

    /// Check if we're within rate limit
    #[inline]
    pub fn within_rate_limit(&self) -> bool {
        match self.max_batches_per_sec {
            None => true,
            Some(max) => {
                let current = self.batches_this_second.fetch_add(1, Ordering::Relaxed);
                current < max as u64
            }
        }
    }

    /// Reset rate limit counter (called every second)
    #[inline]
    pub fn reset_rate_counter(&self) {
        self.batches_this_second.store(0, Ordering::Relaxed);
    }

    /// Try to send a batch to this subscriber
    ///
    /// Returns false if the channel is closed (subscriber disconnected)
    #[inline]
    pub fn try_send(&self, batch: Arc<Batch>) -> bool {
        self.sender.try_send(batch).is_ok()
    }

    /// Check if this subscriber is still connected
    #[inline]
    pub fn is_connected(&self) -> bool {
        !self.sender.is_closed()
    }
}

/// Manages all active subscribers
#[derive(Debug, Default)]
pub struct SubscriberManager {
    /// Active subscribers
    subscribers: RwLock<Vec<Arc<Subscriber>>>,
}

impl SubscriberManager {
    /// Create a new subscriber manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new subscriber
    ///
    /// Returns the subscriber ID and receiver channel
    pub fn subscribe(
        &self,
        request: &SubscribeRequest,
    ) -> Result<(u64, mpsc::Receiver<Arc<Batch>>)> {
        let mut subscribers = self.subscribers.write();

        if subscribers.len() >= MAX_SUBSCRIBERS {
            return Err(TapError::MaxSubscribers {
                max: MAX_SUBSCRIBERS,
            });
        }

        let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        let filter = TapFilter::from_subscribe(request);

        let subscriber = Arc::new(Subscriber::new(
            filter,
            sender,
            request.sample_rate,
            request.max_batches_per_sec,
        ));

        let id = subscriber.id();
        subscribers.push(subscriber);

        Ok((id, receiver))
    }

    /// Unsubscribe by ID
    pub fn unsubscribe(&self, id: u64) -> Result<()> {
        let mut subscribers = self.subscribers.write();
        let original_len = subscribers.len();
        subscribers.retain(|s| s.id() != id);

        if subscribers.len() == original_len {
            return Err(TapError::SubscriberNotFound { id });
        }

        Ok(())
    }

    /// Get number of active subscribers
    pub fn count(&self) -> usize {
        self.subscribers.read().len()
    }

    /// Check if there are any subscribers
    #[inline]
    pub fn has_subscribers(&self) -> bool {
        !self.subscribers.read().is_empty()
    }

    /// Broadcast a batch to all matching subscribers
    ///
    /// Returns the number of subscribers that received the batch
    pub fn broadcast(&self, batch: Arc<Batch>) -> usize {
        let subscribers = self.subscribers.read();
        let mut sent_count = 0;

        for subscriber in subscribers.iter() {
            // Check filter
            if !subscriber.matches(&batch) {
                continue;
            }

            // Check sampling
            if !subscriber.should_sample() {
                continue;
            }

            // Check rate limit
            if !subscriber.within_rate_limit() {
                continue;
            }

            // Try to send
            if subscriber.try_send(Arc::clone(&batch)) {
                sent_count += 1;
            }
        }

        sent_count
    }

    /// Clean up disconnected subscribers
    pub fn cleanup_disconnected(&self) -> usize {
        let mut subscribers = self.subscribers.write();
        let original_len = subscribers.len();
        subscribers.retain(|s| s.is_connected());
        original_len - subscribers.len()
    }

    /// Reset rate limit counters for all subscribers
    pub fn reset_rate_counters(&self) {
        let subscribers = self.subscribers.read();
        for subscriber in subscribers.iter() {
            subscriber.reset_rate_counter();
        }
    }
}

#[cfg(test)]
#[path = "subscriber_test.rs"]
mod tests;

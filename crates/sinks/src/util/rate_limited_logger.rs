//! Rate-limited error logging utility
//!
//! Prevents log spam under heavy error conditions by limiting log frequency.
//! Matches Go implementation behavior: logs at most once per interval,
//! with error count tracking between logs.
//!
//! # Example
//!
//! ```ignore
//! use sinks::util::RateLimitedLogger;
//! use std::time::Duration;
//!
//! let logger = RateLimitedLogger::new(Duration::from_secs(10));
//!
//! // Only logs once per 10 seconds, even if called frequently
//! for _ in 0..1000 {
//!     logger.error("write failed", &io_error);
//! }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Default interval for rate-limited logging (10 seconds, matches Go)
pub const DEFAULT_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum data length to include in log messages (security: prevents log injection)
pub const MAX_DATA_LOG_LENGTH: usize = 256;

/// Rate-limited logger that prevents log spam
///
/// Under heavy error conditions (e.g., disk full), this prevents flooding
/// logs with repetitive error messages. Instead, it logs at most once per
/// interval, including a count of suppressed errors.
///
/// Thread-safe: uses atomic counters and a mutex for the last log time.
pub struct RateLimitedLogger {
    /// Minimum interval between log messages
    min_interval: Duration,

    /// Last time we logged
    last_log_time: Mutex<Option<Instant>>,

    /// Count of errors since last log
    error_count: AtomicU64,

    /// Total errors ever recorded
    total_errors: AtomicU64,
}

impl RateLimitedLogger {
    /// Create a new rate-limited logger with the specified interval
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_log_time: Mutex::new(None),
            error_count: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Create a rate-limited logger with default interval (10 seconds)
    pub fn default_interval() -> Self {
        Self::new(DEFAULT_LOG_INTERVAL)
    }

    /// Record an error and log if enough time has passed
    ///
    /// Returns true if the error was logged, false if it was suppressed.
    pub fn error(&self, message: &str, error: &dyn std::fmt::Display) -> bool {
        // Always increment counters
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);

        // Check if we should log
        let should_log = {
            let mut last_time = self.last_log_time.lock();
            let now = Instant::now();

            match *last_time {
                None => {
                    *last_time = Some(now);
                    true
                }
                Some(last) if now.duration_since(last) >= self.min_interval => {
                    *last_time = Some(now);
                    true
                }
                _ => false,
            }
        };

        if should_log {
            let count = self.error_count.swap(0, Ordering::Relaxed);
            let total = self.total_errors.load(Ordering::Relaxed);

            if count > 1 {
                tracing::error!(
                    message = %message,
                    error = %error,
                    suppressed_count = count - 1,
                    total_errors = total,
                    "error (rate-limited)"
                );
            } else {
                tracing::error!(
                    message = %message,
                    error = %error,
                    total_errors = total,
                    "error"
                );
            }
            true
        } else {
            false
        }
    }

    /// Record an error with associated data (truncated for safety)
    ///
    /// Data is truncated to MAX_DATA_LOG_LENGTH bytes to prevent log injection.
    pub fn error_with_data(
        &self,
        message: &str,
        error: &dyn std::fmt::Display,
        data: &[u8],
    ) -> bool {
        // Always increment counters
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);

        // Check if we should log
        let should_log = {
            let mut last_time = self.last_log_time.lock();
            let now = Instant::now();

            match *last_time {
                None => {
                    *last_time = Some(now);
                    true
                }
                Some(last) if now.duration_since(last) >= self.min_interval => {
                    *last_time = Some(now);
                    true
                }
                _ => false,
            }
        };

        if should_log {
            let count = self.error_count.swap(0, Ordering::Relaxed);
            let total = self.total_errors.load(Ordering::Relaxed);

            // Truncate data for safety
            let truncated_data = if data.len() > MAX_DATA_LOG_LENGTH {
                format!(
                    "{}... (truncated from {} bytes)",
                    String::from_utf8_lossy(&data[..MAX_DATA_LOG_LENGTH]),
                    data.len()
                )
            } else {
                String::from_utf8_lossy(data).to_string()
            };

            if count > 1 {
                tracing::error!(
                    message = %message,
                    error = %error,
                    data = %truncated_data,
                    suppressed_count = count - 1,
                    total_errors = total,
                    "error with data (rate-limited)"
                );
            } else {
                tracing::error!(
                    message = %message,
                    error = %error,
                    data = %truncated_data,
                    total_errors = total,
                    "error with data"
                );
            }
            true
        } else {
            false
        }
    }

    /// Get the current error count since last log
    pub fn pending_error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    /// Get the total error count
    pub fn total_error_count(&self) -> u64 {
        self.total_errors.load(Ordering::Relaxed)
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.error_count.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
        *self.last_log_time.lock() = None;
    }
}

impl Default for RateLimitedLogger {
    fn default() -> Self {
        Self::default_interval()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_rate_limited_logger_new() {
        let logger = RateLimitedLogger::new(Duration::from_secs(5));
        assert_eq!(logger.pending_error_count(), 0);
        assert_eq!(logger.total_error_count(), 0);
    }

    #[test]
    fn test_rate_limited_logger_default() {
        let logger = RateLimitedLogger::default();
        assert_eq!(logger.min_interval, DEFAULT_LOG_INTERVAL);
    }

    #[test]
    fn test_first_error_always_logs() {
        let logger = RateLimitedLogger::new(Duration::from_secs(10));
        let error = io::Error::new(io::ErrorKind::Other, "test error");

        // First error should always log
        let logged = logger.error("test message", &error);
        assert!(logged);
        assert_eq!(logger.total_error_count(), 1);
    }

    #[test]
    fn test_rapid_errors_suppressed() {
        let logger = RateLimitedLogger::new(Duration::from_secs(10));
        let error = io::Error::new(io::ErrorKind::Other, "test error");

        // First error logs
        assert!(logger.error("test", &error));

        // Subsequent rapid errors should be suppressed
        for _ in 0..10 {
            let logged = logger.error("test", &error);
            assert!(!logged);
        }

        // All errors counted
        assert_eq!(logger.total_error_count(), 11);
        // Pending count should be 10 (not reset)
        assert_eq!(logger.pending_error_count(), 10);
    }

    #[test]
    fn test_reset() {
        let logger = RateLimitedLogger::new(Duration::from_secs(10));
        let error = io::Error::new(io::ErrorKind::Other, "test error");

        logger.error("test", &error);
        assert_eq!(logger.total_error_count(), 1);

        logger.reset();
        assert_eq!(logger.total_error_count(), 0);
        assert_eq!(logger.pending_error_count(), 0);
    }

    #[test]
    fn test_error_with_data_truncation() {
        let logger = RateLimitedLogger::new(Duration::from_secs(10));
        let error = io::Error::new(io::ErrorKind::Other, "test error");

        // Create data longer than MAX_DATA_LOG_LENGTH
        let long_data = vec![b'x'; MAX_DATA_LOG_LENGTH + 100];

        let logged = logger.error_with_data("test", &error, &long_data);
        assert!(logged);
    }
}

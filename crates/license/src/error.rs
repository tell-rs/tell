//! License error types.

use thiserror::Error;

/// Errors that can occur during license operations.
#[derive(Debug, Error)]
pub enum LicenseError {
    /// License key format is invalid.
    #[error("invalid license format: {0}")]
    InvalidFormat(String),

    /// Signature verification failed.
    #[error("invalid license signature")]
    InvalidSignature,

    /// License has expired.
    #[error("license expired for {customer} on {expired}")]
    Expired { customer: String, expired: String },

    /// Cryptographic error.
    #[error("crypto error: {0}")]
    CryptoError(String),

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

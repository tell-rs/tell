//! Authentication error types

use std::io;
use thiserror::Error;

/// Result type for auth operations
pub type Result<T> = std::result::Result<T, AuthError>;

/// Errors that can occur during authentication operations
#[derive(Debug, Error)]
pub enum AuthError {
    /// Failed to read API keys file
    #[error("failed to read API keys file '{path}': {source}")]
    IoError {
        /// Path to the file
        path: String,
        /// Underlying IO error
        #[source]
        source: io::Error,
    },

    /// Invalid line format in API keys file
    #[error("invalid format at line {line}: {message}")]
    ParseError {
        /// Line number (1-based)
        line: usize,
        /// Error message
        message: String,
    },

    /// Invalid API key (not valid hex or wrong length)
    #[error("invalid API key at line {line}: {message}")]
    InvalidKey {
        /// Line number (1-based)
        line: usize,
        /// Error message
        message: String,
    },

    /// Empty workspace ID
    #[error("empty workspace ID at line {line}")]
    EmptyWorkspace {
        /// Line number (1-based)
        line: usize,
    },

    /// Duplicate API key
    #[error("duplicate API key at line {line}")]
    DuplicateKey {
        /// Line number (1-based)
        line: usize,
    },

    /// File watcher error
    #[error("file watcher error: {0}")]
    WatchError(String),

    // JWT validation errors
    /// Token is missing or empty
    #[error("missing token")]
    MissingToken,

    /// Token format is invalid (not tell_<jwt>)
    #[error("invalid token format")]
    InvalidTokenFormat,

    /// JWT signature verification failed
    #[error("invalid token signature")]
    InvalidSignature,

    /// Token has expired
    #[error("token expired")]
    TokenExpired,

    /// Token is not yet valid (nbf claim)
    #[error("token not yet valid")]
    TokenNotYetValid,

    /// Token claims are invalid
    #[error("invalid token claims: {0}")]
    InvalidClaims(String),

    // Session errors
    /// Session not found in database
    #[error("session not found")]
    SessionNotFound,

    /// Session has expired
    #[error("session expired")]
    SessionExpired,

    /// Token has been revoked
    #[error("token revoked")]
    TokenRevoked,

    /// Database operation failed
    #[error("database error: {0}")]
    DatabaseError(String),

    /// Workspace access denied
    #[error("workspace access denied: {0}")]
    WorkspaceAccessDenied(String),
}

impl AuthError {
    /// Create an IoError
    pub fn io_error(path: impl Into<String>, source: io::Error) -> Self {
        Self::IoError {
            path: path.into(),
            source,
        }
    }

    /// Create a ParseError
    pub fn parse_error(line: usize, message: impl Into<String>) -> Self {
        Self::ParseError {
            line,
            message: message.into(),
        }
    }

    /// Create an InvalidKey error
    pub fn invalid_key(line: usize, message: impl Into<String>) -> Self {
        Self::InvalidKey {
            line,
            message: message.into(),
        }
    }

    /// Create an EmptyWorkspace error
    pub fn empty_workspace(line: usize) -> Self {
        Self::EmptyWorkspace { line }
    }

    /// Create a DuplicateKey error
    pub fn duplicate_key(line: usize) -> Self {
        Self::DuplicateKey { line }
    }

    /// Create a WatchError
    pub fn watch_error(message: impl Into<String>) -> Self {
        Self::WatchError(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error() {
        let err = AuthError::io_error(
            "/path/to/keys.conf",
            io::Error::new(io::ErrorKind::NotFound, "file not found"),
        );
        assert!(err.to_string().contains("/path/to/keys.conf"));
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_parse_error() {
        let err = AuthError::parse_error(5, "missing colon separator");
        assert!(err.to_string().contains("line 5"));
        assert!(err.to_string().contains("missing colon"));
    }

    #[test]
    fn test_invalid_key() {
        let err = AuthError::invalid_key(3, "must be 32 hex characters");
        assert!(err.to_string().contains("line 3"));
        assert!(err.to_string().contains("32 hex"));
    }

    #[test]
    fn test_empty_workspace() {
        let err = AuthError::empty_workspace(10);
        assert!(err.to_string().contains("line 10"));
        assert!(err.to_string().contains("empty workspace"));
    }

    #[test]
    fn test_duplicate_key() {
        let err = AuthError::duplicate_key(7);
        assert!(err.to_string().contains("line 7"));
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn test_watch_error() {
        let err = AuthError::watch_error("inotify limit reached");
        assert!(err.to_string().contains("inotify"));
    }
}

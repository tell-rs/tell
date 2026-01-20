//! Pipeline error types
//!
//! Error types for the async router and pipeline operations.

use thiserror::Error;

use tell_routing::SinkId;

/// Pipeline errors
#[derive(Debug, Error)]
pub enum PipelineError {
    /// Sink not registered with the router
    #[error("sink not registered: {0}")]
    SinkNotRegistered(SinkId),

    /// Sink channel is closed
    #[error("sink channel closed: {0}")]
    SinkClosed(SinkId),

    /// All target sinks failed to receive the batch
    #[error("all sinks failed for batch")]
    AllSinksFailed,

    /// Channel send error
    #[error("channel send failed: {0}")]
    SendError(String),

    /// Router is shutting down
    #[error("router is shutting down")]
    ShuttingDown,
}

/// Result type for pipeline operations
pub type Result<T> = std::result::Result<T, PipelineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PipelineError::SinkNotRegistered(SinkId::new(5));
        assert!(err.to_string().contains("sink:5"));

        let err = PipelineError::SinkClosed(SinkId::new(3));
        assert!(err.to_string().contains("sink:3"));

        let err = PipelineError::AllSinksFailed;
        assert!(err.to_string().contains("all sinks failed"));

        let err = PipelineError::SendError("test error".into());
        assert!(err.to_string().contains("test error"));

        let err = PipelineError::ShuttingDown;
        assert!(err.to_string().contains("shutting down"));
    }
}

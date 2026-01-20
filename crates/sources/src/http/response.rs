//! HTTP response helpers
//!
//! Response building and batch sending utilities.

use std::net::IpAddr;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use tell_protocol::{BatchBuilder, BatchType};

use super::error::{BatchResult, HttpSourceError};
use super::handlers::HandlerState;
use super::json_types::{ErrorResponse, IngestResponse, LineErrorResponse, PartialResponse};

/// Send batch to pipeline
pub fn send_batch(
    state: &HandlerState,
    data: &[u8],
    batch_type: BatchType,
) -> Result<(), HttpSourceError> {
    let mut builder = BatchBuilder::new(batch_type, state.source_id.clone());
    builder.add(data, 1);

    let batch = builder.finish();
    let connection_id = state.batch_sender.allocate_connection_id();

    match state.batch_sender.try_send(batch, connection_id) {
        Ok(()) => {
            state.metrics.batch_sent();
            Ok(())
        }
        Err(crossfire::TrySendError::Full(_)) => {
            Err(HttpSourceError::ServiceUnavailable("channel full".into()))
        }
        Err(crossfire::TrySendError::Disconnected(_)) => {
            Err(HttpSourceError::ChannelClosed)
        }
    }
}

/// Send batch with workspace_id and source_ip metadata (like TCP source)
pub fn send_batch_with_metadata(
    state: &HandlerState,
    data: &[u8],
    batch_type: BatchType,
    workspace_id: u32,
    source_ip: IpAddr,
) -> Result<(), HttpSourceError> {
    let mut builder = BatchBuilder::new(batch_type, state.source_id.clone());
    builder.set_workspace_id(workspace_id);
    builder.set_source_ip(source_ip);
    builder.add(data, 1);

    let batch = builder.finish();
    let connection_id = state.batch_sender.allocate_connection_id();

    match state.batch_sender.try_send(batch, connection_id) {
        Ok(()) => {
            state.metrics.batch_sent();
            Ok(())
        }
        Err(crossfire::TrySendError::Full(_)) => {
            Err(HttpSourceError::ServiceUnavailable("channel full".into()))
        }
        Err(crossfire::TrySendError::Disconnected(_)) => {
            Err(HttpSourceError::ChannelClosed)
        }
    }
}

/// Build HTTP response from batch result
pub fn build_response(result: BatchResult) -> Response {
    let request_id = generate_request_id();

    if result.is_complete_success() {
        // 202 Accepted
        let response = IngestResponse {
            accepted: result.accepted,
            request_id,
        };
        (StatusCode::ACCEPTED, Json(response)).into_response()
    } else if result.has_accepted() {
        // 207 Multi-Status (partial success)
        let response = PartialResponse {
            accepted: result.accepted,
            rejected: result.rejected,
            errors: result.errors.into_iter().map(|e| LineErrorResponse {
                line: e.line,
                error: e.error,
            }).collect(),
            request_id,
        };
        (StatusCode::MULTI_STATUS, Json(response)).into_response()
    } else {
        // 400 Bad Request (all failed)
        let error = if let Some(first) = result.errors.first() {
            first.error.clone()
        } else {
            "all items rejected".into()
        };
        error_response(StatusCode::BAD_REQUEST, "parse_error", error)
    }
}

/// Create error response
pub fn error_response(status: StatusCode, error: &str, message: impl Into<String>) -> Response {
    let response = ErrorResponse::new(error, message);
    (status, Json(response)).into_response()
}

/// Generate a simple request ID
pub fn generate_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("req_{:x}", ts)
}

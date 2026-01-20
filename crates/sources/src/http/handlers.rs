//! HTTP route handlers
//!
//! Axum handlers for events and logs ingestion endpoints.
//!
//! # Endpoints
//!
//! - `/v1/events` - JSONL events ingestion
//! - `/v1/logs` - JSONL logs ingestion
//! - `/v1/ingest` - Binary FlatBuffer ingestion (zero-copy, high performance)
//! - `/health` - Health check

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use tell_auth::ApiKeyStore;
use tell_protocol::{BatchType, FlatBatch, SourceId};

use super::auth::{
    extract_api_key, extract_api_key_with_workspace, get_source_ip, is_binary_content_type,
};
use super::encoder::{encode_events, encode_logs};
use super::error::BatchResult;
use super::jsonl::{parse_jsonl_events, parse_jsonl_logs};
use super::metrics::HttpSourceMetrics;
use super::response::{
    build_response, error_response, generate_request_id, send_batch, send_batch_with_metadata,
};
use crate::ShardedSender;

/// Shared state for handlers
pub struct HandlerState {
    pub auth_store: Arc<ApiKeyStore>,
    pub batch_sender: ShardedSender,
    pub source_id: SourceId,
    pub metrics: Arc<HttpSourceMetrics>,
    pub max_payload_size: usize,
}

/// POST /v1/events - Ingest analytics events (JSONL)
pub async fn ingest_events(
    State(state): State<Arc<HandlerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.metrics.request_received();
    state.metrics.bytes_received(body.len() as u64);

    // Validate payload size
    if body.len() > state.max_payload_size {
        state.metrics.request_client_error();
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!(
                "payload size {} exceeds limit {}",
                body.len(),
                state.max_payload_size
            ),
        );
    }

    // Extract and validate API key
    let api_key = match extract_api_key(&headers, &state.auth_store) {
        Ok(key) => key,
        Err(e) => {
            state.metrics.auth_failure();
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized", e.to_string());
        }
    };

    // Parse JSONL body
    let (events, parse_errors) = parse_jsonl_events(&body);

    if events.is_empty() && parse_errors.is_empty() {
        state.metrics.request_client_error();
        return error_response(
            StatusCode::BAD_REQUEST,
            "empty_body",
            "no events in request body",
        );
    }

    // Track parse errors
    for _ in &parse_errors {
        state.metrics.parse_error();
    }

    // Encode events to FlatBuffer
    let batch_bytes = match encode_events(&events, &api_key) {
        Ok(bytes) => bytes,
        Err(e) => {
            state.metrics.request_client_error();
            return error_response(StatusCode::BAD_REQUEST, "encoding_error", e.to_string());
        }
    };

    // Send to pipeline
    let send_result = send_batch(&state, &batch_bytes, BatchType::Event);

    // Build result
    let result = BatchResult {
        accepted: if send_result.is_ok() { events.len() } else { 0 },
        rejected: parse_errors.len()
            + if send_result.is_err() {
                events.len()
            } else {
                0
            },
        errors: parse_errors,
    };

    state
        .metrics
        .items_processed(result.accepted, result.rejected);

    if let Err(e) = send_result {
        state.metrics.request_server_error();
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            e.to_string(),
        );
    }

    state.metrics.request_success();
    build_response(result)
}

/// POST /v1/logs - Ingest structured logs (JSONL)
pub async fn ingest_logs(
    State(state): State<Arc<HandlerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.metrics.request_received();
    state.metrics.bytes_received(body.len() as u64);

    // Validate payload size
    if body.len() > state.max_payload_size {
        state.metrics.request_client_error();
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!(
                "payload size {} exceeds limit {}",
                body.len(),
                state.max_payload_size
            ),
        );
    }

    // Extract and validate API key
    let api_key = match extract_api_key(&headers, &state.auth_store) {
        Ok(key) => key,
        Err(e) => {
            state.metrics.auth_failure();
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized", e.to_string());
        }
    };

    // Parse JSONL body
    let (logs, parse_errors) = parse_jsonl_logs(&body);

    if logs.is_empty() && parse_errors.is_empty() {
        state.metrics.request_client_error();
        return error_response(
            StatusCode::BAD_REQUEST,
            "empty_body",
            "no logs in request body",
        );
    }

    for _ in &parse_errors {
        state.metrics.parse_error();
    }

    // Encode logs to FlatBuffer
    let batch_bytes = match encode_logs(&logs, &api_key) {
        Ok(bytes) => bytes,
        Err(e) => {
            state.metrics.request_client_error();
            return error_response(StatusCode::BAD_REQUEST, "encoding_error", e.to_string());
        }
    };

    // Send to pipeline
    let send_result = send_batch(&state, &batch_bytes, BatchType::Log);

    let result = BatchResult {
        accepted: if send_result.is_ok() { logs.len() } else { 0 },
        rejected: parse_errors.len() + if send_result.is_err() { logs.len() } else { 0 },
        errors: parse_errors,
    };

    state
        .metrics
        .items_processed(result.accepted, result.rejected);

    if let Err(e) = send_result {
        state.metrics.request_server_error();
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            e.to_string(),
        );
    }

    state.metrics.request_success();
    build_response(result)
}

/// GET /health - Health check endpoint
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

/// POST /v1/ingest - Ingest raw FlatBuffer batch (zero-copy, high performance)
///
/// Accepts pre-encoded Batch FlatBuffers (same wire format as TCP source).
/// This is the fastest ingestion path since it skips JSON parsing entirely.
///
/// Like TCP source, this handler:
/// - Validates API key and gets workspace_id for routing
/// - Tracks source IP from connection (or X-Forwarded-For)
/// - Sets workspace_id and source_ip on batch for downstream routing
///
/// # Headers
///
/// - `Content-Type: application/x-flatbuffers` or `application/octet-stream`
/// - `X-API-Key: <hex key>` - Required for authentication
/// - `X-Forwarded-For: <ip>` - Optional, used for proxied requests
///
/// # Body
///
/// Raw Batch FlatBuffer bytes (same format as TCP protocol).
pub async fn ingest_binary(
    State(state): State<Arc<HandlerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.metrics.request_received();
    state.metrics.bytes_received(body.len() as u64);

    // Validate payload size
    if body.len() > state.max_payload_size {
        state.metrics.request_client_error();
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!(
                "payload size {} exceeds limit {}",
                body.len(),
                state.max_payload_size
            ),
        );
    }

    // Validate Content-Type
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if !is_binary_content_type(content_type) {
        state.metrics.request_client_error();
        return error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "expected Content-Type: application/x-flatbuffers or application/octet-stream",
        );
    }

    // Extract and validate API key from header, get workspace_id
    let (api_key, workspace_id) = match extract_api_key_with_workspace(&headers, &state.auth_store)
    {
        Ok(result) => result,
        Err(e) => {
            state.metrics.auth_failure();
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized", e.to_string());
        }
    };

    // Validate FlatBuffer structure
    let batch = match FlatBatch::parse(&body) {
        Ok(batch) => batch,
        Err(e) => {
            state.metrics.request_client_error();
            state.metrics.parse_error();
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_flatbuffer",
                format!("failed to parse FlatBuffer batch: {}", e),
            );
        }
    };

    // Verify API key matches (batch may have embedded key, but header takes precedence)
    // The batch's embedded key must match the authenticated key for security
    if let Ok(batch_key) = batch.api_key()
        && *batch_key != api_key
    {
        state.metrics.auth_failure();
        return error_response(
            StatusCode::FORBIDDEN,
            "api_key_mismatch",
            "batch API key does not match authenticated key",
        );
    }

    // Get schema type to determine batch type
    let batch_type = match batch.schema_type() {
        tell_protocol::SchemaType::Event => BatchType::Event,
        tell_protocol::SchemaType::Log => BatchType::Log,
        _ => {
            state.metrics.request_client_error();
            return error_response(
                StatusCode::BAD_REQUEST,
                "unsupported_schema",
                "only Event and Log schema types are supported",
            );
        }
    };

    // Get source IP from connection (check X-Forwarded-For for proxied requests)
    let source_ip = get_source_ip(&headers, addr.ip());

    // Send with workspace_id and source_ip (like TCP source)
    let send_result =
        send_batch_with_metadata(&state, &body, batch_type, workspace_id.as_u32(), source_ip);

    if let Err(e) = send_result {
        state.metrics.request_server_error();
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            e.to_string(),
        );
    }

    state.metrics.items_processed(1, 0);
    state.metrics.request_success();

    // Simple success response for binary mode
    let response = super::json_types::IngestResponse {
        accepted: 1,
        request_id: generate_request_id(),
    };
    (StatusCode::ACCEPTED, Json(response)).into_response()
}

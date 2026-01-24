//! API client functions for the TUI.
//!
//! All HTTP API calls to the Tell backend.

use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::UnboundedSender;

use super::action::{
    Action, BoardData, BoardInfo, MetricValue, QueryResult, StickinessData, TimeRange, TopEvent,
};

/// Load boards from API.
pub fn load_boards(
    auth: &crate::cmd::auth::AuthCredentials,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .get(format!("{}/api/v1/boards", api_url))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        #[derive(serde::Deserialize)]
        struct BoardsResponse {
            boards: Vec<BoardApiInfo>,
        }

        #[derive(serde::Deserialize)]
        struct BoardApiInfo {
            id: String,
            title: String,
            description: Option<String>,
            is_pinned: bool,
            share_hash: Option<String>,
            settings: Option<BoardSettings>,
        }

        #[derive(serde::Deserialize)]
        struct BoardSettings {
            blocks: Option<Vec<serde_json::Value>>,
        }

        match result {
            Ok(response) if response.status().is_success() => {
                if let Ok(resp) = response.json::<BoardsResponse>().await {
                    let boards: Vec<BoardInfo> = resp
                        .boards
                        .into_iter()
                        .map(|b| BoardInfo {
                            id: b.id,
                            title: b.title,
                            description: b.description,
                            is_pinned: b.is_pinned,
                            share_hash: b.share_hash,
                            block_count: b
                                .settings
                                .and_then(|s| s.blocks)
                                .map(|b| b.len())
                                .unwrap_or(0),
                        })
                        .collect();
                    let _ = action_tx.send(Action::BoardsLoaded(boards));
                }
            }
            Ok(response) if response.status().as_u16() == 401 => {
                let _ = action_tx.send(Action::Error(
                    "Session expired. Please /login again.".to_string(),
                ));
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to load boards ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Load a specific board's details.
pub fn load_board_detail(
    auth: &crate::cmd::auth::AuthCredentials,
    board_id: &str,
    time_range: TimeRange,
    show_comparison: bool,
    breakdown: Option<String>,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let board_id = board_id.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();

        // First fetch the board to get metric blocks
        let result = client
            .get(format!("{}/api/v1/boards/{}", api_url, board_id))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        #[derive(serde::Deserialize)]
        struct BoardResponse {
            id: String,
            title: String,
            settings: Option<BoardSettings>,
        }

        #[derive(serde::Deserialize)]
        struct BoardSettings {
            blocks: Option<Vec<BlockInfo>>,
        }

        #[derive(serde::Deserialize, Clone)]
        struct BlockInfo {
            #[serde(rename = "type")]
            block_type: String,
            metric: Option<MetricInfo>,
        }

        #[derive(serde::Deserialize, Clone)]
        struct MetricInfo {
            title: Option<String>,
            metric_type: Option<String>,
        }

        // API response for metrics
        #[derive(serde::Deserialize)]
        struct ApiResponse<T> {
            data: T,
        }

        #[derive(serde::Deserialize)]
        struct TimeSeriesData {
            total: f64,
            comparison: Option<ComparisonData>,
        }

        #[derive(serde::Deserialize)]
        struct ComparisonData {
            percent_change: f64,
        }

        match result {
            Ok(response) if response.status().is_success() => {
                if let Ok(resp) = response.json::<BoardResponse>().await {
                    let blocks = resp.settings.and_then(|s| s.blocks).unwrap_or_default();

                    // Collect metric types we need to fetch
                    let metric_blocks: Vec<_> = blocks
                        .iter()
                        .filter(|b| b.block_type == "metric")
                        .filter_map(|b| b.metric.as_ref())
                        .collect();

                    // Fetch actual values for each metric type
                    let mut metrics = Vec::new();
                    for m in metric_blocks {
                        let metric_type = m.metric_type.clone().unwrap_or_default();
                        let title = m
                            .title
                            .clone()
                            .or_else(|| Some(metric_type.to_uppercase()))
                            .unwrap_or_else(|| "Metric".to_string());

                        // Map metric_type to API endpoint
                        let endpoint = match metric_type.as_str() {
                            "dau" => "/api/v1/metrics/dau",
                            "wau" => "/api/v1/metrics/wau",
                            "mau" => "/api/v1/metrics/mau",
                            "events" => "/api/v1/metrics/events",
                            "logs" => "/api/v1/metrics/logs",
                            "sessions" => "/api/v1/metrics/sessions",
                            _ => continue, // Unknown metric type
                        };

                        // Build URL with range, comparison, and optional breakdown
                        let mut url =
                            format!("{}{}?range={}", api_url, endpoint, time_range.as_param());
                        if show_comparison {
                            url.push_str("&compare=previous");
                        }
                        if let Some(ref dim) = breakdown {
                            url.push_str(&format!("&breakdown={}", dim));
                        }

                        // Fetch the metric value
                        let metric_result = client
                            .get(&url)
                            .header("Authorization", format!("Bearer {}", token))
                            .send()
                            .await;

                        let (value, change_percent) = match metric_result {
                            Ok(resp) if resp.status().is_success() => {
                                if let Ok(data) = resp.json::<ApiResponse<TimeSeriesData>>().await {
                                    let change = data.data.comparison.map(|c| c.percent_change);
                                    (data.data.total, change)
                                } else {
                                    (0.0, None)
                                }
                            }
                            _ => (0.0, None),
                        };

                        metrics.push(MetricValue {
                            title,
                            value,
                            change_percent,
                        });
                    }

                    let board_data = BoardData {
                        id: resp.id,
                        title: resp.title,
                        metrics,
                    };
                    let _ = action_tx.send(Action::BoardDataLoaded(board_data));
                }
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to load board ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Execute a SQL query via the API.
pub fn execute_query(
    auth: &crate::cmd::auth::AuthCredentials,
    query: &str,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let query = query.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .post(format!("{}/api/v1/data/query", api_url))
            .header("Authorization", format!("Bearer {}", token))
            .json(&serde_json::json!({
                "query": query,
                "limit": 1000,
            }))
            .send()
            .await;

        #[derive(serde::Deserialize)]
        struct ApiResponse<T> {
            data: T,
        }

        #[derive(serde::Deserialize)]
        struct QueryResponse {
            columns: Vec<ColumnInfo>,
            rows: Vec<Vec<serde_json::Value>>,
            row_count: usize,
            execution_time_ms: u64,
        }

        #[derive(serde::Deserialize)]
        struct ColumnInfo {
            name: String,
        }

        match result {
            Ok(response) if response.status().is_success() => {
                if let Ok(resp) = response.json::<ApiResponse<QueryResponse>>().await {
                    let query_result = QueryResult {
                        columns: resp.data.columns.into_iter().map(|c| c.name).collect(),
                        rows: resp
                            .data
                            .rows
                            .into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(|v| match v {
                                        serde_json::Value::Null => "NULL".to_string(),
                                        serde_json::Value::String(s) => s,
                                        other => other.to_string(),
                                    })
                                    .collect()
                            })
                            .collect(),
                        row_count: resp.data.row_count,
                        execution_time_ms: resp.data.execution_time_ms,
                    };
                    let _ = action_tx.send(Action::QueryResult(query_result));
                } else {
                    let _ = action_tx.send(Action::QueryFailed("Invalid response".to_string()));
                }
            }
            Ok(response) if response.status().as_u16() == 401 => {
                let _ = action_tx.send(Action::QueryFailed(
                    "Session expired. Please /login again.".to_string(),
                ));
            }
            Ok(response) if response.status().as_u16() == 400 => {
                // Try to get error message from response
                let body = response.text().await.unwrap_or_default();
                let msg = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    json["error"]["message"]
                        .as_str()
                        .or(json["message"].as_str())
                        .unwrap_or("Invalid query")
                        .to_string()
                } else {
                    "Invalid query".to_string()
                };
                let _ = action_tx.send(Action::QueryFailed(msg));
            }
            Ok(response) => {
                let _ = action_tx.send(Action::QueryFailed(format!(
                    "Query failed ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::QueryFailed(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Create a new board.
pub fn create_board(
    auth: &crate::cmd::auth::AuthCredentials,
    title: &str,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let title = title.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .post(format!("{}/api/v1/boards", api_url))
            .header("Authorization", format!("Bearer {}", token))
            .json(&serde_json::json!({ "title": title }))
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                let _ = action_tx.send(Action::BoardUpdated);
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to create board ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Update a board's title.
pub fn update_board(
    auth: &crate::cmd::auth::AuthCredentials,
    board_id: &str,
    title: &str,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let board_id = board_id.to_string();
    let title = title.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .put(format!("{}/api/v1/boards/{}", api_url, board_id))
            .header("Authorization", format!("Bearer {}", token))
            .json(&serde_json::json!({ "title": title }))
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                let _ = action_tx.send(Action::BoardUpdated);
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to update board ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Delete a board.
pub fn delete_board(
    auth: &crate::cmd::auth::AuthCredentials,
    board_id: &str,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let board_id = board_id.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .delete(format!("{}/api/v1/boards/{}", api_url, board_id))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                let _ = action_tx.send(Action::BoardDeleted(board_id));
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to delete board ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Toggle pin status for a board.
pub fn toggle_pin_board(
    auth: &crate::cmd::auth::AuthCredentials,
    board_id: &str,
    is_currently_pinned: bool,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let board_id = board_id.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let endpoint = if is_currently_pinned {
            format!("{}/api/v1/boards/{}/unpin", api_url, board_id)
        } else {
            format!("{}/api/v1/boards/{}/pin", api_url, board_id)
        };

        let result = client
            .post(&endpoint)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                let _ = action_tx.send(Action::BoardUpdated);
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to toggle pin ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Share a board (create public link).
pub fn share_board(
    auth: &crate::cmd::auth::AuthCredentials,
    board_id: &str,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let board_id = board_id.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .post(format!("{}/api/v1/boards/{}/share", api_url, board_id))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        #[derive(serde::Deserialize)]
        struct ShareResponse {
            share_url: String,
        }

        match result {
            Ok(response) if response.status().is_success() => {
                if let Ok(resp) = response.json::<ShareResponse>().await {
                    let _ = action_tx.send(Action::BoardShared(board_id, resp.share_url));
                }
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to share board ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Unshare a board (revoke public link).
pub fn unshare_board(
    auth: &crate::cmd::auth::AuthCredentials,
    board_id: &str,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();
    let board_id = board_id.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .delete(format!("{}/api/v1/boards/{}/share", api_url, board_id))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                let _ = action_tx.send(Action::BoardUnshared(board_id));
            }
            Ok(response) => {
                let _ = action_tx.send(Action::Error(format!(
                    "Failed to unshare board ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Load top events.
pub fn load_top_events(
    auth: &crate::cmd::auth::AuthCredentials,
    time_range: TimeRange,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .get(format!(
                "{}/api/v1/metrics/events/top?range={}",
                api_url,
                time_range.as_param()
            ))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        #[derive(serde::Deserialize)]
        struct ApiResponse {
            data: TopEventsData,
        }

        #[derive(serde::Deserialize)]
        struct TopEventsData {
            events: Vec<EventInfo>,
        }

        #[derive(serde::Deserialize)]
        struct EventInfo {
            name: String,
            count: u64,
        }

        match result {
            Ok(response) if response.status().is_success() => {
                if let Ok(resp) = response.json::<ApiResponse>().await {
                    let events: Vec<TopEvent> = resp
                        .data
                        .events
                        .into_iter()
                        .map(|e| TopEvent {
                            name: e.name,
                            count: e.count,
                        })
                        .collect();
                    let _ = action_tx.send(Action::TopEventsLoaded(events));
                }
            }
            _ => {}
        }
    });

    Ok(())
}

/// Load stickiness metrics.
pub fn load_stickiness(
    auth: &crate::cmd::auth::AuthCredentials,
    time_range: TimeRange,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    let api_url = auth.api_url.clone();
    let token = auth.access_token.clone();

    tokio::spawn(async move {
        let client = reqwest::Client::new();

        #[derive(serde::Deserialize)]
        struct ApiResponse {
            data: StickinessResponse,
        }

        #[derive(serde::Deserialize)]
        struct StickinessResponse {
            ratio: f64,
        }

        // Fetch daily stickiness (DAU/MAU)
        let daily_result = client
            .get(format!(
                "{}/api/v1/metrics/stickiness/daily?range={}",
                api_url,
                time_range.as_param()
            ))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        let daily_ratio = match daily_result {
            Ok(response) if response.status().is_success() => response
                .json::<ApiResponse>()
                .await
                .map(|r| r.data.ratio)
                .unwrap_or(0.0),
            _ => 0.0,
        };

        // Fetch weekly stickiness (DAU/WAU)
        let weekly_result = client
            .get(format!(
                "{}/api/v1/metrics/stickiness/weekly?range={}",
                api_url,
                time_range.as_param()
            ))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await;

        let weekly_ratio = match weekly_result {
            Ok(response) if response.status().is_success() => response
                .json::<ApiResponse>()
                .await
                .map(|r| r.data.ratio)
                .unwrap_or(0.0),
            _ => 0.0,
        };

        let _ = action_tx.send(Action::StickinessLoaded(StickinessData {
            daily_ratio,
            weekly_ratio,
        }));
    });

    Ok(())
}

/// Submit login request.
pub fn submit_login(
    api_url: String,
    email: String,
    password: String,
    action_tx: UnboundedSender<Action>,
) -> Result<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .post(format!("{}/api/v1/auth/login", api_url))
            .json(&serde_json::json!({
                "email": email,
                "password": password,
            }))
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                #[derive(serde::Deserialize)]
                struct LoginResponse {
                    access_token: String,
                }

                if let Ok(login_resp) = response.json::<LoginResponse>().await {
                    // Save credentials
                    let creds = crate::cmd::auth::AuthCredentials {
                        access_token: login_resp.access_token,
                        api_url: api_url.clone(),
                        email: email.clone(),
                    };
                    let _ = crate::cmd::auth::save_credentials(&creds);
                    let _ = action_tx.send(Action::LoginSuccess(email));
                } else {
                    let _ = action_tx.send(Action::LoginFailed("Invalid response".to_string()));
                }
            }
            Ok(response) if response.status().as_u16() == 401 => {
                let _ =
                    action_tx.send(Action::LoginFailed("Invalid email or password".to_string()));
            }
            Ok(response) => {
                let _ = action_tx.send(Action::LoginFailed(format!(
                    "Login failed ({})",
                    response.status()
                )));
            }
            Err(e) => {
                let _ = action_tx.send(Action::LoginFailed(format!("Connection error: {}", e)));
            }
        }
    });

    Ok(())
}

/// Fetch server metrics from control endpoint.
pub fn fetch_server_metrics(action_tx: UnboundedSender<Action>) {
    tokio::spawn(async move {
        let Ok(client) = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        else {
            return;
        };

        if let Ok(resp) = client.get("http://127.0.0.1:3000/metrics").send().await
            && let Ok(text) = resp.text().await
        {
            let _ = action_tx.send(Action::MetricsUpdate(text));
        }
    });
}

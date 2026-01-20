//! Query command - Execute SQL queries against analytics data
//!
//! Supports both ClickHouse (production) and local Arrow IPC (Polars) backends.
//!
//! # Usage
//!
//! ```bash
//! tell query "SELECT * FROM events LIMIT 10"
//! tell query "SELECT event_name, COUNT(*) FROM events GROUP BY event_name" --format json
//! tell query "SELECT * FROM logs WHERE level = 'error'" --format csv
//! ```
//!
//! # Configuration
//!
//! The query command reads from the `[query]` section in the config file.
//! It can reference a sink by name to pull connection details:
//!
//! ```toml
//! # Same credentials for read/write:
//! [query]
//! sink = "clickhouse"  # Uses [sinks.clickhouse] host, database, and credentials
//! workspace_id = 1
//!
//! # Separate read credentials (recommended for production):
//! [query]
//! sink = "clickhouse"              # Gets host/database from sink
//! username = "testdev_reader"      # Override with read-only credentials
//! password = "reader_password"
//! workspace_id = 1
//!
//! # Or specify connection inline (no sink reference):
//! [query]
//! backend = "clickhouse"
//! url = "http://localhost:8123"
//! database = "tell"
//! username = "reader"
//! password = "secret"
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use tell_query::{OutputFormat, QueryConfig, QueryEngine, QueryResult, ResolvedQueryConfig, QueryBackendType};

/// Query command arguments
#[derive(Args, Debug)]
pub struct QueryArgs {
    /// SQL query to execute (SELECT only)
    #[arg(value_name = "SQL")]
    sql: String,

    /// Output format (table, json, csv)
    #[arg(short, long, default_value = "table")]
    format: String,

    /// Config file path (uses [query] section)
    #[arg(short, long)]
    config: Option<PathBuf>,
}

/// Run the query command
pub async fn run(args: QueryArgs) -> Result<()> {
    // Parse output format
    let format: OutputFormat = args
        .format
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid format: {}", e))?;

    // Build resolved config from CLI or config file
    let resolved = build_resolved_config(args.config.as_ref())?;

    // Create query engine
    let engine = QueryEngine::from_resolved_config(&resolved)
        .context("failed to create query engine")?;

    // Execute query
    let result = engine
        .query(&args.sql)
        .await
        .context("query execution failed")?;

    // Output results
    output_result(&result, format)?;

    // Print summary to stderr
    eprintln!(
        "\n{} row(s) in {}ms [{}]",
        result.row_count,
        result.execution_time_ms,
        engine.backend_name()
    );

    Ok(())
}

/// Build ResolvedQueryConfig from config file with sink resolution
pub fn build_resolved_config(config_path: Option<&PathBuf>) -> Result<ResolvedQueryConfig> {
    // Determine config path
    let path = if let Some(p) = config_path {
        if p.exists() {
            p.clone()
        } else {
            return Err(anyhow::anyhow!("config file not found: {}", p.display()));
        }
    } else {
        // Try default paths
        let default_paths = ["configs/config.toml", "config.toml"];
        let mut found = None;
        for p in default_paths {
            let path = PathBuf::from(p);
            if path.exists() {
                found = Some(path);
                break;
            }
        }
        match found {
            Some(p) => p,
            None => return Ok(default_resolved_config()),
        }
    };

    load_and_resolve_config(&path)
}

/// Load config and resolve sink references
fn load_and_resolve_config(path: &PathBuf) -> Result<ResolvedQueryConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;

    let toml_value: toml::Value = toml::from_str(&content)
        .with_context(|| format!("failed to parse config file: {}", path.display()))?;

    // Extract [query] section - if missing, use sensible defaults
    let query_section = toml_value.get("query");
    let query_config: QueryConfig = match query_section {
        Some(section) => section
            .clone()
            .try_into()
            .context("failed to parse [query] section")?,
        None => {
            // No [query] section - use default local backend
            return Ok(default_resolved_config());
        }
    };

    // If sink reference exists, resolve it from [sinks] section
    if let Some(sink_name) = &query_config.sink {
        let sinks_section = toml_value
            .get("sinks")
            .ok_or_else(|| anyhow::anyhow!("query.sink references '{}' but no [sinks] section found", sink_name))?;

        let sink_config = sinks_section
            .get(sink_name)
            .ok_or_else(|| anyhow::anyhow!("sink '{}' not found in [sinks] section", sink_name))?;

        return resolve_from_sink(sink_config, &query_config);
    }

    // No sink reference, use inline config
    ResolvedQueryConfig::from_config(&query_config)
        .map_err(|e| anyhow::anyhow!("{}", e))
}

/// Resolve query config from a sink configuration
fn resolve_from_sink(sink: &toml::Value, query_config: &QueryConfig) -> Result<ResolvedQueryConfig> {
    let sink_type = sink
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match sink_type {
        "clickhouse" | "clickhouse_native" => {
            // Extract ClickHouse connection details
            let host = sink
                .get("host")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("clickhouse sink missing 'host' field"))?;

            // Build URL from host (add http:// if not present)
            let url = if host.starts_with("http://") || host.starts_with("https://") {
                host.to_string()
            } else {
                format!("http://{}", host)
            };

            let database = sink
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();

            // Credentials: query config overrides sink config (for read/write separation)
            let username = query_config.username.clone().or_else(|| {
                sink.get("username")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            });

            let password = query_config.password.clone().or_else(|| {
                sink.get("password")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            });

            Ok(ResolvedQueryConfig {
                backend: QueryBackendType::ClickHouse,
                url: Some(url),
                database: Some(database),
                username,
                password,
                path: None,
                workspace_id: query_config.workspace_id,
            })
        }
        "arrow_ipc" => {
            // Extract Arrow IPC path for local/Polars backend
            let path = sink
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("arrow_ipc sink missing 'path' field"))?;

            Ok(ResolvedQueryConfig {
                backend: QueryBackendType::Local,
                url: None,
                database: None,
                username: None,
                password: None,
                path: Some(PathBuf::from(path)),
                workspace_id: query_config.workspace_id,
            })
        }
        other => Err(anyhow::anyhow!(
            "sink type '{}' not supported for queries (use clickhouse, clickhouse_native, or arrow_ipc)",
            other
        )),
    }
}

/// Default resolved config when no config file or [query] section is found
fn default_resolved_config() -> ResolvedQueryConfig {
    ResolvedQueryConfig {
        backend: QueryBackendType::Local,
        url: None,
        database: None,
        username: None,
        password: None,
        path: Some(PathBuf::from("arrow/")),
        workspace_id: 1,
    }
}

/// Output query result in the specified format
fn output_result(result: &QueryResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => output_table(result),
        OutputFormat::Json => output_json(result),
        OutputFormat::Csv => output_csv(result),
    }
}

/// Output as ASCII table
fn output_table(result: &QueryResult) -> Result<()> {
    if result.is_empty() {
        println!("(empty result)");
        return Ok(());
    }

    // Calculate column widths
    let mut widths: Vec<usize> = result
        .columns
        .iter()
        .map(|c| c.name.len())
        .collect();

    for row in &result.rows {
        for (i, value) in row.iter().enumerate() {
            let len = format_value(value).len();
            if len > widths[i] {
                widths[i] = len;
            }
        }
    }

    // Cap maximum width
    for w in &mut widths {
        if *w > 50 {
            *w = 50;
        }
    }

    // Print header
    let header: Vec<String> = result
        .columns
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!("{:width$}", c.name, width = *w))
        .collect();
    println!("{}", header.join(" | "));

    // Print separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    // Print rows
    for row in &result.rows {
        let values: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(v, w)| {
                let s = format_value(v);
                if s.len() > *w {
                    format!("{}...", &s[..*w - 3])
                } else {
                    format!("{:width$}", s, width = *w)
                }
            })
            .collect();
        println!("{}", values.join(" | "));
    }

    Ok(())
}

/// Output as JSON array of objects
fn output_json(result: &QueryResult) -> Result<()> {
    let objects: Vec<serde_json::Map<String, serde_json::Value>> = result
        .rows
        .iter()
        .map(|row| {
            result
                .columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.name.clone(), val.clone()))
                .collect()
        })
        .collect();

    let json = serde_json::to_string_pretty(&objects)?;
    println!("{}", json);
    Ok(())
}

/// Output as CSV
fn output_csv(result: &QueryResult) -> Result<()> {
    // Header
    let header: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    println!("{}", header.join(","));

    // Rows
    for row in &result.rows {
        let values: Vec<String> = row.iter().map(|v| csv_escape(v)).collect();
        println!("{}", values.join(","));
    }

    Ok(())
}

/// Format a JSON value for display
fn format_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(|v| format_value(v)).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            serde_json::to_string(obj).unwrap_or_else(|_| "{}".to_string())
        }
    }
}

/// Escape value for CSV output
fn csv_escape(value: &serde_json::Value) -> String {
    let s = match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        other => format_value(other),
    };

    // Quote if contains comma, newline, or quote
    if s.contains(',') || s.contains('\n') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

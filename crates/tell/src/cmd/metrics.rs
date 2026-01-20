//! Metrics command - Query analytics metrics
//!
//! # Usage
//!
//! ```bash
//! # Active users
//! tell metrics dau --range 30d
//! tell metrics wau --range 30d --breakdown device_type
//! tell metrics mau --range 90d
//!
//! # Sessions
//! tell metrics sessions --range 30d
//! tell metrics sessions-unique --range 30d
//!
//! # Stickiness (user engagement ratios)
//! tell metrics stickiness-daily --range 30d   # DAU/MAU
//! tell metrics stickiness-weekly --range 30d  # WAU/MAU
//!
//! # Events
//! tell metrics events --range 7d
//! tell metrics events --range 7d --name page_view
//! tell metrics events-top --range 7d --limit 10
//!
//! # Logs
//! tell metrics logs --range 7d
//! tell metrics logs --range 7d --level error
//! tell metrics logs-top --range 7d --limit 10
//!
//! # Drill-down (raw data)
//! tell metrics drill-down users --range 7d --limit 100
//! tell metrics drill-down events --range 7d --limit 100
//! tell metrics drill-down logs --range 7d --limit 100
//! tell metrics drill-down sessions --range 7d --limit 100
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use tell_analytics::{
    CompareMode, Filter, Granularity, LogVolumeMetric, MetricsEngine, StickinessMetric, TimeRange,
    TimeSeriesData, TopEventsMetric, TopLogsMetric,
};
use tell_query::{QueryEngine, ResolvedQueryConfig};

/// Metrics command arguments
#[derive(Args, Debug)]
pub struct MetricsArgs {
    #[command(subcommand)]
    pub command: MetricsCommand,

    /// Config file path (uses [query] section for backend)
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,

    /// Workspace ID
    #[arg(short, long, default_value = "1", global = true)]
    pub workspace: u64,
}

#[derive(Subcommand, Debug)]
pub enum MetricsCommand {
    /// Daily active users
    Dau(TimeSeriesArgs),

    /// Weekly active users
    Wau(TimeSeriesArgs),

    /// Monthly active users
    Mau(TimeSeriesArgs),

    /// Session volume (total session count)
    Sessions(TimeSeriesArgs),

    /// Unique sessions (distinct session IDs)
    SessionsUnique(TimeSeriesArgs),

    /// Daily stickiness (DAU/MAU ratio)
    StickinessDaily(TimeSeriesArgs),

    /// Weekly stickiness (WAU/MAU ratio)
    StickinessWeekly(TimeSeriesArgs),

    /// Event count over time
    Events(EventsArgs),

    /// Top events by count
    EventsTop(TopArgs),

    /// Log volume over time
    Logs(LogsArgs),

    /// Top logs by count
    LogsTop(TopArgs),

    /// Drill down to raw data
    DrillDown(DrillDownArgs),
}

/// Common time series arguments
#[derive(Args, Debug)]
pub struct TimeSeriesArgs {
    /// Time range (e.g., 7d, 30d, 90d, today, ytd, 2024-01-01,2024-01-31)
    #[arg(short, long, default_value = "30d")]
    pub range: String,

    /// Time granularity (minute, hourly, daily, weekly, monthly)
    #[arg(short, long, default_value = "daily")]
    pub granularity: String,

    /// Breakdown dimension (e.g., device_type, country, os)
    #[arg(short, long)]
    pub breakdown: Option<String>,

    /// Compare to previous period (previous, yoy)
    #[arg(short = 'C', long)]
    pub compare: Option<String>,

    /// Output format (table, json, csv)
    #[arg(short, long, default_value = "table")]
    pub format: String,
}

/// Events command arguments
#[derive(Args, Debug)]
pub struct EventsArgs {
    #[command(flatten)]
    pub common: TimeSeriesArgs,

    /// Filter by event name
    #[arg(short, long)]
    pub name: Option<String>,
}

/// Logs command arguments
#[derive(Args, Debug)]
pub struct LogsArgs {
    #[command(flatten)]
    pub common: TimeSeriesArgs,

    /// Filter by log level (debug, info, warn, error)
    #[arg(short, long)]
    pub level: Option<String>,
}

/// Top N arguments
#[derive(Args, Debug)]
pub struct TopArgs {
    /// Time range
    #[arg(short, long, default_value = "30d")]
    pub range: String,

    /// Number of results
    #[arg(short, long, default_value = "10")]
    pub limit: u32,

    /// Output format (table, json, csv)
    #[arg(short, long, default_value = "table")]
    pub format: String,
}

/// Drill-down arguments
#[derive(Args, Debug)]
pub struct DrillDownArgs {
    /// Data type to drill down into (users, events, logs, sessions)
    pub data_type: String,

    /// Time range
    #[arg(short, long, default_value = "7d")]
    pub range: String,

    /// Number of results
    #[arg(short, long, default_value = "100")]
    pub limit: u32,

    /// Output format (table, json, csv)
    #[arg(short, long, default_value = "table")]
    pub format: String,
}

/// Run the metrics command
pub async fn run(args: MetricsArgs) -> Result<()> {
    // Build query config
    let resolved = build_resolved_config(args.config.as_ref())?;

    // Create query engine
    let query_engine = QueryEngine::from_resolved_config(&resolved)
        .context("failed to create query engine")?;

    // Create metrics engine
    let engine = MetricsEngine::new(Box::new(query_engine));

    // Execute the requested metric
    match args.command {
        MetricsCommand::Dau(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let result = engine.dau_with_comparison(&filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::Wau(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let result = engine.wau_with_comparison(&filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::Mau(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let result = engine.mau_with_comparison(&filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::Sessions(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let metric = tell_analytics::SessionsMetric::volume();
            let result = engine.execute_with_comparison(&metric, &filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::SessionsUnique(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let metric = tell_analytics::SessionsMetric::unique();
            let result = engine.execute_with_comparison(&metric, &filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::StickinessDaily(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let metric = StickinessMetric::daily();
            let result = engine.execute_with_comparison(&metric, &filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::StickinessWeekly(ts_args) => {
            let filter = build_filter(&ts_args)?;
            let metric = StickinessMetric::weekly();
            let result = engine.execute_with_comparison(&metric, &filter, args.workspace).await?;
            output_result(&result, &ts_args.format)?;
        }
        MetricsCommand::Events(events_args) => {
            let filter = build_filter(&events_args.common)?;
            let result = if let Some(name) = &events_args.name {
                let metric = tell_analytics::EventCountMetric::for_event(name);
                engine.execute_with_comparison(&metric, &filter, args.workspace).await?
            } else {
                engine.event_count_with_comparison(&filter, args.workspace).await?
            };
            output_result(&result, &events_args.common.format)?;
        }
        MetricsCommand::EventsTop(top_args) => {
            let range = TimeRange::parse(&top_args.range)?;
            let filter = Filter::new(range).with_limit(top_args.limit);
            let metric = TopEventsMetric::new(top_args.limit);
            let result = engine.execute(&metric, &filter, args.workspace).await?;
            output_result(&result, &top_args.format)?;
        }
        MetricsCommand::Logs(logs_args) => {
            let filter = build_filter(&logs_args.common)?;
            let metric = LogVolumeMetric::new(logs_args.level.clone());
            let result = engine.execute_with_comparison(&metric, &filter, args.workspace).await?;
            output_result(&result, &logs_args.common.format)?;
        }
        MetricsCommand::LogsTop(top_args) => {
            let range = TimeRange::parse(&top_args.range)?;
            let filter = Filter::new(range).with_limit(top_args.limit);
            let metric = TopLogsMetric::by_level(top_args.limit);
            let result = engine.execute(&metric, &filter, args.workspace).await?;
            output_result(&result, &top_args.format)?;
        }
        MetricsCommand::DrillDown(drill_args) => {
            let range = TimeRange::parse(&drill_args.range)?;
            let filter = Filter::new(range);

            let result = match drill_args.data_type.to_lowercase().as_str() {
                "users" => engine.drill_down_users(&filter, args.workspace, drill_args.limit).await?,
                "events" => engine.drill_down_events(&filter, args.workspace, drill_args.limit).await?,
                "logs" => engine.drill_down_logs(&filter, args.workspace, drill_args.limit).await?,
                "sessions" => engine.drill_down_sessions(&filter, args.workspace, drill_args.limit).await?,
                _ => return Err(anyhow::anyhow!(
                    "unknown drill-down type: {}. Use one of: users, events, logs, sessions",
                    drill_args.data_type
                )),
            };

            output_raw_result(&result, &drill_args.format)?;
        }
    }

    eprintln!("\n[{}]", engine.backend_name());
    Ok(())
}

fn build_filter(args: &TimeSeriesArgs) -> Result<Filter> {
    let range = TimeRange::parse(&args.range)
        .map_err(|e| anyhow::anyhow!("invalid time range: {}", e))?;

    let granularity = Granularity::parse(&args.granularity)
        .map_err(|e| anyhow::anyhow!("invalid granularity: {}", e))?;

    let mut filter = Filter::new(range).with_granularity(granularity);

    if let Some(breakdown) = &args.breakdown {
        filter = filter.with_breakdown(breakdown.clone());
    }

    if let Some(compare) = &args.compare {
        let compare_mode = CompareMode::parse(compare)
            .map_err(|e| anyhow::anyhow!("invalid compare mode: {}", e))?;
        filter = filter.with_compare(compare_mode);
    }

    Ok(filter)
}

fn build_resolved_config(config_path: Option<&PathBuf>) -> Result<ResolvedQueryConfig> {
    // Reuse query command's config resolution
    crate::cmd::query::build_resolved_config(config_path)
}

fn output_result(result: &TimeSeriesData, format: &str) -> Result<()> {
    match format {
        "json" => {
            let json = serde_json::to_string_pretty(result)?;
            println!("{}", json);
        }
        "csv" => {
            println!("date,value,dimension");
            for point in &result.points {
                let dim = point.dimension.as_deref().unwrap_or("");
                println!("{},{},{}", point.date, point.value, dim);
            }
        }
        _ => {
            // Table format
            if result.is_empty() {
                println!("(no data)");
                return Ok(());
            }

            // Check if we have dimensions (breakdown)
            let has_dimension = result.points.iter().any(|p| p.dimension.is_some());

            if has_dimension {
                println!("{:<20} {:<20} {:>15}", "Date", "Dimension", "Value");
                println!("{}", "-".repeat(58));
                for point in &result.points {
                    let dim = point.dimension.as_deref().unwrap_or("-");
                    println!("{:<20} {:<20} {:>15.0}", point.date, dim, point.value);
                }
            } else {
                println!("{:<20} {:>15}", "Date", "Value");
                println!("{}", "-".repeat(38));
                for point in &result.points {
                    println!("{:<20} {:>15.0}", point.date, point.value);
                }
            }

            // Summary
            println!("{}", "-".repeat(38));
            println!(
                "Total: {:.0}  Min: {:.0}  Max: {:.0}  Avg: {:.1}",
                result.total, result.min, result.max, result.avg
            );

            // Comparison data
            if let Some(comp) = &result.comparison {
                println!();
                println!("Comparison:");
                println!(
                    "  Previous: {:.0}  Change: {:+.0} ({:+.1}%)",
                    comp.previous_total, comp.change, comp.percent_change
                );
            }
        }
    }

    Ok(())
}

fn output_raw_result(result: &tell_query::QueryResult, format: &str) -> Result<()> {
    match format {
        "json" => {
            let json = serde_json::to_string_pretty(&result)?;
            println!("{}", json);
        }
        "csv" => {
            // Print header
            let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
            println!("{}", headers.join(","));

            // Print rows
            for row in &result.rows {
                let values: Vec<String> = row
                    .iter()
                    .map(|v| v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string()))
                    .collect();
                println!("{}", values.join(","));
            }
        }
        _ => {
            // Table format
            if result.row_count == 0 {
                println!("(no data)");
                return Ok(());
            }

            // Calculate column widths
            let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
            let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

            for row in &result.rows {
                for (i, v) in row.iter().enumerate() {
                    let len = v.as_str().map(|s| s.len()).unwrap_or(10);
                    if i < widths.len() && len > widths[i] {
                        widths[i] = len.min(40); // Cap at 40 chars
                    }
                }
            }

            // Print header
            let header_line: Vec<String> = headers
                .iter()
                .enumerate()
                .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
                .collect();
            println!("{}", header_line.join("  "));
            println!("{}", "-".repeat(widths.iter().sum::<usize>() + widths.len() * 2));

            // Print rows
            for row in &result.rows {
                let row_line: Vec<String> = row
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let s = v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string());
                        let width = widths.get(i).copied().unwrap_or(10);
                        if s.len() > width {
                            format!("{}...", &s[..width.saturating_sub(3)])
                        } else {
                            format!("{:width$}", s, width = width)
                        }
                    })
                    .collect();
                println!("{}", row_line.join("  "));
            }

            // Summary
            println!("{}", "-".repeat(widths.iter().sum::<usize>() + widths.len() * 2));
            println!("{} rows", result.row_count);
        }
    }

    Ok(())
}

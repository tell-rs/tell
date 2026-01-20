//! Read command - Read stored data from disk binary files
//!
//! Outputs in human-readable format (similar to stdout sink).
//!
//! # Usage
//!
//! ```bash
//! tell read file.bin                    # output to stdout
//! tell read file.bin > output.log       # redirect to file
//! tell read input-dir/ output-dir/      # batch convert directory
//! ```

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use clap::Args;
use tell_sinks::disk_binary::{BinaryMessage, BinaryReader};

use tell_protocol::{decode_event_data, decode_log_data, FlatBatch, SchemaType};

/// Read command arguments
#[derive(Args, Debug)]
pub struct ReadArgs {
    /// Input file or directory
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Output directory (required if input is directory)
    #[arg(value_name = "OUTPUT")]
    output: Option<PathBuf>,
}

/// Run the read command
pub async fn run(args: ReadArgs) -> Result<()> {
    if args.input.is_dir() {
        read_directory(&args.input, args.output.as_ref())
    } else {
        read_file(&args.input, None)
    }
}

/// Read a single binary file and output to stdout or file
fn read_file(input: &Path, output: Option<&Path>) -> Result<()> {
    let reader = BinaryReader::open(input)
        .with_context(|| format!("failed to open {}", input.display()))?;

    let mut writer: Box<dyn Write> = match output {
        Some(path) => {
            let file = File::create(path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            Box::new(BufWriter::new(file))
        }
        None => Box::new(io::stdout().lock()),
    };

    for result in reader.messages() {
        let msg = result.with_context(|| format!("failed to read message from {}", input.display()))?;
        let line = format_message(&msg);
        writeln!(writer, "{}", line)?;
    }

    Ok(())
}

/// Read all binary files in a directory and output to another directory
fn read_directory(input: &Path, output: Option<&PathBuf>) -> Result<()> {
    let output = output.ok_or_else(|| {
        anyhow::anyhow!("output directory required when input is directory")
    })?;

    fs::create_dir_all(output)
        .with_context(|| format!("failed to create output directory {}", output.display()))?;

    // Walk directory recursively
    walk_directory(input, input, output)?;

    Ok(())
}

/// Recursively walk directory and convert files
fn walk_directory(base: &Path, current: &Path, output: &Path) -> Result<()> {
    for entry in fs::read_dir(current)
        .with_context(|| format!("failed to read directory {}", current.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            walk_directory(base, &path, output)?;
        } else if is_binary_file(&path) {
            // Compute relative path and create output path
            let rel_path = path.strip_prefix(base).unwrap_or(&path);
            let out_path = output.join(rel_path).with_extension("log");

            // Create parent directories
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }

            eprintln!(
                "Converting: {} -> {}",
                path.display(),
                out_path.display()
            );

            read_file(&path, Some(&out_path))?;
        }
    }

    Ok(())
}

/// Check if file is a binary log file
fn is_binary_file(path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name.ends_with(".bin") || name.ends_with(".bin.lz4")
}

/// Format a binary message for human-readable output
fn format_message(msg: &BinaryMessage) -> String {
    let ts = format_timestamp(msg.metadata.batch_timestamp);
    let ip = msg.metadata.source_ip_string();

    // Try to parse as FlatBatch and decode content
    match FlatBatch::parse(&msg.data) {
        Ok(flat_batch) => {
            let schema_type = flat_batch.schema_type();

            match flat_batch.data() {
                Ok(data) => format_decoded(&ts, &ip, schema_type, data),
                Err(e) => format!("{} {} error: data extraction failed: {}", ts, ip, e),
            }
        }
        Err(e) => {
            // Fallback: show raw data info
            format!(
                "{} {} raw: {} bytes (parse error: {})",
                ts,
                ip,
                msg.data.len(),
                e
            )
        }
    }
}

/// Format decoded message content
fn format_decoded(ts: &str, ip: &str, schema_type: SchemaType, data: &[u8]) -> String {
    match schema_type {
        SchemaType::Log => format_logs(ts, ip, data),
        SchemaType::Event => format_events(ts, ip, data),
        other => format!("{} {} {:?}: {} bytes", ts, ip, other, data.len()),
    }
}

/// Format log entries
fn format_logs(ts: &str, ip: &str, data: &[u8]) -> String {
    match decode_log_data(data) {
        Ok(logs) => {
            let mut lines = Vec::new();
            for log in logs {
                let level = format!("{:7}", log.level.as_str());
                let svc = log.service.unwrap_or("-");
                let host = log.source.unwrap_or("");
                let location = if host.is_empty() {
                    svc.to_string()
                } else {
                    format!("{}@{}", svc, host)
                };
                let payload = format_payload(log.payload);

                lines.push(format!(
                    "{} {} log {} {} {}",
                    ts, ip, level, location, payload
                ));
            }
            lines.join("\n")
        }
        Err(e) => format!("{} {} log: decode error: {}", ts, ip, e),
    }
}

/// Format event entries
fn format_events(ts: &str, ip: &str, data: &[u8]) -> String {
    match decode_event_data(data) {
        Ok(events) => {
            let mut lines = Vec::new();
            for event in events {
                let event_type = format!("{:8}", format!("{:?}", event.event_type).to_lowercase());
                let name = event.event_name.unwrap_or("-");
                let payload = format_payload(event.payload);

                lines.push(format!(
                    "{} {} event {} {} {}",
                    ts, ip, event_type, name, payload
                ));
            }
            lines.join("\n")
        }
        Err(e) => format!("{} {} event: decode error: {}", ts, ip, e),
    }
}

/// Format timestamp as HH:MM:SS.mmm
fn format_timestamp(ts_millis: u64) -> String {
    Utc.timestamp_millis_opt(ts_millis as i64)
        .single()
        .map(|dt| dt.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| format!("{}ms", ts_millis))
}

/// Format payload for display (truncate if too long)
fn format_payload(bytes: &[u8]) -> String {
    // Try to parse as JSON for pretty printing
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
        if let Ok(compact) = serde_json::to_string(&json) {
            if compact.len() > 200 {
                return format!("{}...", &compact[..197]);
            }
            return compact;
        }
    }

    // Try as UTF-8 string
    if let Ok(s) = std::str::from_utf8(bytes) {
        if s.len() > 200 {
            return format!("{}...", &s[..197]);
        }
        return s.to_string();
    }

    // Binary data
    format!("<{} bytes>", bytes.len())
}

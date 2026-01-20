//! Unified benchmark and testing tool for Tell
//!
//! Test throughput, message sizes, and crash resilience for all source types.
//!
//! # Usage
//!
//! ```bash
//! # Run all benchmarks and output matrix (default)
//! tell-bench
//! tell-bench all
//! tell-bench all --events 10000000 --clients 5
//!
//! # TCP FlatBuffer source
//! tell-bench tcp load -c 5 --events 10000000
//! tell-bench tcp benchmark -c 5
//! tell-bench tcp crash
//!
//! # HTTP JSON source (JSONL format)
//! tell-bench http-json load -c 5 --events 1000000
//! tell-bench http-json benchmark -c 5
//!
//! # HTTP FlatBuffer source (binary, zero-copy)
//! tell-bench http-fbs load -c 5 --events 10000000
//! tell-bench http-fbs benchmark -c 5
//!
//! # Syslog TCP source
//! tell-bench syslog-tcp load -c 5 --events 10000000
//! tell-bench syslog-tcp benchmark -c 5
//! tell-bench syslog-tcp crash
//!
//! # Syslog UDP source
//! tell-bench syslog-udp load -c 5 --events 10000000
//! tell-bench syslog-udp benchmark -c 5
//! tell-bench syslog-udp crash
//! ```

mod common;
mod http;
mod matrix;
mod syslog_tcp;
mod syslog_udp;
mod tcp;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "tell-bench")]
#[command(about = "Unified benchmark and testing tool for Tell")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    source: Option<Source>,
}

#[derive(Subcommand)]
enum Source {
    /// Run all benchmarks and output markdown matrix (default)
    All(matrix::MatrixArgs),
    /// Test TCP source (FlatBuffer protocol)
    Tcp {
        #[command(subcommand)]
        mode: tcp::TcpMode,
    },
    /// Test HTTP JSON source (JSONL format)
    #[command(name = "http-json")]
    HttpJson {
        #[command(subcommand)]
        mode: http::HttpMode,
    },
    /// Test HTTP FlatBuffer source (binary, zero-copy)
    #[command(name = "http-fbs")]
    HttpFbs {
        #[command(subcommand)]
        mode: http::HttpMode,
    },
    /// Test Syslog TCP source (line-based plaintext)
    #[command(name = "syslog-tcp")]
    SyslogTcp {
        #[command(subcommand)]
        mode: syslog_tcp::SyslogTcpMode,
    },
    /// Test Syslog UDP source (datagram plaintext)
    #[command(name = "syslog-udp")]
    SyslogUdp {
        #[command(subcommand)]
        mode: syslog_udp::SyslogUdpMode,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.source {
        None => matrix::run(matrix::MatrixArgs::default()).await,
        Some(Source::All(args)) => matrix::run(args).await,
        Some(Source::Tcp { mode }) => tcp::run(mode).await,
        Some(Source::HttpJson { mode }) => http::run_json(mode).await,
        Some(Source::HttpFbs { mode }) => http::run_fbs(mode).await,
        Some(Source::SyslogTcp { mode }) => syslog_tcp::run(mode).await,
        Some(Source::SyslogUdp { mode }) => syslog_udp::run(mode).await,
    }
}

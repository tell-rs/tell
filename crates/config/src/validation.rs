//! Configuration validation
//!
//! Validates config consistency:
//! - Referenced sinks in routing exist
//! - Referenced sources in routing exist
//! - Required fields are present for enabled components
//! - No port conflicts between sources
//! - Warns if enabled sources have no routing coverage
//! - Transformer types are valid

use crate::Config;
use crate::error::{ConfigError, Result};
use crate::sinks::SinkConfig;
use crate::transformers::is_known_transformer_type;
use std::collections::HashMap;

/// Built-in sinks that don't need to be declared in config
const BUILTIN_SINKS: &[&str] = &["stdout", "null"];

/// Validate the entire configuration
pub fn validate_config(config: &Config) -> Result<()> {
    validate_sources(config)?;
    validate_sinks(config)?;
    validate_routing(config)?;
    validate_transformers(config)?;
    Ok(())
}

/// Validate source configurations
fn validate_sources(config: &Config) -> Result<()> {
    // Check for port conflicts
    let mut ports: HashMap<u16, Vec<&str>> = HashMap::new();

    for tcp in &config.sources.tcp {
        if tcp.enabled {
            ports.entry(tcp.port).or_default().push("tcp");
        }
    }

    if let Some(ref debug) = config.sources.tcp_debug
        && debug.enabled
    {
        ports.entry(debug.port).or_default().push("tcp_debug");
    }

    if let Some(ref syslog) = config.sources.syslog_tcp
        && syslog.enabled
    {
        ports.entry(syslog.port).or_default().push("syslog_tcp");
    }

    if let Some(ref syslog) = config.sources.syslog_udp
        && syslog.enabled
    {
        // UDP can share port with TCP, so we track separately
        // But multiple UDP sources on same port is a conflict
        let key = syslog.port;
        if ports.get(&key).is_some_and(|v| v.contains(&"syslog_udp")) {
            return Err(ConfigError::duplicate_port(key, "syslog_udp, syslog_udp"));
        }
    }

    // Check for TCP port conflicts (UDP can coexist with TCP on same port)
    for (port, sources) in &ports {
        let tcp_sources: Vec<_> = sources.iter().filter(|s| *s != &"syslog_udp").collect();

        if tcp_sources.len() > 1 {
            let names = tcp_sources
                .iter()
                .copied()
                .copied()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ConfigError::duplicate_port(*port, names));
        }
    }

    Ok(())
}

/// Validate sink configurations
fn validate_sinks(config: &Config) -> Result<()> {
    for (name, sink) in config.sinks.iter() {
        if !sink.is_enabled() {
            continue;
        }

        match sink {
            SinkConfig::DiskBinary(disk) => {
                if disk.path.is_empty() {
                    return Err(ConfigError::missing_field("sink", name, "path"));
                }
            }
            SinkConfig::DiskPlaintext(disk) => {
                if disk.path.is_empty() {
                    return Err(ConfigError::missing_field("sink", name, "path"));
                }
            }
            SinkConfig::Clickhouse(ch) => {
                if ch.host.is_empty() {
                    return Err(ConfigError::missing_field("sink", name, "host"));
                }
            }
            SinkConfig::Parquet(pq) => {
                if pq.path.is_empty() {
                    return Err(ConfigError::missing_field("sink", name, "path"));
                }
            }
            SinkConfig::Forwarder(fwd) => {
                if fwd.target.is_empty() {
                    return Err(ConfigError::missing_field("sink", name, "target"));
                }
                if fwd.api_key.is_empty() {
                    return Err(ConfigError::missing_field("sink", name, "api_key"));
                }
                // Validate API key format (should be 32 hex chars)
                if fwd.api_key.len() != 32 || !fwd.api_key.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(ConfigError::invalid_value(
                        "sink",
                        name,
                        "api_key",
                        "must be exactly 32 hexadecimal characters",
                    ));
                }
            }
            // Null and Stdout don't have required fields
            SinkConfig::Null(_) | SinkConfig::Stdout(_) => {}
        }
    }

    Ok(())
}

/// Validate routing configuration
fn validate_routing(config: &Config) -> Result<()> {
    // Collect all enabled source names
    let enabled_sources = collect_enabled_sources(config);

    // Check that all referenced sinks exist (built-in sinks don't need declaration)
    for sink_name in &config.routing.default {
        if !BUILTIN_SINKS.contains(&sink_name.as_str()) && !config.sinks.contains(sink_name) {
            return Err(ConfigError::unknown_sink(sink_name));
        }
    }

    for rule in &config.routing.rules {
        // Check that match condition is not empty
        if rule.match_condition.is_empty() {
            return Err(ConfigError::invalid_value(
                "routing",
                "rule",
                "match",
                "at least one match condition (source or source_type) must be specified",
            ));
        }

        // Check that referenced source exists (if matching by source name)
        if let Some(ref source_name) = rule.match_condition.source
            && !enabled_sources.contains(&source_name.as_str())
        {
            return Err(ConfigError::unknown_source(source_name));
        }

        // Check that referenced source_type is valid
        if let Some(ref source_type) = rule.match_condition.source_type {
            let valid_types = ["tcp", "syslog"];
            if !valid_types.contains(&source_type.as_str()) {
                return Err(ConfigError::invalid_value(
                    "routing",
                    "rule",
                    "source_type",
                    format!("must be one of: {}", valid_types.join(", ")),
                ));
            }
        }

        // Check that all sinks in the rule exist (built-in sinks don't need declaration)
        for sink_name in &rule.sinks {
            if !BUILTIN_SINKS.contains(&sink_name.as_str()) && !config.sinks.contains(sink_name) {
                return Err(ConfigError::unknown_sink(sink_name));
            }
        }

        // Check that rule has at least one sink
        if rule.sinks.is_empty() {
            return Err(ConfigError::invalid_value(
                "routing",
                "rule",
                "sinks",
                "at least one sink must be specified",
            ));
        }
    }

    // Note: If no routing is configured, serve.rs defaults to stdout
    // so we don't error here - empty routing is valid

    Ok(())
}

/// Validate transformer configurations in routing rules
fn validate_transformers(config: &Config) -> Result<()> {
    for (rule_idx, rule) in config.routing.rules.iter().enumerate() {
        for (transformer_idx, transformer) in rule.transformers.iter().enumerate() {
            // Skip disabled transformers
            if !transformer.enabled {
                continue;
            }

            let location = format!("rule[{}].transformers[{}]", rule_idx, transformer_idx);

            // Check that transformer type is known
            if !is_known_transformer_type(&transformer.transformer_type) {
                return Err(ConfigError::invalid_value(
                    "routing.rules",
                    location,
                    "type",
                    format!(
                        "unknown transformer type '{}', expected one of: noop, pattern_matcher, reduce",
                        transformer.transformer_type
                    ),
                ));
            }

            // Type-specific validation
            if transformer.transformer_type == "pattern_matcher" {
                // Validate similarity_threshold if present
                if let Some(threshold) = transformer.get_float("similarity_threshold")
                    && !(0.0..=1.0).contains(&threshold)
                {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "similarity_threshold",
                        "must be between 0.0 and 1.0",
                    ));
                }

                // Validate cache_size if present
                if let Some(cache_size) = transformer.get_int("cache_size")
                    && cache_size <= 0
                {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "cache_size",
                        "must be a positive integer",
                    ));
                }

                // Validate max_child_nodes if present
                if let Some(nodes) = transformer.get_int("max_child_nodes")
                    && nodes <= 0
                {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "max_child_nodes",
                        "must be a positive integer",
                    ));
                }
            }

            // Reduce transformer validation
            if transformer.transformer_type == "reduce" {
                // Validate window_ms if present
                if let Some(window) = transformer.get_int("window_ms")
                    && window <= 0
                {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "window_ms",
                        "must be a positive integer",
                    ));
                }

                // Validate max_events if present
                if let Some(max) = transformer.get_int("max_events")
                    && max <= 0
                {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "max_events",
                        "must be a positive integer",
                    ));
                }

                // Validate min_events if present
                if let Some(min) = transformer.get_int("min_events")
                    && min <= 0
                {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "min_events",
                        "must be a positive integer",
                    ));
                }

                // Validate min_events <= max_events
                let min_events = transformer.get_int("min_events").unwrap_or(2);
                let max_events = transformer.get_int("max_events").unwrap_or(1000);
                if min_events > max_events {
                    return Err(ConfigError::invalid_value(
                        "routing.rules",
                        &location,
                        "min_events",
                        format!("cannot exceed max_events ({})", max_events),
                    ));
                }
            }
            // noop doesn't need specific validation
        }
    }

    Ok(())
}

/// Collect all enabled source names
fn collect_enabled_sources(config: &Config) -> Vec<&str> {
    let mut sources = Vec::new();

    for tcp in &config.sources.tcp {
        if tcp.enabled {
            sources.push("tcp");
        }
    }

    if let Some(ref debug) = config.sources.tcp_debug
        && debug.enabled
    {
        sources.push("tcp_debug");
    }

    if let Some(ref syslog) = config.sources.syslog_tcp
        && syslog.enabled
    {
        sources.push("syslog_tcp");
    }

    if let Some(ref syslog) = config.sources.syslog_udp
        && syslog.enabled
    {
        sources.push("syslog_udp");
    }

    sources
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_valid_minimal_config() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_port_conflict() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sources.tcp_debug]
port = 50000
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("50000"));
    }

    #[test]
    fn test_missing_disk_path() {
        let toml = r#"
[sinks.disk_plaintext]
type = "disk_plaintext"
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("path"));
    }

    #[test]
    fn test_missing_clickhouse_host() {
        let toml = r#"
[sinks.clickhouse]
type = "clickhouse"
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("host"));
    }

    #[test]
    fn test_disabled_sink_no_validation() {
        // Disabled sinks don't need required fields
        let toml = r#"
[sinks.clickhouse]
type = "clickhouse"
enabled = false
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_routing_unknown_sink() {
        let toml = r#"
[sinks.stdout]
type = "stdout"

[routing]
default = ["nonexistent"]
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn test_routing_rule_unknown_sink() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout", "missing_sink"]
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("missing_sink"));
    }

    #[test]
    fn test_routing_empty_match() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = {}
sinks = ["stdout"]
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("match"));
    }

    #[test]
    fn test_routing_empty_sinks() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = []
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("sink"));
    }

    #[test]
    fn test_forwarder_invalid_api_key() {
        let toml = r#"
[sinks.forwarder]
type = "forwarder"
target = "collector.example.com:8081"
api_key = "tooshort"
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("api_key"));
        assert!(err.to_string().contains("hexadecimal"));
    }

    #[test]
    fn test_forwarder_valid() {
        let toml = r#"
[sinks.forwarder]
type = "forwarder"
target = "collector.example.com:8081"
api_key = "0123456789abcdef0123456789abcdef"
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_valid_full_config() {
        let toml = r#"
[global]
buffer_size = 524288

[log]
level = "debug"

[metrics]
enabled = true
interval = "5s"

[[sources.tcp]]
port = 50000

[[sources.tcp]]
port = 8081
forwarding_mode = true

[sources.tcp_debug]
port = 50001

[sinks.stdout]
type = "stdout"

[sinks.disk_plaintext]
type = "disk_plaintext"
path = "logs/"

[sinks.clickhouse]
type = "clickhouse"
host = "localhost:9000"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["clickhouse", "disk_plaintext"]

[[routing.rules]]
match = { source = "tcp_debug" }
sinks = ["stdout"]
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_routing_unknown_source() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "typo_tcp" }
sinks = ["stdout"]
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("typo_tcp"));
    }

    #[test]
    fn test_routing_invalid_source_type() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source_type = "invalid_type" }
sinks = ["stdout"]
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("source_type"));
    }

    #[test]
    fn test_source_without_route_defaults_to_stdout() {
        // Empty routing is valid - serve.rs will default to stdout
        let toml = r#"
[[sources.tcp]]
port = 50000

[routing]
# No default, no rules - will default to stdout at runtime
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_source_covered_by_default() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]
# No explicit rule for tcp, but default covers it
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_source_covered_by_source_type() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sources.tcp_debug]
port = 50001

[sinks.stdout]
type = "stdout"

[routing]
# source_type = "tcp" covers both tcp and tcp_debug

[[routing.rules]]
match = { source_type = "tcp" }
sinks = ["stdout"]
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    // ========================================================================
    // Transformer Validation Tests
    // ========================================================================

    #[test]
    fn test_valid_transformer_config() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 0.5
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_unknown_transformer_type() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "unknown_transformer"
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unknown_transformer"));
    }

    #[test]
    fn test_invalid_similarity_threshold_too_high() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 1.5
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("similarity_threshold"));
    }

    #[test]
    fn test_invalid_similarity_threshold_negative() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "pattern_matcher"
similarity_threshold = -0.1
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("similarity_threshold"));
    }

    #[test]
    fn test_invalid_cache_size() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "pattern_matcher"
cache_size = -100
"#;
        let result = Config::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("cache_size"));
    }

    #[test]
    fn test_disabled_transformer_skips_validation() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "unknown_type"
enabled = false
"#;
        // Should pass because the transformer is disabled
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_noop_transformer_valid() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[routing.rules.transformers]]
type = "noop"
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_builtin_sinks_without_declaration() {
        // stdout and null are built-in sinks that don't need explicit declaration
        let toml = r#"
[[sources.tcp]]
port = 50000

[routing]
default = ["stdout"]
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_builtin_null_sink_in_rules() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["null"]
"#;
        let config = Config::from_str(toml).unwrap();
        assert!(validate_config(&config).is_ok());
    }
}

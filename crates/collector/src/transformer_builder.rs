//! Transformer Builder
//!
//! Converts transformer configurations from routing rules into actual
//! transformer chain instances that can be registered with the Router.

use std::collections::HashMap;

use anyhow::{Context, Result};
use cdp_config::{RoutingConfig, TransformerInstanceConfig};
use cdp_protocol::SourceId;
use cdp_transform::{
    Chain, FilterConfig, FilterTransformer, NoopTransformer, PatternConfig, PatternTransformer,
    RedactConfig, RedactTransformer, ReduceConfig, ReduceTransformer,
};
use tracing::{info, warn};

/// Build transformer chains from routing configuration
///
/// Returns a map of SourceId -> Chain for sources that have transformers configured.
/// Each routing rule with transformers will create chains for all sources it matches.
pub fn build_source_transformers(
    routing_config: &RoutingConfig,
) -> Result<HashMap<SourceId, Chain>> {
    let mut source_chains: HashMap<SourceId, Chain> = HashMap::new();

    for rule in &routing_config.rules {
        if rule.transformers.is_empty() {
            continue;
        }

        // Determine which source IDs this rule applies to
        let source_ids = get_source_ids_for_rule(rule);

        for source_id in source_ids {
            if source_chains.contains_key(&source_id) {
                warn!(
                    source = %source_id,
                    "multiple transformer chains configured for same source, using first"
                );
                continue;
            }

            // Build a separate chain for each source (chains contain Box<dyn Transformer>)
            let chain = build_chain_from_configs(&rule.transformers)
                .context("failed to build transformer chain")?;

            if !chain.is_enabled() {
                continue;
            }

            info!(
                source = %source_id,
                transformers = ?chain.names(),
                "configured transformer chain for source"
            );

            source_chains.insert(source_id, chain);
        }
    }

    Ok(source_chains)
}

/// Get the source IDs that a routing rule applies to
fn get_source_ids_for_rule(rule: &cdp_config::RoutingRule) -> Vec<SourceId> {
    let mut source_ids = Vec::new();

    // Exact source match
    if let Some(ref source_name) = rule.match_condition.source {
        source_ids.push(SourceId::new(source_name));
    }

    // Source type match - expand to all known sources of that type
    if let Some(ref source_type) = rule.match_condition.source_type {
        match source_type.as_str() {
            "tcp" => {
                source_ids.push(SourceId::new("tcp_main"));
                source_ids.push(SourceId::new("tcp_debug"));
            }
            "syslog" => {
                source_ids.push(SourceId::new("syslog_tcp"));
                source_ids.push(SourceId::new("syslog_udp"));
            }
            _ => {
                warn!(source_type = %source_type, "unknown source type for transformer mapping");
            }
        }
    }

    source_ids
}

/// Build a transformer chain from a list of transformer configs
fn build_chain_from_configs(configs: &[TransformerInstanceConfig]) -> Result<Chain> {
    let mut transformers: Vec<Box<dyn cdp_transform::Transformer>> = Vec::new();

    for config in configs {
        if !config.enabled {
            continue;
        }

        let transformer: Box<dyn cdp_transform::Transformer> =
            match config.transformer_type.as_str() {
                "noop" => Box::new(NoopTransformer),

                "pattern_matcher" => {
                    let pattern_config = PatternConfig::try_from(config)
                        .map_err(|e| anyhow::anyhow!("invalid pattern_matcher config: {}", e))?;
                    Box::new(
                        PatternTransformer::new(pattern_config)
                            .context("failed to create pattern transformer")?,
                    )
                }

                "reduce" => {
                    let reduce_config = ReduceConfig::try_from(config)
                        .map_err(|e| anyhow::anyhow!("invalid reduce config: {}", e))?;
                    Box::new(
                        ReduceTransformer::new(reduce_config)
                            .context("failed to create reduce transformer")?,
                    )
                }

                "filter" => {
                    let filter_config = FilterConfig::try_from(config)
                        .map_err(|e| anyhow::anyhow!("invalid filter config: {}", e))?;
                    Box::new(
                        FilterTransformer::new(filter_config)
                            .context("failed to create filter transformer")?,
                    )
                }

                "redact" => {
                    let redact_config = RedactConfig::try_from(config)
                        .map_err(|e| anyhow::anyhow!("invalid redact config: {}", e))?;
                    Box::new(
                        RedactTransformer::new(redact_config)
                            .context("failed to create redact transformer")?,
                    )
                }

                other => {
                    anyhow::bail!("unknown transformer type: {}", other);
                }
            };

        transformers.push(transformer);
    }

    Ok(Chain::new(transformers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdp_config::RoutingConfig;

    #[test]
    fn test_build_empty_config() {
        let config = RoutingConfig::default();
        let chains = build_source_transformers(&config).unwrap();
        assert!(chains.is_empty());
    }

    #[test]
    fn test_build_noop_transformer() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]

[[rules.transformers]]
type = "noop"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        assert!(chains.contains_key(&SourceId::new("tcp_main")));
    }

    #[test]
    fn test_build_pattern_matcher() {
        let toml = r#"
[[rules]]
match = { source_type = "syslog" }
sinks = ["stdout"]

[[rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 0.6
cache_size = 50000
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        // Should create chains for both syslog_tcp and syslog_udp
        assert_eq!(chains.len(), 2);
        assert!(chains.contains_key(&SourceId::new("syslog_tcp")));
        assert!(chains.contains_key(&SourceId::new("syslog_udp")));
    }

    #[test]
    fn test_disabled_transformer_skipped() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]

[[rules.transformers]]
type = "noop"
enabled = false
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        // Chain should be empty (disabled transformers filtered out)
        assert!(chains.is_empty());
    }

    #[test]
    fn test_multiple_transformers_in_chain() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]

[[rules.transformers]]
type = "pattern_matcher"

[[rules.transformers]]
type = "noop"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        let chain = chains.get(&SourceId::new("tcp_main")).unwrap();
        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_rules_without_transformers_ignored() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]
# No transformers

[[rules]]
match = { source = "syslog_tcp" }
sinks = ["stdout"]

[[rules.transformers]]
type = "noop"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        // Only syslog_tcp should have a chain
        assert_eq!(chains.len(), 1);
        assert!(!chains.contains_key(&SourceId::new("tcp_main")));
        assert!(chains.contains_key(&SourceId::new("syslog_tcp")));
    }

    #[test]
    fn test_build_reduce_transformer() {
        let toml = r#"
[[rules]]
match = { source = "error_logs" }
sinks = ["clickhouse"]

[[rules.transformers]]
type = "reduce"
group_by = ["error_code", "service"]
window_ms = 10000
max_events = 500
min_events = 5
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        assert!(chains.contains_key(&SourceId::new("error_logs")));
        let chain = chains.get(&SourceId::new("error_logs")).unwrap();
        assert_eq!(chain.names(), vec!["reduce"]);
    }

    #[test]
    fn test_build_reduce_with_defaults() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]

[[rules.transformers]]
type = "reduce"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        let chain = chains.get(&SourceId::new("tcp_main")).unwrap();
        assert_eq!(chain.names(), vec!["reduce"]);
    }

    #[test]
    fn test_build_filter_transformer() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]

[[rules.transformers]]
type = "filter"
action = "drop"

[[rules.transformers.conditions]]
field = "level"
operator = "eq"
value = "debug"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        let chain = chains.get(&SourceId::new("tcp_main")).unwrap();
        assert_eq!(chain.names(), vec!["filter"]);
    }

    #[test]
    fn test_build_redact_transformer() {
        let toml = r#"
[[rules]]
match = { source = "tcp_main" }
sinks = ["stdout"]

[[rules.transformers]]
type = "redact"
strategy = "redact"
patterns = ["email", "phone"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        let chain = chains.get(&SourceId::new("tcp_main")).unwrap();
        assert_eq!(chain.names(), vec!["redact"]);
    }

    #[test]
    fn test_build_redact_with_hash() {
        let toml = r#"
[[rules]]
match = { source = "pii_logs" }
sinks = ["clickhouse"]

[[rules.transformers]]
type = "redact"
strategy = "hash"
hash_key = "workspace-secret-key"
patterns = ["email", "ssn_us", "ipv4"]
scan_all = true
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let chains = build_source_transformers(&config).unwrap();

        assert_eq!(chains.len(), 1);
        let chain = chains.get(&SourceId::new("pii_logs")).unwrap();
        assert_eq!(chain.names(), vec!["redact"]);
    }
}

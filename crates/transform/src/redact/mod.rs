//! Redact Transformer - Remove or pseudonymize PII
//!
//! Detects and redacts PII from events for compliance.
//!
//! # Job To Be Done
//!
//! Remove or pseudonymize PII from events to enable compliant analytics.
//! Use hash strategy to preserve correlation without storing actual PII.
//!
//! # Configuration
//!
//! | Option | Type | Default | Description |
//! |--------|------|---------|-------------|
//! | `strategy` | string | `"redact"` | Redaction strategy: `redact` or `hash` |
//! | `hash_key` | string | - | Secret key for hashing (required if strategy=hash) |
//! | `patterns` | array | - | Built-in patterns to detect (see below) |
//! | `fields` | array | - | Targeted fields with specific patterns |
//! | `custom_patterns` | array | - | Custom regex patterns |
//! | `scan_all` | bool | `false` | Scan all string values (slower but comprehensive) |
//!
//! ## Strategies
//!
//! | Strategy | Output | Use Case |
//! |----------|--------|----------|
//! | `redact` | `[REDACTED]` | Complete removal, no correlation |
//! | `hash` | `usr_abc123xyz` | Deterministic pseudonym, enables correlation |
//!
//! ## Built-in Patterns
//!
//! | Pattern | Description | Hash Prefix |
//! |---------|-------------|-------------|
//! | `email` | Email addresses | `usr_` |
//! | `phone` | Phone numbers (E.164 and common formats) | `phn_` |
//! | `credit_card` | Credit card numbers (13-19 digits) | `cc_` |
//! | `ssn_us` | US Social Security Numbers (XXX-XX-XXXX) | `ssn_` |
//! | `cpr_dk` | Danish CPR numbers (DDMMYY-XXXX) | `cpr_` |
//! | `nino_uk` | UK National Insurance Numbers | `nin_` |
//! | `bsn_nl` | Dutch BSN numbers (9 digits) | `bsn_` |
//! | `ipv4` | IPv4 addresses | `ip4_` |
//! | `ipv6` | IPv6 addresses | `ip6_` |
//! | `passport` | Passport numbers (generic 6-9 alphanum) | `pas_` |
//! | `iban` | IBAN numbers | `iba_` |
//!
//! ## Targeted Fields
//!
//! For better performance, specify exact field paths instead of `scan_all`:
//!
//! | Option | Type | Description |
//! |--------|------|-------------|
//! | `path` | string | JSON field path (dot notation: `user.email`) |
//! | `pattern` | string | Pattern name to match |
//! | `strategy` | string | Override strategy for this field |
//!
//! ## Custom Patterns
//!
//! | Option | Type | Description |
//! |--------|------|-------------|
//! | `name` | string | Pattern identifier |
//! | `regex` | string | Regex pattern to match |
//! | `prefix` | string | Hash prefix (for hash strategy) |
//!
//! # TOML Examples
//!
//! ```toml
//! # Basic redaction - replace with [REDACTED]
//! [[routing.rules.transformers]]
//! type = "redact"
//! strategy = "redact"
//! patterns = ["email", "phone", "ssn_us"]
//! scan_all = true
//! ```
//!
//! ```toml
//! # Pseudonymization - deterministic hashes for correlation
//! [[routing.rules.transformers]]
//! type = "redact"
//! strategy = "hash"
//! hash_key = "workspace-secret-key-min-32-chars"
//! patterns = ["email", "ipv4", "ipv6"]
//! scan_all = true
//! ```
//!
//! ```toml
//! # Targeted fields (faster than scan_all)
//! [[routing.rules.transformers]]
//! type = "redact"
//! strategy = "hash"
//! hash_key = "secret"
//!
//! [[routing.rules.transformers.fields]]
//! path = "user.email"
//! pattern = "email"
//!
//! [[routing.rules.transformers.fields]]
//! path = "client_ip"
//! pattern = "ipv4"
//! strategy = "redact"  # Override: redact IPs, hash emails
//! ```
//!
//! ```toml
//! # Custom pattern for employee IDs
//! [[routing.rules.transformers]]
//! type = "redact"
//! strategy = "hash"
//! hash_key = "secret"
//! scan_all = true
//!
//! [[routing.rules.transformers.custom_patterns]]
//! name = "employee_id"
//! regex = "EMP-\\d{6}"
//! prefix = "emp_"
//! ```
//!
//! # Hash Output Format
//!
//! When using `strategy = "hash"`, values are replaced with deterministic pseudonyms:
//!
//! - Format: `{prefix}{base62_hash}`
//! - Example: `user@example.com` → `usr_7kJ9mNpQ3xYz`
//! - Same input always produces same hash (for correlation)
//! - Different hash_key produces different hashes (workspace isolation)
//!
//! # Rust Example
//!
//! ```ignore
//! let config = RedactConfig::new()
//!     .with_strategy(RedactStrategy::Hash)
//!     .with_hash_key("workspace-secret")
//!     .with_pattern(PatternType::Email)
//!     .with_pattern(PatternType::Phone);
//!
//! let transformer = RedactTransformer::new(config)?;
//! let batch = transformer.transform(batch).await?;
//! ```

mod config;
mod hasher;
mod patterns;

pub use config::{CustomPattern, PatternType, RedactConfig, RedactStrategy, TargetedField};
pub use hasher::PseudonymHasher;
pub use patterns::{find_all_matches, get_pattern, matches_pattern, PATTERNS};

use crate::registry::{TransformerConfig, TransformerFactory};
use crate::{TransformError, TransformResult, Transformer};
use tell_protocol::{Batch, BatchBuilder};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;

/// Metrics for the redact transformer
#[derive(Debug, Default)]
pub struct RedactMetrics {
    /// Batches processed
    pub batches_processed: AtomicU64,
    /// Messages processed
    pub messages_processed: AtomicU64,
    /// Fields redacted
    pub fields_redacted: AtomicU64,
    /// Patterns matched
    pub patterns_matched: AtomicU64,
}

/// Redact transformer
///
/// Detects and redacts PII from events. Stateless.
pub struct RedactTransformer {
    config: RedactConfig,
    hasher: Option<PseudonymHasher>,
    metrics: RedactMetrics,
}

impl RedactTransformer {
    /// Create a new redact transformer
    pub fn new(config: RedactConfig) -> TransformResult<Self> {
        config.validate().map_err(TransformError::config)?;

        let hasher = if config.strategy == RedactStrategy::Hash {
            Some(PseudonymHasher::new(config.hash_key.as_ref().unwrap()))
        } else {
            None
        };

        Ok(Self {
            config,
            hasher,
            metrics: RedactMetrics::default(),
        })
    }

    /// Get transformer metrics
    pub fn metrics(&self) -> &RedactMetrics {
        &self.metrics
    }

    /// Process a batch, redacting PII
    fn process_batch(&self, batch: Batch) -> Batch {
        self.metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

        let msg_count = batch.message_count();
        self.metrics.messages_processed.fetch_add(msg_count as u64, Ordering::Relaxed);

        let mut builder = BatchBuilder::new(batch.batch_type(), batch.source_id().clone());

        for i in 0..msg_count {
            let message = match batch.get_message(i) {
                Some(m) => m,
                None => continue,
            };

            let redacted = self.redact_message(message);
            builder.add_raw(&redacted);
        }

        builder.finish()
    }

    /// Redact PII from a single message
    fn redact_message(&self, message: &[u8]) -> Vec<u8> {
        // Try to parse as JSON
        let mut json: serde_json::Value = match serde_json::from_slice(message) {
            Ok(v) => v,
            Err(_) => {
                // Non-JSON: scan as plain text if scan_all is enabled
                if self.config.scan_all {
                    return self.redact_plain_text(message);
                }
                return message.to_vec();
            }
        };

        // Process targeted fields first (more efficient)
        for field in &self.config.fields {
            self.redact_json_field(&mut json, &field.path, &field.pattern, field.strategy);
        }

        // If scan_all, scan all string values
        if self.config.scan_all {
            self.scan_and_redact_json(&mut json);
        }

        serde_json::to_vec(&json).unwrap_or_else(|_| message.to_vec())
    }

    /// Redact a specific JSON field by path
    fn redact_json_field(
        &self,
        json: &mut serde_json::Value,
        path: &str,
        pattern_name: &str,
        strategy_override: Option<RedactStrategy>,
    ) {
        let strategy = strategy_override.unwrap_or(self.config.strategy);

        // Navigate to the field
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part - this is the field to redact
                if let Some(obj) = current.as_object_mut()
                    && let Some(value) = obj.get_mut(*part)
                {
                    self.redact_value(value, pattern_name, strategy);
                }
            } else {
                // Navigate deeper
                match current {
                    serde_json::Value::Object(map) => {
                        if let Some(next) = map.get_mut(*part) {
                            current = next;
                        } else {
                            return;
                        }
                    }
                    _ => return,
                }
            }
        }
    }

    /// Redact a JSON value based on pattern
    fn redact_value(&self, value: &mut serde_json::Value, pattern_name: &str, strategy: RedactStrategy) {
        if let serde_json::Value::String(s) = value {
            // Check if it matches the expected pattern
            let (matches, prefix) = self.check_pattern_match(s, pattern_name);

            if matches {
                self.metrics.patterns_matched.fetch_add(1, Ordering::Relaxed);
                self.metrics.fields_redacted.fetch_add(1, Ordering::Relaxed);

                *value = serde_json::Value::String(self.apply_redaction(s, &prefix, strategy));
            }
        }
    }

    /// Check if a value matches a pattern, returning (matches, prefix)
    fn check_pattern_match(&self, value: &str, pattern_name: &str) -> (bool, String) {
        // Check built-in patterns
        if let Some(pt) = PatternType::parse(pattern_name)
            && matches_pattern(pt, value)
        {
            return (true, pt.hash_prefix().to_string());
        }

        // Check custom patterns
        for cp in &self.config.custom_patterns {
            if cp.name == pattern_name && cp.regex.is_match(value) {
                return (true, cp.prefix.clone());
            }
        }

        (false, String::new())
    }

    /// Scan all string values in JSON and redact matches
    fn scan_and_redact_json(&self, json: &mut serde_json::Value) {
        match json {
            serde_json::Value::String(s) => {
                let redacted = self.scan_and_redact_string(s);
                if redacted != *s {
                    *s = redacted;
                }
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    self.scan_and_redact_json(item);
                }
            }
            serde_json::Value::Object(map) => {
                for value in map.values_mut() {
                    self.scan_and_redact_json(value);
                }
            }
            _ => {}
        }
    }

    /// Scan a string for patterns and redact all matches
    fn scan_and_redact_string(&self, text: &str) -> String {
        let mut result = text.to_string();

        // Check each configured pattern
        for pattern_type in &self.config.patterns {
            if let Some(re) = get_pattern(*pattern_type) {
                let prefix = pattern_type.hash_prefix();

                // Find and replace all matches
                for m in re.find_iter(text) {
                    let matched = m.as_str();
                    let replacement = self.apply_redaction(matched, prefix, self.config.strategy);

                    self.metrics.patterns_matched.fetch_add(1, Ordering::Relaxed);
                    self.metrics.fields_redacted.fetch_add(1, Ordering::Relaxed);

                    result = result.replace(matched, &replacement);
                }
            }
        }

        // Check custom patterns
        for cp in &self.config.custom_patterns {
            for m in cp.regex.find_iter(text) {
                let matched = m.as_str();
                let replacement = self.apply_redaction(matched, &cp.prefix, self.config.strategy);

                self.metrics.patterns_matched.fetch_add(1, Ordering::Relaxed);
                self.metrics.fields_redacted.fetch_add(1, Ordering::Relaxed);

                result = result.replace(matched, &replacement);
            }
        }

        result
    }

    /// Apply redaction to a matched value
    fn apply_redaction(&self, value: &str, prefix: &str, strategy: RedactStrategy) -> String {
        match strategy {
            RedactStrategy::Redact => "[REDACTED]".to_string(),
            RedactStrategy::Hash => {
                if let Some(ref hasher) = self.hasher {
                    hasher.hash(value, prefix)
                } else {
                    "[REDACTED]".to_string()
                }
            }
        }
    }

    /// Redact plain text (non-JSON)
    fn redact_plain_text(&self, message: &[u8]) -> Vec<u8> {
        let text = match std::str::from_utf8(message) {
            Ok(s) => s,
            Err(_) => return message.to_vec(),
        };

        let redacted = self.scan_and_redact_string(text);
        redacted.into_bytes()
    }
}

impl Transformer for RedactTransformer {
    fn transform<'a>(
        &'a self,
        batch: Batch,
    ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>> {
        Box::pin(async move { Ok(self.process_batch(batch)) })
    }

    fn name(&self) -> &'static str {
        "redact"
    }

    fn enabled(&self) -> bool {
        self.config.enabled
    }
}

impl std::fmt::Debug for RedactTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedactTransformer")
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .finish()
    }
}

/// Factory for creating redact transformers
#[derive(Debug, Clone, Copy)]
pub struct RedactFactory;

impl TransformerFactory for RedactFactory {
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        let redact_config = parse_redact_config(config)?;
        Ok(Box::new(RedactTransformer::new(redact_config)?))
    }

    fn name(&self) -> &'static str {
        "redact"
    }

    fn default_config(&self) -> Option<TransformerConfig> {
        let mut config = HashMap::new();
        config.insert("strategy".to_string(), toml::Value::String("redact".to_string()));
        Some(config)
    }
}

/// Parse RedactConfig from TransformerConfig map
fn parse_redact_config(config: &TransformerConfig) -> TransformResult<RedactConfig> {
    let mut instance = tell_config::TransformerInstanceConfig::noop();
    instance.transformer_type = "redact".to_string();

    for (k, v) in config {
        instance.options.insert(k.clone(), v.clone());
    }

    if let Some(toml::Value::Boolean(false)) = config.get("enabled") {
        instance.enabled = false;
    }

    RedactConfig::try_from(&instance).map_err(TransformError::config)
}

//! Redact transformer configuration

use cdp_config::TransformerInstanceConfig;

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;

/// Redaction strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RedactStrategy {
    /// Replace with [REDACTED]
    #[default]
    Redact,
    /// Replace with deterministic hash (pseudonymization)
    Hash,
}

/// Built-in pattern types for common PII
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternType {
    /// Email addresses (RFC 5322 simplified)
    Email,
    /// Phone numbers (E.164 + common formats)
    Phone,
    /// Credit card numbers (Luhn-valid 13-19 digits)
    CreditCard,
    /// US Social Security Number (XXX-XX-XXXX)
    SsnUs,
    /// Danish CPR number (DDMMYY-XXXX)
    CprDk,
    /// UK National Insurance Number
    NinoUk,
    /// Dutch BSN (Burgerservicenummer)
    BsnNl,
    /// IPv4 address
    Ipv4,
    /// IPv6 address
    Ipv6,
    /// Passport number (alphanumeric 6-9)
    Passport,
    /// IBAN (International Bank Account Number)
    Iban,
}

impl PatternType {
    /// Get the hash prefix for this pattern type
    pub fn hash_prefix(&self) -> &'static str {
        match self {
            PatternType::Email => "usr_",
            PatternType::Phone => "phn_",
            PatternType::CreditCard => "cc_",
            PatternType::SsnUs => "ssn_",
            PatternType::CprDk => "cpr_",
            PatternType::NinoUk => "nin_",
            PatternType::BsnNl => "bsn_",
            PatternType::Ipv4 => "ip4_",
            PatternType::Ipv6 => "ip6_",
            PatternType::Passport => "pas_",
            PatternType::Iban => "iba_",
        }
    }

    /// Parse pattern type from string
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "email" => Some(PatternType::Email),
            "phone" => Some(PatternType::Phone),
            "credit_card" => Some(PatternType::CreditCard),
            "ssn_us" => Some(PatternType::SsnUs),
            "cpr_dk" => Some(PatternType::CprDk),
            "nino_uk" => Some(PatternType::NinoUk),
            "bsn_nl" => Some(PatternType::BsnNl),
            "ipv4" => Some(PatternType::Ipv4),
            "ipv6" => Some(PatternType::Ipv6),
            "passport" => Some(PatternType::Passport),
            "iban" => Some(PatternType::Iban),
            _ => None,
        }
    }
}

/// A custom pattern definition
#[derive(Debug, Clone)]
pub struct CustomPattern {
    /// Pattern name
    pub name: String,
    /// Regex pattern
    pub regex: regex::Regex,
    /// Hash prefix (for hash strategy)
    pub prefix: String,
}

/// A targeted field to redact
#[derive(Debug, Clone)]
pub struct TargetedField {
    /// JSON path (dot notation)
    pub path: String,
    /// Pattern to match (built-in or custom name)
    pub pattern: String,
    /// Override strategy for this field
    pub strategy: Option<RedactStrategy>,
}

/// Configuration for the redact transformer
#[derive(Debug, Clone)]
pub struct RedactConfig {
    /// Whether the transformer is enabled
    pub enabled: bool,
    /// Default redaction strategy
    pub strategy: RedactStrategy,
    /// HMAC key for hash strategy (required if strategy is Hash)
    pub hash_key: Option<String>,
    /// Built-in patterns to detect (scan mode)
    pub patterns: Vec<PatternType>,
    /// Targeted fields (faster than scan)
    pub fields: Vec<TargetedField>,
    /// Custom patterns
    pub custom_patterns: Vec<CustomPattern>,
    /// Scan all string fields (slower but comprehensive)
    pub scan_all: bool,
}

impl Default for RedactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strategy: RedactStrategy::Redact,
            hash_key: None,
            patterns: Vec::new(),
            fields: Vec::new(),
            custom_patterns: Vec::new(),
            scan_all: false,
        }
    }
}

impl RedactConfig {
    /// Create a new config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the redaction strategy
    pub fn with_strategy(mut self, strategy: RedactStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set the hash key (required for Hash strategy)
    pub fn with_hash_key(mut self, key: impl Into<String>) -> Self {
        self.hash_key = Some(key.into());
        self
    }

    /// Add a built-in pattern
    pub fn with_pattern(mut self, pattern: PatternType) -> Self {
        if !self.patterns.contains(&pattern) {
            self.patterns.push(pattern);
        }
        self
    }

    /// Add multiple built-in patterns
    pub fn with_patterns(mut self, patterns: Vec<PatternType>) -> Self {
        for p in patterns {
            if !self.patterns.contains(&p) {
                self.patterns.push(p);
            }
        }
        self
    }

    /// Add a targeted field
    pub fn with_field(mut self, field: TargetedField) -> Self {
        self.fields.push(field);
        self
    }

    /// Add a custom pattern
    pub fn with_custom_pattern(mut self, pattern: CustomPattern) -> Self {
        self.custom_patterns.push(pattern);
        self
    }

    /// Enable scan all mode
    pub fn with_scan_all(mut self) -> Self {
        self.scan_all = true;
        self
    }

    /// Disable the transformer
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Hash strategy requires a key
        if self.strategy == RedactStrategy::Hash && self.hash_key.is_none() {
            return Err("hash strategy requires hash_key to be set".to_string());
        }

        // Must have at least one pattern or field
        if self.patterns.is_empty() && self.fields.is_empty() && self.custom_patterns.is_empty() {
            return Err("at least one pattern, field, or custom_pattern is required".to_string());
        }

        // Validate custom patterns have valid regex (already compiled, but check prefix)
        for cp in &self.custom_patterns {
            if cp.prefix.is_empty() {
                return Err(format!("custom pattern '{}' requires a prefix", cp.name));
            }
        }

        Ok(())
    }
}

impl TryFrom<&TransformerInstanceConfig> for RedactConfig {
    type Error = String;

    fn try_from(config: &TransformerInstanceConfig) -> Result<Self, Self::Error> {
        let mut redact_config = RedactConfig::default();

        if !config.enabled {
            redact_config.enabled = false;
        }

        // Parse strategy
        if let Some(strategy) = config.get_str("strategy") {
            redact_config.strategy = match strategy {
                "redact" => RedactStrategy::Redact,
                "hash" => RedactStrategy::Hash,
                other => return Err(format!("unknown strategy: {}", other)),
            };
        }

        // Parse hash_key
        if let Some(key) = config.get_str("hash_key") {
            redact_config.hash_key = Some(key.to_string());
        }

        // Parse scan_all
        if let Some(scan) = config.get_bool("scan_all") {
            redact_config.scan_all = scan;
        }

        // Parse patterns array
        if let Some(patterns) = config.get_string_array("patterns") {
            for p in patterns {
                if let Some(pt) = PatternType::parse(&p) {
                    redact_config.patterns.push(pt);
                } else {
                    return Err(format!("unknown pattern type: {}", p));
                }
            }
        }

        // Parse fields array
        if let Some(fields) = config.options.get("fields")
            && let Some(arr) = fields.as_array()
        {
            for field_value in arr {
                let field = parse_targeted_field(field_value)?;
                redact_config.fields.push(field);
            }
        }

        // Parse custom_patterns array
        if let Some(customs) = config.options.get("custom_patterns")
            && let Some(arr) = customs.as_array()
        {
            for cp_value in arr {
                let cp = parse_custom_pattern(cp_value)?;
                redact_config.custom_patterns.push(cp);
            }
        }

        redact_config.validate()?;
        Ok(redact_config)
    }
}

/// Parse a targeted field from TOML
fn parse_targeted_field(value: &toml::Value) -> Result<TargetedField, String> {
    let table = value.as_table().ok_or("field must be a table")?;

    let path = table
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("field requires 'path'")?
        .to_string();

    let pattern = table
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or("field requires 'pattern'")?
        .to_string();

    let strategy = table.get("strategy").and_then(|v| v.as_str()).map(|s| {
        match s {
            "hash" => RedactStrategy::Hash,
            _ => RedactStrategy::Redact,
        }
    });

    Ok(TargetedField { path, pattern, strategy })
}

/// Parse a custom pattern from TOML
fn parse_custom_pattern(value: &toml::Value) -> Result<CustomPattern, String> {
    let table = value.as_table().ok_or("custom_pattern must be a table")?;

    let name = table
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or("custom_pattern requires 'name'")?
        .to_string();

    let regex_str = table
        .get("regex")
        .and_then(|v| v.as_str())
        .ok_or("custom_pattern requires 'regex'")?;

    let regex = regex::Regex::new(regex_str)
        .map_err(|e| format!("invalid regex '{}': {}", regex_str, e))?;

    let prefix = table
        .get("prefix")
        .and_then(|v| v.as_str())
        .unwrap_or("cst_")
        .to_string();

    Ok(CustomPattern { name, regex, prefix })
}

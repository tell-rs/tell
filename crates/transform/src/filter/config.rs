//! Filter transformer configuration

use tell_config::TransformerInstanceConfig;

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;

/// Filter action - what to do when condition matches
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterAction {
    /// Drop events that match the condition
    #[default]
    Drop,
    /// Keep only events that match the condition (drop non-matches)
    Keep,
}

/// How to combine multiple conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MatchMode {
    /// All conditions must match (AND)
    #[default]
    All,
    /// Any condition must match (OR)
    Any,
}

/// Comparison operator for conditions
#[derive(Debug, Clone)]
pub enum Operator {
    /// Equals
    Eq,
    /// Not equals
    Ne,
    /// String contains
    Contains,
    /// String starts with
    StartsWith,
    /// String ends with
    EndsWith,
    /// Regex match
    Regex(regex::Regex),
    /// Field exists
    Exists,
    /// Greater than (numeric)
    Gt,
    /// Less than (numeric)
    Lt,
    /// Greater than or equal (numeric)
    Gte,
    /// Less than or equal (numeric)
    Lte,
}

/// A single filter condition
#[derive(Debug, Clone)]
pub struct Condition {
    /// JSON field path (dot notation, e.g., "user.email")
    pub field: String,
    /// Comparison operator
    pub operator: Operator,
    /// Value to compare against (not used for Exists)
    pub value: Option<String>,
}

impl Condition {
    /// Create a new condition
    pub fn new(field: impl Into<String>, operator: Operator, value: Option<String>) -> Self {
        Self {
            field: field.into(),
            operator,
            value,
        }
    }

    /// Create an equals condition
    pub fn eq(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(field, Operator::Eq, Some(value.into()))
    }

    /// Create a not-equals condition
    pub fn ne(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(field, Operator::Ne, Some(value.into()))
    }

    /// Create a contains condition
    pub fn contains(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(field, Operator::Contains, Some(value.into()))
    }

    /// Create a starts_with condition
    pub fn starts_with(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(field, Operator::StartsWith, Some(value.into()))
    }

    /// Create an exists condition
    pub fn exists(field: impl Into<String>) -> Self {
        Self::new(field, Operator::Exists, None)
    }

    /// Create a regex condition
    pub fn regex(field: impl Into<String>, pattern: &str) -> Result<Self, String> {
        let re = regex::Regex::new(pattern)
            .map_err(|e| format!("invalid regex '{}': {}", pattern, e))?;
        Ok(Self::new(
            field,
            Operator::Regex(re),
            Some(pattern.to_string()),
        ))
    }
}

/// Configuration for the filter transformer
#[derive(Debug, Clone)]
pub struct FilterConfig {
    /// Whether the transformer is enabled
    pub enabled: bool,
    /// Action to take when condition matches
    pub action: FilterAction,
    /// How to combine multiple conditions
    pub match_mode: MatchMode,
    /// Conditions to evaluate
    pub conditions: Vec<Condition>,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            action: FilterAction::Drop,
            match_mode: MatchMode::All,
            conditions: Vec::new(),
        }
    }
}

impl FilterConfig {
    /// Create a new config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the filter action
    pub fn with_action(mut self, action: FilterAction) -> Self {
        self.action = action;
        self
    }

    /// Set the match mode
    pub fn with_match_mode(mut self, mode: MatchMode) -> Self {
        self.match_mode = mode;
        self
    }

    /// Add a condition
    pub fn with_condition(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    /// Add multiple conditions
    pub fn with_conditions(mut self, conditions: Vec<Condition>) -> Self {
        self.conditions.extend(conditions);
        self
    }

    /// Disable the transformer
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.conditions.is_empty() {
            return Err("at least one condition is required".to_string());
        }

        for (i, condition) in self.conditions.iter().enumerate() {
            if condition.field.is_empty() {
                return Err(format!("condition {} has empty field", i));
            }

            // Exists doesn't need a value, others do
            if !matches!(condition.operator, Operator::Exists) && condition.value.is_none() {
                return Err(format!(
                    "condition {} requires a value for operator {:?}",
                    i, condition.operator
                ));
            }
        }

        Ok(())
    }
}

impl TryFrom<&TransformerInstanceConfig> for FilterConfig {
    type Error = String;

    fn try_from(config: &TransformerInstanceConfig) -> Result<Self, Self::Error> {
        let mut filter_config = FilterConfig::default();

        if !config.enabled {
            filter_config.enabled = false;
        }

        // Parse action
        if let Some(action) = config.get_str("action") {
            filter_config.action = match action {
                "drop" => FilterAction::Drop,
                "keep" => FilterAction::Keep,
                other => return Err(format!("unknown action: {}", other)),
            };
        }

        // Parse match mode
        if let Some(mode) = config.get_str("match") {
            filter_config.match_mode = match mode {
                "all" => MatchMode::All,
                "any" => MatchMode::Any,
                other => return Err(format!("unknown match mode: {}", other)),
            };
        }

        // Parse single condition (simple syntax)
        if let Some(field) = config.get_str("field") {
            let operator_str = config.get_str("operator").unwrap_or("eq");
            let value = config.get_str("value").map(|s| s.to_string());

            let operator = parse_operator(operator_str, value.as_deref())?;
            filter_config.conditions.push(Condition {
                field: field.to_string(),
                operator,
                value,
            });
        }

        // Parse conditions array
        if let Some(conditions) = config.options.get("conditions")
            && let Some(arr) = conditions.as_array()
        {
            for cond_value in arr {
                let condition = parse_condition_from_toml(cond_value)?;
                filter_config.conditions.push(condition);
            }
        }

        filter_config.validate()?;
        Ok(filter_config)
    }
}

/// Parse operator from string
fn parse_operator(op: &str, value: Option<&str>) -> Result<Operator, String> {
    match op {
        "eq" => Ok(Operator::Eq),
        "ne" => Ok(Operator::Ne),
        "contains" => Ok(Operator::Contains),
        "starts_with" => Ok(Operator::StartsWith),
        "ends_with" => Ok(Operator::EndsWith),
        "exists" => Ok(Operator::Exists),
        "gt" => Ok(Operator::Gt),
        "lt" => Ok(Operator::Lt),
        "gte" => Ok(Operator::Gte),
        "lte" => Ok(Operator::Lte),
        "regex" => {
            let pattern = value.ok_or("regex operator requires a value")?;
            let re = regex::Regex::new(pattern)
                .map_err(|e| format!("invalid regex '{}': {}", pattern, e))?;
            Ok(Operator::Regex(re))
        }
        other => Err(format!("unknown operator: {}", other)),
    }
}

/// Parse a condition from TOML value
fn parse_condition_from_toml(value: &toml::Value) -> Result<Condition, String> {
    let table = value.as_table().ok_or("condition must be a table")?;

    let field = table
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or("condition requires 'field'")?
        .to_string();

    let operator_str = table
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("eq");

    let value_str = table
        .get("value")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let operator = parse_operator(operator_str, value_str.as_deref())?;

    Ok(Condition {
        field,
        operator,
        value: value_str,
    })
}

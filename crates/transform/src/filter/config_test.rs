//! Tests for filter config

use super::*;

#[test]
fn test_default_config() {
    let config = FilterConfig::default();
    assert!(config.enabled);
    assert_eq!(config.action, FilterAction::Drop);
    assert_eq!(config.match_mode, MatchMode::All);
    assert!(config.conditions.is_empty());
}

#[test]
fn test_config_builder() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Keep)
        .with_match_mode(MatchMode::Any)
        .with_condition(Condition::eq("level", "debug"));

    assert_eq!(config.action, FilterAction::Keep);
    assert_eq!(config.match_mode, MatchMode::Any);
    assert_eq!(config.conditions.len(), 1);
}

#[test]
fn test_condition_builders() {
    let eq = Condition::eq("field", "value");
    assert_eq!(eq.field, "field");
    assert!(matches!(eq.operator, Operator::Eq));
    assert_eq!(eq.value, Some("value".to_string()));

    let ne = Condition::ne("field", "value");
    assert!(matches!(ne.operator, Operator::Ne));

    let contains = Condition::contains("field", "substr");
    assert!(matches!(contains.operator, Operator::Contains));

    let exists = Condition::exists("field");
    assert!(matches!(exists.operator, Operator::Exists));
    assert!(exists.value.is_none());

    let regex = Condition::regex("field", r"^\d+$").unwrap();
    assert!(matches!(regex.operator, Operator::Regex(_)));
}

#[test]
fn test_regex_condition_invalid() {
    let result = Condition::regex("field", r"[invalid");
    assert!(result.is_err());
}

#[test]
fn test_validate_empty_conditions() {
    let config = FilterConfig::default();
    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("at least one condition"));
}

#[test]
fn test_validate_empty_field() {
    let config = FilterConfig::new()
        .with_condition(Condition::eq("", "value"));
    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("empty field"));
}

#[test]
fn test_validate_missing_value() {
    let config = FilterConfig::new()
        .with_condition(Condition {
            field: "level".to_string(),
            operator: Operator::Eq,
            value: None,
        });
    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("requires a value"));
}

#[test]
fn test_validate_exists_no_value_ok() {
    let config = FilterConfig::new()
        .with_condition(Condition::exists("user_id"));
    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_try_from_simple() {
    let toml = r#"
type = "filter"
action = "drop"
field = "level"
operator = "eq"
value = "debug"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert_eq!(config.action, FilterAction::Drop);
    assert_eq!(config.conditions.len(), 1);
    assert_eq!(config.conditions[0].field, "level");
    assert!(matches!(config.conditions[0].operator, Operator::Eq));
    assert_eq!(config.conditions[0].value, Some("debug".to_string()));
}

#[test]
fn test_try_from_keep_action() {
    let toml = r#"
type = "filter"
action = "keep"
field = "level"
operator = "eq"
value = "error"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert_eq!(config.action, FilterAction::Keep);
}

#[test]
fn test_try_from_match_any() {
    let toml = r#"
type = "filter"
match = "any"
field = "level"
value = "debug"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert_eq!(config.match_mode, MatchMode::Any);
}

#[test]
fn test_try_from_multiple_conditions() {
    let toml = r#"
type = "filter"
action = "drop"
match = "all"

[[conditions]]
field = "level"
operator = "eq"
value = "debug"

[[conditions]]
field = "env"
operator = "eq"
value = "production"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert_eq!(config.conditions.len(), 2);
    assert_eq!(config.conditions[0].field, "level");
    assert_eq!(config.conditions[1].field, "env");
}

#[test]
fn test_try_from_regex_operator() {
    let toml = r#"
type = "filter"
field = "path"
operator = "regex"
value = "^/api/v[0-9]+/"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert!(matches!(config.conditions[0].operator, Operator::Regex(_)));
}

#[test]
fn test_try_from_exists_operator() {
    let toml = r#"
type = "filter"
field = "user_id"
operator = "exists"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert!(matches!(config.conditions[0].operator, Operator::Exists));
}

#[test]
fn test_try_from_disabled() {
    let toml = r#"
type = "filter"
enabled = false
field = "level"
value = "debug"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = FilterConfig::try_from(&instance).unwrap();

    assert!(!config.enabled);
}

#[test]
fn test_try_from_invalid_action() {
    let toml = r#"
type = "filter"
action = "invalid"
field = "level"
value = "debug"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let result = FilterConfig::try_from(&instance);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown action"));
}

#[test]
fn test_try_from_invalid_operator() {
    let toml = r#"
type = "filter"
field = "level"
operator = "invalid"
value = "debug"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let result = FilterConfig::try_from(&instance);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown operator"));
}

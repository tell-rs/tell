//! Tests for filter and condition handling

use crate::filter::{CompareMode, Condition, Granularity, Operator};

#[test]
fn test_operator_parse() {
    assert_eq!(Operator::parse("eq").unwrap(), Operator::Eq);
    assert_eq!(Operator::parse("=").unwrap(), Operator::Eq);
    assert_eq!(Operator::parse("==").unwrap(), Operator::Eq);

    assert_eq!(Operator::parse("ne").unwrap(), Operator::Ne);
    assert_eq!(Operator::parse("!=").unwrap(), Operator::Ne);

    assert_eq!(Operator::parse("gt").unwrap(), Operator::Gt);
    assert_eq!(Operator::parse(">").unwrap(), Operator::Gt);

    assert_eq!(Operator::parse("gte").unwrap(), Operator::Gte);
    assert_eq!(Operator::parse(">=").unwrap(), Operator::Gte);

    assert_eq!(Operator::parse("lt").unwrap(), Operator::Lt);
    assert_eq!(Operator::parse("<").unwrap(), Operator::Lt);

    assert_eq!(Operator::parse("contains").unwrap(), Operator::Contains);
    assert_eq!(Operator::parse("like").unwrap(), Operator::Contains);

    assert_eq!(Operator::parse("in").unwrap(), Operator::In);
    assert_eq!(Operator::parse("not_in").unwrap(), Operator::NotIn);

    assert_eq!(Operator::parse("is_set").unwrap(), Operator::IsSet);
    assert_eq!(Operator::parse("isset").unwrap(), Operator::IsSet);

    assert_eq!(Operator::parse("regex").unwrap(), Operator::Regex);
    assert_eq!(Operator::parse("~").unwrap(), Operator::Regex);
}

#[test]
fn test_operator_parse_case_insensitive() {
    assert_eq!(Operator::parse("EQ").unwrap(), Operator::Eq);
    assert_eq!(Operator::parse("Contains").unwrap(), Operator::Contains);
    assert_eq!(Operator::parse("IS_SET").unwrap(), Operator::IsSet);
}

#[test]
fn test_operator_parse_invalid() {
    assert!(Operator::parse("invalid").is_err());
    assert!(Operator::parse("").is_err());
}

#[test]
fn test_granularity_parse() {
    assert_eq!(Granularity::parse("minute").unwrap(), Granularity::Minute);
    assert_eq!(Granularity::parse("min").unwrap(), Granularity::Minute);
    assert_eq!(Granularity::parse("1m").unwrap(), Granularity::Minute);

    assert_eq!(Granularity::parse("hourly").unwrap(), Granularity::Hourly);
    assert_eq!(Granularity::parse("hour").unwrap(), Granularity::Hourly);
    assert_eq!(Granularity::parse("1h").unwrap(), Granularity::Hourly);

    assert_eq!(Granularity::parse("daily").unwrap(), Granularity::Daily);
    assert_eq!(Granularity::parse("day").unwrap(), Granularity::Daily);

    assert_eq!(Granularity::parse("weekly").unwrap(), Granularity::Weekly);
    assert_eq!(Granularity::parse("monthly").unwrap(), Granularity::Monthly);
    assert_eq!(
        Granularity::parse("quarterly").unwrap(),
        Granularity::Quarterly
    );
    assert_eq!(Granularity::parse("yearly").unwrap(), Granularity::Yearly);
}

#[test]
fn test_granularity_parse_invalid() {
    assert!(Granularity::parse("invalid").is_err());
    assert!(Granularity::parse("").is_err());
}

#[test]
fn test_granularity_clickhouse_fn() {
    assert_eq!(Granularity::Minute.clickhouse_fn(), "toStartOfMinute");
    assert_eq!(Granularity::Hourly.clickhouse_fn(), "toStartOfHour");
    assert_eq!(Granularity::Daily.clickhouse_fn(), "toDate");
    assert_eq!(Granularity::Weekly.clickhouse_fn(), "toStartOfWeek");
    assert_eq!(Granularity::Monthly.clickhouse_fn(), "toStartOfMonth");
    assert_eq!(Granularity::Quarterly.clickhouse_fn(), "toStartOfQuarter");
    assert_eq!(Granularity::Yearly.clickhouse_fn(), "toStartOfYear");
}

#[test]
fn test_compare_mode_parse() {
    assert_eq!(
        CompareMode::parse("previous").unwrap(),
        CompareMode::Previous
    );
    assert_eq!(CompareMode::parse("prev").unwrap(), CompareMode::Previous);
    assert_eq!(
        CompareMode::parse("previous_period").unwrap(),
        CompareMode::Previous
    );

    assert_eq!(
        CompareMode::parse("previous_year").unwrap(),
        CompareMode::PreviousYear
    );
    assert_eq!(
        CompareMode::parse("yoy").unwrap(),
        CompareMode::PreviousYear
    );
    assert_eq!(
        CompareMode::parse("year_over_year").unwrap(),
        CompareMode::PreviousYear
    );
}

#[test]
fn test_compare_mode_parse_invalid() {
    assert!(CompareMode::parse("invalid").is_err());
}

#[test]
fn test_condition_eq() {
    let cond = Condition::eq("device_type", "mobile");
    assert_eq!(cond.field, "device_type");
    assert_eq!(cond.operator, Operator::Eq);
}

#[test]
fn test_condition_in() {
    let cond = Condition::is_in("country", vec!["US".to_string(), "UK".to_string()]);
    assert_eq!(cond.field, "country");
    assert_eq!(cond.operator, Operator::In);
}

#[test]
fn test_condition_is_set() {
    let cond = Condition::is_set("user_id");
    assert_eq!(cond.field, "user_id");
    assert_eq!(cond.operator, Operator::IsSet);
}

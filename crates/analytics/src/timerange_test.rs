//! Tests for time range parsing

use chrono::{Duration, Utc};

use crate::timerange::TimeRange;

#[test]
fn test_parse_relative_days() {
    let range = TimeRange::parse("7d").unwrap();
    assert_eq!(range.days(), 7);

    let range = TimeRange::parse("30d").unwrap();
    assert_eq!(range.days(), 30);

    let range = TimeRange::parse("90d").unwrap();
    assert_eq!(range.days(), 90);
}

#[test]
fn test_parse_relative_hours() {
    let range = TimeRange::parse("24h").unwrap();
    // 24h from start of that day to end of today
    assert!(range.duration() >= Duration::hours(23));
}

#[test]
fn test_parse_relative_weeks() {
    let range = TimeRange::parse("2w").unwrap();
    assert_eq!(range.days(), 14);
}

#[test]
fn test_parse_relative_months() {
    let range = TimeRange::parse("3m").unwrap();
    // 3m uses calendar months (89-93 days depending on which months)
    let days = range.days();
    assert!(
        (89..=93).contains(&days),
        "Expected 89-93 days, got {}",
        days
    );
}

#[test]
fn test_parse_6m_shortcut() {
    let range = TimeRange::parse("6m").unwrap();
    // 6m uses calendar months (181-186 days depending on which months)
    let days = range.days();
    assert!(
        (181..=186).contains(&days),
        "Expected 181-186 days, got {}",
        days
    );
}

#[test]
fn test_parse_12m_shortcut() {
    let range = TimeRange::parse("12m").unwrap();
    // 12m uses calendar months (365-366 days)
    let days = range.days();
    assert!(
        (365..=366).contains(&days),
        "Expected 365-366 days, got {}",
        days
    );
}

#[test]
fn test_parse_relative_years() {
    let range = TimeRange::parse("1y").unwrap();
    // Approximate: 365 days
    assert_eq!(range.days(), 365);
}

#[test]
fn test_parse_predefined_today() {
    let range = TimeRange::parse("today").unwrap();
    let now = Utc::now();
    assert_eq!(range.start.date_naive(), now.date_naive());
    assert_eq!(range.end.date_naive(), now.date_naive());
}

#[test]
fn test_parse_predefined_yesterday() {
    let range = TimeRange::parse("yesterday").unwrap();
    let yesterday = Utc::now() - Duration::days(1);
    assert_eq!(range.start.date_naive(), yesterday.date_naive());
    assert_eq!(range.end.date_naive(), yesterday.date_naive());
}

#[test]
fn test_parse_predefined_case_insensitive() {
    assert!(TimeRange::parse("TODAY").is_ok());
    assert!(TimeRange::parse("Today").is_ok());
    assert!(TimeRange::parse("7D").is_ok());
}

#[test]
fn test_parse_custom_range() {
    let range = TimeRange::parse("2024-01-01,2024-01-31").unwrap();
    assert_eq!(range.start.format("%Y-%m-%d").to_string(), "2024-01-01");
    assert_eq!(range.end.format("%Y-%m-%d").to_string(), "2024-01-31");
    // Jan 1-31 = 31 calendar days (inclusive)
    assert_eq!(range.days(), 31);
}

#[test]
fn test_parse_custom_range_with_spaces() {
    let range = TimeRange::parse("  2024-01-01 , 2024-01-31  ").unwrap();
    assert_eq!(range.days(), 31);
}

#[test]
fn test_parse_invalid() {
    assert!(TimeRange::parse("invalid").is_err());
    assert!(TimeRange::parse("").is_err());
    assert!(TimeRange::parse("0d").is_err());
    assert!(TimeRange::parse("-7d").is_err());
}

#[test]
fn test_previous_period() {
    let range = TimeRange::parse("7d").unwrap();
    let prev = range.previous_period();

    // Previous period should be same duration
    assert_eq!(prev.days(), range.days());
    // Previous period should end before current starts
    assert!(prev.end < range.start);
}

#[test]
fn test_previous_year() {
    let range = TimeRange::parse("2024-06-01,2024-06-30").unwrap();
    let prev_year = range.previous_year();

    assert_eq!(prev_year.start.format("%Y-%m-%d").to_string(), "2023-06-01");
    assert_eq!(prev_year.end.format("%Y-%m-%d").to_string(), "2023-06-30");
}

#[test]
fn test_new_validates_order() {
    let now = Utc::now();
    let yesterday = now - Duration::days(1);

    // Valid: start before end
    assert!(TimeRange::new(yesterday, now).is_ok());

    // Invalid: end before start
    assert!(TimeRange::new(now, yesterday).is_err());
}

#[test]
fn test_mtd_starts_at_first_of_month() {
    let range = TimeRange::parse("mtd").unwrap();
    assert_eq!(range.start.day(), 1);
}

use chrono::Datelike;

#[test]
fn test_ytd_starts_at_jan_1() {
    let range = TimeRange::parse("ytd").unwrap();
    assert_eq!(range.start.month(), 1);
    assert_eq!(range.start.day(), 1);
}

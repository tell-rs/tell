//! Time range parsing and calculations
//!
//! Supports predefined ranges (7d, 30d, ytd) and custom date ranges.
//! Calculates comparison periods (previous period, year-over-year).

use chrono::{DateTime, Duration, NaiveDate, Timelike, Utc};

use crate::error::{AnalyticsError, Result};

/// A time range for analytics queries
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeRange {
    /// Start of the range (inclusive)
    pub start: DateTime<Utc>,
    /// End of the range (inclusive)
    pub end: DateTime<Utc>,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Self> {
        if end < start {
            return Err(AnalyticsError::InvalidTimeRange(
                "end must be after start".to_string(),
            ));
        }
        Ok(Self { start, end })
    }

    /// Parse a time range string
    ///
    /// Supported formats:
    /// - Relative: `1h`, `24h`, `7d`, `30d`, `90d`, `1y`
    /// - Predefined: `today`, `yesterday`, `wtd`, `mtd`, `qtd`, `ytd`
    /// - Custom: `2024-01-01,2024-01-31`
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim().to_lowercase();
        let now = Utc::now();

        // Try predefined ranges first
        if let Some(range) = Self::parse_predefined(&s, now) {
            return Ok(range);
        }

        // Try relative ranges (7d, 30d, etc.)
        if let Some(range) = Self::parse_relative(&s, now)? {
            return Ok(range);
        }

        // Try custom date range
        if let Some(range) = Self::parse_custom(&s)? {
            return Ok(range);
        }

        Err(AnalyticsError::InvalidTimeRange(format!(
            "unknown time range format: {}",
            s
        )))
    }

    /// Get the previous period of the same duration
    pub fn previous_period(&self) -> Self {
        let duration = self.end - self.start;
        Self {
            start: self.start - duration - Duration::seconds(1),
            end: self.start - Duration::seconds(1),
        }
    }

    /// Get the same period from the previous year
    pub fn previous_year(&self) -> Self {
        Self {
            start: shift_year(self.start, -1),
            end: shift_year(self.end, -1),
        }
    }

    /// Get the duration of this range
    pub fn duration(&self) -> Duration {
        self.end - self.start
    }

    /// Get the number of calendar days in this range (inclusive)
    ///
    /// For example, Jan 1 to Jan 7 returns 7 (both endpoints included).
    pub fn days(&self) -> i64 {
        // Add 1 because both start and end dates are inclusive
        self.duration().num_days() + 1
    }
}

impl TimeRange {
    fn parse_predefined(s: &str, now: DateTime<Utc>) -> Option<Self> {
        let today_start = start_of_day(now);
        let today_end = end_of_day(now);

        match s {
            "today" => Some(Self {
                start: today_start,
                end: today_end,
            }),
            "yesterday" => Some(Self {
                start: today_start - Duration::days(1),
                end: today_end - Duration::days(1),
            }),
            "wtd" => Some(Self {
                start: start_of_week(now),
                end: today_end,
            }),
            "mtd" => Some(Self {
                start: start_of_month(now),
                end: today_end,
            }),
            "qtd" => Some(Self {
                start: start_of_quarter(now),
                end: today_end,
            }),
            "ytd" => Some(Self {
                start: start_of_year(now),
                end: today_end,
            }),
            // Month shortcuts (calendar months, not 30-day approximations)
            "3m" => Some(Self {
                start: start_of_day(shift_months(now, -3)),
                end: today_end,
            }),
            "6m" => Some(Self {
                start: start_of_day(shift_months(now, -6)),
                end: today_end,
            }),
            "12m" => Some(Self {
                start: start_of_day(shift_months(now, -12)),
                end: today_end,
            }),
            _ => None,
        }
    }

    fn parse_relative(s: &str, now: DateTime<Utc>) -> Result<Option<Self>> {
        let today_end = end_of_day(now);

        // Match patterns like 1h, 24h, 7d, 30d, 3m, 1y
        let (num, unit) = match extract_num_unit(s) {
            Some(v) => v,
            None => return Ok(None),
        };

        let duration = match unit {
            'h' => Duration::hours(num),
            'd' => Duration::days(num - 1), // 7d means today + 6 previous days = 7 days total
            'w' => Duration::weeks(num) - Duration::days(1),
            'm' => Duration::days(num * 30 - 1),
            'y' => Duration::days(num * 365 - 1),
            _ => return Ok(None),
        };

        let start = start_of_day(now - duration);
        Ok(Some(Self {
            start,
            end: today_end,
        }))
    }

    fn parse_custom(s: &str) -> Result<Option<Self>> {
        // Format: 2024-01-01,2024-01-31
        if !s.contains(',') {
            return Ok(None);
        }

        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return Ok(None);
        }

        let start_date = parse_date(parts[0].trim())?;
        let end_date = parse_date(parts[1].trim())?;

        let start = start_of_day_naive(start_date);
        let end = end_of_day_naive(end_date);

        Self::new(start, end).map(Some)
    }
}

// Helper functions for date calculations

fn extract_num_unit(s: &str) -> Option<(i64, char)> {
    if s.is_empty() {
        return None;
    }

    let unit = s.chars().last()?;
    if !unit.is_ascii_alphabetic() {
        return None;
    }

    let num_str = &s[..s.len() - 1];
    let num: i64 = num_str.parse().ok()?;

    if num <= 0 {
        return None;
    }

    Some((num, unit))
}

fn parse_date(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
        AnalyticsError::InvalidTimeRange(format!("invalid date format: {} (use YYYY-MM-DD)", s))
    })
}

fn start_of_day(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

fn end_of_day(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.date_naive()
        .and_hms_opt(23, 59, 59)
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

fn start_of_day_naive(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(0, 0, 0)
        .map(|t| t.and_utc())
        .unwrap_or_else(Utc::now)
}

fn end_of_day_naive(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(23, 59, 59)
        .map(|t| t.and_utc())
        .unwrap_or_else(Utc::now)
}

fn start_of_week(dt: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::Datelike;
    let days_from_monday = dt.weekday().num_days_from_monday();
    start_of_day(dt - Duration::days(days_from_monday as i64))
}

fn start_of_month(dt: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::Datelike;
    dt.date_naive()
        .with_day(1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

fn start_of_quarter(dt: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::Datelike;
    let quarter_start_month = ((dt.month() - 1) / 3) * 3 + 1;
    dt.date_naive()
        .with_month(quarter_start_month)
        .and_then(|d| d.with_day(1))
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

fn start_of_year(dt: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::Datelike;
    dt.date_naive()
        .with_month(1)
        .and_then(|d| d.with_day(1))
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

fn shift_year(dt: DateTime<Utc>, years: i32) -> DateTime<Utc> {
    use chrono::Datelike;
    dt.date_naive()
        .with_year(dt.year() + years)
        .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), dt.second()))
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

/// Shift a date by a number of months (positive or negative)
///
/// Handles month/year boundaries correctly. If the target day doesn't exist
/// (e.g., Jan 31 - 1 month = Dec 31, not Feb 31), uses the last day of the month.
fn shift_months(dt: DateTime<Utc>, months: i32) -> DateTime<Utc> {
    use chrono::Datelike;

    let total_months = dt.month() as i32 + months;
    let year_delta = if total_months <= 0 {
        (total_months - 12) / 12
    } else {
        (total_months - 1) / 12
    };

    let new_year = dt.year() + year_delta;
    let new_month = ((total_months - 1).rem_euclid(12) + 1) as u32;

    // Try to keep the same day, but clamp to last day of month if needed
    let day = dt.day();
    dt.date_naive()
        .with_year(new_year)
        .and_then(|d| d.with_month(new_month))
        .and_then(|d| d.with_day(day).or_else(|| last_day_of_month(d)))
        .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), dt.second()))
        .map(|t| t.and_utc())
        .unwrap_or(dt)
}

/// Get the last day of the month for a given date
fn last_day_of_month(date: NaiveDate) -> Option<NaiveDate> {
    use chrono::Datelike;
    let (year, month) = (date.year(), date.month());
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    };
    next_month.map(|d| d - Duration::days(1))
}

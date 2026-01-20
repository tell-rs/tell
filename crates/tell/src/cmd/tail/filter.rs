//! Content filters for decoded messages (client-side filtering)
//!
//! These filters run on the CLI after decoding FlatBuffers.
//! They allow filtering by event name, log level, and substring patterns.

use tell_protocol::{DecodedEvent, DecodedLogEntry, LogLevel};
use std::collections::HashSet;

/// Content filter for decoded messages
#[derive(Debug, Default)]
pub struct ContentFilter {
    /// Filter by event names (glob-style matching)
    event_names: Option<Vec<Pattern>>,
    /// Filter by log levels (match any in set)
    log_levels: Option<HashSet<LogLevel>>,
    /// Filter by substring on any string field
    substring: Option<String>,
}

/// Simple glob-style pattern (supports * and ?)
#[derive(Debug, Clone)]
pub struct Pattern {
    pattern: String,
}

impl Pattern {
    /// Create a new pattern from a glob string
    pub fn new(glob: &str) -> Self {
        Self {
            pattern: glob.to_string(),
        }
    }

    /// Check if a string matches this pattern
    #[inline]
    pub fn matches(&self, s: &str) -> bool {
        glob_match(&self.pattern, s)
    }
}

/// Simple glob matching (supports `*` and `?`)
///
/// - `*` matches any sequence of characters (including empty)
/// - `?` matches exactly one character
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();

    let mut pi = 0; // pattern index
    let mut ti = 0; // text index
    let mut star_pi = None; // pattern index after last *
    let mut star_ti = None; // text index when * was matched

    while ti < t.len() || pi < p.len() {
        if pi < p.len() {
            match p[pi] {
                '*' => {
                    // Skip consecutive stars
                    while pi < p.len() && p[pi] == '*' {
                        pi += 1;
                    }
                    // If * is at end of pattern, match everything
                    if pi == p.len() {
                        return true;
                    }
                    // Save position for backtracking
                    star_pi = Some(pi);
                    star_ti = Some(ti);
                    continue;
                }
                '?' if ti < t.len() => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                c if ti < t.len() && c == t[ti] => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }

        // Mismatch - try backtracking
        if let (Some(spi), Some(sti)) = (star_pi, star_ti)
            && sti < t.len()
        {
            pi = spi;
            star_ti = Some(sti + 1);
            ti = sti + 1;
            continue;
        }

        return false;
    }

    true
}

impl ContentFilter {
    /// Create a new empty filter (matches everything)
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if filter needs decoding (has any content filters set)
    pub fn needs_decode(&self) -> bool {
        self.event_names.is_some() || self.log_levels.is_some() || self.substring.is_some()
    }

    /// Add event name filter (glob pattern)
    pub fn with_event_names(mut self, patterns: Vec<&str>) -> Self {
        if patterns.is_empty() {
            return self;
        }

        let compiled: Vec<Pattern> = patterns.iter().map(|p| Pattern::new(p)).collect();
        self.event_names = Some(compiled);
        self
    }

    /// Add log level filter
    pub fn with_log_levels(mut self, levels: Vec<LogLevel>) -> Self {
        if levels.is_empty() {
            return self;
        }
        self.log_levels = Some(levels.into_iter().collect());
        self
    }

    /// Add substring filter (matches any string field)
    pub fn with_substring(mut self, text: &str) -> Self {
        if text.is_empty() {
            return self;
        }
        self.substring = Some(text.to_string());
        self
    }

    /// Check if an event matches the filter
    pub fn matches_event(&self, event: &DecodedEvent<'_>) -> bool {
        // Check event name filter
        if let Some(ref patterns) = self.event_names {
            let name = event.event_name.unwrap_or("");
            if !patterns.iter().any(|p| p.matches(name)) {
                return false;
            }
        }

        // Check substring on string fields
        if let Some(text) = &self.substring {
            let name_matches = event.event_name.is_some_and(|n| n.contains(text.as_str()));
            let payload_matches = !event.payload.is_empty()
                && std::str::from_utf8(event.payload).is_ok_and(|s| s.contains(text.as_str()));

            if !name_matches && !payload_matches {
                return false;
            }
        }

        true
    }

    /// Check if a log entry matches the filter
    pub fn matches_log(&self, log: &DecodedLogEntry<'_>) -> bool {
        // Check log level filter
        if let Some(ref levels) = self.log_levels
            && !levels.contains(&log.level)
        {
            return false;
        }

        // Check substring on string fields
        if let Some(text) = &self.substring {
            let source_matches = log.source.is_some_and(|s| s.contains(text.as_str()));
            let service_matches = log.service.is_some_and(|s| s.contains(text.as_str()));
            let payload_matches = !log.payload.is_empty()
                && std::str::from_utf8(log.payload).is_ok_and(|s| s.contains(text.as_str()));

            if !source_matches && !service_matches && !payload_matches {
                return false;
            }
        }

        true
    }
}


#[cfg(test)]
#[path = "filter_test.rs"]
mod filter_test;

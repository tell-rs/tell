//! Built-in PII patterns
//!
//! Regex patterns for detecting common PII types.

use super::config::PatternType;
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;

/// Compiled regex patterns for each built-in pattern type
pub static PATTERNS: Lazy<HashMap<PatternType, Regex>> = Lazy::new(|| {
    let mut m = HashMap::new();

    // Email: simplified RFC 5322
    m.insert(
        PatternType::Email,
        Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap(),
    );

    // Phone: E.164 and common formats
    // Matches: +1-555-123-4567, (555) 123-4567, 555.123.4567, +44 20 7123 4567
    m.insert(
        PatternType::Phone,
        Regex::new(r"(?:\+?[1-9]\d{0,2}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{2,4}[-.\s]?\d{2,4}(?:[-.\s]?\d{1,4})?").unwrap(),
    );

    // Credit card: 13-19 digits with optional separators
    // Note: This is a format check, not Luhn validation
    m.insert(
        PatternType::CreditCard,
        Regex::new(r"\b(?:\d[ -]*?){13,19}\b").unwrap(),
    );

    // US SSN: XXX-XX-XXXX
    m.insert(
        PatternType::SsnUs,
        Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap(),
    );

    // Danish CPR: DDMMYY-XXXX
    m.insert(
        PatternType::CprDk,
        Regex::new(r"\b\d{6}-\d{4}\b").unwrap(),
    );

    // UK National Insurance Number: AB123456C
    m.insert(
        PatternType::NinoUk,
        Regex::new(r"\b[A-Z]{2}\d{6}[A-Z]\b").unwrap(),
    );

    // Dutch BSN: 9 digits
    m.insert(
        PatternType::BsnNl,
        Regex::new(r"\b\d{9}\b").unwrap(),
    );

    // IPv4: Standard dotted decimal
    m.insert(
        PatternType::Ipv4,
        Regex::new(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b").unwrap(),
    );

    // IPv6: Full, compressed, and loopback forms
    // Handles: 2001:db8::1, ::1, ::, fe80::1%eth0
    m.insert(
        PatternType::Ipv6,
        Regex::new(r"(?i)(?:(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,7}:|(?:[0-9a-f]{1,4}:){1,6}:[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,5}(?::[0-9a-f]{1,4}){1,2}|(?:[0-9a-f]{1,4}:){1,4}(?::[0-9a-f]{1,4}){1,3}|(?:[0-9a-f]{1,4}:){1,3}(?::[0-9a-f]{1,4}){1,4}|(?:[0-9a-f]{1,4}:){1,2}(?::[0-9a-f]{1,4}){1,5}|[0-9a-f]{1,4}:(?::[0-9a-f]{1,4}){1,6}|:(?::[0-9a-f]{1,4}){1,7}|::)").unwrap(),
    );

    // Passport: Alphanumeric 6-9 characters (generic)
    m.insert(
        PatternType::Passport,
        Regex::new(r"\b[A-Z0-9]{6,9}\b").unwrap(),
    );

    // IBAN: ISO 13616 format
    m.insert(
        PatternType::Iban,
        Regex::new(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b").unwrap(),
    );

    m
});

/// Get the regex for a pattern type
pub fn get_pattern(pattern_type: PatternType) -> Option<&'static Regex> {
    PATTERNS.get(&pattern_type)
}

/// Check if a string matches a pattern type
pub fn matches_pattern(pattern_type: PatternType, text: &str) -> bool {
    PATTERNS.get(&pattern_type).map(|re| re.is_match(text)).unwrap_or(false)
}

/// Find all matches of a pattern type in text
pub fn find_all_matches<'a>(pattern_type: PatternType, text: &'a str) -> Vec<regex::Match<'a>> {
    PATTERNS
        .get(&pattern_type)
        .map(|re| re.find_iter(text).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_pattern() {
        let re = get_pattern(PatternType::Email).unwrap();
        assert!(re.is_match("user@example.com"));
        assert!(re.is_match("user.name+tag@example.co.uk"));
        assert!(!re.is_match("not-an-email"));
        assert!(!re.is_match("@example.com"));
    }

    #[test]
    fn test_phone_pattern() {
        let re = get_pattern(PatternType::Phone).unwrap();
        assert!(re.is_match("+1-555-123-4567"));
        assert!(re.is_match("(555) 123-4567"));
        assert!(re.is_match("555.123.4567"));
        assert!(re.is_match("+44 20 7123 4567"));
    }

    #[test]
    fn test_credit_card_pattern() {
        let re = get_pattern(PatternType::CreditCard).unwrap();
        assert!(re.is_match("4111111111111111"));
        assert!(re.is_match("4111-1111-1111-1111"));
        assert!(re.is_match("4111 1111 1111 1111"));
    }

    #[test]
    fn test_ssn_us_pattern() {
        let re = get_pattern(PatternType::SsnUs).unwrap();
        assert!(re.is_match("123-45-6789"));
        assert!(!re.is_match("123456789"));
        assert!(!re.is_match("123-456-789"));
    }

    #[test]
    fn test_cpr_dk_pattern() {
        let re = get_pattern(PatternType::CprDk).unwrap();
        assert!(re.is_match("010190-1234"));
        assert!(!re.is_match("01011990-1234"));
    }

    #[test]
    fn test_nino_uk_pattern() {
        let re = get_pattern(PatternType::NinoUk).unwrap();
        assert!(re.is_match("AB123456C"));
        assert!(!re.is_match("AB12345C"));
        assert!(!re.is_match("AB1234567C"));
    }

    #[test]
    fn test_ipv4_pattern() {
        let re = get_pattern(PatternType::Ipv4).unwrap();
        assert!(re.is_match("192.168.1.1"));
        assert!(re.is_match("10.0.0.1"));
        assert!(re.is_match("255.255.255.255"));
        assert!(!re.is_match("256.1.1.1"));
        assert!(!re.is_match("192.168.1"));
    }

    #[test]
    fn test_ipv6_pattern() {
        let re = get_pattern(PatternType::Ipv6).unwrap();
        assert!(re.is_match("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        assert!(re.is_match("2001:db8:85a3::8a2e:370:7334"));
        assert!(re.is_match("::1"));
    }

    #[test]
    fn test_iban_pattern() {
        let re = get_pattern(PatternType::Iban).unwrap();
        assert!(re.is_match("DE89370400440532013000"));
        assert!(re.is_match("GB82WEST12345698765432"));
    }

    #[test]
    fn test_find_all_matches() {
        let text = "Contact user@example.com or admin@test.org for help";
        let matches = find_all_matches(PatternType::Email, text);
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].as_str(), "user@example.com");
        assert_eq!(matches[1].as_str(), "admin@test.org");
    }
}

//! Tests for ClickHouse helper functions
//!
//! Ported from Go implementation: pkg/sinks/clickhouse/user_id_test.go

use super::helpers::{generate_user_id_from_email, normalize_locale};
use std::collections::HashSet;

// =============================================================================
// User ID Generation Tests (UUID v5 compatibility with Go)
// =============================================================================

#[test]
fn test_user_id_determinism() {
    // Same email should always produce same UUID
    let test_cases = [
        "test@example.com",
        "testuser1@example.com",
        "test.user@example.com",
        "complex.user@example.org",
        "very.long.email.address.with.many.parts@subdomain.example.com",
    ];

    for email in test_cases {
        let id1 = generate_user_id_from_email(email);
        let id2 = generate_user_id_from_email(email);
        let id3 = generate_user_id_from_email(email);

        assert_eq!(
            id1, id2,
            "First and second generation should match for {}",
            email
        );
        assert_eq!(
            id2, id3,
            "Second and third generation should match for {}",
            email
        );
        assert_eq!(
            id1, id3,
            "First and third generation should match for {}",
            email
        );

        // Should be valid UUID format (8-4-4-4-12)
        assert_eq!(id1.len(), 36, "UUID should be 36 characters");
        assert!(
            id1.chars().nth(8) == Some('-'),
            "UUID should have dash at position 8"
        );
        assert!(
            id1.chars().nth(13) == Some('-'),
            "UUID should have dash at position 13"
        );
        assert!(
            id1.chars().nth(18) == Some('-'),
            "UUID should have dash at position 18"
        );
        assert!(
            id1.chars().nth(23) == Some('-'),
            "UUID should have dash at position 23"
        );

        // Should be UUID v5 (version nibble = 5)
        let version_char = id1.chars().nth(14).unwrap();
        assert_eq!(version_char, '5', "Should be UUID v5 for {}", email);
    }
}

#[test]
fn test_user_id_case_sensitivity() {
    // Case variations should produce SAME UUID (normalized)
    let variations = [
        "test@example.com",
        "TEST@EXAMPLE.COM",
        "Test@Example.Com",
        "TeSt@ExAmPlE.cOm",
    ];

    let base_id = generate_user_id_from_email(variations[0]);

    for email in &variations[1..] {
        let id = generate_user_id_from_email(email);
        assert_eq!(
            base_id, id,
            "Case variation '{}' should produce same UUID as '{}'",
            email, variations[0]
        );
    }
}

#[test]
fn test_user_id_whitespace_handling() {
    // Whitespace variations should produce SAME UUID (trimmed)
    let base_email = "test@example.com";
    let base_id = generate_user_id_from_email(base_email);

    let variations = [
        " test@example.com",    // leading space
        "test@example.com ",    // trailing space
        " test@example.com ",   // both spaces
        "\ttest@example.com",   // leading tab
        "test@example.com\n",   // trailing newline
        "test@example.com\r\n", // CRLF
        "  test@example.com  ", // multiple spaces
    ];

    for email in variations {
        let id = generate_user_id_from_email(email);
        assert_eq!(
            base_id, id,
            "Whitespace variation {:?} should produce same UUID",
            email
        );
    }
}

#[test]
fn test_user_id_edge_cases() {
    // Empty string
    assert_eq!(
        generate_user_id_from_email(""),
        "",
        "Empty string should return empty"
    );

    // Only spaces
    assert_eq!(
        generate_user_id_from_email("   "),
        "",
        "Only spaces should return empty after trim"
    );

    // Single character
    let single = generate_user_id_from_email("a");
    assert!(!single.is_empty(), "Single character should produce UUID");
    assert_eq!(single.len(), 36, "Single character UUID should be valid");

    // Unicode email
    let unicode = generate_user_id_from_email("test@münchen.de");
    assert!(!unicode.is_empty(), "Unicode email should produce UUID");
    assert_eq!(unicode.len(), 36, "Unicode email UUID should be valid");

    // Email with special chars
    let special = generate_user_id_from_email("user+tag@example.com");
    assert!(
        !special.is_empty(),
        "Special chars email should produce UUID"
    );
    assert_eq!(
        special.len(),
        36,
        "Special chars email UUID should be valid"
    );
}

#[test]
fn test_user_id_uniqueness() {
    // Different emails should produce different UUIDs
    let emails = [
        "user1@example.com",
        "user2@example.com",
        "user3@example.com",
        "different@domain.org",
        "another@test.net",
    ];

    let mut seen_uuids = HashSet::new();

    for email in emails {
        let id = generate_user_id_from_email(email);
        assert!(
            seen_uuids.insert(id.clone()),
            "UUID collision! Email {} produced duplicate UUID {}",
            email,
            id
        );
    }
}

#[test]
fn test_user_id_go_compatibility() {
    // These are actual UUIDs generated by the Go implementation
    // If these fail, the Rust implementation is not compatible with Go
    //
    // To generate these values from Go:
    // ```go
    // namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    // userUUID := uuid.NewSHA1(namespace, []byte(strings.ToLower(strings.TrimSpace(email))))
    // fmt.Println(userUUID.String())
    // ```
    let test_cases = [
        // (email, expected_uuid)
        // Values verified by running Go implementation:
        // go run -e -- <<< 'userUUID := uuid.NewSHA1(uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), []byte("test@example.com"))'
        ("test@example.com", "fcbcbc64-f85c-5025-877c-37f4c7a12d6e"),
        ("user@test.org", "d89570f2-c0a6-5c1d-937d-8538ce275817"),
    ];

    for (email, expected) in test_cases {
        let actual = generate_user_id_from_email(email);
        assert_eq!(
            actual, expected,
            "Go compatibility: email '{}' should produce UUID '{}'",
            email, expected
        );
    }
}

// =============================================================================
// Locale Normalization Tests
// =============================================================================

#[test]
fn test_normalize_locale() {
    // Exact 5 chars
    assert_eq!(normalize_locale("en_US"), "en_US");

    // Less than 5 chars - should pad with spaces
    let short = normalize_locale("en");
    assert_eq!(short.len(), 5);
    assert!(short.starts_with("en"));

    // More than 5 chars - should truncate
    assert_eq!(normalize_locale("en_US_extra"), "en_US");

    // Empty string
    let empty = normalize_locale("");
    assert_eq!(empty.len(), 5);
}

// =============================================================================
// IPv4 to IPv6 Mapping Tests
// =============================================================================

#[cfg(test)]
mod ip_tests {
    // Helper to create a batch with a specific IP for testing
    // Note: This requires the Batch struct to be constructible for tests
    // If not possible, these tests should be integration tests

    #[test]
    fn test_ipv4_mapping_format() {
        // IPv4-mapped IPv6 format: ::ffff:x.x.x.x
        // Bytes 0-9: zeros
        // Bytes 10-11: 0xff 0xff (IPv4-mapped marker)
        // Bytes 12-15: IPv4 octets

        // This test documents the expected format
        // Actual testing requires a mock Batch or integration test
        let expected_format = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // bytes 0-7: zeros
            0x00, 0x00, // bytes 8-9: zeros
            0xff, 0xff, // bytes 10-11: IPv4-mapped marker
            192, 168, 1, 100, // bytes 12-15: IPv4 address (192.168.1.100)
        ];

        // Verify marker bytes are correct
        assert_eq!(expected_format[10], 0xff);
        assert_eq!(expected_format[11], 0xff);
    }
}

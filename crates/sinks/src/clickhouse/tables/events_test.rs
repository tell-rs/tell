//! Tests for event table row types

use super::events::{ContextRow, EventRow};

#[test]
fn test_event_row_creation() {
    let row = EventRow {
        timestamp: 1700000000000,
        event_name: "page_view".to_string(),
        device_id: [0x01; 16],
        session_id: [0x02; 16],
        properties: r#"{"page": "/home"}"#.to_string(),
        raw: r#"{"page": "/home"}"#.to_string(),
        source_ip: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
    };

    assert_eq!(row.timestamp, 1700000000000);
    assert_eq!(row.event_name, "page_view");
    assert_eq!(row.device_id, [0x01; 16]);
}

#[test]
fn test_context_row_creation() {
    let row = ContextRow {
        timestamp: 1700000000000,
        device_id: [0x01; 16],
        session_id: [0x02; 16],
        device_type: "mobile".to_string(),
        device_model: "iPhone 14 Pro".to_string(),
        operating_system: "iOS".to_string(),
        os_version: "16.4".to_string(),
        app_version: "2.1.0".to_string(),
        app_build: "1234".to_string(),
        timezone: "America/New_York".to_string(),
        locale: "en_US".to_string(),
        country: "US".to_string(),
        region: "NY".to_string(),
        city: "New York".to_string(),
        properties: "{}".to_string(),
        source_ip: [0; 16],
    };

    assert_eq!(row.device_type, "mobile");
    assert_eq!(row.operating_system, "iOS");
    assert_eq!(row.locale, "en_US");
}

#[test]
fn test_ipv4_mapped_ipv6() {
    // IPv4-mapped IPv6 for 192.168.1.1
    let ip: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1];

    let row = EventRow {
        timestamp: 0,
        event_name: String::new(),
        device_id: [0; 16],
        session_id: [0; 16],
        properties: String::new(),
        raw: String::new(),
        source_ip: ip,
    };

    assert_eq!(row.source_ip[10], 0xff);
    assert_eq!(row.source_ip[11], 0xff);
    assert_eq!(row.source_ip[12], 192);
    assert_eq!(row.source_ip[13], 168);
    assert_eq!(row.source_ip[14], 1);
    assert_eq!(row.source_ip[15], 1);
}

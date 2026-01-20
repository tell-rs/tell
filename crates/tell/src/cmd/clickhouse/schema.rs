//! ClickHouse schema definitions
//!
//! Embedded SQL templates for Tell schema v1.3.
//! Workspace name is substituted at runtime.

/// Schema version for migrations
pub const SCHEMA_VERSION: &str = "1.3";

/// All table names in creation order
pub const TABLE_NAMES: &[&str] = &[
    "events_v1",
    "context_v1",
    "users_v1",
    "user_devices",
    "user_traits",
    "logs_v1",
    "pattern_registry",
    "pattern_groups",
    "snapshots_v1",
];

/// Generate CREATE DATABASE statement
pub fn create_database(workspace: &str) -> String {
    format!("CREATE DATABASE IF NOT EXISTS {workspace}")
}

/// Generate DROP DATABASE statement
pub fn drop_database(workspace: &str) -> String {
    format!("DROP DATABASE IF EXISTS {workspace}")
}

/// Generate all CREATE TABLE statements
pub fn create_tables(workspace: &str) -> Vec<String> {
    vec![
        create_events_table(workspace),
        create_context_table(workspace),
        create_users_table(workspace),
        create_user_devices_table(workspace),
        create_user_traits_table(workspace),
        create_logs_table(workspace),
        create_pattern_registry_table(workspace),
        create_pattern_groups_table(workspace),
        create_snapshots_table(workspace),
    ]
}

/// Generate all CREATE INDEX statements
pub fn create_indexes(workspace: &str) -> Vec<String> {
    vec![
        // Events indexes
        format!(
            "ALTER TABLE {workspace}.events_v1 ADD INDEX IF NOT EXISTS idx_device_id device_id TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
        format!(
            "ALTER TABLE {workspace}.events_v1 ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
        // Context indexes
        format!(
            "ALTER TABLE {workspace}.context_v1 ADD INDEX IF NOT EXISTS idx_device_id device_id TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
        // Users indexes
        format!(
            "ALTER TABLE {workspace}.users_v1 ADD INDEX IF NOT EXISTS idx_email email TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
        // User devices indexes
        format!(
            "ALTER TABLE {workspace}.user_devices ADD INDEX IF NOT EXISTS idx_device_id device_id TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
        // User traits indexes
        format!(
            "ALTER TABLE {workspace}.user_traits ADD INDEX IF NOT EXISTS idx_trait_key trait_key TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
    ]
}

/// Generate CREATE USER statements
pub fn create_users(
    workspace: &str,
    collector_password: &str,
    dashboard_password: &str,
) -> Vec<String> {
    vec![
        format!(
            "CREATE USER IF NOT EXISTS {workspace}_collector IDENTIFIED BY '{collector_password}'"
        ),
        format!(
            "CREATE USER IF NOT EXISTS {workspace}_dashboard IDENTIFIED BY '{dashboard_password}'"
        ),
    ]
}

/// Generate DROP USER statements
pub fn drop_users(workspace: &str) -> Vec<String> {
    vec![
        format!("DROP USER IF EXISTS {workspace}_collector"),
        format!("DROP USER IF EXISTS {workspace}_dashboard"),
    ]
}

/// Generate GRANT statements for collector user
pub fn grant_collector(workspace: &str) -> Vec<String> {
    let tables_insert = [
        "events_v1",
        "context_v1",
        "users_v1",
        "user_devices",
        "user_traits",
        "logs_v1",
        "pattern_registry",
        "pattern_groups",
        "snapshots_v1",
    ];

    let tables_select = ["pattern_registry", "pattern_groups"];

    let mut grants: Vec<String> = tables_insert
        .iter()
        .map(|t| format!("GRANT INSERT ON {workspace}.{t} TO {workspace}_collector"))
        .collect();

    grants.extend(
        tables_select
            .iter()
            .map(|t| format!("GRANT SELECT ON {workspace}.{t} TO {workspace}_collector")),
    );

    grants
}

/// Generate GRANT statements for dashboard user
pub fn grant_dashboard(workspace: &str) -> Vec<String> {
    TABLE_NAMES
        .iter()
        .map(|t| format!("GRANT SELECT ON {workspace}.{t} TO {workspace}_dashboard"))
        .collect()
}

// =============================================================================
// Table definitions
// =============================================================================

fn create_events_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.events_v1 (
    timestamp DateTime64(3),
    event_name LowCardinality(String),
    device_id UUID,
    session_id UUID,
    source_ip FixedString(16),
    properties JSON,
    _raw String CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_name, device_id)
TTL timestamp + INTERVAL 5 YEAR
COMMENT 'Core behavioral events - all user actions and system events'"#
    )
}

fn create_context_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.context_v1 (
    timestamp DateTime64(3),
    device_id UUID,
    session_id UUID,
    source_ip FixedString(16),
    device_type LowCardinality(String),
    device_model LowCardinality(String),
    operating_system LowCardinality(String),
    os_version String,
    app_version String,
    app_build String,
    timezone String,
    locale FixedString(5),
    country LowCardinality(String),
    region String,
    city String,
    properties JSON
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_type, device_model, operating_system, app_version)
TTL timestamp + INTERVAL 5 YEAR
COMMENT 'Device and session context - sent once per session via CONTEXT events'"#
    )
}

fn create_users_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.users_v1 (
    user_id String,
    email String,
    name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(updated_at)
ORDER BY user_id
COMMENT 'Core user identity - basic profile data updated via IDENTIFY events'"#
    )
}

fn create_user_devices_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.user_devices (
    user_id String,
    device_id UUID,
    linked_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(linked_at)
ORDER BY (user_id, device_id, linked_at)
COMMENT 'User to device relationship mapping - enables cross-device identity resolution'"#
    )
}

fn create_user_traits_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.user_traits (
    user_id String,
    trait_key String,
    trait_value String,
    updated_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(updated_at)
ORDER BY (user_id, trait_key, updated_at)
COMMENT 'User traits and attributes - flexible key-value store updated via IDENTIFY events'"#
    )
}

fn create_logs_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.logs_v1 (
    timestamp DateTime64(3),
    level Enum8('EMERGENCY'=0, 'ALERT'=1, 'CRITICAL'=2, 'ERROR'=3, 'WARNING'=4, 'NOTICE'=5, 'INFO'=6, 'DEBUG'=7, 'TRACE'=8),
    source LowCardinality(String),
    service LowCardinality(String),
    session_id UUID,
    source_ip FixedString(16),
    pattern_id Nullable(UInt64),
    message JSON,
    _raw String CODEC(ZSTD(3)),
    INDEX pattern_idx pattern_id TYPE minmax GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, service, level)
TTL timestamp + INTERVAL 2 YEAR
COMMENT 'Application and system logs - syslog-compatible with structured data and pattern extraction'"#
    )
}

fn create_pattern_registry_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.pattern_registry (
    pattern_id UInt64,
    service String,
    template String CODEC(ZSTD),
    canonical_name String CODEC(ZSTD),
    category LowCardinality(String),
    tags Array(String) DEFAULT [],
    embedding Array(Float32) CODEC(ZSTD),
    first_seen Nullable(DateTime),
    last_seen Nullable(DateTime),
    occurrence_count Nullable(UInt64),
    last_updated DateTime DEFAULT now(),
    metadata String DEFAULT '{{}}' CODEC(ZSTD),
    PRIMARY KEY pattern_id
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY pattern_id
COMMENT 'Log pattern registry - templates are immutable, metadata is mutable'"#
    )
}

fn create_pattern_groups_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.pattern_groups (
    group_id UInt32,
    group_name String,
    pattern_ids Array(UInt64),
    embedding Array(Float32),
    metadata String DEFAULT '{{}}',
    PRIMARY KEY group_id
) ENGINE = MergeTree()
ORDER BY group_id
COMMENT 'Semantic groups for organizing related log patterns'"#
    )
}

fn create_snapshots_table(workspace: &str) -> String {
    format!(
        r#"CREATE TABLE IF NOT EXISTS {workspace}.snapshots_v1 (
    timestamp DateTime64(3),
    connector LowCardinality(String),
    entity String,
    metrics JSON
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, connector, entity)
TTL timestamp + INTERVAL 5 YEAR
COMMENT 'Connector snapshots - periodic metrics from external sources (GitHub, Shopify, etc.)'"#
    )
}

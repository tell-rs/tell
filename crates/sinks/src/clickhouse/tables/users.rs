//! User table row types (users_v1, user_devices, user_traits)

use clickhouse::Row;
use serde::Serialize;

use super::fixed_bytes_16;

/// User row for IDENTIFY events (users_v1 table)
///
/// ```sql
/// CREATE TABLE users_v1 (
///     user_id String,
///     email String,
///     name String,
///     updated_at DateTime64(3)
/// ) ENGINE = ReplacingMergeTree(updated_at)
/// ORDER BY user_id;
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct UserRow {
    /// User ID (generated from email via UUID v5)
    pub user_id: String,

    /// User email address
    pub email: String,

    /// User display name
    pub name: String,

    /// Last update timestamp in milliseconds
    pub updated_at: i64,
}

/// User device relationship (user_devices table)
///
/// ```sql
/// CREATE TABLE user_devices (
///     user_id String,
///     device_id UUID,
///     linked_at DateTime64(3)
/// ) ENGINE = ReplacingMergeTree(linked_at)
/// ORDER BY (user_id, device_id);
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct UserDeviceRow {
    /// User ID (from IDENTIFY)
    pub user_id: String,

    /// Device UUID (16 bytes)
    #[serde(with = "fixed_bytes_16")]
    pub device_id: [u8; 16],

    /// When the device was linked to the user
    pub linked_at: i64,
}

/// User trait key-value pair (user_traits table)
///
/// ```sql
/// CREATE TABLE user_traits (
///     user_id String,
///     trait_key String,
///     trait_value String,
///     updated_at DateTime64(3)
/// ) ENGINE = ReplacingMergeTree(updated_at)
/// ORDER BY (user_id, trait_key);
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct UserTraitRow {
    /// User ID (from IDENTIFY)
    pub user_id: String,

    /// Trait key name
    pub trait_key: String,

    /// Trait value as string
    pub trait_value: String,

    /// Last update timestamp
    pub updated_at: i64,
}

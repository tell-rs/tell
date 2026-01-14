//! ClickHouse table row types for CDP v1.1 schema
//!
//! Each module corresponds to a logical table group in ClickHouse.

mod events;
mod logs;
mod snapshots;
mod users;

pub use events::{ContextRow, EventRow};
pub use logs::LogRow;
pub use snapshots::SnapshotRow;
pub use users::{UserDeviceRow, UserRow, UserTraitRow};

/// Serialize [u8; 16] as bytes for ClickHouse FixedString(16) / UUID
pub(crate) mod fixed_bytes_16 {
    use serde::{Serialize, Serializer};

    pub fn serialize<S>(bytes: &[u8; 16], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        bytes.as_slice().serialize(serializer)
    }
}

#[cfg(test)]
#[path = "events_test.rs"]
mod events_test;

#[cfg(test)]
#[path = "users_test.rs"]
mod users_test;

#[cfg(test)]
#[path = "logs_test.rs"]
mod logs_test;

#[cfg(test)]
#[path = "snapshots_test.rs"]
mod snapshots_test;

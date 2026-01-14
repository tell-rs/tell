//! CDP Collector - Authentication
//!
//! API key management with hot reload support.
//!
//! # Overview
//!
//! This crate provides:
//! - `WorkspaceId` - Lightweight identifier for workspaces
//! - `ApiKeyStore` - Thread-safe map of API keys to workspace IDs
//! - `ApiKeyManager` - File-based key loading with hot reload
//!
//! # Zero-Copy Design
//!
//! - API keys are stored as `[u8; 16]` arrays (no heap allocation)
//! - Validation is O(1) HashMap lookup
//! - No allocations in the hot path
//!
//! # File Format
//!
//! API keys are stored in a simple text file:
//! ```text
//! # comments start with #
//! 000102030405060708090a0b0c0d0e0f:1
//! deadbeefdeadbeefdeadbeefdeadbeef:2
//! ```
//!
//! Each line contains:
//! - 32 hex characters (16 bytes) for the API key
//! - Colon separator
//! - Workspace ID (numeric u32, matching Go's `type WorkspaceID uint32`)
//!
//! # Example
//!
//! ```ignore
//! use cdp_auth::{ApiKeyStore, WorkspaceId};
//!
//! // Load keys from file
//! let store = ApiKeyStore::from_file("configs/apikeys.conf")?;
//!
//! // Validate key (hot path - zero allocation)
//! let key: [u8; 16] = [0x00, 0x01, 0x02, ...];
//! if let Some(workspace) = store.validate(&key) {
//!     println!("Valid key for workspace: {}", workspace);
//! }
//! ```

mod error;
mod store;
mod workspace;

#[cfg(test)]
mod store_test;

pub use error::{AuthError, Result};
pub use store::{ApiKey, ApiKeyStore, SharedApiKeyStore};
pub use workspace::WorkspaceId;

/// Length of API key in bytes
pub const API_KEY_LENGTH: usize = 16;

/// Length of API key in hex characters
pub const API_KEY_HEX_LENGTH: usize = API_KEY_LENGTH * 2;

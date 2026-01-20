//! Tell - Authentication
//!
//! Authentication, authorization, and API key management.
//!
//! # Overview
//!
//! Simple RBAC with 4 roles and 3 permissions:
//!
//! | Role | Capabilities |
//! |------|--------------|
//! | `Viewer` | View analytics and shared content |
//! | `Editor` | Create/edit own content |
//! | `Admin` | Manage workspace |
//! | `Platform` | Cross-workspace ops |
//!
//! # Two Auth Systems
//!
//! ## Streaming API Keys (Collector)
//!
//! High-performance hex keys for data ingestion:
//! ```text
//! 000102030405060708090a0b0c0d0e0f:1
//! ```
//! - O(1) lookup, zero allocation
//! - Maps directly to workspace ID
//!
//! ## HTTP API Tokens (API)
//!
//! JWT tokens for dashboard/API:
//! ```text
//! tell_eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
//! ```
//! - Contains user ID, workspace ID, role
//! - API keys inherit creator's role

mod claims;
mod error;
pub mod password;
mod provider;
mod roles;
mod store;
mod user;
mod user_store;
mod workspace;

/// Test utilities for generating JWT tokens
pub mod test_utils;

#[cfg(test)]
mod store_test;

// Streaming API key types
pub use error::{AuthError, Result};
pub use store::{ApiKey, ApiKeyStore, SharedApiKeyStore};
pub use workspace::WorkspaceId;

// RBAC types
pub use claims::{TOKEN_PREFIX, TokenClaims, extract_jwt, is_api_token_format};
pub use roles::{Permission, Role};
pub use user::UserInfo;

// Auth providers
pub use provider::{AuthProvider, LocalJwtProvider};

// Local user store
pub use user_store::{LocalUserStore, StoredUser};

/// Length of streaming API key in bytes
pub const API_KEY_LENGTH: usize = 16;

/// Length of streaming API key in hex characters
pub const API_KEY_HEX_LENGTH: usize = API_KEY_LENGTH * 2;

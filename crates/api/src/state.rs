//! Application state
//!
//! Shared state for API handlers including the metrics engine and control plane.

use std::sync::Arc;
use std::time::Duration;

use tell_analytics::MetricsEngine;
use tell_auth::{AuthProvider, LocalJwtProvider, LocalUserStore};
use tell_control::ControlPlane;

use crate::auth::HasAuthProvider;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    /// Metrics engine for analytics queries
    pub metrics: Arc<MetricsEngine>,
    /// Authentication provider
    pub auth: Arc<dyn AuthProvider>,
    /// Control plane database (workspaces, boards, etc.)
    pub control: Option<Arc<ControlPlane>>,
    /// Local user store (only for local auth)
    pub user_store: Option<Arc<LocalUserStore>>,
    /// JWT secret for token generation (only for local auth)
    pub jwt_secret: Option<Vec<u8>>,
    /// JWT expiration time
    pub jwt_expires_in: Duration,
}

impl AppState {
    /// Create new application state with local auth and generated secret
    ///
    /// This is a convenience constructor for CLI use. Creates a local auth
    /// provider with a random secret. No user store is created - this mode
    /// allows analytics queries without authentication.
    pub fn new(metrics: MetricsEngine) -> Self {
        // Generate a random secret for local development
        let secret: [u8; 32] = rand::random();
        let auth = Arc::new(LocalJwtProvider::new(&secret));

        Self {
            metrics: Arc::new(metrics),
            auth,
            control: None,
            user_store: None,
            jwt_secret: Some(secret.to_vec()),
            jwt_expires_in: Duration::from_secs(24 * 60 * 60),
        }
    }

    /// Create new application state with local auth
    pub fn with_local_auth(
        metrics: MetricsEngine,
        auth: Arc<dyn AuthProvider>,
        user_store: LocalUserStore,
        jwt_secret: &[u8],
        jwt_expires_in: Duration,
    ) -> Self {
        Self {
            metrics: Arc::new(metrics),
            auth,
            control: None,
            user_store: Some(Arc::new(user_store)),
            jwt_secret: Some(jwt_secret.to_vec()),
            jwt_expires_in,
        }
    }

    /// Create new application state with external auth (WorkOS)
    pub fn with_external_auth(metrics: MetricsEngine, auth: Arc<dyn AuthProvider>) -> Self {
        Self {
            metrics: Arc::new(metrics),
            auth,
            control: None,
            user_store: None,
            jwt_secret: None,
            jwt_expires_in: Duration::from_secs(24 * 60 * 60),
        }
    }

    /// Set the control plane database
    pub fn with_control(mut self, control: ControlPlane) -> Self {
        self.control = Some(Arc::new(control));
        self
    }
}

impl HasAuthProvider for AppState {
    fn auth_provider(&self) -> Arc<dyn AuthProvider> {
        Arc::clone(&self.auth)
    }
}

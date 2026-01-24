//! Application state
//!
//! Shared state for API handlers including the metrics engine and control plane.

use std::sync::Arc;
use std::time::Duration;

use tell_analytics::MetricsEngine;
use tell_auth::{
    AllowAllMembership, AuthError, AuthProvider, AuthService, ControlPlaneUserStore,
    LocalJwtProvider, LocalUserStore, Membership, MembershipProvider, MembershipStatus, Role,
    UserStore,
};
use tell_control::ControlPlane;

use crate::auth::{HasAuthProvider, HasMembershipProvider, HasUserStore};
use crate::routes::ops::ServerMetrics;

// =============================================================================
// ControlPlane MembershipProvider Adapter
// =============================================================================

/// Adapter that implements MembershipProvider using ControlPlane
///
/// Bridges the gap between tell-control's WorkspaceMembership and
/// tell-auth's Membership trait.
#[derive(Clone)]
pub struct ControlPlaneMembershipProvider {
    control: Arc<ControlPlane>,
}

impl ControlPlaneMembershipProvider {
    pub fn new(control: Arc<ControlPlane>) -> Self {
        Self { control }
    }
}

#[async_trait::async_trait]
impl MembershipProvider for ControlPlaneMembershipProvider {
    async fn get_membership(
        &self,
        user_id: &str,
        workspace_id: &str,
    ) -> tell_auth::Result<Option<Membership>> {
        let membership = self
            .control
            .workspaces()
            .get_member(workspace_id, user_id)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        Ok(membership.map(|m| {
            // Convert tell_control::MemberRole to tell_auth::Role
            let role = match m.role {
                tell_control::MemberRole::Viewer => Role::Viewer,
                tell_control::MemberRole::Editor => Role::Editor,
                tell_control::MemberRole::Admin => Role::Admin,
            };

            // Convert tell_control::MemberStatus to tell_auth::MembershipStatus
            let status = match m.status {
                tell_control::MemberStatus::Active => MembershipStatus::Active,
                tell_control::MemberStatus::Invited => MembershipStatus::Invited,
                tell_control::MemberStatus::Suspended => MembershipStatus::Removed,
            };

            Membership {
                user_id: m.user_id,
                workspace_id: m.workspace_id,
                role,
                status,
            }
        }))
    }
}

// =============================================================================
// AppState
// =============================================================================

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    /// Metrics engine for analytics queries
    pub metrics: Arc<MetricsEngine>,
    /// Authentication provider (for token validation in middleware)
    pub auth: Arc<dyn AuthProvider>,
    /// Auth service (login, logout, refresh with session management)
    pub auth_service: Option<Arc<AuthService>>,
    /// Control plane database (workspaces, boards, etc.)
    pub control: Option<Arc<ControlPlane>>,
    /// User store for auth (either LocalUserStore or ControlPlaneUserStore)
    pub user_store: Option<Arc<dyn UserStore>>,
    /// Legacy: local user store reference (for ValidatedWorkspace extractor)
    /// TODO: deprecate in favor of MembershipProvider pattern
    pub local_user_store: Option<Arc<LocalUserStore>>,
    /// JWT secret for token generation (only for local auth)
    pub jwt_secret: Option<Vec<u8>>,
    /// JWT expiration time
    pub jwt_expires_in: Duration,
    /// Server metrics (populated when running `tell serve`)
    pub server_metrics: Option<Arc<ServerMetrics>>,
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
            auth_service: None,
            control: None,
            user_store: None,
            local_user_store: None,
            jwt_secret: Some(secret.to_vec()),
            jwt_expires_in: Duration::from_secs(24 * 60 * 60),
            server_metrics: None,
        }
    }

    /// Create new application state with local auth (SQLite-backed LocalUserStore)
    ///
    /// This is the legacy mode that uses a separate SQLite database for users.
    /// For production, prefer `with_control_plane_auth` which uses the unified
    /// Turso database.
    pub fn with_local_auth(
        metrics: MetricsEngine,
        auth: Arc<dyn AuthProvider>,
        user_store: LocalUserStore,
        jwt_secret: &[u8],
        jwt_expires_in: Duration,
    ) -> Self {
        use tell_auth::AuthServiceConfig;

        let user_store = Arc::new(user_store);

        // Create auth service with session management
        let auth_config = AuthServiceConfig::new(jwt_secret.to_vec()).with_ttl(jwt_expires_in);
        let auth_service = Arc::new(AuthService::new(
            Arc::clone(&user_store) as Arc<dyn UserStore>,
            auth_config,
        ));

        Self {
            metrics: Arc::new(metrics),
            auth,
            auth_service: Some(auth_service),
            control: None,
            user_store: Some(user_store.clone()),
            local_user_store: Some(user_store),
            jwt_secret: Some(jwt_secret.to_vec()),
            jwt_expires_in,
            server_metrics: None,
        }
    }

    /// Create new application state with control plane auth (Turso-backed)
    ///
    /// This is the production mode that uses the unified control plane database
    /// for both user management and workspace/board storage.
    pub fn with_control_plane_auth(
        metrics: MetricsEngine,
        auth: Arc<dyn AuthProvider>,
        control: ControlPlane,
        jwt_secret: &[u8],
        jwt_expires_in: Duration,
    ) -> Self {
        use tell_auth::AuthServiceConfig;

        let control = Arc::new(control);

        // Create ControlPlane-backed user store
        let user_store = Arc::new(ControlPlaneUserStore::new(Arc::clone(&control)));

        // Create auth service with session management
        let auth_config = AuthServiceConfig::new(jwt_secret.to_vec()).with_ttl(jwt_expires_in);
        let auth_service = Arc::new(AuthService::new(
            Arc::clone(&user_store) as Arc<dyn UserStore>,
            auth_config,
        ));

        Self {
            metrics: Arc::new(metrics),
            auth,
            auth_service: Some(auth_service),
            control: Some(control),
            user_store: Some(user_store),
            local_user_store: None, // Not using LocalUserStore
            jwt_secret: Some(jwt_secret.to_vec()),
            jwt_expires_in,
            server_metrics: None,
        }
    }

    /// Create new application state with external auth (WorkOS)
    pub fn with_external_auth(metrics: MetricsEngine, auth: Arc<dyn AuthProvider>) -> Self {
        Self {
            metrics: Arc::new(metrics),
            auth,
            auth_service: None,
            control: None,
            user_store: None,
            local_user_store: None,
            jwt_secret: None,
            jwt_expires_in: Duration::from_secs(24 * 60 * 60),
            server_metrics: None,
        }
    }

    /// Set the control plane database
    pub fn with_control(mut self, control: ControlPlane) -> Self {
        self.control = Some(Arc::new(control));
        self
    }

    /// Set server metrics (for `tell serve` mode)
    pub fn with_server_metrics(mut self, server_metrics: ServerMetrics) -> Self {
        self.server_metrics = Some(Arc::new(server_metrics));
        self
    }
}

impl HasAuthProvider for AppState {
    fn auth_provider(&self) -> Arc<dyn AuthProvider> {
        Arc::clone(&self.auth)
    }
}

impl HasUserStore for AppState {
    fn user_store(&self) -> Option<Arc<LocalUserStore>> {
        // Return local_user_store for legacy ValidatedWorkspace extractor
        // New code should use HasMembershipProvider pattern
        self.local_user_store.clone()
    }
}

impl HasMembershipProvider for AppState {
    fn membership_provider(&self) -> Option<Arc<dyn MembershipProvider>> {
        // Prefer ControlPlane if available (production mode)
        if let Some(ref control) = self.control {
            return Some(Arc::new(ControlPlaneMembershipProvider::new(Arc::clone(
                control,
            ))));
        }

        // Fall back to local_user_store if available (local auth mode without control plane)
        // LocalUserStore implements MembershipProvider for workspace membership validation
        if let Some(ref store) = self.local_user_store {
            return Some(Arc::clone(store) as Arc<dyn MembershipProvider>);
        }

        // No membership system - allow all (development mode)
        Some(Arc::new(AllowAllMembership))
    }
}

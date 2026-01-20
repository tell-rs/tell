//! Admin routes
//!
//! Administrative endpoints for workspace, user, and API key management.

pub mod api_keys;
pub mod invites;
pub mod users;
pub mod workspaces;

pub use api_keys::{admin_routes as apikey_admin_routes, user_routes as apikey_user_routes};
pub use invites::{admin_routes as invite_admin_routes, public_routes as invite_public_routes};
pub use users::routes as user_admin_routes;
pub use workspaces::{
    admin_routes as workspace_admin_routes, user_routes as workspace_user_routes,
};

//! Database connection and schema management
//!
//! Uses Turso (async SQLite-compatible) for the control plane database.
//!
//! # Architecture
//!
//! Following the Go cdp-api pattern, we have two database layers:
//! - **Control DB** (this crate): Global singleton for users, workspaces, memberships
//! - **Workspace DBs**: Per-workspace databases for boards, metrics (lazy-loaded)

use tokio::sync::RwLock;
use tracing::{debug, info};
use turso::{Builder, Database};

use crate::error::{ControlError, Result};

/// Control plane database manager
///
/// Manages the global control database and per-workspace databases.
/// Thread-safe with lazy-loading of workspace databases.
pub struct ControlPlane {
    /// Control database (singleton)
    control_db: Database,
    /// Workspace databases (lazy-loaded, cached)
    workspace_dbs: RwLock<std::collections::HashMap<String, Database>>,
    /// Data directory for database files
    data_dir: String,
}

impl ControlPlane {
    /// Create a new control plane with file-based storage
    ///
    /// # Arguments
    /// * `data_dir` - Directory for database files (e.g., "data/")
    ///
    /// Creates:
    /// - `{data_dir}/control/data.db` - Control database
    /// - `{data_dir}/{workspace_id}/data.db` - Per-workspace databases
    pub async fn new(data_dir: impl Into<String>) -> Result<Self> {
        let data_dir = data_dir.into();

        // Ensure control directory exists
        let control_dir = format!("{}/control", data_dir);
        std::fs::create_dir_all(&control_dir).map_err(|e| {
            ControlError::invalid("data_dir", format!("failed to create directory: {}", e))
        })?;

        // Open control database
        let control_path = format!("{}/data.db", control_dir);
        info!(path = %control_path, "Opening control database");

        let control_db = Builder::new_local(&control_path).build().await?;

        let cp = Self {
            control_db,
            workspace_dbs: RwLock::new(std::collections::HashMap::new()),
            data_dir,
        };

        // Initialize schema
        cp.init_control_schema().await?;

        Ok(cp)
    }

    /// Create a new control plane with in-memory storage (for testing)
    pub async fn new_memory() -> Result<Self> {
        let control_db = Builder::new_local(":memory:").build().await?;

        let cp = Self {
            control_db,
            workspace_dbs: RwLock::new(std::collections::HashMap::new()),
            data_dir: String::new(),
        };

        cp.init_control_schema().await?;

        Ok(cp)
    }

    /// Get the control database
    pub fn control_db(&self) -> &Database {
        &self.control_db
    }

    /// Get or create a workspace database
    ///
    /// Lazy-loads the database on first access and caches it.
    /// Database is Clone (internally Arc), so this returns a cloned handle.
    ///
    /// For in-memory mode (testing), creates in-memory databases.
    /// For file-based mode (production), creates file-based databases.
    pub async fn workspace_db(&self, workspace_id: &str) -> Result<Database> {
        // Check cache first (read lock)
        {
            let cache = self.workspace_dbs.read().await;
            if let Some(db) = cache.get(workspace_id) {
                return Ok(db.clone());
            }
        }

        // Not in cache, create it (write lock)
        let mut cache = self.workspace_dbs.write().await;

        // Double-check after acquiring write lock
        if let Some(db) = cache.get(workspace_id) {
            return Ok(db.clone());
        }

        let db = if self.data_dir.is_empty() {
            // In-memory mode (testing)
            debug!(workspace_id, "Creating in-memory workspace database");
            Builder::new_local(":memory:").build().await?
        } else {
            // File-based mode (production)
            let ws_dir = format!("{}/{}", self.data_dir, workspace_id);
            std::fs::create_dir_all(&ws_dir).map_err(|e| {
                ControlError::invalid("workspace_id", format!("failed to create directory: {}", e))
            })?;

            let ws_path = format!("{}/data.db", ws_dir);
            debug!(workspace_id, path = %ws_path, "Opening workspace database");

            Builder::new_local(&ws_path).build().await?
        };

        // Initialize workspace schema
        Self::init_workspace_schema(&db).await?;

        cache.insert(workspace_id.to_string(), db.clone());

        Ok(db)
    }

    /// Initialize the control database schema
    async fn init_control_schema(&self) -> Result<()> {
        let conn = self.control_db.connect()?;

        // === Auth tables ===

        // Users table
        conn.execute(SCHEMA_USERS, ()).await?;
        conn.execute(INDEX_USERS_EMAIL, ()).await?;

        // Sessions table
        conn.execute(SCHEMA_SESSIONS, ()).await?;
        conn.execute(INDEX_SESSIONS_USER, ()).await?;
        conn.execute(INDEX_SESSIONS_TOKEN, ()).await?;

        // Revoked tokens table
        conn.execute(SCHEMA_REVOKED_TOKENS, ()).await?;

        // === Workspace tables ===

        // Workspaces table
        conn.execute(SCHEMA_WORKSPACES, ()).await?;

        // Workspace memberships table
        conn.execute(SCHEMA_MEMBERSHIPS, ()).await?;

        // Shared links table
        conn.execute(SCHEMA_SHARED_LINKS, ()).await?;

        // HTTP API keys table
        conn.execute(SCHEMA_API_KEYS, ()).await?;

        // Workspace invites table
        conn.execute(SCHEMA_INVITES, ()).await?;

        // Indexes
        conn.execute(INDEX_MEMBERSHIPS_USER, ()).await?;
        conn.execute(INDEX_SHARED_LINKS_RESOURCE, ()).await?;
        conn.execute(INDEX_API_KEYS_USER, ()).await?;
        conn.execute(INDEX_INVITES_WORKSPACE, ()).await?;
        conn.execute(INDEX_INVITES_TOKEN, ()).await?;

        info!("Control database schema initialized");
        Ok(())
    }

    /// Initialize a workspace database schema
    async fn init_workspace_schema(db: &Database) -> Result<()> {
        let conn = db.connect()?;

        // Boards table
        conn.execute(SCHEMA_BOARDS, ()).await?;

        // Metrics table (saved metric configurations)
        conn.execute(SCHEMA_METRICS, ()).await?;

        // Workspace settings table
        conn.execute(SCHEMA_WORKSPACE_SETTINGS, ()).await?;

        // Indexes
        conn.execute(INDEX_BOARDS_WORKSPACE, ()).await?;
        conn.execute(INDEX_BOARDS_OWNER, ()).await?;
        conn.execute(INDEX_METRICS_WORKSPACE, ()).await?;
        conn.execute(INDEX_METRICS_OWNER, ()).await?;

        debug!("Workspace database schema initialized");
        Ok(())
    }
}

// =============================================================================
// Control Database Schema - Auth Tables
// =============================================================================

const SCHEMA_USERS: &str = r#"
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer',
    created_at TEXT NOT NULL,
    last_login TEXT
)
"#;

const SCHEMA_SESSIONS: &str = r#"
CREATE TABLE IF NOT EXISTS auth_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    token TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    ip_address TEXT,
    user_agent TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
)
"#;

const SCHEMA_REVOKED_TOKENS: &str = r#"
CREATE TABLE IF NOT EXISTS revoked_tokens (
    token_id TEXT PRIMARY KEY,
    revoked_at TEXT NOT NULL,
    reason TEXT
)
"#;

const INDEX_USERS_EMAIL: &str = "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)";

const INDEX_SESSIONS_USER: &str =
    "CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON auth_sessions(user_id)";

const INDEX_SESSIONS_TOKEN: &str =
    "CREATE INDEX IF NOT EXISTS idx_sessions_token ON auth_sessions(token)";

// =============================================================================
// Control Database Schema - Workspace Tables
// =============================================================================

const SCHEMA_WORKSPACES: &str = r#"
CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    clickhouse_database TEXT NOT NULL,
    settings TEXT DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"#;

const SCHEMA_MEMBERSHIPS: &str = r#"
CREATE TABLE IF NOT EXISTS workspace_memberships (
    user_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, workspace_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
)
"#;

const SCHEMA_SHARED_LINKS: &str = r#"
CREATE TABLE IF NOT EXISTS shared_links (
    hash TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
)
"#;

const SCHEMA_API_KEYS: &str = r#"
CREATE TABLE IF NOT EXISTS user_api_keys (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    key_hash TEXT NOT NULL,
    key_prefix TEXT NOT NULL,
    permissions TEXT DEFAULT '[]',
    active INTEGER DEFAULT 1,
    last_used_at TEXT,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"#;

const INDEX_MEMBERSHIPS_USER: &str =
    "CREATE INDEX IF NOT EXISTS idx_memberships_user ON workspace_memberships(user_id)";

const INDEX_SHARED_LINKS_RESOURCE: &str = "CREATE INDEX IF NOT EXISTS idx_shared_links_resource ON shared_links(resource_type, resource_id)";

const INDEX_API_KEYS_USER: &str =
    "CREATE INDEX IF NOT EXISTS idx_api_keys_user ON user_api_keys(user_id)";

const SCHEMA_INVITES: &str = r#"
CREATE TABLE IF NOT EXISTS workspace_invites (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    role TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending',
    created_by TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
)
"#;

const INDEX_INVITES_WORKSPACE: &str =
    "CREATE INDEX IF NOT EXISTS idx_invites_workspace ON workspace_invites(workspace_id)";

const INDEX_INVITES_TOKEN: &str =
    "CREATE INDEX IF NOT EXISTS idx_invites_token ON workspace_invites(token)";

// =============================================================================
// Workspace Database Schema
// =============================================================================

const SCHEMA_BOARDS: &str = r#"
CREATE TABLE IF NOT EXISTS boards (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    settings TEXT DEFAULT '{}',
    is_pinned INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"#;

const SCHEMA_METRICS: &str = r#"
CREATE TABLE IF NOT EXISTS saved_metrics (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    query TEXT NOT NULL,
    display TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"#;

const SCHEMA_WORKSPACE_SETTINGS: &str = r#"
CREATE TABLE IF NOT EXISTS workspace_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"#;

const INDEX_BOARDS_WORKSPACE: &str =
    "CREATE INDEX IF NOT EXISTS idx_boards_workspace ON boards(workspace_id)";

const INDEX_BOARDS_OWNER: &str = "CREATE INDEX IF NOT EXISTS idx_boards_owner ON boards(owner_id)";

const INDEX_METRICS_WORKSPACE: &str =
    "CREATE INDEX IF NOT EXISTS idx_metrics_workspace ON saved_metrics(workspace_id)";

const INDEX_METRICS_OWNER: &str =
    "CREATE INDEX IF NOT EXISTS idx_metrics_owner ON saved_metrics(owner_id)";

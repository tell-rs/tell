//! ClickHouse sink configuration
//!
//! Configuration for connecting to ClickHouse and mapping tables.

use std::time::Duration;

use clickhouse::Client;

// =============================================================================
// Constants (matching Go implementation)
// =============================================================================

/// Default batch size limit per table
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Default flush interval
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Default retry attempts
pub const DEFAULT_RETRY_ATTEMPTS: usize = 3;

/// Default connection timeout
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for ClickHouse sink
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP URL (e.g., "http://localhost:8123")
    pub url: String,

    /// Database name
    pub database: String,

    /// Username for authentication (optional)
    pub username: Option<String>,

    /// Password for authentication (optional)
    pub password: Option<String>,

    /// Table names (CDP v1.1 schema)
    pub tables: TableNames,

    /// Batch size per table (rows before flush)
    pub batch_size: usize,

    /// Flush interval
    pub flush_interval: Duration,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Number of retry attempts
    pub retry_attempts: usize,

    /// Base delay for exponential backoff
    pub retry_base_delay: Duration,

    /// Maximum retry delay
    pub retry_max_delay: Duration,
}

/// Table names for CDP schema
#[derive(Debug, Clone)]
pub struct TableNames {
    /// TRACK events table
    pub events: String,
    /// Users table (IDENTIFY)
    pub users: String,
    /// User devices table (IDENTIFY)
    pub user_devices: String,
    /// User traits table (IDENTIFY)
    pub user_traits: String,
    /// Context table (CONTEXT)
    pub context: String,
    /// Logs table
    pub logs: String,
    /// Connector snapshots table
    pub snapshots: String,
}

impl Default for TableNames {
    fn default() -> Self {
        Self {
            events: "events_v1".into(),
            users: "users_v1".into(),
            user_devices: "user_devices".into(),
            user_traits: "user_traits".into(),
            context: "context_v1".into(),
            logs: "logs_v1".into(),
            snapshots: "snapshots_v1".into(),
        }
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".into(),
            database: "default".into(),
            username: None,
            password: None,
            tables: TableNames::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

impl ClickHouseConfig {
    /// Set the ClickHouse URL
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Set the database name
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Set authentication credentials
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Set custom table names
    pub fn with_tables(mut self, tables: TableNames) -> Self {
        self.tables = tables;
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the flush interval
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set the number of retry attempts
    pub fn with_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry_attempts = attempts;
        self
    }

    /// Build the ClickHouse client from this config
    pub fn build_client(&self) -> Client {
        let mut client = Client::default()
            .with_url(&self.url)
            .with_database(&self.database);

        if let Some(ref username) = self.username {
            client = client.with_user(username);
        }

        if let Some(ref password) = self.password {
            client = client.with_password(password);
        }

        client
    }
}

//! Actions for TUI state management.
//!
//! Actions are messages passed between components via channels.

/// Actions that can be dispatched in the TUI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Render the UI
    Render,
    /// Quit the application
    Quit,
    /// Tick (periodic update)
    Tick,

    // Navigation
    /// Switch to a view
    Navigate(View),
    /// Go back to previous view
    Back,
    /// Focus next component
    FocusNext,
    /// Focus previous component
    FocusPrevious,

    // Commands
    /// Execute a command (e.g., "/help", "/init")
    Command(String),
    /// Show help
    Help,
    /// Start the server
    StartServer,
    /// Stop the server
    StopServer,

    // Init wizard
    /// Start init wizard
    InitWizard,
    /// Select persona in init
    SelectPersona(Persona),
    /// Toggle source in init
    ToggleSource(String),
    /// Toggle sink in init
    ToggleSink(String),
    /// Complete init
    CompleteInit,

    // Status updates
    /// Server status changed
    ServerStatus(ServerStatus),
    /// New event received (for live tail)
    NewEvent(String),
    /// Server metrics update (JSON from control endpoint)
    MetricsUpdate(String),
    /// Error occurred
    Error(String),

    // Auth
    /// Login successful
    LoginSuccess(String), // email
    /// Login failed
    LoginFailed(String), // error message
    /// Logout
    Logout,

    // Boards
    /// Boards loaded from API
    BoardsLoaded(Vec<BoardInfo>),
    /// Select a board to view
    SelectBoard(String), // board id
    /// Board data loaded
    BoardDataLoaded(BoardData),

    // Query
    /// Execute a SQL query
    ExecuteQuery(String),
    /// Query results loaded
    QueryResult(QueryResult),
    /// Query failed
    QueryFailed(String),

    // Board management
    /// Create a new board
    CreateBoard(String), // title
    /// Update board title
    UpdateBoard(String, String), // board_id, new_title
    /// Delete a board
    DeleteBoard(String), // board_id
    /// Pin/unpin a board
    TogglePinBoard(String), // board_id
    /// Board created/updated successfully
    BoardUpdated,
    /// Board deleted successfully
    BoardDeleted(String), // board_id

    // Board sharing
    /// Share a board (create public link)
    ShareBoard(String), // board_id
    /// Unshare a board (revoke public link)
    UnshareBoard(String), // board_id
    /// Board shared successfully
    BoardShared(String, String), // board_id, share_url
    /// Board unshared successfully
    BoardUnshared(String), // board_id

    // Enhanced metrics
    /// Change time range for metrics
    SetTimeRange(TimeRange),
    /// Toggle comparison mode
    ToggleComparison,
    /// Set breakdown dimension
    SetBreakdown(Option<String>),
    /// Refresh metrics with current settings
    RefreshMetrics,
    /// Top events loaded
    TopEventsLoaded(Vec<TopEvent>),
    /// Stickiness data loaded
    StickinessLoaded(StickinessData),

    // API Keys
    /// API keys loaded
    ApiKeysLoaded(Vec<ApiKeyInfo>),
    /// Create new API key
    CreateApiKey(String), // name
    /// Delete API key
    DeleteApiKey(String), // name
    /// API key created successfully
    ApiKeyCreated(String, String), // name, hex_key
    /// API key deleted successfully
    ApiKeyDeleted(String), // name
    /// Show full API key
    ShowApiKey(String), // name
    /// Full API key revealed
    ApiKeyRevealed(String, String), // name, hex_key
}

/// Board info for list view
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoardInfo {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub is_pinned: bool,
    pub block_count: usize,
    pub share_hash: Option<String>,
}

/// Board data for detail view
#[derive(Debug, Clone, PartialEq)]
pub struct BoardData {
    pub id: String,
    pub title: String,
    pub metrics: Vec<MetricValue>,
}

// Manual Eq impl since f64 doesn't impl Eq
impl Eq for BoardData {}

/// A metric value to display
#[derive(Debug, Clone, PartialEq)]
pub struct MetricValue {
    pub title: String,
    pub value: f64,
    pub change_percent: Option<f64>,
}

// Manual Eq impl since f64 doesn't impl Eq
impl Eq for MetricValue {}

/// Views in the TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum View {
    /// Welcome screen (no config)
    #[default]
    Welcome,
    /// Init wizard
    Init,
    /// Main dashboard (server running)
    Dashboard,
    /// Config view
    Config,
    /// Sources management
    Sources,
    /// Sinks management
    Sinks,
    /// Plugins/connectors
    Plugins,
    /// Help screen
    Help,
    /// Query interface
    Query,
    /// Live event tail
    Tail,
    /// Login view
    Login,
    /// Boards list
    Boards,
    /// Single board detail
    BoardDetail,
    /// API keys management
    ApiKeys,
}

/// User persona for init wizard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Persona {
    MobileApp,
    WebApp,
    Backend,
    LogInfra,
    Custom,
}

impl Persona {
    /// Main display name for the persona.
    pub fn name_main(&self) -> &'static str {
        match self {
            Self::MobileApp => "Apps",
            Self::WebApp => "Web app",
            Self::Backend => "Backend services",
            Self::LogInfra => "Log infrastructure",
            Self::Custom => "Custom",
        }
    }

    /// Detail/description shown in parentheses (greyed).
    pub fn name_detail(&self) -> Option<&'static str> {
        match self {
            Self::MobileApp => Some("iOS, Android"),
            Self::WebApp => Some("SaaS, HTTP-based"),
            Self::Backend => Some("logs, metrics"),
            Self::LogInfra => Some("syslog forwarding"),
            Self::Custom => Some("pick sources manually"),
        }
    }

    /// Full display name for the persona.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MobileApp => "Apps (iOS, Android)",
            Self::WebApp => "Web app (SaaS, HTTP-based)",
            Self::Backend => "Backend services (logs, metrics)",
            Self::LogInfra => "Log infrastructure (syslog forwarding)",
            Self::Custom => "Custom (pick sources manually)",
        }
    }

    /// Recommended sources for this persona.
    pub fn recommended_sources(&self) -> Vec<SourceType> {
        match self {
            Self::MobileApp => vec![SourceType::Tcp],
            Self::WebApp => vec![SourceType::Http],
            Self::Backend => vec![SourceType::Http, SourceType::Tcp],
            Self::LogInfra => vec![SourceType::SyslogTcp, SourceType::SyslogUdp],
            Self::Custom => vec![],
        }
    }

    /// Recommended sinks for this persona.
    pub fn recommended_sinks(&self) -> Vec<SinkType> {
        match self {
            Self::MobileApp | Self::WebApp => vec![SinkType::ClickHouse, SinkType::Stdout],
            Self::Backend | Self::LogInfra => vec![SinkType::ClickHouse, SinkType::ArrowIpc],
            Self::Custom => vec![],
        }
    }
}

/// Available source types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceType {
    Http,
    Tcp,
    SyslogTcp,
    SyslogUdp,
}

impl SourceType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Http => "HTTP (REST API)",
            Self::Tcp => "TCP (FlatBuffers)",
            Self::SyslogTcp => "Syslog TCP (RFC 5424)",
            Self::SyslogUdp => "Syslog UDP (RFC 5424)",
        }
    }

    /// Config key for this source type.
    pub fn config_key(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::SyslogTcp => "syslog_tcp",
            Self::SyslogUdp => "syslog_udp",
        }
    }

    /// Default port for this source type.
    pub fn default_port(&self) -> u16 {
        match self {
            Self::Http => 8080,
            Self::Tcp => 50000,
            Self::SyslogTcp => 1514,
            Self::SyslogUdp => 1514,
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Http, Self::Tcp, Self::SyslogTcp, Self::SyslogUdp]
    }
}

/// Available sink types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkType {
    ClickHouse,
    ArrowIpc,
    DiskBinary,
    Stdout,
}

impl SinkType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ClickHouse => "ClickHouse (analytics DB)",
            Self::ArrowIpc => "Arrow IPC (columnar files)",
            Self::DiskBinary => "Disk binary (archive)",
            Self::Stdout => "Stdout (debugging)",
        }
    }

    /// Config key for this sink type.
    pub fn config_key(&self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::ArrowIpc => "arrow_ipc",
            Self::DiskBinary => "disk_binary",
            Self::Stdout => "stdout",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::ClickHouse,
            Self::ArrowIpc,
            Self::DiskBinary,
            Self::Stdout,
        ]
    }
}

/// Init wizard step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InitStep {
    #[default]
    Persona,
    Sources,
    Sinks,
    ApiKey,
    Complete,
}

/// Init wizard state.
#[derive(Debug, Clone, Default)]
pub struct InitState {
    pub step: InitStep,
    pub persona: Option<Persona>,
    pub sources: Vec<SourceType>,
    pub sinks: Vec<SinkType>,
    pub api_key: Option<String>,
}

/// Server status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerStatus {
    #[default]
    Stopped,
    Starting,
    Running,
    Stopping,
    Error,
}

impl ServerStatus {
    /// Status indicator character.
    pub fn indicator(&self) -> &'static str {
        match self {
            Self::Stopped => "○",
            Self::Starting => "◐",
            Self::Running => "●",
            Self::Stopping => "◑",
            Self::Error => "✗",
        }
    }
}

/// Query view state.
#[derive(Debug, Clone, Default)]
pub struct QueryState {
    /// SQL query input
    pub query: String,
    /// Query results (columns, rows)
    pub result: Option<QueryResult>,
    /// Whether query is executing
    pub loading: bool,
    /// Error message
    pub error: Option<String>,
    /// Cursor position in query input
    pub cursor: usize,
    /// Scroll offset for results
    pub scroll: usize,
}

/// Query result for display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data as strings
    pub rows: Vec<Vec<String>>,
    /// Total row count
    pub row_count: usize,
    /// Execution time in ms
    pub execution_time_ms: u64,
}

/// Time range for metrics queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeRange {
    #[default]
    Week,
    Month,
    Quarter,
}

impl TimeRange {
    /// API query parameter value.
    pub fn as_param(&self) -> &'static str {
        match self {
            Self::Week => "7d",
            Self::Month => "30d",
            Self::Quarter => "90d",
        }
    }

    /// Display label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Week => "7 days",
            Self::Month => "30 days",
            Self::Quarter => "90 days",
        }
    }

    /// Cycle to next time range.
    pub fn next(&self) -> Self {
        match self {
            Self::Week => Self::Month,
            Self::Month => Self::Quarter,
            Self::Quarter => Self::Week,
        }
    }
}

/// Top event data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopEvent {
    pub name: String,
    pub count: u64,
}

/// Stickiness metrics data.
#[derive(Debug, Clone, PartialEq)]
pub struct StickinessData {
    /// DAU/MAU ratio (0.0 - 1.0)
    pub daily_ratio: f64,
    /// DAU/WAU ratio (0.0 - 1.0)
    pub weekly_ratio: f64,
}

// Manual Eq impl since f64 doesn't impl Eq
impl Eq for StickinessData {}

/// API key info for list view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyInfo {
    pub name: Option<String>,
    pub key_preview: String, // First 8 chars
    pub workspace_id: u32,
}

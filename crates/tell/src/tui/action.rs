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
}

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

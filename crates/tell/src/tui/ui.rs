//! UI rendering for the TUI.
//!
//! Contains all view rendering functions.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use super::action::{
    BoardData, BoardInfo, InitState, InitStep, QueryState, ServerStatus, SinkType, SourceType,
    TimeRange, View,
};
use super::app::{BoardEditMode, LoginState, ServerMetrics};
use super::theme::Theme;

/// Format a number with K/M/B suffix.
pub fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format bytes with KB/MB/GB suffix.
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Render view-specific content.
#[allow(clippy::too_many_arguments)]
pub fn render_view_content<'a>(
    view: View,
    server_status: ServerStatus,
    theme: &Theme,
    error: Option<&str>,
    init_state: &InitState,
    selected_index: usize,
    config_path: &std::path::Path,
    tail_events: &[String],
    tail_connected: bool,
    tail_scroll: usize,
    visible_lines: usize,
    server_metrics: Option<&ServerMetrics>,
    auth: Option<&'a crate::cmd::auth::AuthCredentials>,
    login_state: &LoginState,
    boards: &[BoardInfo],
    current_board: Option<&'a BoardData>,
    query_state: &'a QueryState,
    time_range: TimeRange,
    show_comparison: bool,
    breakdown: Option<&str>,
    api_keys: &[super::action::ApiKeyInfo],
    revealed_api_key: Option<&(String, String)>,
) -> Vec<Line<'a>> {
    let mut lines = match view {
        View::Welcome => render_welcome(theme),
        View::Dashboard => render_dashboard(theme, server_status, server_metrics),
        View::Init => render_init_step(theme, init_state, selected_index),
        View::Config => render_config(theme, config_path),
        View::Help => render_help(theme),
        View::Sources => render_sources(theme),
        View::Sinks => render_sinks(theme),
        View::Plugins => render_plugins(theme),
        View::Query => render_query(theme, query_state, visible_lines),
        View::Tail => render_tail(
            theme,
            tail_events,
            tail_connected,
            tail_scroll,
            visible_lines,
        ),
        View::Login => render_login(theme, login_state),
        View::Boards => render_boards(theme, boards, auth, selected_index),
        View::BoardDetail => {
            render_board_detail(theme, current_board, time_range, show_comparison, breakdown)
        }
        View::ApiKeys => render_api_keys(theme, api_keys, revealed_api_key, selected_index),
    };

    // Add error message if present
    if let Some(err) = error {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            format!("Error: {}", err),
            theme.error_style(),
        )]));
    }

    lines
}

fn render_welcome(theme: &Theme) -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Analytics that tell the whole story.",
            theme.muted_style(),
        )]),
        Line::from(""),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  No configuration found.",
            theme.muted_style(),
        )]),
        Line::from(""),
        Line::from(vec![
            Span::raw("  Press "),
            Span::styled("/", Style::default().bold()),
            Span::raw(" and type "),
            Span::styled("init", theme.brand_style()),
            Span::raw(" to get started."),
        ]),
    ]
}

fn render_dashboard(
    theme: &Theme,
    server_status: ServerStatus,
    server_metrics: Option<&ServerMetrics>,
) -> Vec<Line<'static>> {
    let status_style = match server_status {
        ServerStatus::Running => theme.status_running(),
        ServerStatus::Error => theme.status_error(),
        _ => theme.status_stopped(),
    };

    // Format uptime from server metrics
    let uptime_str = server_metrics
        .map(|m| {
            let secs = m.uptime_secs;
            if secs < 60 {
                format!("{}s", secs)
            } else if secs < 3600 {
                format!("{}m {}s", secs / 60, secs % 60)
            } else if secs < 86400 {
                format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
            } else {
                format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
            }
        })
        .unwrap_or_else(|| "-".to_string());

    let mut lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(server_status.indicator(), status_style),
            Span::styled(format!(" Server {:?}", server_status), status_style),
            if server_metrics.is_some() {
                Span::styled(format!("  (uptime: {})", uptime_str), theme.muted_style())
            } else {
                Span::raw("")
            },
        ]),
        Line::from(""),
    ];

    // Show real server metrics from control endpoint
    if let Some(metrics) = server_metrics {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  Pipeline",
            theme.brand_style().bold(),
        )]));
        lines.push(Line::from(vec![
            Span::styled("    Messages: ", theme.muted_style()),
            Span::styled(
                format_number(metrics.messages_processed),
                Style::default().bold(),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("    Bytes:    ", theme.muted_style()),
            Span::styled(
                format_bytes(metrics.bytes_processed),
                Style::default().bold(),
            ),
        ]));

        // Sources
        if !metrics.sources.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                "  Sources",
                theme.brand_style().bold(),
            )]));
            for source in &metrics.sources {
                lines.push(Line::from(vec![
                    Span::styled(format!("    {} ", source.id), theme.muted_style()),
                    Span::raw(format!(
                        "{} msgs, {} conns",
                        format_number(source.messages_received),
                        source.connections_active
                    )),
                ]));
            }
        }

        // Sinks
        if !metrics.sinks.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                "  Sinks",
                theme.brand_style().bold(),
            )]));
            for sink in &metrics.sinks {
                let errors_str = if sink.errors > 0 {
                    format!(", {} errors", sink.errors)
                } else {
                    String::new()
                };
                lines.push(Line::from(vec![
                    Span::styled(format!("    {} ", sink.id), theme.muted_style()),
                    Span::raw(format!(
                        "{} msgs{}",
                        format_number(sink.messages_written),
                        errors_str
                    )),
                ]));
            }
        }
    }

    lines
}

fn render_config(theme: &Theme, config_path: &std::path::Path) -> Vec<Line<'static>> {
    let config_content = std::fs::read_to_string(config_path);

    let mut lines = vec![
        Line::from(""),
        Line::from("  Configuration"),
        Line::from(""),
    ];

    match config_content {
        Ok(content) => {
            lines.push(Line::from(vec![
                Span::styled("  Path: ", theme.muted_style()),
                Span::styled(
                    config_path
                        .canonicalize()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| config_path.display().to_string()),
                    theme.brand_style(),
                ),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                "  ─".to_string() + &"─".repeat(40),
                theme.muted_style(),
            )]));
            lines.push(Line::from(""));

            // Show full config contents
            for line in content.lines() {
                let styled_line = if line.starts_with('#') {
                    Span::styled(format!("  {}", line), theme.muted_style())
                } else if line.starts_with('[') {
                    Span::styled(format!("  {}", line), theme.brand_style())
                } else {
                    Span::raw(format!("  {}", line))
                };
                lines.push(Line::from(vec![styled_line]));
            }
        }
        Err(_) => {
            lines.push(Line::from(vec![Span::styled(
                "  No config file found.",
                theme.muted_style(),
            )]));
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::raw("  Run "),
                Span::styled("/init", theme.brand_style()),
                Span::raw(" to create one."),
            ]));
        }
    }

    lines
}

fn render_help(theme: &Theme) -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from("  Commands"),
        Line::from(""),
        Line::from(vec![
            Span::styled("    /init", theme.brand_style()),
            Span::styled("      Setup wizard", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /start", theme.brand_style()),
            Span::styled("     Start the server", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /stop", theme.brand_style()),
            Span::styled("      Stop the server", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /sources", theme.brand_style()),
            Span::styled("   Manage data sources", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /sinks", theme.brand_style()),
            Span::styled("     Manage data sinks", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /plugins", theme.brand_style()),
            Span::styled("   Browse connectors", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /tail", theme.brand_style()),
            Span::styled("      Live event stream", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    /query", theme.brand_style()),
            Span::styled("     SQL query interface", theme.muted_style()),
        ]),
        Line::from(""),
        Line::from("  Keyboard"),
        Line::from(""),
        Line::from(vec![
            Span::styled("    /", Style::default().bold()),
            Span::styled("          Command mode", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    Esc", Style::default().bold()),
            Span::styled("        Go back", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    q", Style::default().bold()),
            Span::styled("          Quit", theme.muted_style()),
        ]),
    ]
}

fn render_sources(theme: &Theme) -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from("  Sources"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  No sources configured.",
            theme.muted_style(),
        )]),
        Line::from(""),
        Line::from(vec![
            Span::raw("  Press "),
            Span::styled("/", Style::default().bold()),
            Span::raw(" and type "),
            Span::styled("init", theme.brand_style()),
            Span::raw(" to add sources."),
        ]),
    ]
}

fn render_sinks(theme: &Theme) -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from("  Sinks"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  No sinks configured.",
            theme.muted_style(),
        )]),
        Line::from(""),
        Line::from(vec![
            Span::raw("  Press "),
            Span::styled("/", Style::default().bold()),
            Span::raw(" and type "),
            Span::styled("init", theme.brand_style()),
            Span::raw(" to add sinks."),
        ]),
    ]
}

fn render_plugins(theme: &Theme) -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from("  Connectors"),
        Line::from(""),
        Line::from(vec![
            Span::styled("    ●", theme.success_style()),
            Span::raw(" GitHub"),
            Span::styled("      Webhooks and events", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    ●", theme.success_style()),
            Span::raw(" Shopify"),
            Span::styled("     E-commerce analytics", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    ○", theme.muted_style()),
            Span::raw(" Stripe"),
            Span::styled("      Payment events", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::styled("    ○", theme.muted_style()),
            Span::raw(" Linear"),
            Span::styled("      Issue tracking", theme.muted_style()),
        ]),
    ]
}

fn render_query<'a>(
    theme: &Theme,
    query_state: &'a QueryState,
    visible_lines: usize,
) -> Vec<Line<'a>> {
    let mut lines = vec![Line::from(""), Line::from("  SQL Query"), Line::from("")];

    // Query input area
    lines.push(Line::from(vec![
        Span::styled("  > ", theme.brand_style()),
        Span::raw(&query_state.query),
        Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
    ]));
    lines.push(Line::from(""));

    // Show loading or error
    if query_state.loading {
        lines.push(Line::from(vec![Span::styled(
            "  Executing query...",
            theme.muted_style(),
        )]));
    } else if let Some(ref err) = query_state.error {
        lines.push(Line::from(vec![Span::styled(
            format!("  Error: {}", err),
            theme.error_style(),
        )]));
    } else if let Some(ref result) = query_state.result {
        // Show result summary
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {} rows", result.row_count),
                theme.success_style(),
            ),
            Span::styled(
                format!(" ({} ms)", result.execution_time_ms),
                theme.muted_style(),
            ),
        ]));
        lines.push(Line::from(""));

        if !result.columns.is_empty() {
            // Column headers
            let header: Vec<Span> = result
                .columns
                .iter()
                .enumerate()
                .flat_map(|(i, col)| {
                    let mut spans = vec![Span::styled(
                        format!("{:<15}", col),
                        Style::default().bold(),
                    )];
                    if i < result.columns.len() - 1 {
                        spans.push(Span::raw(" │ "));
                    }
                    spans
                })
                .collect();
            lines.push(Line::from(vec![Span::raw("  ")]).patch_style(Style::default()));
            lines.push(Line::from(
                std::iter::once(Span::raw("  "))
                    .chain(header)
                    .collect::<Vec<_>>(),
            ));

            // Separator
            let sep_len = result.columns.len() * 18;
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled("─".repeat(sep_len.min(60)), theme.muted_style()),
            ]));

            // Data rows (limited by scroll and visible lines)
            let max_rows = visible_lines.saturating_sub(12);
            let start = query_state.scroll;
            let end = (start + max_rows).min(result.rows.len());

            for row in &result.rows[start..end] {
                let row_spans: Vec<Span> = row
                    .iter()
                    .enumerate()
                    .flat_map(|(i, val)| {
                        let display = if val.len() > 15 {
                            format!("{}…", &val[..14])
                        } else {
                            format!("{:<15}", val)
                        };
                        let mut spans = vec![Span::raw(display)];
                        if i < row.len() - 1 {
                            spans.push(Span::styled(" │ ", theme.muted_style()));
                        }
                        spans
                    })
                    .collect();
                lines.push(Line::from(
                    std::iter::once(Span::raw("  "))
                        .chain(row_spans)
                        .collect::<Vec<_>>(),
                ));
            }

            // Scroll indicator
            if result.rows.len() > max_rows {
                lines.push(Line::from(""));
                lines.push(Line::from(vec![Span::styled(
                    format!("  [{}-{}/{}] ↑↓ scroll", start + 1, end, result.rows.len()),
                    theme.muted_style(),
                )]));
            }
        }
    } else {
        // Help text
        lines.push(Line::from(vec![Span::styled(
            "  Type a SQL query and press Enter to execute.",
            theme.muted_style(),
        )]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  Examples:",
            theme.muted_style(),
        )]));
        lines.push(Line::from(vec![
            Span::raw("    "),
            Span::styled("SELECT * FROM events_v1 LIMIT 10", theme.brand_style()),
        ]));
        lines.push(Line::from(vec![
            Span::raw("    "),
            Span::styled(
                "SELECT event_name, COUNT(*) FROM events_v1 GROUP BY event_name",
                theme.brand_style(),
            ),
        ]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  Enter", Style::default().bold()),
        Span::styled(" run  ", theme.muted_style()),
        Span::styled("Esc", Style::default().bold()),
        Span::styled(" back  ", theme.muted_style()),
        Span::styled("Ctrl+C", Style::default().bold()),
        Span::styled(" clear", theme.muted_style()),
    ]));

    lines
}

fn render_tail(
    theme: &Theme,
    tail_events: &[String],
    tail_connected: bool,
    tail_scroll: usize,
    visible_lines: usize,
) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::raw("  Live Events "),
            if tail_connected {
                Span::styled("● connected", theme.success_style())
            } else {
                Span::styled("○ disconnected", theme.muted_style())
            },
        ]),
        Line::from(""),
    ];

    if tail_events.is_empty() {
        if tail_connected {
            lines.push(Line::from(vec![Span::styled(
                "  Waiting for events...",
                theme.muted_style(),
            )]));
        } else {
            lines.push(Line::from(vec![
                Span::styled("  Server not running. Use ", theme.muted_style()),
                Span::styled("/start", theme.brand_style()),
                Span::styled(" first.", theme.muted_style()),
            ]));
        }
    } else {
        // Show events with scrolling (most recent at bottom)
        let max_events = visible_lines.saturating_sub(4); // Reserve header lines
        let start = tail_scroll.min(tail_events.len().saturating_sub(max_events));
        let end = (start + max_events).min(tail_events.len());

        for event in &tail_events[start..end] {
            // Color-code based on event type
            let style = if event.contains("[ERROR]") || event.contains("error") {
                theme.error_style()
            } else if event.contains("[WARN]") || event.contains("warning") {
                Style::default().fg(Color::Yellow)
            } else if event.contains("[event]") {
                theme.brand_style()
            } else {
                Style::default()
            };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(event.clone(), style),
            ]));
        }

        // Show scroll indicator if there's more
        if tail_events.len() > max_events {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                format!("  [{}/{}] ↑↓ scroll", end, tail_events.len()),
                theme.muted_style(),
            )]));
        }
    }

    lines
}

fn render_login(theme: &Theme, login_state: &LoginState) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(""), Line::from("  Login"), Line::from("")];

    if login_state.loading {
        lines.push(Line::from(vec![Span::styled(
            "  Logging in...",
            theme.muted_style(),
        )]));
    } else {
        // Email field
        let email_style = if login_state.focus == 0 {
            theme.selected_style()
        } else {
            Style::default()
        };
        let email_indicator = if login_state.focus == 0 { "▸" } else { " " };
        lines.push(Line::from(vec![
            Span::styled(format!("  {} Email:    ", email_indicator), email_style),
            Span::styled(login_state.email.value().to_string(), email_style),
            if login_state.focus == 0 {
                Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK))
            } else {
                Span::raw("")
            },
        ]));

        // Password field
        let password_style = if login_state.focus == 1 {
            theme.selected_style()
        } else {
            Style::default()
        };
        let password_indicator = if login_state.focus == 1 { "▸" } else { " " };
        let masked_password = "*".repeat(login_state.password.len());
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {} Password: ", password_indicator),
                password_style,
            ),
            Span::styled(masked_password, password_style),
            if login_state.focus == 1 {
                Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK))
            } else {
                Span::raw("")
            },
        ]));

        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("  Tab", Style::default().bold()),
            Span::styled(" switch field  ", theme.muted_style()),
            Span::styled("Enter", Style::default().bold()),
            Span::styled(" login  ", theme.muted_style()),
            Span::styled("Esc", Style::default().bold()),
            Span::styled(" cancel", theme.muted_style()),
        ]));

        if let Some(ref err) = login_state.error {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                format!("  {}", err),
                theme.error_style(),
            )]));
        }
    }

    lines
}

fn render_boards<'a>(
    theme: &Theme,
    boards: &[BoardInfo],
    auth: Option<&'a crate::cmd::auth::AuthCredentials>,
    selected_index: usize,
) -> Vec<Line<'a>> {
    let mut lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::raw("  Boards "),
            if let Some(a) = auth {
                Span::styled(format!("({})", a.email), theme.muted_style())
            } else {
                Span::raw("")
            },
        ]),
        Line::from(""),
    ];

    if boards.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "  No boards found.",
            theme.muted_style(),
        )]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  Create boards in the web dashboard.",
            theme.muted_style(),
        )]));
    } else {
        for (i, board) in boards.iter().enumerate() {
            let marker = if i == selected_index { "▸" } else { " " };
            let style = if i == selected_index {
                theme.selected_style()
            } else {
                Style::default()
            };
            let pin_marker = if board.is_pinned { "📌 " } else { "" };
            let share_marker = if board.share_hash.is_some() {
                " 🔗"
            } else {
                ""
            };
            lines.push(Line::from(vec![
                Span::styled(
                    format!("    {} {}{}", marker, pin_marker, board.title),
                    style,
                ),
                Span::styled(share_marker, Style::default().fg(Color::Cyan)),
                Span::styled(
                    format!("  ({} blocks)", board.block_count),
                    theme.muted_style(),
                ),
            ]));
            if let Some(ref desc) = board.description {
                lines.push(Line::from(vec![Span::styled(
                    format!("        {}", desc),
                    theme.muted_style(),
                )]));
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("  Enter", Style::default().bold()),
            Span::styled(" view  ", theme.muted_style()),
            Span::styled("/", Style::default().bold()),
            Span::styled(
                " commands (new, edit, delete, pin, share)",
                theme.muted_style(),
            ),
        ]));
    }

    lines
}

fn render_api_keys<'a>(
    theme: &Theme,
    api_keys: &[super::action::ApiKeyInfo],
    revealed_key: Option<&(String, String)>,
    selected_index: usize,
) -> Vec<Line<'a>> {
    let mut lines = vec![
        Line::from(""),
        Line::from(vec![Span::styled("  API Keys", Style::default().bold())]),
        Line::from(""),
    ];

    // Show revealed key if any
    if let Some((name, full_key)) = revealed_key {
        lines.push(Line::from(vec![Span::styled(
            format!("  Key: {}", name),
            Style::default().fg(Color::Green),
        )]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            format!("    {}", full_key),
            Style::default().fg(Color::Yellow).bold(),
        )]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  Copy this key to your SDK configuration.",
            theme.muted_style(),
        )]));
        lines.push(Line::from(""));
    }

    // List keys
    if api_keys.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "  No API keys configured.",
            theme.muted_style(),
        )]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "  Create a new key with: /new <name>",
            theme.muted_style(),
        )]));
    } else {
        lines.push(Line::from(vec![Span::styled(
            format!("  {:<20} {:<12} {}", "Name", "Key", "Workspace"),
            theme.muted_style(),
        )]));
        lines.push(Line::from(vec![Span::styled(
            format!("  {}", "-".repeat(46)),
            theme.muted_style(),
        )]));

        for (i, key) in api_keys.iter().enumerate() {
            let marker = if i == selected_index { "▸" } else { " " };
            let style = if i == selected_index {
                theme.selected_style()
            } else {
                Style::default()
            };
            let name = key.name.as_deref().unwrap_or("(unnamed)");
            lines.push(Line::from(vec![Span::styled(
                format!(
                    "  {} {:<20} {:<12} {}",
                    marker, name, key.key_preview, key.workspace_id
                ),
                style,
            )]));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("/", Style::default().bold()),
            Span::styled(
                " commands (new <name>, show <name>, delete <name>)",
                theme.muted_style(),
            ),
        ]));
    }

    lines
}

fn render_board_detail<'a>(
    theme: &Theme,
    current_board: Option<&'a BoardData>,
    time_range: TimeRange,
    show_comparison: bool,
    breakdown: Option<&str>,
) -> Vec<Line<'a>> {
    let mut lines = vec![Line::from("")];

    if let Some(board) = current_board {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(&board.title, theme.brand_style().bold()),
        ]));

        // Time range, comparison, and breakdown indicator
        let comparison_indicator = if show_comparison { " vs prev" } else { "" };
        let breakdown_indicator = breakdown.map(|b| format!(" by {}", b)).unwrap_or_default();
        lines.push(Line::from(vec![Span::styled(
            format!(
                "  {}{}{}",
                time_range.label(),
                comparison_indicator,
                breakdown_indicator
            ),
            theme.muted_style(),
        )]));
        lines.push(Line::from(""));

        if board.metrics.is_empty() {
            lines.push(Line::from(vec![Span::styled(
                "  No metrics in this board.",
                theme.muted_style(),
            )]));
        } else {
            for metric in &board.metrics {
                let change_span = if show_comparison {
                    if let Some(pct) = metric.change_percent {
                        let (sign, color) = if pct >= 0.0 {
                            ("+", Color::Green)
                        } else {
                            ("", Color::Red)
                        };
                        Span::styled(format!("  {}{:.1}%", sign, pct), Style::default().fg(color))
                    } else {
                        Span::raw("")
                    }
                } else {
                    Span::raw("")
                };

                lines.push(Line::from(vec![
                    Span::styled(format!("    {} ", metric.title), theme.muted_style()),
                    Span::styled(format!("{:.0}", metric.value), Style::default().bold()),
                    change_span,
                ]));
            }
        }
    } else {
        lines.push(Line::from(vec![Span::styled(
            "  Loading board...",
            theme.muted_style(),
        )]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  /", Style::default().bold()),
        Span::styled(
            " commands (7d, 30d, 90d, compare, refresh, share)",
            theme.muted_style(),
        ),
    ]));

    lines
}

/// Render init wizard step content.
pub fn render_init_step(
    theme: &Theme,
    init_state: &InitState,
    selected_index: usize,
) -> Vec<Line<'static>> {
    match init_state.step {
        InitStep::Persona => render_init_persona(theme, selected_index),
        InitStep::Sources => render_init_sources(theme, init_state, selected_index),
        InitStep::Sinks => render_init_sinks(theme, init_state, selected_index),
        InitStep::ApiKey => render_init_api_key(theme, init_state),
        InitStep::Complete => render_init_complete(theme),
    }
}

fn render_init_persona(theme: &Theme, selected_index: usize) -> Vec<Line<'static>> {
    use super::action::Persona;
    let personas = [
        Persona::MobileApp,
        Persona::WebApp,
        Persona::Backend,
        Persona::LogInfra,
        Persona::Custom,
    ];

    let mut lines = vec![
        Line::from(""),
        Line::from("  Setup Wizard"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  What are you building?",
            theme.muted_style(),
        )]),
        Line::from(""),
    ];

    for (i, persona) in personas.iter().enumerate() {
        let marker = if i == selected_index { "▸" } else { " " };
        let style = if i == selected_index {
            theme.selected_style()
        } else {
            Style::default()
        };
        let mut spans = vec![
            Span::styled(format!("    {} ", marker), style),
            Span::styled(persona.name_main().to_string(), style),
        ];
        if let Some(detail) = persona.name_detail() {
            spans.push(Span::styled(format!(" ({})", detail), theme.muted_style()));
        }
        lines.push(Line::from(spans));
    }

    lines
}

fn render_init_sources(
    theme: &Theme,
    init_state: &InitState,
    selected_index: usize,
) -> Vec<Line<'static>> {
    let all_sources = SourceType::all();
    let recommended = init_state
        .persona
        .map(|p| p.recommended_sources())
        .unwrap_or_default();

    let mut lines = vec![
        Line::from(""),
        Line::from("  Setup Wizard"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Select data sources:",
            theme.muted_style(),
        )]),
        Line::from(""),
    ];

    for (i, source) in all_sources.iter().enumerate() {
        let is_selected = init_state.sources.contains(source);
        let is_recommended = recommended.contains(source);
        let marker = if i == selected_index { "▸" } else { " " };
        let checkbox = if is_selected { "●" } else { "○" };
        let checkbox_style = if is_selected {
            theme.success_style()
        } else {
            theme.muted_style()
        };

        let style = if i == selected_index {
            theme.selected_style()
        } else {
            Style::default()
        };

        let mut spans = vec![
            Span::styled(format!("    {} ", marker), style),
            Span::styled(checkbox, checkbox_style),
            Span::raw(" "),
            Span::styled(source.name().to_string(), style),
        ];

        if is_recommended {
            spans.push(Span::styled(" (recommended)", theme.muted_style()));
        }

        lines.push(Line::from(spans));
    }

    lines
}

fn render_init_sinks(
    theme: &Theme,
    init_state: &InitState,
    selected_index: usize,
) -> Vec<Line<'static>> {
    let all_sinks = SinkType::all();
    let recommended = init_state
        .persona
        .map(|p| p.recommended_sinks())
        .unwrap_or_default();

    let mut lines = vec![
        Line::from(""),
        Line::from("  Setup Wizard"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Select data sinks:",
            theme.muted_style(),
        )]),
        Line::from(""),
    ];

    for (i, sink) in all_sinks.iter().enumerate() {
        let is_selected = init_state.sinks.contains(sink);
        let is_recommended = recommended.contains(sink);
        let marker = if i == selected_index { "▸" } else { " " };
        let checkbox = if is_selected { "●" } else { "○" };
        let checkbox_style = if is_selected {
            theme.success_style()
        } else {
            theme.muted_style()
        };

        let style = if i == selected_index {
            theme.selected_style()
        } else {
            Style::default()
        };

        let mut spans = vec![
            Span::styled(format!("    {} ", marker), style),
            Span::styled(checkbox, checkbox_style),
            Span::raw(" "),
            Span::styled(sink.name().to_string(), style),
        ];

        if is_recommended {
            spans.push(Span::styled(" (recommended)", theme.muted_style()));
        }

        lines.push(Line::from(spans));
    }

    lines
}

fn render_init_api_key(theme: &Theme, init_state: &InitState) -> Vec<Line<'static>> {
    let api_key = init_state
        .api_key
        .clone()
        .unwrap_or_else(|| "(key not generated)".to_string());

    vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ✓", theme.success_style()),
            Span::raw(" Config created"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled("  Your API key:", theme.muted_style())]),
        Line::from(""),
        Line::from(vec![
            Span::raw("    "),
            Span::styled(api_key, Style::default().fg(Color::Yellow).bold()),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Copy this key to your SDK configuration.",
            theme.muted_style(),
        )]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  View anytime: /apikeys show default",
            theme.muted_style(),
        )]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Press Enter to start the server.",
            theme.muted_style(),
        )]),
    ]
}

fn render_init_complete(theme: &Theme) -> Vec<Line<'static>> {
    vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ✓", theme.success_style()),
            Span::raw(" Your Tell server is ready."),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled("  Next steps:", theme.muted_style())]),
        Line::from(""),
        Line::from(vec![
            Span::raw("    "),
            Span::styled("/start", theme.brand_style()),
            Span::styled("  Start the server", theme.muted_style()),
        ]),
        Line::from(vec![
            Span::raw("    "),
            Span::styled("/tail", theme.brand_style()),
            Span::styled("    Watch live events", theme.muted_style()),
        ]),
    ]
}

/// Render command popup overlay.
pub fn render_command_popup(
    frame: &mut ratatui::Frame,
    area: Rect,
    theme: &Theme,
    input: &str,
    cursor_pos: usize,
    selected_cmd_index: usize,
    commands: &[(&str, &str)],
) {
    // Filter commands based on input
    let filter = input.strip_prefix('/').unwrap_or(input).to_lowercase();
    let filtered: Vec<_> = commands
        .iter()
        .filter(|(cmd, _)| filter.is_empty() || cmd.starts_with(&filter))
        .collect();

    // Calculate popup size - full width with margins, minimum height to prevent jumping
    let min_visible_cmds = 4; // Show at least 4 command slots to prevent jumping
    let cmd_count = filtered.len().max(min_visible_cmds);
    let popup_height = (cmd_count + 2).min(14) as u16; // +2 for input + border
    let popup_width = area.width.saturating_sub(2); // Full width with 1 margin each side

    // Position popup at bottom of screen
    let popup_y = area.height.saturating_sub(popup_height + 1);
    let popup_x = 1;

    let popup_area = Rect {
        x: popup_x,
        y: popup_y,
        width: popup_width,
        height: popup_height,
    };

    // Clear background
    frame.render_widget(Clear, popup_area);

    // Draw popup with subtle border
    let popup_block = Block::default()
        .borders(Borders::TOP)
        .border_style(theme.muted_style());
    let inner = popup_block.inner(popup_area);
    frame.render_widget(popup_block, popup_area);

    // Split inner area: commands list + input line
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),    // Commands
            Constraint::Length(1), // Input
        ])
        .split(inner);

    // Render filtered commands
    let mut cmd_lines: Vec<Line> = Vec::new();
    for (i, (cmd, desc)) in filtered.iter().enumerate() {
        let is_selected = i == selected_cmd_index;
        let (cmd_style, desc_style, bg_style) = if is_selected {
            // Highlight selected command with brand color and background
            (
                theme.selected_bg_style(),
                theme.selection_bg_style(),
                theme.selection_bg_style(),
            )
        } else {
            (Style::default(), theme.muted_style(), Style::default())
        };

        // Calculate padding to fill the row with background
        let content_len = 1 + 1 + 12 + 1 + desc.len(); // space + / + cmd + space + desc
        let padding = inner_chunks[0].width.saturating_sub(content_len as u16) as usize;

        cmd_lines.push(Line::from(vec![
            Span::styled(" ", bg_style),
            Span::styled(format!("/{:<12}", cmd), cmd_style),
            Span::styled(format!(" {}", desc), desc_style),
            Span::styled(" ".repeat(padding), bg_style),
        ]));
    }

    if cmd_lines.is_empty() {
        cmd_lines.push(Line::from(vec![Span::styled(
            " No matching commands",
            theme.muted_style(),
        )]));
    }

    frame.render_widget(Paragraph::new(cmd_lines), inner_chunks[0]);

    // Render input line with prompt
    let input_line = Line::from(vec![
        Span::raw(" "),
        Span::styled(input, theme.brand_style()),
    ]);
    frame.render_widget(Paragraph::new(input_line), inner_chunks[1]);

    // Position cursor after the input text (accounting for leading space)
    frame.set_cursor_position((inner_chunks[1].x + 1 + cursor_pos as u16, inner_chunks[1].y));
}

/// Render board edit dialog overlay.
pub fn render_board_edit_dialog(
    frame: &mut ratatui::Frame,
    area: Rect,
    theme: &Theme,
    edit_mode: &BoardEditMode,
) {
    let (title, content_lines, show_input) = match edit_mode {
        BoardEditMode::Create { title } => (
            "New Board",
            vec![
                Line::from(vec![
                    Span::raw(" Title: "),
                    Span::styled(title.value(), theme.brand_style()),
                    Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled(" Enter", Style::default().bold()),
                    Span::styled(" create  ", theme.muted_style()),
                    Span::styled("Esc", Style::default().bold()),
                    Span::styled(" cancel", theme.muted_style()),
                ]),
            ],
            true,
        ),
        BoardEditMode::Edit { title, .. } => (
            "Edit Board",
            vec![
                Line::from(vec![
                    Span::raw(" Title: "),
                    Span::styled(title.value(), theme.brand_style()),
                    Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled(" Enter", Style::default().bold()),
                    Span::styled(" save  ", theme.muted_style()),
                    Span::styled("Esc", Style::default().bold()),
                    Span::styled(" cancel", theme.muted_style()),
                ]),
            ],
            true,
        ),
        BoardEditMode::Delete { title, .. } => (
            "Delete Board",
            vec![
                Line::from(vec![Span::styled(
                    format!(" Delete \"{}\"?", title),
                    Style::default().fg(Color::Red),
                )]),
                Line::from(""),
                Line::from(vec![
                    Span::styled(" y", Style::default().bold().fg(Color::Red)),
                    Span::styled(" delete  ", theme.muted_style()),
                    Span::styled("n/Esc", Style::default().bold()),
                    Span::styled(" cancel", theme.muted_style()),
                ]),
            ],
            false,
        ),
    };

    // Calculate popup size - centered dialog
    let popup_height = (content_lines.len() + 2) as u16; // +2 for border
    let popup_width = 40.min(area.width.saturating_sub(4));

    // Center the popup
    let popup_x = (area.width.saturating_sub(popup_width)) / 2;
    let popup_y = (area.height.saturating_sub(popup_height)) / 2;

    let popup_area = Rect {
        x: popup_x,
        y: popup_y,
        width: popup_width,
        height: popup_height,
    };

    // Clear background
    frame.render_widget(Clear, popup_area);

    // Draw popup with border
    let popup_block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(theme.brand_style());
    let inner = popup_block.inner(popup_area);
    frame.render_widget(popup_block, popup_area);

    // Render content
    frame.render_widget(Paragraph::new(content_lines), inner);

    // Position cursor if input mode
    if show_input {
        let cursor_pos = match edit_mode {
            BoardEditMode::Create { title } => title.visual_cursor(),
            BoardEditMode::Edit { title, .. } => title.visual_cursor(),
            _ => 0,
        };
        frame.set_cursor_position((inner.x + 8 + cursor_pos as u16, inner.y));
    }
}

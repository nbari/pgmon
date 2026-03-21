use crate::{
    pg::conninfo::describe_connection_target,
    tui::app::{App, DatabaseView, Tab},
};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Flex, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, Borders, Cell, Chart, Clear, Dataset, GraphType, Paragraph, Row, Table, Tabs,
        Wrap,
    },
};

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Tabs
            Constraint::Min(0),    // Content
            Constraint::Length(3), // Footer
        ])
        .split(f.area());

    if let (Some(tabs_area), Some(content), Some(footer)) =
        (chunks.first(), chunks.get(1), chunks.get(2))
    {
        draw_tabs(f, app, *tabs_area);
        if app.current_tab == Tab::Activity {
            draw_dashboard(f, app, *content);
        } else {
            draw_table(f, app, *content);
        }
        draw_footer(f, app, *footer);
    }

    if app.error_state.is_some() {
        draw_error_modal(f, app);
    } else if app.statement_detail.is_some() {
        draw_statement_detail_modal(f, app);
    } else if app.loading_state.is_some() {
        draw_loading_modal(f, app);
    }
}

fn draw_dashboard(f: &mut Frame, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(18), // Overview + chart
            Constraint::Min(0),     // Session browser
        ])
        .split(area);

    if let (Some(top), Some(bottom)) = (rows.first(), rows.get(1)) {
        let panels = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
            .split(*top);

        if let (Some(left), Some(right)) = (panels.first(), panels.get(1)) {
            draw_conn_chart(f, app, *left);
            draw_activity_summary_panel(f, app, *right);
        }
        draw_activity_sessions_panel(f, app, *bottom);
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn draw_conn_chart(f: &mut Frame, app: &App, area: Rect) {
    let history = &app.dashboard.conn_history;

    // Use the full chart width (minus borders) as the number of data points
    let chart_width = area.width.saturating_sub(10) as usize;
    let total_data = resample(history, chart_width, |&(_, _, t)| t as f64);
    let idle_data = resample(history, chart_width, |&(_, i, _)| i as f64);

    let (_, idle_now, total_now) = history.back().copied().unwrap_or((0, 0, 0));

    // auto-scale Y to observed peak + 10% headroom, minimum 10
    let observed_max = history
        .iter()
        .map(|(_, _, t)| *t)
        .max()
        .unwrap_or(0)
        .max(10);
    let max_y = (observed_max as f64 * 1.1).ceil();
    let x_max = chart_width.max(1) as f64;

    // idle severity: green if low, yellow if >50%, red if >80%
    let idle_pct = if total_now > 0 {
        idle_now * 100 / total_now
    } else {
        0
    };
    let idle_color = if idle_pct > 80 {
        Color::Red
    } else if idle_pct > 50 {
        Color::Yellow
    } else {
        Color::Green
    };

    let datasets = vec![
        Dataset::default()
            .marker(symbols::Marker::HalfBlock)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Cyan))
            .data(&total_data),
        Dataset::default()
            .marker(symbols::Marker::HalfBlock)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(idle_color))
            .data(&idle_data),
    ];

    let mid_y = max_y / 2.0;

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(Line::from(vec![
                    Span::raw(" "),
                    Span::styled("━", Style::default().fg(Color::Cyan)),
                    Span::styled(
                        format!(" connected: {total_now}  "),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled("━", Style::default().fg(idle_color)),
                    Span::styled(
                        format!(" idle: {idle_now} ({idle_pct}%)  "),
                        Style::default().fg(idle_color).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!("max: {} ", app.dashboard.summary.max_connections)),
                ])),
        )
        .x_axis(
            Axis::default()
                .style(Style::default().fg(Color::DarkGray))
                .bounds([0.0, x_max]),
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::DarkGray))
                .bounds([0.0, max_y])
                .labels(vec![
                    Span::raw("0"),
                    Span::raw(format!("{}", mid_y as i64)),
                    Span::raw(format!("{}", max_y as i64)),
                ]),
        );

    f.render_widget(chart, area);
}

/// Linearly interpolate history into `target_len` evenly spaced points.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn resample<F>(
    history: &std::collections::VecDeque<(i64, i64, i64)>,
    target_len: usize,
    extract: F,
) -> Vec<(f64, f64)>
where
    F: Fn(&(i64, i64, i64)) -> f64,
{
    if history.is_empty() || target_len == 0 {
        return Vec::new();
    }
    if history.len() == 1 {
        let v = extract(&history[0]);
        return (0..target_len).map(|i| (i as f64, v)).collect();
    }
    let src_len = history.len();
    (0..target_len)
        .map(|i| {
            let t = i as f64 / (target_len - 1).max(1) as f64;
            let pos = t * (src_len - 1) as f64;
            let lo = (pos as usize).min(src_len - 2);
            let hi = lo + 1;
            let frac = pos - lo as f64;
            let v = extract(&history[lo]) * (1.0 - frac) + extract(&history[hi]) * frac;
            (i as f64, v)
        })
        .collect()
}

fn draw_activity_summary_panel(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Activity Summary ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width < 12 || inner.height < 3 {
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(inner);

    if let Some(meta_row) = rows.first() {
        f.render_widget(Paragraph::new(activity_summary_meta_row(app)), *meta_row);
    }

    if let Some(summary_area) = rows.get(1) {
        draw_activity_summary_sections(f, app, *summary_area);
    }
}

fn activity_summary_meta_row(app: &App) -> Line<'static> {
    let summary = &app.dashboard.summary;

    Line::from(vec![
        Span::styled(
            format!("PostgreSQL {}", summary.server_version),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            describe_connection_target(&app.dsn),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("Ref. {}ms", app.refresh_ms),
            Style::default().fg(Color::White),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled("Duration: query", Style::default().fg(Color::White)),
    ])
}

fn draw_activity_summary_sections(f: &mut Frame, app: &App, area: Rect) {
    let sections = activity_summary_sections(app);
    if sections.is_empty() || area.width == 0 || area.height == 0 {
        return;
    }

    let columns = activity_summary_columns(area.width);
    let section_rows = activity_summary_rows(sections.len(), columns);
    let mut constraints = section_rows
        .iter()
        .map(|indexes| {
            let height = indexes
                .iter()
                .filter_map(|index| index.and_then(|section_index| sections.get(section_index)))
                .map(section_line_count)
                .max()
                .unwrap_or(1);
            Constraint::Length(u16::try_from(height).unwrap_or(u16::MAX))
        })
        .collect::<Vec<_>>();
    constraints.push(Constraint::Fill(1));

    let row_areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (row_index, indexes) in section_rows.iter().enumerate() {
        let Some(row_area) = row_areas.get(row_index) else {
            break;
        };

        let column_areas = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Fill(1); columns])
            .split(*row_area);

        for (column_index, section_index) in indexes.iter().enumerate() {
            let Some(section_area) = column_areas.get(column_index) else {
                continue;
            };
            let Some(section_index) = section_index else {
                continue;
            };
            let Some(section) = sections.get(*section_index) else {
                continue;
            };
            draw_activity_summary_section(f, *section_area, section);
        }
    }
}

fn activity_summary_sections(app: &App) -> Vec<ActivitySummarySection> {
    let summary = &app.dashboard.summary;
    let sessions = &summary.session_counts;
    let process = &summary.process;

    vec![
        ActivitySummarySection {
            title: "Global",
            metrics: vec![
                ActivitySummaryMetric::new("Uptime", format_uptime(summary.uptime_seconds)),
                ActivitySummaryMetric::new("DBs", summary.database_count.to_string()),
                ActivitySummaryMetric::new("Size", format_bytes(summary.total_database_bytes)),
                ActivitySummaryMetric::new("Growth/s", summary.rates.growth_bytes_per_sec.clone()),
                ActivitySummaryMetric::new("Cache hit", format!("{:.2}%", summary.cache_hit_pct)),
                ActivitySummaryMetric::new("Rollback", format!("{:.2}%", summary.rollback_pct)),
            ],
        },
        ActivitySummarySection {
            title: "Sessions",
            metrics: vec![
                ActivitySummaryMetric::new(
                    "Total / Max",
                    format!("{}/{}", sessions.total, summary.max_connections),
                ),
                ActivitySummaryMetric::new("Active", sessions.active.to_string()),
                ActivitySummaryMetric::new("Idle", sessions.idle.to_string()),
                ActivitySummaryMetric::new("Idle in txn", sessions.idle_in_transaction.to_string()),
                ActivitySummaryMetric::new(
                    "Idle txn abrt",
                    sessions.idle_in_transaction_aborted.to_string(),
                ),
                ActivitySummaryMetric::new("Waiting", sessions.waiting.to_string()),
            ],
        },
        ActivitySummarySection {
            title: "Activity",
            metrics: vec![
                ActivitySummaryMetric::new("TPS", summary.rates.tps.clone()),
                ActivitySummaryMetric::new("Insert/s", summary.rates.inserts_per_sec.clone()),
                ActivitySummaryMetric::new("Update/s", summary.rates.updates_per_sec.clone()),
                ActivitySummaryMetric::new("Delete/s", summary.rates.deletes_per_sec.clone()),
                ActivitySummaryMetric::new(
                    "Tuples/s",
                    summary.rates.tuples_returned_per_sec.clone(),
                ),
                ActivitySummaryMetric::new(
                    "Temp files/s",
                    summary.rates.temp_files_per_sec.clone(),
                ),
                ActivitySummaryMetric::new("Temp size/s", summary.rates.temp_bytes_per_sec.clone()),
            ],
        },
        ActivitySummarySection {
            title: "Workers & Replication",
            metrics: vec![
                ActivitySummaryMetric::new(
                    "Workers",
                    format!("{}/{}", process.worker_total, process.max_worker_processes),
                ),
                ActivitySummaryMetric::new(
                    "Logical",
                    format!(
                        "{}/{}",
                        process.logical_workers, process.max_logical_workers
                    ),
                ),
                ActivitySummaryMetric::new(
                    "Parallel",
                    format!(
                        "{}/{}",
                        process.parallel_workers, process.max_parallel_workers
                    ),
                ),
                ActivitySummaryMetric::new(
                    "Autovacuum",
                    format!(
                        "{}/{}",
                        process.autovacuum_workers, process.max_autovacuum_workers
                    ),
                ),
                ActivitySummaryMetric::new(
                    "WAL senders",
                    format!("{}/{}", process.wal_senders, process.max_wal_senders),
                ),
                ActivitySummaryMetric::new("WAL receivers", process.wal_receivers.to_string()),
                ActivitySummaryMetric::new(
                    "Repl. slots",
                    format!(
                        "{}/{}",
                        process.replication_slots, process.max_replication_slots
                    ),
                ),
            ],
        },
    ]
}

fn draw_activity_summary_section(f: &mut Frame, area: Rect, section: &ActivitySummarySection) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let content = activity_summary_section_lines(section, area.width);
    let widget = Paragraph::new(content);
    f.render_widget(widget, area);
}

fn activity_summary_section_lines(
    section: &ActivitySummarySection,
    width: u16,
) -> Vec<Line<'static>> {
    let mut lines = Vec::with_capacity(section.metrics.len() + 1);
    lines.push(Line::styled(
        section.title,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    lines.extend(
        section
            .metrics
            .iter()
            .map(|metric| activity_summary_metric_line(metric, width)),
    );
    lines
}

fn activity_summary_metric_line(metric: &ActivitySummaryMetric, width: u16) -> Line<'static> {
    let label = format!("{:<12}", metric.label);
    let value_width = metric.value.chars().count();
    let base_width = label.chars().count() + value_width + 1;
    let padding = usize::from(width).saturating_sub(base_width).max(1);

    Line::from(vec![
        Span::styled(label, Style::default().fg(Color::DarkGray)),
        Span::raw(" ".repeat(padding)),
        Span::styled(
            metric.value.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

fn activity_summary_columns(width: u16) -> usize {
    if width >= 84 {
        3
    } else if width >= 56 {
        2
    } else {
        1
    }
}

fn activity_summary_rows(section_count: usize, columns: usize) -> Vec<Vec<Option<usize>>> {
    if section_count == 0 || columns == 0 {
        return Vec::new();
    }

    if section_count == 4 && columns >= 3 {
        return vec![vec![Some(0), Some(1), Some(2)], vec![Some(3), None, None]];
    }

    let mut rows = Vec::new();
    let mut next_section = 0usize;

    while next_section < section_count {
        let mut row = Vec::with_capacity(columns);
        for _ in 0..columns {
            if next_section < section_count {
                row.push(Some(next_section));
                next_section += 1;
            } else {
                row.push(None);
            }
        }
        rows.push(row);
    }

    rows
}

fn section_line_count(section: &ActivitySummarySection) -> usize {
    section.metrics.len() + 1
}

struct ActivitySummarySection {
    title: &'static str,
    metrics: Vec<ActivitySummaryMetric>,
}

struct ActivitySummaryMetric {
    label: &'static str,
    value: String,
}

impl ActivitySummaryMetric {
    fn new(label: &'static str, value: String) -> Self {
        Self { label, value }
    }
}

fn draw_activity_sessions_panel(f: &mut Frame, app: &App, area: Rect) {
    let header_cells = [
        "PID", "XMIN", "Database", "App", "User", "Client", "Time+", "Waiting", "State", "Query",
    ];
    let widths = [
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(16),
        Constraint::Length(16),
        Constraint::Length(10),
        Constraint::Length(12),
        Constraint::Length(14),
        Constraint::Min(20),
    ];

    let rows = app
        .dashboard
        .sessions
        .iter()
        .enumerate()
        .map(|(i, session)| {
            let is_selected = app.selected_row == Some(i);
            let style = if is_selected {
                Style::default().fg(Color::Black).bg(Color::White)
            } else if session.blocked_by_count > 0 {
                Style::default().fg(Color::Yellow)
            } else if session.blocked_count > 0 {
                Style::default().fg(Color::Red)
            } else if matches!(
                session.state.as_str(),
                "idle in transaction" | "idle in transaction (aborted)"
            ) {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::Green)
            };
            let dim_style = if is_selected {
                style
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let wait_style = if is_selected {
                style
            } else {
                Style::default().fg(Color::Red)
            };
            let state_style = if is_selected {
                style
            } else {
                Style::default().fg(Color::Yellow)
            };
            Row::new(vec![
                Cell::from(session.pid.as_str()),
                Cell::from(session.xmin.as_str()),
                Cell::from(session.database.as_str()).style(dim_style),
                Cell::from(session.application.as_str()).style(dim_style),
                Cell::from(session.user.as_str()).style(dim_style),
                Cell::from(session.client.as_str()),
                Cell::from(format_duration_hms(session.duration_seconds)).style(
                    activity_time_style(is_selected, session.duration_seconds, style),
                ),
                Cell::from(activity_wait_cell(session)).style(wait_style),
                Cell::from(activity_state_cell(session)).style(state_style),
                Cell::from(session.query.as_str()),
            ])
            .style(style)
        });

    let count = app.dashboard.sessions.len();
    let title = format!(
        " Sessions: {} ({count}) | a:Active w:Waiting b:Blocking t:IdleInTxn ",
        app.activity_subview.label()
    );
    let table = Table::new(rows, widths)
        .header(
            Row::new(header_cells.iter().map(|h| Cell::from(*h)))
                .style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
                .bottom_margin(1),
        )
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
}

fn activity_wait_cell(session: &crate::pg::client::ActivitySession) -> String {
    if session.blocked_count > 0 {
        format!("blocking {}", session.blocked_count)
    } else if session.blocked_by_count > 0 {
        format!("blocked by {}", session.blocked_by_count)
    } else if session.wait_info.is_empty() {
        "-".to_string()
    } else {
        session.wait_info.clone()
    }
}

fn activity_state_cell(session: &crate::pg::client::ActivitySession) -> String {
    if session.blocked_count > 0 {
        format!("blocking | {}", session.state)
    } else {
        session.state.clone()
    }
}

fn activity_time_style(is_selected: bool, duration_seconds: i64, selected_style: Style) -> Style {
    if is_selected {
        return selected_style;
    }

    if duration_seconds < 3 {
        Style::default().fg(Color::Green)
    } else if duration_seconds < 10 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Red)
    }
}

fn format_duration_hms(duration_seconds: i64) -> String {
    let duration_seconds = duration_seconds.max(0);
    let hours = duration_seconds / 3600;
    let minutes = (duration_seconds % 3600) / 60;
    let seconds = duration_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn format_uptime(uptime_seconds: i64) -> String {
    let uptime_seconds = uptime_seconds.max(0);
    let days = uptime_seconds / 86_400;
    let hours = (uptime_seconds % 86_400) / 3600;
    let minutes = (uptime_seconds % 3600) / 60;

    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

fn format_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let negative = bytes < 0;
    let absolute = u128::from(bytes.unsigned_abs());
    let mut unit_index = 0usize;
    let mut divisor = 1u128;

    while unit_index + 1 < UNITS.len() && absolute >= divisor * 1024 {
        divisor *= 1024;
        unit_index += 1;
    }

    let unit = UNITS.get(unit_index).copied().unwrap_or("B");
    let sign = if negative { "-" } else { "" };

    if unit_index == 0 {
        format!("{sign}{absolute} {unit}")
    } else {
        let scaled_tenths = absolute.saturating_mul(10) / divisor;
        let whole = scaled_tenths / 10;
        let tenths = scaled_tenths % 10;
        format!("{sign}{whole}.{tenths} {unit}")
    }
}

fn draw_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles = vec![
        "1:Activity",
        "2:Database",
        "3:Locks",
        "4:IO",
        "5:Statements",
    ];
    let selected_tab = match app.current_tab {
        Tab::Activity => 0,
        Tab::Database => 1,
        Tab::Locks => 2,
        Tab::IO => 3,
        Tab::Statements => 4,
    };
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("Views"))
        .select(selected_tab)
        .style(Style::default().fg(Color::Cyan))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, area);
}

fn draw_table(f: &mut Frame, app: &App, area: Rect) {
    let header_cells = table_header_cells(app);
    let widths = table_widths(app);

    let rows = app.data.iter().enumerate().map(|(i, items)| {
        let style = table_row_style(app, i, items);
        Row::new(items.iter().map(|c| Cell::from(c.as_str()))).style(style)
    });

    let table = Table::new(rows, widths)
        .header(
            Row::new(header_cells.iter().map(|h| Cell::from(*h)))
                .style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
                .bottom_margin(1),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(table_title(app)),
        );
    f.render_widget(table, area);
}

fn table_header_cells(app: &App) -> Vec<&'static str> {
    match app.current_tab {
        Tab::Activity => vec![
            "PID", "User", "DB", "State", "Query", "Start", "App", "Client",
        ],
        Tab::Database => match &app.database_view {
            DatabaseView::Summary => vec![
                "DB",
                "Backends",
                "Commits",
                "Rollbacks",
                "Hit %",
                "Temp Bytes",
                "Deadlocks",
                "Reset",
            ],
            DatabaseView::Tables { .. } => vec!["Node", "Type", "Children/Rows", "Size"],
        },
        Tab::Locks => vec!["Target", "Mode", "Waiters", "Holders"],
        Tab::IO => vec!["Backend", "Read", "Write", "Time Read", "Time Write"],
        Tab::Statements => vec!["Query", "Total", "Mean", "Calls", "Read", "Write"],
    }
}

fn table_widths(app: &App) -> Vec<Constraint> {
    match app.current_tab {
        Tab::Activity => vec![
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Percentage(40),
            Constraint::Length(25),
            Constraint::Length(15),
            Constraint::Length(15),
        ],
        Tab::Database => match &app.database_view {
            DatabaseView::Summary => vec![
                Constraint::Length(18),
                Constraint::Length(10),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(25),
            ],
            DatabaseView::Tables { .. } => vec![
                Constraint::Percentage(45),
                Constraint::Length(18),
                Constraint::Length(16),
                Constraint::Length(16),
            ],
        },
        Tab::Locks => vec![
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
        Tab::IO => vec![
            Constraint::Percentage(35),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(14),
            Constraint::Length(14),
        ],
        Tab::Statements => vec![
            Constraint::Percentage(52),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(12),
        ],
    }
}

fn table_row_style(app: &App, row_index: usize, items: &[String]) -> Style {
    let is_schema_row = app.current_tab == Tab::Database
        && matches!(app.database_view, DatabaseView::Tables { .. })
        && items.get(1).is_some_and(|value| value == "schema");

    if app.selected_row == Some(row_index) {
        Style::default().fg(Color::Black).bg(Color::White)
    } else if is_schema_row {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    }
}

fn table_title(app: &App) -> String {
    match &app.database_view {
        DatabaseView::Summary if app.current_tab == Tab::Database => format!(
            " Database View | rows: {} | top-n: {} | Enter:Browse ",
            app.data.len(),
            app.top_n
        ),
        DatabaseView::Tables { database } if app.current_tab == Tab::Database => format!(
            " Database Tree | {database} | nodes: {} | Esc:Back ",
            app.data.len()
        ),
        _ => format!(
            " {:?} View | rows: {} | top-n: {} ",
            app.current_tab,
            app.data.len(),
            app.top_n
        ),
    }
}

fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let footer_text = if let Some(notice) = app.notice_state.as_ref() {
        notice.message.clone()
    } else if app.error_state.is_some() {
        "q:Quit | r:Retry | 1-5:Switch Tab".to_string()
    } else if app.current_tab == Tab::Activity {
        if let Some(session) = app
            .selected_row
            .and_then(|selected_row| app.dashboard.sessions.get(selected_row))
        {
            format!(
                "QUERY: {} | Enter:Save Query | a/w/b/t:Subviews",
                session.query.replace('\n', " ")
            )
        } else {
            "q:Quit | 1-5:Switch Tab | ↑↓:Navigate | Enter:Save Query | a/w/b/t:Subviews"
                .to_string()
        }
    } else if let Some(row_data) = app
        .selected_row
        .and_then(|selected_row| app.data.get(selected_row))
    {
        match app.current_tab {
            Tab::Statements => {
                let query = row_data.first().map_or("", String::as_str);
                format!(
                    "QUERY: {} | Enter:Save Query | i:Details",
                    query.replace('\n', " ")
                )
            }
            Tab::Database => match &app.database_view {
                DatabaseView::Summary => {
                    let database = row_data.first().map_or("", String::as_str);
                    format!("DB: {database} | Enter:Browse Tables")
                }
                DatabaseView::Tables { database } => {
                    let node = row_data.first().map_or("", String::as_str);
                    format!("DB: {database} | NODE: {} | Esc:Back", node.trim())
                }
            },
            _ => "q:Quit | 1-5:Switch Tab | ↑↓:Navigate".to_string(),
        }
    } else {
        match app.current_tab {
            Tab::Activity => {
                "q:Quit | 1-5:Switch Tab | ↑↓:Navigate | Enter:Save Query | a/w/b/t:Subviews"
                    .to_string()
            }
            Tab::Statements => {
                "q:Quit | 1-5:Switch Tab | ↑↓:Navigate | Enter:Save Query | i:Details".to_string()
            }
            Tab::Database => match &app.database_view {
                DatabaseView::Summary => {
                    "q:Quit | 1-5:Switch Tab | ↑↓:Navigate | Enter:Browse Tables".to_string()
                }
                DatabaseView::Tables { .. } => {
                    "q:Quit | 1-5:Switch Tab | ↑↓:Navigate | Esc:Back".to_string()
                }
            },
            _ => "q:Quit | 1-5:Switch Tab | ↑↓:Navigate".to_string(),
        }
    };

    let footer = Paragraph::new(Line::from(vec![Span::raw(footer_text)]))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, area);
}

fn draw_loading_modal(f: &mut Frame, app: &App) {
    let Some(loading_state) = app.loading_state.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 70, 7);
    let elapsed = loading_state.started_at.elapsed().as_secs();
    let content = vec![
        Line::styled(
            "Connecting",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(loading_state.message.as_str()),
        Line::raw(format!("Elapsed: {elapsed}s")),
        Line::raw("Press q to quit."),
    ];

    let modal = Paragraph::new(content).alignment(Alignment::Center).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Connection ")
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, area);
    f.render_widget(modal, area);
}

fn draw_statement_detail_modal(f: &mut Frame, app: &App) {
    let Some(detail) = app.statement_detail.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 85, 14);
    let content = vec![
        Line::styled(
            "Statement Detail",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(format!(
            "total: {} ms | mean: {} ms | calls: {}",
            detail.total_time, detail.mean_time, detail.calls
        )),
        Line::raw(format!(
            "read: {} ms | write: {} ms",
            detail.read_time, detail.write_time
        )),
        Line::raw(""),
        Line::raw(detail.query.as_str()),
        Line::raw(""),
        Line::styled(
            "Enter:Save Query | Esc:Close",
            Style::default().fg(Color::Cyan),
        ),
    ];

    let modal = Paragraph::new(content).wrap(Wrap { trim: true }).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Statement ")
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, area);
    f.render_widget(modal, area);
}

fn draw_error_modal(f: &mut Frame, app: &App) {
    let Some(error_state) = app.error_state.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 80, 9);
    let content = vec![
        Line::styled(
            error_state.title.as_str(),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(error_state.details.as_str()),
        Line::raw(""),
        Line::styled(
            "Press r to retry or q to quit.",
            Style::default().fg(Color::Yellow),
        ),
    ];

    let modal = Paragraph::new(content)
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Error ")
                .style(Style::default().bg(Color::Black)),
        );

    f.render_widget(Clear, area);
    f.render_widget(modal, area);
}

fn centered_rect(area: Rect, width_percentage: u16, height: u16) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(height),
        Constraint::Fill(1),
    ])
    .flex(Flex::Center)
    .split(area);
    let Some(mid_row) = vertical.get(1) else {
        return area;
    };

    let horizontal = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Percentage(width_percentage),
        Constraint::Fill(1),
    ])
    .flex(Flex::Center)
    .split(*mid_row);

    horizontal.get(1).copied().unwrap_or(*mid_row)
}

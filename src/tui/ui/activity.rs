use crate::tui::app::{
    ActivityChartMetric, ActivitySummaryMetric, ActivitySummarySection, App,
    format::{
        activity_state_cell, activity_wait_cell, format_activity_query, format_bytes,
        format_duration_hms, format_uptime,
    },
};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Cell, Chart, Dataset, GraphType, Paragraph, Row, Table},
};

pub fn draw_dashboard(f: &mut Frame, app: &mut App, area: Rect) {
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
            draw_activity_chart(f, app, *left);
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
fn draw_activity_chart(f: &mut Frame, app: &App, area: Rect) {
    let bounds = ChartBounds::new(area);

    match app.activity_chart_metric {
        ActivityChartMetric::Connections => {
            draw_connections_chart(f, app, area, bounds);
        }
        ActivityChartMetric::Tps => draw_scalar_chart(
            f,
            area,
            bounds,
            ActivityChartMetric::Tps,
            &app.dashboard.chart_history.tps,
            Color::Cyan,
            false,
        ),
        ActivityChartMetric::Dml => draw_dml_chart(f, app, area, bounds),
        ActivityChartMetric::TempBytesPerSec => draw_scalar_chart(
            f,
            area,
            bounds,
            ActivityChartMetric::TempBytesPerSec,
            &app.dashboard.chart_history.temp_bytes_per_sec,
            Color::Magenta,
            true,
        ),
        ActivityChartMetric::GrowthBytesPerSec => draw_scalar_chart(
            f,
            area,
            bounds,
            ActivityChartMetric::GrowthBytesPerSec,
            &app.dashboard.chart_history.growth_bytes_per_sec,
            Color::LightBlue,
            true,
        ),
    }
}

#[derive(Clone, Copy)]
struct ChartBounds {
    width: usize,
    x_max: f64,
}

impl ChartBounds {
    fn new(area: Rect) -> Self {
        let width_u16 = area.width.saturating_sub(10);
        Self {
            width: usize::from(width_u16),
            x_max: f64::from(width_u16.max(1)),
        }
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn draw_connections_chart(f: &mut Frame, app: &App, area: Rect, bounds: ChartBounds) {
    let history = &app.dashboard.chart_history.connections;
    let total_data = resample(history, bounds.width, |&(_, _, total)| total as f64);
    let idle_data = resample(history, bounds.width, |&(_, idle, _)| idle as f64);
    let (_, idle_now, total_now) = history.back().copied().unwrap_or((0, 0, 0));
    let observed_max = history
        .iter()
        .map(|(_, _, total)| *total)
        .max()
        .unwrap_or(0)
        .max(10);
    let max_y = (observed_max as f64 * 1.1).ceil();
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
        single_line_dataset(&total_data, Color::Cyan),
        single_line_dataset(&idle_data, idle_color),
    ];
    let title = Line::from(vec![
        chart_metric_title(ActivityChartMetric::Connections),
        Span::styled("━", Style::default().fg(Color::Cyan)),
        Span::styled(
            format!(" total: {total_now}  "),
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
    ]);
    render_chart(
        f,
        area,
        datasets,
        title,
        bounds.x_max,
        max_y,
        chart_axis_labels(max_y, false),
    );
}

fn draw_dml_chart(f: &mut Frame, app: &App, area: Rect, bounds: ChartBounds) {
    let history = &app.dashboard.chart_history;
    let insert_data = resample(&history.inserts_per_sec, bounds.width, |value| *value);
    let update_data = resample(&history.updates_per_sec, bounds.width, |value| *value);
    let delete_data = resample(&history.deletes_per_sec, bounds.width, |value| *value);
    let insert_now = history.inserts_per_sec.back().copied().unwrap_or(0.0);
    let update_now = history.updates_per_sec.back().copied().unwrap_or(0.0);
    let delete_now = history.deletes_per_sec.back().copied().unwrap_or(0.0);
    let max_y = chart_max(
        history
            .inserts_per_sec
            .iter()
            .chain(history.updates_per_sec.iter())
            .chain(history.deletes_per_sec.iter())
            .copied(),
        1.0,
    );
    let datasets = vec![
        single_line_dataset(&insert_data, Color::Green),
        single_line_dataset(&update_data, Color::Yellow),
        single_line_dataset(&delete_data, Color::Red),
    ];
    let title = Line::from(vec![
        chart_metric_title(ActivityChartMetric::Dml),
        Span::styled("━", Style::default().fg(Color::Green)),
        Span::styled(
            format!(" ins: {}  ", format_rate_value(insert_now)),
            Style::default().fg(Color::Green),
        ),
        Span::styled("━", Style::default().fg(Color::Yellow)),
        Span::styled(
            format!(" upd: {}  ", format_rate_value(update_now)),
            Style::default().fg(Color::Yellow),
        ),
        Span::styled("━", Style::default().fg(Color::Red)),
        Span::styled(
            format!(" del: {} ", format_rate_value(delete_now)),
            Style::default().fg(Color::Red),
        ),
    ]);
    render_chart(
        f,
        area,
        datasets,
        title,
        bounds.x_max,
        max_y,
        chart_axis_labels(max_y, false),
    );
}

fn draw_scalar_chart(
    f: &mut Frame,
    area: Rect,
    bounds: ChartBounds,
    metric: ActivityChartMetric,
    history: &std::collections::VecDeque<f64>,
    color: Color,
    bytes_mode: bool,
) {
    let data = resample(history, bounds.width, |value| *value);
    let current = history.back().copied().unwrap_or(0.0);
    let max_y = chart_max(history.iter().copied(), 1.0);
    let datasets = vec![single_line_dataset(&data, color)];
    let title = Line::from(vec![
        chart_metric_title(metric),
        Span::styled(
            format!("current: {}", format_chart_current(current, bytes_mode)),
            Style::default().fg(color),
        ),
    ]);
    render_chart(
        f,
        area,
        datasets,
        title,
        bounds.x_max,
        max_y,
        chart_axis_labels(max_y, bytes_mode),
    );
}

fn chart_metric_title(metric: ActivityChartMetric) -> Span<'static> {
    Span::styled(
        format!(" {} (m) ", metric.label()),
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )
}

fn format_chart_current(value: f64, bytes_mode: bool) -> String {
    if bytes_mode {
        format!(
            "{}/s",
            format_bytes(round_to_i64_saturating(value.max(0.0)))
        )
    } else {
        format_rate_value(value)
    }
}

/// Linearly interpolate history into `target_len` evenly spaced points.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn resample<T, F>(
    history: &std::collections::VecDeque<T>,
    target_len: usize,
    extract: F,
) -> Vec<(f64, f64)>
where
    F: Fn(&T) -> f64,
{
    if target_len == 0 {
        return Vec::new();
    }

    let Some(first) = history.front() else {
        return Vec::new();
    };

    if history.len() == 1 {
        let v = extract(first);
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
            let lo_value = history.get(lo).map_or_else(|| extract(first), &extract);
            let hi_value = history.get(hi).map_or(lo_value, &extract);
            let v = lo_value * (1.0 - frac) + hi_value * frac;
            (i as f64, v)
        })
        .collect()
}

fn single_line_dataset(data: &[(f64, f64)], color: Color) -> Dataset<'_> {
    Dataset::default()
        .marker(symbols::Marker::HalfBlock)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(color))
        .data(data)
}

fn chart_max(values: impl Iterator<Item = f64>, minimum: f64) -> f64 {
    let observed_max = values.fold(minimum, f64::max).max(minimum);
    (observed_max * 1.1).ceil()
}

fn chart_axis_labels(max_y: f64, bytes_mode: bool) -> Vec<Span<'static>> {
    let mid_y = max_y / 2.0;
    vec![
        Span::raw(format_chart_value(0.0, bytes_mode)),
        Span::raw(format_chart_value(mid_y, bytes_mode)),
        Span::raw(format_chart_value(max_y, bytes_mode)),
    ]
}

fn format_chart_value(value: f64, bytes_mode: bool) -> String {
    if bytes_mode {
        format_bytes(round_to_i64_saturating(value.max(0.0)))
    } else {
        format_rate_value(value)
    }
}

fn format_rate_value(value: f64) -> String {
    if value < 10.0 {
        format!("{value:.1}")
    } else {
        round_to_i64_saturating(value).to_string()
    }
}

fn round_to_i64_saturating(value: f64) -> i64 {
    const I64_MAX_F64: f64 = 9_223_372_036_854_775_807.0;
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;

    if !value.is_finite() {
        return 0;
    }

    let rounded = value.round();
    if rounded >= I64_MAX_F64 {
        return i64::MAX;
    }
    if rounded <= I64_MIN_F64 {
        return i64::MIN;
    }

    format!("{rounded:.0}")
        .parse::<i64>()
        .unwrap_or(if rounded.is_sign_negative() {
            i64::MIN
        } else {
            i64::MAX
        })
}

fn render_chart(
    f: &mut Frame,
    area: Rect,
    datasets: Vec<Dataset<'_>>,
    title: Line<'_>,
    x_max: f64,
    max_y: f64,
    y_labels: Vec<Span<'static>>,
) {
    let chart = Chart::new(datasets)
        .block(Block::default().borders(Borders::ALL).title(title))
        .x_axis(
            Axis::default()
                .style(Style::default().fg(Color::DarkGray))
                .bounds([0.0, x_max]),
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::DarkGray))
                .bounds([0.0, max_y])
                .labels(y_labels),
        );

    f.render_widget(chart, area);
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
    let latency = app.connection_last_refresh_duration().map_or_else(
        || "Latency -".to_string(),
        |duration| format!("Latency {}ms", duration.as_millis()),
    );
    let status = if app.is_offline() {
        let retry_in = app
            .offline_retry_remaining()
            .map_or(0, |duration| duration.as_secs());
        format!("Offline (retry {retry_in}s)")
    } else {
        let last_ok = app
            .connection_last_refresh_elapsed()
            .map_or(0, |duration| duration.as_secs());
        format!("Last ok {last_ok}s ago")
    };

    Line::from(vec![
        Span::styled(
            format!("PostgreSQL {}", summary.server_version),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            app.connection_target().to_string(),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("Refresh(r) {}ms", app.refresh_ms),
            Style::default().fg(Color::White),
        ),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(latency, Style::default().fg(Color::White)),
        Span::styled("  |  ", Style::default().fg(Color::DarkGray)),
        Span::styled(status, Style::default().fg(Color::White)),
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
                ActivitySummaryMetric::new("Commits", summary.total_commits.to_string()),
                ActivitySummaryMetric::new("Rollbacks", summary.total_rollbacks.to_string()),
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
    if width >= 56 { 2 } else { 1 }
}

fn activity_summary_rows(section_count: usize, columns: usize) -> Vec<Vec<Option<usize>>> {
    if section_count == 0 || columns == 0 {
        return Vec::new();
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

fn draw_activity_sessions_panel(f: &mut Frame, app: &mut App, area: Rect) {
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
            let white_style = if is_selected {
                style
            } else {
                Style::default().fg(Color::White)
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
                Cell::from(session.pid.as_str()).style(white_style),
                Cell::from(session.xmin.as_str()).style(white_style),
                Cell::from(session.database.as_str()).style(dim_style),
                Cell::from(session.application.as_str()).style(dim_style),
                Cell::from(session.user.as_str()).style(dim_style),
                Cell::from(session.client.as_str()).style(white_style),
                Cell::from(format_duration_hms(session.duration_seconds)).style(
                    activity_time_style(is_selected, session.duration_seconds, style),
                ),
                Cell::from(activity_wait_cell(session)).style(wait_style),
                Cell::from(activity_state_cell(session)).style(state_style),
                Cell::from(format_activity_query(session)).style(white_style),
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
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().fg(Color::Black).bg(Color::White));

    f.render_stateful_widget(table, area, &mut app.table_state);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_chart_current_formats_rates_and_bytes() {
        assert_eq!(format_chart_current(1.25, false), "1.2");
        assert_eq!(format_chart_current(2048.0, true), "2.0 KiB/s");
    }

    #[test]
    fn test_chart_max_applies_minimum_and_headroom() {
        assert!((chart_max([0.0, 0.5].into_iter(), 1.0) - 2.0).abs() < f64::EPSILON);
        assert!((chart_max([12.0, 18.0].into_iter(), 1.0) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_activity_summary_columns_switches_to_two_columns_at_threshold() {
        assert_eq!(activity_summary_columns(55), 1);
        assert_eq!(activity_summary_columns(56), 2);
    }

    #[test]
    fn test_activity_summary_rows_pads_last_row() {
        assert_eq!(
            activity_summary_rows(3, 2),
            vec![vec![Some(0), Some(1)], vec![Some(2), None]]
        );
        assert!(activity_summary_rows(0, 2).is_empty());
    }

    #[test]
    fn test_activity_time_style_uses_threshold_colors() {
        assert_eq!(
            activity_time_style(false, 2, Style::default()).fg,
            Some(Color::Green)
        );
        assert_eq!(
            activity_time_style(false, 5, Style::default()).fg,
            Some(Color::Yellow)
        );
        assert_eq!(
            activity_time_style(false, 12, Style::default()).fg,
            Some(Color::Red)
        );
    }
}

use crate::tui::app::{
    ActivityChartMetric, ActivitySubview, ActivitySummaryMetric, ActivitySummarySection, App,
    format::{
        activity_state_cell, activity_wait_cell, format_activity_query, format_bytes, format_uptime,
    },
};
use chrono::{DateTime, Utc};
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
            app.config
                .get_view_color("chart", "line1")
                .unwrap_or(Color::Cyan),
            false,
        ),
        ActivityChartMetric::Dml => draw_dml_chart(f, app, area, bounds),
        ActivityChartMetric::TempBytesPerSec => draw_scalar_chart(
            f,
            area,
            bounds,
            ActivityChartMetric::TempBytesPerSec,
            &app.dashboard.chart_history.temp_bytes_per_sec,
            app.config
                .get_view_color("chart", "line1")
                .unwrap_or(Color::Magenta),
            true,
        ),
        ActivityChartMetric::GrowthBytesPerSec => draw_scalar_chart(
            f,
            area,
            bounds,
            ActivityChartMetric::GrowthBytesPerSec,
            &app.dashboard.chart_history.growth_bytes_per_sec,
            app.config
                .get_view_color("chart", "line1")
                .unwrap_or(Color::LightBlue),
            true,
        ),
        ActivityChartMetric::Checkpoints => draw_checkpoints_panel(f, app, area),
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
    let line1_color = app
        .config
        .get_view_color("chart", "line1")
        .unwrap_or(Color::Cyan);
    let datasets = vec![
        single_line_dataset(&total_data, line1_color),
        single_line_dataset(&idle_data, idle_color),
    ];
    let title = Line::from(vec![
        chart_metric_title(ActivityChartMetric::Connections),
        Span::styled("━", Style::default().fg(line1_color)),
        Span::styled(
            format!(" total: {total_now}  "),
            Style::default()
                .fg(line1_color)
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
    let line1_color = app
        .config
        .get_view_color("chart", "line1")
        .unwrap_or(Color::Green);
    let line2_color = app
        .config
        .get_view_color("chart", "line2")
        .unwrap_or(Color::Yellow);
    let line3_color = app
        .config
        .get_view_color("chart", "line3")
        .unwrap_or(Color::Red);

    let datasets = vec![
        single_line_dataset(&insert_data, line1_color),
        single_line_dataset(&update_data, line2_color),
        single_line_dataset(&delete_data, line3_color),
    ];
    let title = Line::from(vec![
        chart_metric_title(ActivityChartMetric::Dml),
        Span::styled("━", Style::default().fg(line1_color)),
        Span::styled(
            format!(" ins: {}  ", format_rate_value(insert_now)),
            Style::default().fg(line1_color),
        ),
        Span::styled("━", Style::default().fg(line2_color)),
        Span::styled(
            format!(" upd: {}  ", format_rate_value(update_now)),
            Style::default().fg(line2_color),
        ),
        Span::styled("━", Style::default().fg(line3_color)),
        Span::styled(
            format!(" del: {} ", format_rate_value(delete_now)),
            Style::default().fg(line3_color),
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

/// Cumulative checkpoints chart: timed (driven by `checkpoint_timeout`) vs
/// requested (driven by `max_wal_size` or a manual `CHECKPOINT`). A rising
/// requested line means WAL is filling before the timeout elapses — a signal to
/// raise `max_wal_size`. The title also surfaces the two relevant settings.
/// Render the Checkpoints readout (in place of a chart, since checkpoints are rare
/// discrete events). Shows whether checkpoints are timeout- or `max_wal_size`-driven
/// and how much I/O they cause, with an operator-facing verdict.
fn draw_checkpoints_panel(f: &mut Frame, app: &App, area: Rect) {
    let cp = &app.dashboard.summary.checkpoint;
    let total = cp.timed.saturating_add(cp.requested);
    let req_pct = checkpoint_request_pct(cp.timed, cp.requested);
    let mb_per = checkpoint_mb_per(cp.buffers_written, total);
    let write_per = per_checkpoint_ms(cp.write_time_ms, total);
    let sync_per = per_checkpoint_ms(cp.sync_time_ms, total);
    let interval = checkpoint_interval_seconds(Utc::now(), cp.stats_reset, total);
    let wal_rate = wal_bytes_per_second(cp.wal_bytes, cp.wal_elapsed_seconds);
    let (verdict, verdict_color) = checkpoint_verdict(
        cp.in_recovery,
        cp.seconds_since_last_checkpoint,
        interval,
        cp.checkpoint_timeout_seconds,
        mb_per,
        write_per + sync_per,
        total,
    );

    let interval_str = interval.map_or_else(|| "-".to_string(), format_seconds_short);
    let last_ckpt_str = {
        let age = cp.seconds_since_last_checkpoint.map_or_else(
            || "-".to_string(),
            |age| format!("{} ago", format_seconds_short(age)),
        );
        if cp.in_recovery {
            format!("{age} (standby)")
        } else {
            age
        }
    };

    let lines = vec![
        checkpoint_metric_line(
            "Interval",
            format!(
                "{interval_str}  (timeout {})",
                format_seconds_short(cp.checkpoint_timeout_seconds)
            ),
        ),
        checkpoint_metric_line("Last ckpt", last_ckpt_str),
        checkpoint_metric_line(
            "Frequency",
            format!(
                "{} timed · {} req  ({req_pct:.0}% req)",
                cp.timed, cp.requested
            ),
        ),
        checkpoint_metric_line(
            "I/O / ckpt",
            format!(
                "{mb_per:.1} MB · write {} · sync {}",
                format_millis(write_per),
                format_millis(sync_per)
            ),
        ),
        checkpoint_metric_line(
            "WAL",
            format!(
                "{}/s · {}",
                format_bytes(round_to_i64_saturating(wal_rate)),
                format_bytes(cp.wal_bytes)
            ),
        ),
        checkpoint_metric_line(
            "Segment",
            format!(
                "seg {} · min {}",
                format_bytes(cp.wal_segment_size_bytes),
                format_megabytes(cp.min_wal_size_mb)
            ),
        ),
        checkpoint_metric_line(
            "Tuning",
            format!(
                "max_wal {} · compl {:.1}",
                format_megabytes(cp.max_wal_size_mb),
                cp.completion_target
            ),
        ),
        Line::from(vec![
            Span::styled("→ ", Style::default().fg(verdict_color)),
            Span::styled(
                verdict,
                Style::default()
                    .fg(verdict_color)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];

    let title = Line::from(chart_metric_title(ActivityChartMetric::Checkpoints));
    let block = Block::default().borders(Borders::ALL).title(title);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

/// One `label  value` line for the checkpoints readout.
fn checkpoint_metric_line(label: &'static str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(" {label:<11}"),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            value,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

/// Average megabytes written per checkpoint (buffers are 8 KiB pages).
fn checkpoint_mb_per(buffers_written: i64, count: i64) -> f64 {
    if count <= 0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let bytes = buffers_written.max(0) as f64 * 8192.0;
    #[allow(clippy::cast_precision_loss)]
    let per = bytes / count as f64;
    per / (1024.0 * 1024.0)
}

/// Average milliseconds per checkpoint for a cumulative time counter.
fn per_checkpoint_ms(total_ms: f64, count: i64) -> f64 {
    if count <= 0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let divisor = count as f64;
    (total_ms.max(0.0)) / divisor
}

/// Average seconds between checkpoints since stats were last reset, using the
/// total checkpoint count (`timed + requested`). On low-write systems scheduled
/// checkpoints are often skipped (so `timed` can be 0) while the system still
/// checkpoints on a healthy cadence; dividing by the total reflects the real
/// interval instead of returning nothing.
fn checkpoint_interval_seconds(
    now: DateTime<Utc>,
    stats_reset: Option<DateTime<Utc>>,
    total: i64,
) -> Option<i64> {
    let reset = stats_reset?;
    if total <= 0 {
        return None;
    }
    let elapsed = now.signed_duration_since(reset).num_seconds();
    if elapsed <= 0 {
        return None;
    }
    Some(elapsed / total)
}

/// Average WAL bytes generated per second since `pg_stat_wal` was last reset. A
/// tiny rate is the decisive signal that `max_wal_size` cannot be the checkpoint
/// trigger, regardless of how `PostgreSQL` labels the checkpoints.
fn wal_bytes_per_second(wal_bytes: i64, elapsed_seconds: f64) -> f64 {
    if elapsed_seconds <= 0.0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let bytes = wal_bytes.max(0) as f64;
    bytes / elapsed_seconds
}

/// Format a millisecond duration compactly (`7ms`, `1.2s`).
fn format_millis(ms: f64) -> String {
    if ms < 1000.0 {
        format!("{ms:.0}ms")
    } else {
        format!("{:.1}s", ms / 1000.0)
    }
}

/// Threshold (MiB written per checkpoint) above which frequent checkpoints are
/// treated as genuine `max_wal_size` pressure rather than trivial-I/O churn.
/// Calibrated against a low-write production node that flushed ~0.1 MB/ckpt yet was
/// labelled "wal"/requested by `PostgreSQL` because of forced segment switches.
const CHECKPOINT_MB_PRESSURE_THRESHOLD: f64 = 16.0;

/// A checkpoint that has not completed in more than this multiple of
/// `checkpoint_timeout` indicates a lagging or stalled checkpointer — the genuinely
/// dangerous condition (a restart in this state can trigger very long WAL replay).
const CHECKPOINT_STALE_FACTOR: i64 = 2;

/// Operator verdict for the checkpoints readout, ordered by operational severity.
///
/// The dangerous condition is a **lagging/stalled checkpointer**, not the
/// `max_wal_size` label: if checkpoints stop completing (or take longer than a whole
/// cycle), a restart can force enormous WAL replay. So staleness and overrun are
/// checked first and flagged red. Frequency is only advisory — `PostgreSQL` removed
/// `checkpoint_warning` in 18 and short write bursts are normal — so frequent
/// checkpoints are yellow, and we only mention `max_wal_size` when they also flush
/// real I/O. On a standby the counters describe restartpoints, so the wording
/// adapts. Note: this tool never recommends changing `checkpoint_timeout`, which is
/// an availability/RTO trade-off rather than a throughput knob.
fn checkpoint_verdict(
    in_recovery: bool,
    seconds_since_last: Option<i64>,
    interval_seconds: Option<i64>,
    timeout_seconds: i64,
    mb_per_ckpt: f64,
    total_ms_per_ckpt: f64,
    total: i64,
) -> (&'static str, Color) {
    if total <= 0 {
        return ("No checkpoints yet", Color::DarkGray);
    }
    // 1) Stalled: nothing has completed in well over the timeout.
    let stalled = matches!(
        (seconds_since_last, timeout_seconds),
        (Some(age), timeout)
            if timeout > 0 && age > timeout.saturating_mul(CHECKPOINT_STALE_FACTOR)
    );
    if stalled {
        return if in_recovery {
            ("Restartpointer lagging (stale)", Color::Red)
        } else {
            ("Checkpointer lagging (stale)", Color::Red)
        };
    }
    // 2) Overrunning: an average checkpoint takes longer than a whole cycle to
    //    finish despite doing real I/O — storage cannot keep up.
    let total_ms = round_to_i64_saturating(total_ms_per_ckpt);
    let overrunning =
        timeout_seconds > 0 && mb_per_ckpt > 0.0 && total_ms > timeout_seconds.saturating_mul(1000);
    if overrunning {
        return if in_recovery {
            ("Restartpoints overrun timeout (I/O-bound)", Color::Red)
        } else {
            ("Checkpoints overrun timeout (I/O-bound)", Color::Red)
        };
    }
    // 3) Frequency is advisory: frequent + real I/O hints at max_wal_size; frequent
    //    + trivial I/O is manual/DDL/backup/segment churn, not max_wal_size.
    let too_frequent = matches!(
        (interval_seconds, timeout_seconds),
        (Some(interval), timeout) if timeout > 0 && interval.saturating_mul(2) < timeout
    );
    if too_frequent {
        if mb_per_ckpt >= CHECKPOINT_MB_PRESSURE_THRESHOLD {
            return ("Frequent + I/O: consider max_wal_size", Color::Yellow);
        }
        return ("Frequent low-I/O: manual/DDL/backup", Color::Yellow);
    }
    if in_recovery {
        ("Healthy (restartpoints keeping up)", Color::Green)
    } else {
        ("Healthy (timeout-paced)", Color::Green)
    }
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
                .fg(app
                    .config
                    .get_view_color("table", "header_fg")
                    .unwrap_or(Color::Yellow))
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

/// Share of checkpoints triggered by `max_wal_size` (or a manual `CHECKPOINT`)
/// rather than `checkpoint_timeout`, as a percentage of all checkpoints. A high
/// value indicates WAL fills before the timeout elapses, suggesting `max_wal_size`
/// should be raised. Returns 0.0 when no checkpoints have occurred yet.
fn checkpoint_request_pct(timed: i64, requested: i64) -> f64 {
    let total = timed.saturating_add(requested);
    if total <= 0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let pct = requested as f64 / total as f64 * 100.0;
    pct
}

/// Format a duration given in whole seconds compactly (e.g. `30s`, `5min`,
/// `1m30s`), used for the `checkpoint_timeout` setting.
fn format_seconds_short(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds % 60 == 0 {
        format!("{}min", seconds / 60)
    } else {
        format!("{}m{}s", seconds / 60, seconds % 60)
    }
}

/// Format a size given in whole megabytes (as `PostgreSQL` reports `max_wal_size`)
/// using the shared byte formatter (e.g. `1.0 GiB`).
fn format_megabytes(megabytes: i64) -> String {
    format_bytes(megabytes.saturating_mul(1024 * 1024))
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

/// Accent color used to highlight the active session subview in the sessions panel
/// title. Mirrors the per-state row colors so the selected filter reads the same as
/// the rows it shows (Active=Green, Waiting=Yellow, Blocking=Magenta,
/// Idle in transaction=Cyan).
fn subview_indicator_color(subview: ActivitySubview) -> Color {
    match subview {
        ActivitySubview::All => Color::White,
        ActivitySubview::Active => Color::Green,
        ActivitySubview::Waiting => Color::Yellow,
        ActivitySubview::Blocking => Color::Magenta,
        ActivitySubview::IdleInTransaction => Color::Cyan,
    }
}

/// Build the sessions panel title, highlighting the currently active subview filter
/// (bold, in its accent color) so it stands out from the dimmed inactive ones.
fn activity_sessions_title(count: usize, active: ActivitySubview) -> Line<'static> {
    let mut spans = vec![Span::raw(format!(" Sessions ({count}) | "))];
    for (index, subview) in ActivitySubview::ALL.into_iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw(" "));
        }
        let text = format!("{}:{}", subview.label(), subview.name());
        let style = if subview == active {
            Style::default()
                .fg(subview_indicator_color(subview))
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        spans.push(Span::styled(text, style));
    }
    spans.push(Span::raw(" "));
    Line::from(spans)
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
            session_to_row(session, is_selected)
        });

    let count = app.dashboard.sessions.len();
    let title = activity_sessions_title(count, app.activity_subview);
    let table = Table::new(rows, widths)
        .header(
            Row::new(header_cells.iter().map(|h| Cell::from(*h)))
                .style(
                    Style::default()
                        .fg(app
                            .config
                            .get_view_color("table", "header_fg")
                            .unwrap_or(Color::Yellow))
                        .add_modifier(Modifier::BOLD),
                )
                .bottom_margin(1),
        )
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(
            Style::default()
                .fg(app
                    .config
                    .get_view_color("table", "highlight_fg")
                    .unwrap_or(Color::Black))
                .bg(app
                    .config
                    .get_view_color("table", "highlight_bg")
                    .unwrap_or(Color::White)),
        );

    f.render_stateful_widget(table, area, &mut app.table_state);
}

/// Color used for an Activity session row, by severity. Mirrors how `pg_activity`
/// distinguishes states: a transaction left `idle in transaction (aborted)` is the
/// most dangerous idle state (it still holds locks/snapshot but will only roll
/// back), so it gets a distinct, more alarming color than a plain
/// `idle in transaction`.
fn session_state_color(session: &crate::pg::client::ActivitySession) -> Color {
    if session.blocked_by_count > 0 {
        Color::Red // Blocked sessions are Red (CRITICAL)
    } else if session.blocked_count > 0 {
        Color::Magenta // Blocking sessions are Magenta (ACTION REQUIRED)
    } else if session.state.contains("idle in transaction (aborted)") {
        Color::LightRed // Aborted idle-in-transaction is the riskiest idle state
    } else if session.state.contains("idle in transaction") {
        Color::Cyan // Idle in transaction is Cyan (WARNING)
    } else if session.state == "active" {
        Color::Green // Active is Green (OK)
    } else {
        Color::DarkGray // Idle/Other is Dim
    }
}

fn session_to_row(session: &crate::pg::client::ActivitySession, is_selected: bool) -> Row<'_> {
    // 1. Determine Row Style (Warnings)
    let base_style = if is_selected {
        Style::default().fg(Color::Black).bg(Color::White)
    } else {
        Style::default().fg(session_state_color(session))
    };

    let white_style = if is_selected {
        base_style
    } else {
        Style::default().fg(Color::White)
    };
    let dim_style = if is_selected {
        base_style
    } else {
        Style::default().fg(Color::DarkGray)
    };

    Row::new(vec![
        Cell::from(session.pid.as_str()).style(white_style),
        Cell::from(session.xmin.as_str()).style(white_style),
        Cell::from(session.database.as_str()).style(dim_style),
        Cell::from(session.application.as_str()).style(dim_style),
        Cell::from(session.user.as_str()).style(dim_style),
        Cell::from(session.client.as_str()).style(white_style),
        Cell::from(crate::tui::app::format::format_duration_hms(
            session.duration_seconds,
        ))
        .style(activity_time_style(
            is_selected,
            session.duration_seconds,
            base_style,
        )),
        Cell::from(activity_wait_cell(session)).style(if is_selected {
            base_style
        } else if session.blocked_by_count > 0 {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        }),
        Cell::from(activity_state_cell(session)).style(if is_selected {
            base_style
        } else if session.state.contains("idle in transaction") {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else if session.state == "active" {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::DarkGray)
        }),
        Cell::from(format_activity_query(session)).style(white_style),
    ])
    .style(base_style)
}

fn activity_time_style(is_selected: bool, duration_seconds: i64, selected_style: Style) -> Style {
    if is_selected {
        return selected_style;
    }

    if duration_seconds < 60 {
        Style::default().fg(Color::Green)
    } else if duration_seconds < 300 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
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
            activity_time_style(false, 59, Style::default()).fg,
            Some(Color::Green)
        );
        assert_eq!(
            activity_time_style(false, 299, Style::default()).fg,
            Some(Color::Yellow)
        );
        assert_eq!(
            activity_time_style(false, 301, Style::default()).fg,
            Some(Color::Red)
        );
    }

    #[test]
    fn test_subview_indicator_color_is_distinct_per_subview() {
        let colors: Vec<Color> = ActivitySubview::ALL
            .into_iter()
            .map(subview_indicator_color)
            .collect();
        assert_eq!(
            colors,
            vec![
                Color::White,
                Color::Green,
                Color::Yellow,
                Color::Magenta,
                Color::Cyan
            ]
        );
    }

    #[test]
    fn test_activity_sessions_title_highlights_active_subview() {
        let title = activity_sessions_title(7, ActivitySubview::Blocking);

        // The leading segment shows the session count.
        assert!(
            title
                .spans
                .first()
                .is_some_and(|span| span.content.as_ref().contains("Sessions (7)"))
        );

        // Every subview is listed, and only the active one is bold + accent-colored;
        // the inactive ones are dimmed (DarkGray, not bold).
        for subview in ActivitySubview::ALL {
            let label = format!("{}:{}", subview.label(), subview.name());
            let span = title
                .spans
                .iter()
                .find(|span| span.content.as_ref() == label);
            let styled_correctly = span.is_some_and(|span| {
                if subview == ActivitySubview::Blocking {
                    span.style.fg == Some(subview_indicator_color(subview))
                        && span.style.add_modifier.contains(Modifier::BOLD)
                } else {
                    span.style.fg == Some(Color::DarkGray)
                        && !span.style.add_modifier.contains(Modifier::BOLD)
                }
            });
            assert!(styled_correctly, "subview {label} styled incorrectly");
        }
    }

    fn session_with_state(state: &str) -> crate::pg::client::ActivitySession {
        crate::pg::client::ActivitySession {
            pid: "1".to_string(),
            backend_type: "client backend".to_string(),
            xmin: String::new(),
            database: "db".to_string(),
            application: "app".to_string(),
            user: "user".to_string(),
            client: "127.0.0.1".to_string(),
            duration_seconds: 0,
            wait_info: String::new(),
            state: state.to_string(),
            query: String::new(),
            blocked_by_count: 0,
            blocked_count: 0,
        }
    }

    #[test]
    fn test_session_state_color_distinguishes_aborted_idle_in_transaction() {
        // Aligns with pg_activity: aborted idle-in-transaction is the riskiest idle
        // state and must be visually distinct from a plain idle-in-transaction.
        let aborted = session_state_color(&session_with_state("idle in transaction (aborted)"));
        let idle_in_tx = session_state_color(&session_with_state("idle in transaction"));

        assert_eq!(aborted, Color::LightRed);
        assert_eq!(idle_in_tx, Color::Cyan);
        assert_ne!(aborted, idle_in_tx);

        // Sanity: the other states keep their established colors.
        assert_eq!(
            session_state_color(&session_with_state("active")),
            Color::Green
        );
        assert_eq!(
            session_state_color(&session_with_state("idle")),
            Color::DarkGray
        );
    }

    #[test]
    fn test_session_state_color_prioritizes_blocked_and_blocking() {
        let mut blocked = session_with_state("active");
        blocked.blocked_by_count = 1;
        assert_eq!(session_state_color(&blocked), Color::Red);

        let mut blocking = session_with_state("active");
        blocking.blocked_count = 2;
        assert_eq!(session_state_color(&blocking), Color::Magenta);
    }

    #[test]
    fn test_checkpoint_request_pct() {
        // No checkpoints yet → 0%.
        assert!((checkpoint_request_pct(0, 0) - 0.0).abs() < f64::EPSILON);
        // All timed → 0% requested (healthy: driven by checkpoint_timeout).
        assert!((checkpoint_request_pct(33, 0) - 0.0).abs() < f64::EPSILON);
        // All requested → 100% (driven by max_wal_size).
        assert!((checkpoint_request_pct(0, 5) - 100.0).abs() < f64::EPSILON);
        // Mixed.
        assert!((checkpoint_request_pct(3, 1) - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_format_seconds_short() {
        assert_eq!(format_seconds_short(30), "30s");
        assert_eq!(format_seconds_short(300), "5min");
        assert_eq!(format_seconds_short(90), "1m30s");
        assert_eq!(format_seconds_short(-5), "0s");
    }

    #[test]
    fn test_format_megabytes() {
        assert_eq!(format_megabytes(1024), "1.0 GiB");
        assert_eq!(format_megabytes(0), "0 B");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_dashboard_renders_checkpoints_panel_when_selected() {
        use ratatui::{Terminal, backend::TestBackend};
        let mut app = crate::tui::app::App::new_for_test(
            String::new(),
            0,
            None,
            1000,
            0,
            "activity",
            "",
            crate::config::Config::default(),
        );
        app.activity_chart_metric = crate::tui::app::ActivityChartMetric::Checkpoints;
        app.dashboard.summary.checkpoint.timed = 55;
        app.dashboard.summary.checkpoint.requested = 0;
        app.dashboard.summary.checkpoint.buffers_written = 20341;
        app.dashboard.summary.checkpoint.write_time_ms = 658_434.0;
        app.dashboard.summary.checkpoint.sync_time_ms = 363.0;
        app.dashboard.summary.checkpoint.checkpoint_timeout_seconds = 300;
        app.dashboard.summary.checkpoint.max_wal_size_mb = 1024;
        app.dashboard.summary.checkpoint.completion_target = 0.9;
        app.dashboard.summary.checkpoint.min_wal_size_mb = 80;
        app.dashboard.summary.checkpoint.wal_segment_size_bytes = 16 * 1024 * 1024;
        app.dashboard.summary.checkpoint.wal_bytes = 5 * 1024 * 1024 * 1024;
        app.dashboard.summary.checkpoint.wal_elapsed_seconds = 3600.0;

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| draw_dashboard(f, &mut app, f.area()))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect();
        assert!(text.contains("Checkpoints"), "panel title not visible");
        assert!(text.contains("Frequency"), "frequency row not visible");
        assert!(text.contains("I/O / ckpt"), "I/O row not visible");
        assert!(text.contains("WAL"), "WAL row not visible");
        assert!(text.contains("Segment"), "WAL segment row not visible");
        assert!(text.contains("seg"), "WAL segment size not visible");
        assert!(text.contains("Healthy"), "verdict not visible");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_checkpoints_panel_low_write_is_healthy_not_max_wal() {
        // Reproduces the field false positive: a low-write node with 0 timed and
        // 100% requested checkpoints at the timeout cadence, flushing ~0.1 MB each,
        // must read Healthy rather than "raise max_wal_size".
        use chrono::Duration;
        use ratatui::{Terminal, backend::TestBackend};
        let mut app = crate::tui::app::App::new_for_test(
            String::new(),
            0,
            None,
            1000,
            0,
            "activity",
            "",
            crate::config::Config::default(),
        );
        app.activity_chart_metric = crate::tui::app::ActivityChartMetric::Checkpoints;
        let cp = &mut app.dashboard.summary.checkpoint;
        cp.timed = 0;
        cp.requested = 309;
        cp.buffers_written = 3918; // ~0.1 MB/ckpt
        cp.write_time_ms = 375_100.0;
        cp.sync_time_ms = 1087.0;
        cp.checkpoint_timeout_seconds = 1800;
        cp.max_wal_size_mb = 2048;
        cp.min_wal_size_mb = 512;
        cp.completion_target = 0.9;
        cp.wal_segment_size_bytes = 128 * 1024 * 1024;
        cp.wal_bytes = 36_949_392; // ~37 MB total
        cp.wal_elapsed_seconds = 362_671.0; // ~100 B/s
        // ~309 checkpoints over the elapsed window → ~20 min interval.
        cp.stats_reset = Some(Utc::now() - Duration::seconds(362_671));

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| draw_dashboard(f, &mut app, f.area()))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect();
        assert!(text.contains("Healthy"), "expected Healthy verdict");
        assert!(
            !text.contains("raise max_wal_size"),
            "must not recommend raising max_wal_size on a low-write node"
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_checkpoints_panel_flags_stalled_checkpointer() {
        // A checkpointer that hasn't completed in well over checkpoint_timeout is the
        // genuinely dangerous condition: it must be surfaced (red "lagging"), and the
        // "Last ckpt" age must be visible to the operator.
        use chrono::Duration;
        use ratatui::{Terminal, backend::TestBackend};
        let mut app = crate::tui::app::App::new_for_test(
            String::new(),
            0,
            None,
            1000,
            0,
            "activity",
            "",
            crate::config::Config::default(),
        );
        app.activity_chart_metric = crate::tui::app::ActivityChartMetric::Checkpoints;
        let cp = &mut app.dashboard.summary.checkpoint;
        cp.timed = 10;
        cp.requested = 5;
        cp.buffers_written = 5_000_000;
        cp.write_time_ms = 1_000_000.0;
        cp.sync_time_ms = 50_000.0;
        cp.checkpoint_timeout_seconds = 300; // 5 min
        cp.max_wal_size_mb = 1024;
        cp.completion_target = 0.9;
        cp.stats_reset = Some(Utc::now() - Duration::seconds(3600));
        // Last checkpoint completed 50 minutes ago vs a 5-minute timeout → stalled.
        cp.seconds_since_last_checkpoint = Some(3000);

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| draw_dashboard(f, &mut app, f.area()))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect();
        assert!(text.contains("Last ckpt"), "Last ckpt row not visible");
        assert!(
            text.contains("lagging"),
            "stalled checkpointer must be flagged as lagging"
        );
    }

    #[test]
    fn test_checkpoint_mb_per() {
        // 20341 buffers (8 KiB) over 55 checkpoints ≈ 2.9 MB each.
        let mb = checkpoint_mb_per(20341, 55);
        assert!((mb - 2.89).abs() < 0.05, "got {mb}");
        assert!((checkpoint_mb_per(100, 0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_per_checkpoint_ms() {
        assert!((per_checkpoint_ms(363.0, 55) - 6.6).abs() < 0.1);
        assert!((per_checkpoint_ms(100.0, 0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_format_millis() {
        assert_eq!(format_millis(7.0), "7ms");
        assert_eq!(format_millis(1500.0), "1.5s");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_checkpoint_interval_seconds() {
        use chrono::TimeZone;
        let reset = Utc.timestamp_opt(1_000_000, 0).single();
        let now = Utc.timestamp_opt(1_000_000 + 3000, 0).single().unwrap();
        // 3000s elapsed over 10 total checkpoints → 300s interval.
        assert_eq!(checkpoint_interval_seconds(now, reset, 10), Some(300));
        // Even with 0 timed checkpoints, a non-zero total still yields an interval
        // (low-write systems skip scheduled checkpoints but still checkpoint).
        assert_eq!(checkpoint_interval_seconds(now, reset, 6), Some(500));
        // No checkpoints at all → no interval.
        assert_eq!(checkpoint_interval_seconds(now, reset, 0), None);
        assert_eq!(checkpoint_interval_seconds(now, None, 10), None);
    }

    #[test]
    fn test_checkpoint_verdict() {
        // Args: (in_recovery, age_since_last, interval, timeout, mb, total_ms, total).
        // No checkpoints yet.
        assert_eq!(
            checkpoint_verdict(false, None, None, 1800, 0.0, 0.0, 0).1,
            Color::DarkGray
        );

        // Field false positive: low-write, at-timeout, trivial I/O, recent ckpt.
        let (msg, color) = checkpoint_verdict(false, Some(600), Some(1200), 1800, 0.1, 1200.0, 309);
        assert_eq!(color, Color::Green);
        assert_eq!(msg, "Healthy (timeout-paced)");

        // Stalled checkpointer (age > 2x timeout) is the dangerous case → red.
        let (msg, color) =
            checkpoint_verdict(false, Some(4000), Some(1200), 1800, 0.1, 1200.0, 309);
        assert_eq!(color, Color::Red);
        assert_eq!(msg, "Checkpointer lagging (stale)");

        // Overrunning: average checkpoint exceeds a whole cycle, with real I/O → red.
        let (msg, color) =
            checkpoint_verdict(false, Some(600), Some(1700), 1800, 50.0, 2_000_000.0, 100);
        assert_eq!(color, Color::Red);
        assert_eq!(msg, "Checkpoints overrun timeout (I/O-bound)");

        // Frequent + real I/O → advisory yellow, mentions max_wal_size.
        let (msg, color) = checkpoint_verdict(false, Some(300), Some(300), 1800, 32.0, 500.0, 200);
        assert_eq!(color, Color::Yellow);
        assert_eq!(msg, "Frequent + I/O: consider max_wal_size");

        // Frequent + trivial I/O → manual/DDL/backup, not max_wal_size.
        let (msg, color) = checkpoint_verdict(false, Some(300), Some(300), 1800, 0.1, 50.0, 200);
        assert_eq!(color, Color::Yellow);
        assert_eq!(msg, "Frequent low-I/O: manual/DDL/backup");

        // Standby uses restartpoint wording.
        assert_eq!(
            checkpoint_verdict(true, Some(600), Some(1200), 1800, 0.1, 1200.0, 50).0,
            "Healthy (restartpoints keeping up)"
        );
        assert_eq!(
            checkpoint_verdict(true, Some(5000), Some(1200), 1800, 0.1, 1200.0, 50).0,
            "Restartpointer lagging (stale)"
        );

        // Unknown staleness/interval must not false-alarm.
        assert_eq!(
            checkpoint_verdict(false, None, None, 1800, 100.0, 10.0, 55).1,
            Color::Green
        );
    }

    #[test]
    fn test_wal_bytes_per_second() {
        // ~37 MB over ~362k seconds ≈ 100 B/s (the observed low-write node).
        let rate = wal_bytes_per_second(36_949_392, 362_671.0);
        assert!((rate - 101.9).abs() < 1.0, "got {rate}");
        assert!((wal_bytes_per_second(1000, 0.0) - 0.0).abs() < f64::EPSILON);
        assert!((wal_bytes_per_second(-5, 10.0) - 0.0).abs() < f64::EPSILON);
    }

    /// Documented decision matrix for `checkpoint_verdict`, ordered by severity:
    /// a lagging/stalled or overrunning checkpointer (red) is the dangerous case and
    /// outranks frequency (yellow advisory). Only "frequent AND real I/O" mentions
    /// `max_wal_size`. Timeout is 1800s unless noted, so: stale if age over 3600s,
    /// overrun if avg total over 1.8M ms (with I/O), too-frequent if interval < 900s.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_checkpoint_verdict_matrix() {
        struct Case {
            name: &'static str,
            in_recovery: bool,
            age: Option<i64>,
            interval: Option<i64>,
            timeout: i64,
            mb: f64,
            total_ms: f64,
            total: i64,
            msg: &'static str,
            color: Color,
        }
        let cases = [
            Case {
                name: "no checkpoints since reset",
                in_recovery: false,
                age: None,
                interval: None,
                timeout: 1800,
                mb: 0.0,
                total_ms: 0.0,
                total: 0,
                msg: "No checkpoints yet",
                color: Color::DarkGray,
            },
            Case {
                name: "field false positive: 100% requested, at-timeout, trivial I/O",
                in_recovery: false,
                age: Some(600),
                interval: Some(1200),
                timeout: 1800,
                mb: 0.1,
                total_ms: 1200.0,
                total: 309,
                msg: "Healthy (timeout-paced)",
                color: Color::Green,
            },
            Case {
                name: "write-heavy but completes within the cycle: big I/O is normal",
                in_recovery: false,
                age: Some(1700),
                interval: Some(1800),
                timeout: 1800,
                mb: 200.0,
                total_ms: 600_000.0,
                total: 480,
                msg: "Healthy (timeout-paced)",
                color: Color::Green,
            },
            Case {
                name: "genuine WAL pressure: frequent + heavy I/O",
                in_recovery: false,
                age: Some(200),
                interval: Some(300),
                timeout: 1800,
                mb: 64.0,
                total_ms: 5000.0,
                total: 600,
                msg: "Frequent + I/O: consider max_wal_size",
                color: Color::Yellow,
            },
            Case {
                name: "frequent manual CHECKPOINT cron: low I/O, not max_wal_size",
                in_recovery: false,
                age: Some(200),
                interval: Some(300),
                timeout: 1800,
                mb: 0.1,
                total_ms: 50.0,
                total: 600,
                msg: "Frequent low-I/O: manual/DDL/backup",
                color: Color::Yellow,
            },
            Case {
                name: "frequent large-segment forced switches: low I/O",
                in_recovery: false,
                age: Some(100),
                interval: Some(120),
                timeout: 1800,
                mb: 0.1,
                total_ms: 50.0,
                total: 900,
                msg: "Frequent low-I/O: manual/DDL/backup",
                color: Color::Yellow,
            },
            Case {
                name: "stalled checkpointer: no completion in over 2x timeout",
                in_recovery: false,
                age: Some(5400),
                interval: Some(1200),
                timeout: 1800,
                mb: 0.1,
                total_ms: 1200.0,
                total: 309,
                msg: "Checkpointer lagging (stale)",
                color: Color::Red,
            },
            Case {
                name: "I/O-bound overrun: checkpoint takes longer than a whole cycle",
                in_recovery: false,
                age: Some(1700),
                interval: Some(1700),
                timeout: 1800,
                mb: 30.0,
                total_ms: 3_130_000.0,
                total: 100,
                msg: "Checkpoints overrun timeout (I/O-bound)",
                color: Color::Red,
            },
            Case {
                name: "precedence: stalled outranks frequency",
                in_recovery: false,
                age: Some(4000),
                interval: Some(200),
                timeout: 1800,
                mb: 64.0,
                total_ms: 5000.0,
                total: 700,
                msg: "Checkpointer lagging (stale)",
                color: Color::Red,
            },
            Case {
                name: "precedence: overrun outranks frequency",
                in_recovery: false,
                age: Some(600),
                interval: Some(200),
                timeout: 1800,
                mb: 64.0,
                total_ms: 2_000_000.0,
                total: 700,
                msg: "Checkpoints overrun timeout (I/O-bound)",
                color: Color::Red,
            },
            Case {
                name: "unknown interval, heavy I/O: cannot judge frequency, stay calm",
                in_recovery: false,
                age: Some(600),
                interval: None,
                timeout: 1800,
                mb: 500.0,
                total_ms: 100.0,
                total: 55,
                msg: "Healthy (timeout-paced)",
                color: Color::Green,
            },
            Case {
                name: "unknown timeout (misconfig): never trip any threshold",
                in_recovery: false,
                age: Some(99_999),
                interval: Some(1),
                timeout: 0,
                mb: 500.0,
                total_ms: 9_999_999.0,
                total: 55,
                msg: "Healthy (timeout-paced)",
                color: Color::Green,
            },
            Case {
                name: "standby healthy: restartpoints keeping up",
                in_recovery: true,
                age: Some(600),
                interval: Some(1200),
                timeout: 1800,
                mb: 0.1,
                total_ms: 1200.0,
                total: 50,
                msg: "Healthy (restartpoints keeping up)",
                color: Color::Green,
            },
            Case {
                name: "standby stalled: restartpointer lagging",
                in_recovery: true,
                age: Some(5400),
                interval: Some(1200),
                timeout: 1800,
                mb: 0.1,
                total_ms: 1200.0,
                total: 50,
                msg: "Restartpointer lagging (stale)",
                color: Color::Red,
            },
            Case {
                name: "standby overrun: restartpoints I/O-bound",
                in_recovery: true,
                age: Some(1700),
                interval: Some(1700),
                timeout: 1800,
                mb: 30.0,
                total_ms: 3_130_000.0,
                total: 100,
                msg: "Restartpoints overrun timeout (I/O-bound)",
                color: Color::Red,
            },
            Case {
                name: "short timeout (5min) low-write: 4min cadence is healthy",
                in_recovery: false,
                age: Some(240),
                interval: Some(240),
                timeout: 300,
                mb: 0.1,
                total_ms: 50.0,
                total: 200,
                msg: "Healthy (timeout-paced)",
                color: Color::Green,
            },
        ];
        for c in cases {
            let (msg, color) = checkpoint_verdict(
                c.in_recovery,
                c.age,
                c.interval,
                c.timeout,
                c.mb,
                c.total_ms,
                c.total,
            );
            assert_eq!(msg, c.msg, "msg for case: {}", c.name);
            assert_eq!(color, c.color, "color for case: {}", c.name);
        }
    }

    /// Exact threshold boundaries, so re-tuning a constant cannot silently flip a
    /// verdict without a failing test.
    #[test]
    fn test_checkpoint_verdict_boundaries() {
        // Staleness boundary: stale iff age > STALE_FACTOR*timeout (strict).
        // age == 2*timeout is NOT stale; one second over is.
        assert_eq!(
            checkpoint_verdict(false, Some(3600), Some(1200), 1800, 0.1, 1200.0, 100).0,
            "Healthy (timeout-paced)",
            "age == 2x timeout must not be stale"
        );
        assert_eq!(
            checkpoint_verdict(false, Some(3601), Some(1200), 1800, 0.1, 1200.0, 100).0,
            "Checkpointer lagging (stale)",
            "age just over 2x timeout is stale"
        );

        // Overrun boundary: overrun iff avg total > timeout*1000 (strict) AND I/O > 0.
        assert_eq!(
            checkpoint_verdict(false, Some(600), Some(1700), 1800, 1.0, 1_800_000.0, 100).0,
            "Healthy (timeout-paced)",
            "total == timeout is not yet overrun"
        );
        assert_eq!(
            checkpoint_verdict(false, Some(600), Some(1700), 1800, 1.0, 1_800_001.0, 100).0,
            "Checkpoints overrun timeout (I/O-bound)",
            "total just over timeout is overrun"
        );
        assert_eq!(
            checkpoint_verdict(false, Some(600), Some(1700), 1800, 0.0, 5_000_000.0, 100).0,
            "Healthy (timeout-paced)",
            "overrun requires real I/O (mb > 0)"
        );

        // Frequency boundary: too_frequent iff interval*2 < timeout (strict).
        assert_eq!(
            checkpoint_verdict(false, Some(600), Some(900), 1800, 64.0, 50.0, 100).0,
            "Healthy (timeout-paced)",
            "interval == timeout/2 must not be too frequent"
        );
        assert_eq!(
            checkpoint_verdict(false, Some(600), Some(899), 1800, 64.0, 50.0, 100).0,
            "Frequent + I/O: consider max_wal_size",
            "interval just below timeout/2 must be too frequent"
        );

        // MB pressure boundary at exactly 16.0 (>= trips).
        assert_eq!(
            checkpoint_verdict(
                false,
                Some(600),
                Some(300),
                1800,
                CHECKPOINT_MB_PRESSURE_THRESHOLD,
                50.0,
                100
            )
            .0,
            "Frequent + I/O: consider max_wal_size",
            "mb == threshold counts as pressure"
        );
        assert_eq!(
            checkpoint_verdict(
                false,
                Some(600),
                Some(300),
                1800,
                CHECKPOINT_MB_PRESSURE_THRESHOLD - 0.1,
                50.0,
                100
            )
            .0,
            "Frequent low-I/O: manual/DDL/backup",
            "mb just below threshold is not pressure"
        );
    }
}

use crate::tui::app::{App, Tab};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Cell, Chart, Dataset, GraphType, Paragraph, Row, Table, Tabs},
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
}

fn draw_dashboard(f: &mut Frame, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(14), // Oscilloscope + stats panel
            Constraint::Min(0),     // Active queries
        ])
        .split(area);

    if let (Some(top), Some(bottom)) = (rows.first(), rows.get(1)) {
        let panels = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(*top);

        if let (Some(left), Some(right)) = (panels.first(), panels.get(1)) {
            draw_conn_chart(f, app, *left);
            draw_stats_panel(f, app, *right);
        }
        draw_active_queries_panel(f, app, *bottom);
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
            .name("connected")
            .marker(symbols::Marker::HalfBlock)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Cyan))
            .data(&total_data),
        Dataset::default()
            .name("idle")
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
                    Span::raw(format!("max: {} ", app.dashboard.max_connections)),
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

fn draw_stats_panel(f: &mut Frame, app: &App, area: Rect) {
    let mut lines = conn_state_lines(app);
    lines.push(Line::raw(""));
    lines.extend(perf_lines(app));

    let widget =
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(" Stats "));
    f.render_widget(widget, area);
}

fn conn_state_lines(app: &App) -> Vec<Line<'static>> {
    let total: i64 = app.dashboard.conn_by_state.iter().map(|(_, c)| c).sum();

    let mut lines: Vec<Line> = app
        .dashboard
        .conn_by_state
        .iter()
        .map(|(state, count)| {
            let color = match state.as_str() {
                "active" => Color::Green,
                "idle in transaction" => Color::Yellow,
                "idle in transaction (aborted)" => Color::Red,
                "idle" => Color::Cyan,
                _ => Color::Gray,
            };
            let label = match state.as_str() {
                "active" => "active",
                "idle" => "idle",
                "idle in transaction" => "idle in tx",
                "idle in transaction (aborted)" => "idle in tx (abort)",
                "fastpath function call" => "fastpath",
                "disabled" => "disabled",
                other => other,
            };
            Line::from(vec![
                Span::raw(format!("  {label:<20}")),
                Span::styled(
                    format!("{count:>4}"),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
            ])
        })
        .collect();

    lines.push(Line::from(Span::raw("  ────────────────────────")));
    lines.push(Line::from(vec![
        Span::raw(format!("  {:<20}", "total")),
        Span::styled(
            format!("{total:>4}"),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]));
    lines
}

fn perf_lines(app: &App) -> Vec<Line<'static>> {
    let cache_color = if app.dashboard.cache_hit_pct >= 99.0 {
        Color::Green
    } else if app.dashboard.cache_hit_pct >= 95.0 {
        Color::Yellow
    } else {
        Color::Red
    };
    let conn_pct = if app.dashboard.max_connections > 0 {
        app.dashboard.total_backends * 100 / app.dashboard.max_connections
    } else {
        0
    };
    let conn_color = if conn_pct < 70 {
        Color::Green
    } else if conn_pct < 90 {
        Color::Yellow
    } else {
        Color::Red
    };

    vec![
        Line::from(vec![
            Span::raw(format!("  {:<20}", "cache hit")),
            Span::styled(
                format!("{:.1}%", app.dashboard.cache_hit_pct),
                Style::default()
                    .fg(cache_color)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::raw(format!("  {:<20}", "commits")),
            Span::styled(
                format!("{}", app.dashboard.total_commits),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::raw(format!("  {:<20}", "rollbacks")),
            Span::styled(
                format!("{}", app.dashboard.total_rollbacks),
                Style::default().fg(if app.dashboard.total_rollbacks > 0 {
                    Color::Yellow
                } else {
                    Color::Gray
                }),
            ),
        ]),
        Line::from(vec![
            Span::raw(format!("  {:<20}", "max conns")),
            Span::styled(
                format!(
                    "{} / {} ({}%)",
                    app.dashboard.total_backends, app.dashboard.max_connections, conn_pct
                ),
                Style::default().fg(conn_color).add_modifier(Modifier::BOLD),
            ),
        ]),
    ]
}

fn draw_active_queries_panel(f: &mut Frame, app: &App, area: Rect) {
    let header_cells = ["PID", "User", "DB", "Duration", "Query"];
    let widths = [
        Constraint::Length(8),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(10),
        Constraint::Min(20),
    ];

    let rows = app
        .dashboard
        .active_queries
        .iter()
        .enumerate()
        .map(|(i, items)| {
            let style = if i == app.selected_row {
                Style::default().fg(Color::Black).bg(Color::White)
            } else {
                Style::default().fg(Color::Green)
            };
            Row::new(items.iter().map(|c| Cell::from(c.as_str()))).style(style)
        });

    let count = app.dashboard.active_queries.len();
    let title = format!(" Active Queries ({count}) | top-n: {} ", app.top_n);
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
    let header_cells = match app.current_tab {
        Tab::Activity => vec![
            "PID", "User", "DB", "State", "Query", "Start", "App", "Client",
        ],
        Tab::Database => vec![
            "DB",
            "Backends",
            "Commits",
            "Rollbacks",
            "Read",
            "Hit",
            "Fetched",
            "Reset",
        ],
        Tab::Locks => vec!["Relation", "Mode", "Granted", "PID"],
        Tab::IO => vec!["Backend", "Read", "Write", "Time Read", "Time Write"],
        Tab::Statements => vec!["Query", "Total", "Mean", "Calls", "Read", "Write"],
    };

    let widths = match app.current_tab {
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
        _ => vec![Constraint::Percentage(20); header_cells.len()],
    };

    let rows = app.data.iter().enumerate().map(|(i, items)| {
        let style = if i == app.selected_row {
            Style::default().fg(Color::Black).bg(Color::White)
        } else {
            Style::default()
        };
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
                .title(format!("{:?} View", app.current_tab)),
        );
    f.render_widget(table, area);
}

fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let footer_text = if app.current_tab == Tab::Activity {
        if let Some(row_data) = app.dashboard.active_queries.get(app.selected_row) {
            let query = row_data.get(4).map_or("", String::as_str);
            format!("QUERY: {}", query.replace('\n', " "))
        } else {
            "q:Quit | 1-5:Switch Tab | ↑↓:Navigate".to_string()
        }
    } else if let Some(row_data) = app.data.get(app.selected_row) {
        match app.current_tab {
            Tab::Statements => {
                let query = row_data.first().map_or("", String::as_str);
                format!("QUERY: {}", query.replace('\n', " "))
            }
            _ => "q:Quit | 1-5:Switch Tab | ↑↓:Navigate".to_string(),
        }
    } else {
        "q:Quit | 1-5:Switch Tab | ↑↓:Navigate".to_string()
    };

    let footer = Paragraph::new(Line::from(vec![Span::raw(footer_text)]))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, area);
}

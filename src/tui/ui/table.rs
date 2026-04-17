use crate::pg::client::CapabilityStatus;
use crate::tui::app::format::table_header_cells;
use crate::tui::app::{App, DatabaseView, Tab};
use crate::tui::ui::common::centered_rect;
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table},
};

pub fn draw_table(f: &mut Frame, app: &mut App, area: Rect) {
    if let Some(reason) = unavailable_capability_reason(app) {
        draw_unavailable_panel(f, area, unavailable_title(app.current_tab), reason);
        return;
    }

    let header_cells = table_header_cells_adapter(app);
    let widths = table_widths(app);

    let rows = app.data.iter().enumerate().map(|(i, items)| {
        let is_selected = app.selected_row == Some(i);
        let row_style = table_row_style(app, i, items);

        let cells = items
            .iter()
            .take(header_cells.len())
            .enumerate()
            .map(|(col_index, c)| {
                if let Some(cell_style) = table_cell_style(app, col_index, c, is_selected) {
                    Cell::from(c.as_str()).style(cell_style)
                } else {
                    Cell::from(c.as_str())
                }
            });

        Row::new(cells).style(row_style)
    });

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
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(table_title(app)),
        )
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

fn unavailable_capability_reason(app: &App) -> Option<&str> {
    match app.current_view_capability()? {
        CapabilityStatus::Unavailable(reason) => Some(reason.as_str()),
        CapabilityStatus::Available | CapabilityStatus::Unknown => None,
    }
}

fn unavailable_title(tab: Tab) -> &'static str {
    match tab {
        Tab::Statements => "pg_stat_statements Unavailable",
        Tab::IO => "pg_stat_io Unavailable",
        _ => "Feature Unavailable",
    }
}

fn draw_unavailable_panel(f: &mut Frame, area: Rect, title: &str, reason: &str) {
    let panel_area = centered_rect(area, 72, 7);
    let paragraph = Paragraph::new(vec![
        Line::styled(
            title,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::styled(reason, Style::default().fg(Color::White)),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Capability ")
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, panel_area);
    f.render_widget(paragraph, panel_area);
}

#[allow(clippy::too_many_lines)]
fn table_cell_style(app: &App, col_index: usize, value: &str, is_selected: bool) -> Option<Style> {
    if is_selected {
        return None;
    }

    match app.current_tab {
        Tab::Statements => match col_index {
            0..=1 => return Some(Style::default().fg(Color::White)),
            2 => {
                if let Ok(total) = value.parse::<f64>() {
                    if total > 5000.0 {
                        return Some(Style::default().fg(Color::Red));
                    } else if total > 1000.0 {
                        return Some(Style::default().fg(Color::Yellow));
                    }
                    return Some(Style::default().fg(Color::Green));
                }
            }
            3 => {
                if let Ok(mean) = value.parse::<f64>() {
                    if mean > 100.0 {
                        return Some(Style::default().fg(Color::Red));
                    } else if mean > 10.0 {
                        return Some(Style::default().fg(Color::Yellow));
                    }
                    return Some(Style::default().fg(Color::Green));
                }
            }
            4..=6 => return Some(Style::default().fg(Color::DarkGray)),
            _ => {}
        },
        Tab::Database => match &app.database_view {
            DatabaseView::Summary => match col_index {
                0 | 1 => return Some(Style::default().fg(Color::White)),
                4 => {
                    if let Ok(pct) = value.trim_end_matches('%').parse::<f64>() {
                        if pct < 90.0 {
                            return Some(Style::default().fg(Color::Red));
                        } else if pct < 98.0 {
                            return Some(Style::default().fg(Color::Yellow));
                        }
                        return Some(Style::default().fg(Color::Green));
                    }
                    return None;
                }
                6 => {
                    if let Ok(deadlocks) = value.parse::<i64>() {
                        if deadlocks > 0 {
                            return Some(Style::default().fg(Color::Red));
                        }
                        return Some(Style::default().fg(Color::Green));
                    }
                    return None;
                }
                2 | 3 | 5 | 7 => return Some(Style::default().fg(Color::DarkGray)),
                _ => return None,
            },
            DatabaseView::Tables { .. } => match col_index {
                0 => return Some(Style::default().fg(Color::White)),
                1..=3 => return Some(Style::default().fg(Color::DarkGray)),
                _ => return None,
            },
        },
        Tab::Locks => match col_index {
            0..=1 => return Some(Style::default().fg(Color::Red)),
            2..=4 | 6 => return Some(Style::default().fg(Color::White)),
            5 => {
                if let Ok(sec) = value.parse::<i64>() {
                    if sec > 60 {
                        return Some(Style::default().fg(Color::Red));
                    } else if sec > 10 {
                        return Some(Style::default().fg(Color::Yellow));
                    }
                    return Some(Style::default().fg(Color::Green));
                }
            }
            _ => {}
        },
        Tab::IO => match col_index {
            0..=2 => return Some(Style::default().fg(Color::White)),
            3..=4 => return Some(Style::default().fg(Color::DarkGray)),
            5..=6 => {
                if let Ok(ms) = value.parse::<f64>() {
                    if ms > 1000.0 {
                        return Some(Style::default().fg(Color::Red));
                    } else if ms > 100.0 {
                        return Some(Style::default().fg(Color::Yellow));
                    }
                    return Some(Style::default().fg(Color::Green));
                }
            }
            _ => {}
        },
        Tab::Tools => match col_index {
            0 => return Some(Style::default().fg(Color::White)),
            1 => return Some(Style::default().fg(Color::DarkGray)),
            _ => {}
        },
        Tab::Settings => match col_index {
            0 => return Some(Style::default().fg(Color::White)),
            1 => {
                let color = app
                    .config
                    .get_view_color("settings", "value")
                    .unwrap_or(Color::Blue);
                return Some(Style::default().fg(color));
            }
            2..=4 => return Some(Style::default().fg(Color::DarkGray)),
            _ => {}
        },
        Tab::Replication | Tab::Activity => {}
    }

    None
}

fn table_header_cells_adapter(app: &App) -> Vec<&'static str> {
    table_header_cells(app.current_tab, &app.database_view)
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
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Percentage(25),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Min(20),
        ],
        Tab::IO => vec![
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(15),
            Constraint::Length(15),
        ],
        Tab::Statements => vec![
            Constraint::Length(12),
            Constraint::Percentage(40),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(12),
        ],
        Tab::Tools => vec![Constraint::Length(40), Constraint::Percentage(70)],
        Tab::Replication => Vec::new(),
        Tab::Settings => vec![
            Constraint::Percentage(25),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Percentage(20),
            Constraint::Percentage(35),
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
    let limit_str = if app.top_n == 0 {
        "All".to_string()
    } else {
        app.top_n.to_string()
    };

    let filter_hint = if app.search_query.is_empty() {
        String::new()
    } else {
        format!(" [Filter: {}]", app.search_query)
    };

    match &app.database_view {
        DatabaseView::Summary if app.current_tab == Tab::Database => format!(
            " Database View | rows: {} | top: {limit_str}(n){filter_hint} | Enter:Browse ",
            app.data.len()
        ),
        DatabaseView::Tables { database } if app.current_tab == Tab::Database => format!(
            " Database Tree | {database} | nodes: {} | Esc:Back{filter_hint} ",
            app.data.len()
        ),
        _ if app.current_tab == Tab::Settings => format!(
            " Settings View | rows: {} | Search(/){filter_hint} ",
            app.data.len()
        ),
        _ => format!(
            " {:?} View | rows: {} | top: {limit_str}(n){filter_hint} ",
            app.current_tab,
            app.data.len()
        ),
    }
}

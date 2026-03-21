use crate::tui::app::{App, DatabaseView, Tab};
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
};

pub fn draw_table(f: &mut Frame, app: &mut App, area: Rect) {
    let header_cells = table_header_cells(app);
    let widths = table_widths(app);

    let rows = app.data.iter().enumerate().map(|(i, items)| {
        let is_selected = app.selected_row == Some(i);
        let row_style = table_row_style(app, i, items);

        let cells = items.iter().enumerate().map(|(col_index, c)| {
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
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
                .bottom_margin(1),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(table_title(app)),
        )
        .row_highlight_style(Style::default().fg(Color::Black).bg(Color::White));

    f.render_stateful_widget(table, area, &mut app.table_state);
}

#[allow(clippy::too_many_lines)]
fn table_cell_style(app: &App, col_index: usize, value: &str, is_selected: bool) -> Option<Style> {
    if is_selected {
        return None;
    }

    match app.current_tab {
        Tab::Statements => match col_index {
            0 => return Some(Style::default().fg(Color::White)),
            1 => {
                if let Ok(total) = value.parse::<f64>() {
                    if total > 5000.0 {
                        return Some(Style::default().fg(Color::Red));
                    } else if total > 1000.0 {
                        return Some(Style::default().fg(Color::Yellow));
                    }
                    return Some(Style::default().fg(Color::Green));
                }
            }
            2 => {
                if let Ok(mean) = value.parse::<f64>() {
                    if mean > 100.0 {
                        return Some(Style::default().fg(Color::Red));
                    } else if mean > 10.0 {
                        return Some(Style::default().fg(Color::Yellow));
                    }
                    return Some(Style::default().fg(Color::Green));
                }
            }
            3..=5 => return Some(Style::default().fg(Color::DarkGray)),
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
            0 | 1 => return Some(Style::default().fg(Color::White)),
            2..=4 => return Some(Style::default().fg(Color::DarkGray)),
            _ => {}
        },
        Tab::Replication | Tab::Activity => {}
    }

    None
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
        Tab::Locks => vec![
            "Blocking PID",
            "Blocked PID",
            "User",
            "Target",
            "Mode",
            "Time(s)",
            "Query",
        ],
        Tab::IO => vec![
            "Backend",
            "Object",
            "Context",
            "Reads",
            "Writes",
            "Time Read(ms)",
            "Time Write(ms)",
        ],
        Tab::Statements => vec![
            "Query",
            "Total(ms)",
            "Mean(ms)",
            "Calls",
            "Read(ms)",
            "Write(ms)",
        ],
        Tab::Tools => vec!["Action", "Description"],
        Tab::Replication => Vec::new(),
        Tab::Settings => vec!["Name", "Value", "Unit", "Category", "Description"],
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
            Constraint::Percentage(52),
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
        _ => format!(
            " {:?} View | rows: {} | top: {limit_str}(n){filter_hint} ",
            app.current_tab,
            app.data.len()
        ),
    }
}

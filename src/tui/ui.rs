use crate::tui::app::{App, Tab};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs},
};

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Length(3), // Tabs
            Constraint::Min(0),    // Table
            Constraint::Length(3), // Footer / Status
        ])
        .split(f.area());

    if let Some(chunk) = chunks.first() {
        draw_header(f, app, *chunk);
    }
    if let Some(chunk) = chunks.get(1) {
        draw_tabs(f, app, *chunk);
    }
    if let Some(chunk) = chunks.get(2) {
        draw_table(f, app, *chunk);
    }
    if let Some(chunk) = chunks.get(3) {
        draw_footer(f, app, *chunk);
    }
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let header = Paragraph::new(format!(
        "DSN: {} | Refresh: {}ms | Top N: {}",
        app.dsn, app.refresh_ms, app.top_n
    ))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("pgmon - Connection Info"),
    );
    f.render_widget(header, area);
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
    let footer_text = if let Some(row_data) = app.data.get(app.selected_row) {
        // Show full query if on activity or statements
        match app.current_tab {
            Tab::Activity | Tab::Statements => {
                let idx = if app.current_tab == Tab::Activity {
                    4
                } else {
                    0
                };
                let query = row_data.get(idx).map_or("", String::as_str);
                format!("SELECTED QUERY: {}", query.replace('\n', " "))
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

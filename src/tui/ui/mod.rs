pub mod activity;
pub mod common;
pub mod modals;
pub mod table;

use crate::tui::app::{App, DatabaseView, InputMode, Tab};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
};

use activity::draw_dashboard;
use common::truncate_str;
use modals::{
    draw_confirm_modal, draw_error_modal, draw_loading_modal, draw_refresh_modal,
    draw_statement_detail_modal, draw_top_n_modal,
};
use table::draw_table;

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
    } else if app.confirm_action.is_some() {
        draw_confirm_modal(f, app);
    } else if app.refresh_interval_modal.is_some() {
        draw_refresh_modal(f, app);
    } else if app.top_n_modal.is_some() {
        draw_top_n_modal(f, app);
    } else if app.statement_detail.is_some() {
        draw_statement_detail_modal(f, app);
    } else if app.loading_state.is_some() {
        draw_loading_modal(f, app);
    }
}

fn draw_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles = vec![
        "1:Activity",
        "2:Database",
        "3:Locks",
        "4:IO",
        "5:Statements",
        "6:Tools",
        "7:Settings",
    ];
    let selected_tab = match app.current_tab {
        Tab::Activity => 0,
        Tab::Database => 1,
        Tab::Locks => 2,
        Tab::IO => 3,
        Tab::Statements => 4,
        Tab::Tools => 5,
        Tab::Settings => 6,
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

#[allow(clippy::too_many_lines)]
fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let mut footer_spans = Vec::new();

    if app.input_mode == InputMode::Search {
        footer_spans.extend(vec![
            Span::styled(
                "SEARCH: ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(&app.search_query, Style::default().fg(Color::LightBlue)),
            Span::styled("█", Style::default().fg(Color::White)),
            Span::styled(
                "  |  Esc:Cancel | Enter:Keep Filter",
                Style::default().fg(Color::DarkGray),
            ),
        ]);
    } else if let Some(notice) = app.notice_state.as_ref() {
        footer_spans.push(Span::styled(
            notice.message.clone(),
            Style::default().fg(Color::Green),
        ));
    } else if app.error_state.is_some() {
        footer_spans.extend(vec![
            Span::styled(
                "q",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":Quit | "),
            Span::styled(
                "r",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(":Retry | 1-7:Switch Tab"),
        ]);
    } else if app.current_tab == Tab::Activity {
        if let Some(session) = app
            .selected_row
            .and_then(|selected_row| app.dashboard.sessions.get(selected_row))
        {
            footer_spans.extend(vec![
                Span::styled("QUERY: ", Style::default().fg(Color::DarkGray)),
                Span::raw(truncate_str(session.query.as_str(), 60)),
                Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    "Enter",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(":Save Query | a/w/b/t:Subviews"),
            ]);
        } else {
            footer_spans.push(Span::raw(
                "q:Quit | 1-7:Switch Tab | ↑↓:Navigate | Enter:Save Query | a/w/b/t:Subviews",
            ));
        }
    } else if let Some(row_data) = app
        .selected_row
        .and_then(|selected_row| app.data.get(selected_row))
    {
        match app.current_tab {
            Tab::Statements => {
                let query = row_data.first().map_or("", String::as_str);
                footer_spans.extend(vec![
                    Span::styled("QUERY: ", Style::default().fg(Color::DarkGray)),
                    Span::raw(truncate_str(query, 60)),
                    Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        "Enter",
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(":Save Query | "),
                    Span::styled(
                        "i",
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(":Details"),
                ]);
            }
            Tab::Database => match &app.database_view {
                DatabaseView::Summary => {
                    let database = row_data.first().map_or("", String::as_str);
                    footer_spans.extend(vec![
                        Span::styled("DB: ", Style::default().fg(Color::DarkGray)),
                        Span::raw(database),
                        Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            "Enter",
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(":Browse Tables"),
                    ]);
                }
                DatabaseView::Tables { database } => {
                    let node = row_data.first().map_or("", String::as_str);
                    footer_spans.extend(vec![
                        Span::styled("DB: ", Style::default().fg(Color::DarkGray)),
                        Span::raw(database),
                        Span::styled(" | NODE: ", Style::default().fg(Color::DarkGray)),
                        Span::raw(node.trim()),
                        Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            "Esc",
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(":Back"),
                    ]);
                }
            },
            _ => footer_spans.push(Span::raw("q:Quit | 1-7:Switch Tab | ↑↓:Navigate")),
        }
    } else {
        match app.current_tab {
            Tab::Activity => {
                footer_spans.push(Span::raw(
                    "q:Quit | 1-7:Switch Tab | ↑↓:Navigate | Enter:Save Query | a/w/b/t:Subviews",
                ));
            }
            Tab::Statements => {
                footer_spans.push(Span::raw(
                    "q:Quit | 1-7:Switch Tab | ↑↓:Navigate | Enter:Save Query | i:Details",
                ));
            }
            Tab::Database => match &app.database_view {
                DatabaseView::Summary => {
                    footer_spans.push(Span::raw(
                        "q:Quit | 1-7:Switch Tab | ↑↓:Navigate | Enter:Browse Tables",
                    ));
                }
                DatabaseView::Tables { .. } => {
                    footer_spans.push(Span::raw(
                        "q:Quit | 1-7:Switch Tab | ↑↓:Navigate | Esc:Back",
                    ));
                }
            },
            _ => footer_spans.push(Span::raw("q:Quit | 1-7:Switch Tab | ↑↓:Navigate")),
        }
    }

    let footer =
        Paragraph::new(Line::from(footer_spans)).block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, area);
}

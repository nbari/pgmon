pub mod activity;
pub mod common;
pub mod modals;
pub mod replication;
pub mod table;

use crate::config::parse_color;
use crate::pg::client::CapabilityStatus;
use crate::tui::app::{App, DatabaseView, InputMode, Tab};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
};

use crate::tui::app::format::format_activity_query;
use activity::draw_dashboard;
use common::truncate_str;
use modals::{
    draw_confirm_modal, draw_deadlock_modal, draw_error_modal, draw_explain_modal, draw_help_modal,
    draw_loading_modal, draw_query_detail_modal, draw_refresh_modal, draw_table_definition_modal,
    draw_theme_modal, draw_top_n_modal,
};
use replication::draw_replication;
use table::draw_table;

pub fn draw(f: &mut Frame, app: &mut App) {
    let show_controls = app.config.ui.show_controls;
    let constraints = if show_controls {
        vec![
            Constraint::Length(3), // Tabs
            Constraint::Min(0),    // Content
            Constraint::Length(3), // Footer
        ]
    } else {
        vec![
            Constraint::Length(3), // Tabs
            Constraint::Min(0),    // Content
        ]
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(constraints)
        .split(f.area());

    if let (Some(tabs_area), Some(content)) = (chunks.first(), chunks.get(1)) {
        draw_tabs(f, app, *tabs_area);
        if app.current_tab == Tab::Activity {
            draw_dashboard(f, app, *content);
        } else if app.current_tab == Tab::Replication {
            draw_replication(f, app, *content);
        } else {
            draw_table(f, app, *content);
        }

        if let Some(footer_area) = chunks.get(2).filter(|_| show_controls) {
            draw_footer(f, app, *footer_area);
        }
    }

    if app.error_state.is_some() {
        draw_error_modal(f, app);
    } else if app.confirm_action.is_some() {
        draw_confirm_modal(f, app);
    } else if app.refresh_interval_modal.is_some() {
        draw_refresh_modal(f, app);
    } else if app.top_n_modal.is_some() {
        draw_top_n_modal(f, app);
    } else if app.theme_modal.is_some() {
        draw_theme_modal(f, app);
    } else if app.help_modal.is_some() {
        draw_help_modal(f, app);
    } else if app.query_detail.is_some() {
        draw_query_detail_modal(f, app);
    } else if app.explain_plan.is_some() {
        draw_explain_modal(f, app);
    } else if app.deadlock_graph.is_some() {
        draw_deadlock_modal(f, app);
    } else if app.table_definition.is_some() {
        draw_table_definition_modal(f, app);
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
        "6:Replication",
        "7:Settings",
        "8:Tools",
    ];
    let selected_tab = match app.current_tab {
        Tab::Activity => 0,
        Tab::Database => 1,
        Tab::Locks => 2,
        Tab::IO => 3,
        Tab::Statements => 4,
        Tab::Replication => 5,
        Tab::Settings => 6,
        Tab::Tools => 7,
    };
    let header_color = parse_color(&app.config.ui.header_border_color).unwrap_or(Color::Cyan);
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Views")
                .style(Style::default().fg(header_color)),
        )
        .select(selected_tab)
        .style(Style::default().fg(header_color))
        .highlight_style(
            Style::default()
                .fg(app
                    .config
                    .get_view_color("table", "header_fg")
                    .unwrap_or(Color::Yellow))
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, area);
}

#[allow(clippy::too_many_lines)]
fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let mut footer_spans = Vec::new();
    let accent_color = app
        .config
        .get_view_color("table", "header_fg")
        .unwrap_or(Color::Yellow);

    if app.input_mode == InputMode::Search {
        footer_spans.extend(vec![
            Span::styled(
                "SEARCH",
                Style::default()
                    .fg(accent_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(": ", Style::default().fg(Color::DarkGray)),
            Span::styled(&app.search_query, Style::default().fg(Color::LightBlue)),
            Span::styled("█", Style::default().fg(Color::White)),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "Esc",
                Style::default()
                    .fg(accent_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(":Cancel | ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "Enter",
                Style::default()
                    .fg(accent_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(":Keep Filter", Style::default().fg(Color::DarkGray)),
        ]);
    } else if let Some(notice) = app.notice_state.as_ref() {
        footer_spans.push(Span::styled(
            notice.message.clone(),
            Style::default().fg(Color::Green),
        ));
    } else if app.error_state.is_some() {
        footer_spans.extend(style_keybinding(
            "q:Quit | r:Retry | 1-8:Switch Tab",
            accent_color,
        ));
    } else if matches!(
        app.current_view_capability(),
        Some(CapabilityStatus::Unavailable(_))
    ) {
        footer_spans.extend(style_keybinding(
            "q:Quit | hjkl:Navigate | 1-8:Tabs",
            accent_color,
        ));
    } else if app.current_tab == Tab::Activity {
        if let Some(session) = app
            .selected_row
            .and_then(|selected_row| app.dashboard.sessions.get(selected_row))
        {
            footer_spans.extend(vec![
                Span::styled("QUERY: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    truncate_str(&format_activity_query(session), 60),
                    Style::default().fg(Color::White),
                ),
                Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            ]);
            footer_spans.extend(style_keybinding(
                "e:Export | /:Search | Enter:Save Query | a/w/b/t:Subviews | m:Metric",
                accent_color,
            ));
        } else {
            footer_spans.extend(style_keybinding(
                "q:Quit | hjkl:Navigate | 1-8:Tabs | e:Export | /:Search | Enter:Save Query | a/w/b/t:Subviews | m:Metric",
                accent_color,
            ));
        }
    } else if app.current_tab == Tab::Replication {
        let receiver_summary = app
            .replication
            .receiver_summary
            .as_deref()
            .unwrap_or("Receiver: inactive");
        footer_spans.extend(style_keybinding(
            "q:Quit | hjkl:Navigate | 1-8:Tabs | /:Search | n:Top",
            accent_color,
        ));
        footer_spans.extend(vec![
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled(receiver_summary, Style::default().fg(Color::White)),
        ]);
    } else if let Some(row_data) = app
        .selected_row
        .and_then(|selected_row| app.data.get(selected_row))
    {
        match app.current_tab {
            Tab::Statements => {
                let query = row_data.first().map_or("", String::as_str);
                footer_spans.extend(vec![
                    Span::styled("QUERY: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(truncate_str(query, 60), Style::default().fg(Color::White)),
                    Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                ]);
                footer_spans.extend(style_keybinding(
                    "e:Export | Enter:Save Query | i:Details",
                    accent_color,
                ));
            }
            Tab::Database => match &app.database_view {
                DatabaseView::Summary => {
                    let database = row_data.first().map_or("", String::as_str);
                    footer_spans.extend(vec![
                        Span::styled("DB: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(database, Style::default().fg(Color::White)),
                        Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                    ]);
                    footer_spans.extend(style_keybinding("Enter:Browse Tables", accent_color));
                }
                DatabaseView::Tables { database } => {
                    let node = row_data.first().map_or("", String::as_str);
                    footer_spans.extend(vec![
                        Span::styled("DB: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(database, Style::default().fg(Color::White)),
                        Span::styled(" | NODE: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(node.trim(), Style::default().fg(Color::White)),
                        Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                    ]);
                    footer_spans.extend(style_keybinding(
                        "s:Show | v:Vacuum | R:Reindex | Esc:Back",
                        accent_color,
                    ));
                }
            },
            Tab::Settings => {
                footer_spans.extend(style_keybinding(
                    "q:Quit | hjkl:Navigate | 1-8:Tabs | /:Search",
                    accent_color,
                ));
            }
            _ => footer_spans.extend(style_keybinding(
                "q:Quit | hjkl:Navigate | 1-8:Tabs",
                accent_color,
            )),
        }
    } else {
        match app.current_tab {
            Tab::Activity => {
                footer_spans.extend(style_keybinding(
                    "q:Quit | hjkl:Navigate | 1-8:Tabs | e:Export | /:Search | Enter:Save Query | a/w/b/t:Subviews | m:Metric",
                    accent_color,
                ));
            }
            Tab::Replication => {
                footer_spans.extend(style_keybinding(
                    "q:Quit | hjkl:Navigate | 1-8:Tabs | /:Search | n:Top",
                    accent_color,
                ));
            }
            Tab::Statements => {
                footer_spans.extend(style_keybinding(
                    "q:Quit | hjkl:Navigate | 1-8:Tabs | e:Export | Enter:Save Query | i:Details",
                    accent_color,
                ));
            }
            Tab::Database => match &app.database_view {
                DatabaseView::Summary => {
                    footer_spans.extend(style_keybinding(
                        "q:Quit | hjkl:Navigate | 1-8:Tabs | Enter:Browse Tables",
                        accent_color,
                    ));
                }
                DatabaseView::Tables { .. } => {
                    footer_spans.extend(style_keybinding(
                        "q:Quit | hjkl:Navigate | 1-8:Tabs | s:Show | v:Vacuum | R:Reindex | Esc:Back",
                        accent_color,
                    ));
                }
            },
            Tab::Locks => {
                footer_spans.extend(style_keybinding(
                    "q:Quit | hjkl:Navigate | 1-8:Tabs | i:Visualizer",
                    accent_color,
                ));
            }
            Tab::Settings => {
                footer_spans.extend(style_keybinding(
                    "q:Quit | hjkl:Navigate | 1-8:Tabs | /:Search",
                    accent_color,
                ));
            }
            _ => footer_spans.extend(style_keybinding(
                "q:Quit | hjkl:Navigate | 1-8:Tabs",
                accent_color,
            )),
        }
    }

    append_offline_status(&mut footer_spans, app, accent_color);
    append_theme_binding(&mut footer_spans, app, accent_color);
    append_help_binding(&mut footer_spans, app, accent_color);
    append_link_state(&mut footer_spans, app);

    let footer_color = parse_color(&app.config.ui.footer_border_color).unwrap_or(Color::Cyan);
    let footer = Paragraph::new(Line::from(footer_spans)).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Controls")
            .style(Style::default().fg(footer_color)),
    );
    f.render_widget(footer, area);
}

fn style_keybinding(text: &str, accent_color: Color) -> Vec<Span<'_>> {
    let mut spans = Vec::new();
    let parts: Vec<&str> = text.split(" | ").collect();
    for (i, part) in parts.iter().enumerate() {
        if let Some((key, desc)) = part.split_once(':') {
            spans.push(Span::styled(
                key,
                Style::default()
                    .fg(accent_color)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(":", Style::default().fg(Color::DarkGray)));
            spans.push(Span::styled(desc, Style::default().fg(Color::White)));
        } else {
            spans.push(Span::styled(*part, Style::default().fg(Color::White)));
        }
        if i < parts.len() - 1 {
            spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
        }
    }
    spans
}

fn append_offline_status(footer_spans: &mut Vec<Span<'_>>, app: &App, accent_color: Color) {
    if !app.is_offline() {
        return;
    }

    if !footer_spans.is_empty() {
        footer_spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
    }

    footer_spans.push(Span::styled(
        "OFFLINE",
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    ));
    footer_spans.push(Span::styled(": ", Style::default().fg(Color::DarkGray)));

    let status = if app.is_reconnecting() {
        let error = truncate_str(app.offline_error_summary().unwrap_or("refresh failed"), 50);
        format!("reconnecting | error: {error}")
    } else {
        let retry_in = app
            .offline_retry_remaining()
            .map_or(0, |duration| duration.as_secs());
        let last_ok = app
            .offline_last_success_elapsed()
            .map_or(0, |duration| duration.as_secs());
        let error = truncate_str(app.offline_error_summary().unwrap_or("refresh failed"), 40);
        format!("retry in {retry_in}s | last ok {last_ok}s ago | {error}")
    };

    footer_spans.push(Span::styled(status, Style::default().fg(Color::LightRed)));

    if app.can_manual_reconnect() {
        footer_spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
        footer_spans.extend(style_keybinding("c:Reconnect", accent_color));
    }
}

fn append_theme_binding(footer_spans: &mut Vec<Span<'_>>, app: &App, accent_color: Color) {
    if app.input_mode != InputMode::Normal
        || !app.has_theme_options()
        || app.notice_state.is_some()
        || app.error_state.is_some()
    {
        return;
    }

    if !footer_spans.is_empty() {
        footer_spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
    }
    footer_spans.extend(style_keybinding("T:Theme", accent_color));
}

fn append_help_binding(footer_spans: &mut Vec<Span<'_>>, app: &App, accent_color: Color) {
    if app.input_mode != InputMode::Normal
        || app.notice_state.is_some()
        || app.error_state.is_some()
    {
        return;
    }

    if !footer_spans.is_empty() {
        footer_spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
    }
    footer_spans.extend(style_keybinding("?:Help", accent_color));
}

fn append_link_state(footer_spans: &mut Vec<Span<'_>>, app: &App) {
    if app.notice_state.is_some() || app.error_state.is_some() || app.is_offline() {
        return;
    }

    let (link_state, style) = if app.high_latency_detected() {
        (
            "Slow link",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        ("Stable link", Style::default().fg(Color::DarkGray))
    };

    if !footer_spans.is_empty() {
        footer_spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
    }

    footer_spans.push(Span::styled(link_state, style));
}

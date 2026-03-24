use super::common::centered_rect;
use crate::tui::app::{App, QueryDetailSource, TableDefinitionState, format::is_normalized_query};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
};

pub fn draw_loading_modal(f: &mut Frame, app: &App) {
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

#[allow(clippy::too_many_lines)]
pub fn draw_query_detail_modal(f: &mut Frame, app: &App) {
    let Some(detail) = app.query_detail.as_ref() else {
        return;
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let modal_height = (f64::from(f.area().height) * 0.9) as u16;
    let area = centered_rect(f.area(), 95, modal_height);
    f.render_widget(Clear, area);

    let title = if detail.database.is_empty() {
        " Query Detail ".to_string()
    } else {
        format!(" Query Detail ({}) ", detail.database)
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::styled(
            title,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Black));
    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let mut constraints = vec![
        ratatui::layout::Constraint::Length(2), // Title + Warning
    ];

    if let Some(act) = &detail.activity_detail {
        constraints.push(ratatui::layout::Constraint::Length(4)); // Basic Activity Info
        if !act.blockers.is_empty() {
            #[allow(clippy::cast_possible_truncation)]
            constraints.push(ratatui::layout::Constraint::Length(
                (act.blockers.len() as u16 + 2).min(8),
            ));
        }
        if !act.locks.is_empty() {
            #[allow(clippy::cast_possible_truncation)]
            constraints.push(ratatui::layout::Constraint::Length(
                (act.locks.len() as u16 + 2).min(8),
            ));
        }
    }

    if detail.stats.is_some() {
        constraints.push(ratatui::layout::Constraint::Length(3)); // Stats
    }

    constraints.push(ratatui::layout::Constraint::Min(0)); // Query (Wraps)
    constraints.push(ratatui::layout::Constraint::Length(1)); // Footer

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints(constraints)
        .split(inner_area);

    let mut current_chunk = 0;

    // 1. Render Title/Header
    if let Some(area) = chunks.get(current_chunk) {
        f.render_widget(
            Paragraph::new(vec![
                Line::styled(
                    "Query Information",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                if detail.source == QueryDetailSource::Statements {
                    Line::styled(
                        "NOTE: Statements view uses normalized pg_stat_statements SQL. Explain is not available here.",
                        Style::default().fg(Color::Magenta),
                    )
                } else if is_normalized_query(detail.query.as_str()) {
                    Line::styled(
                        "NOTE: Normalized query ($1). Explain is unavailable without actual values.",
                        Style::default().fg(Color::Magenta),
                    )
                } else {
                    Line::raw("")
                },
            ]),
            *area,
        );
    }
    current_chunk += 1;

    // 2. Render Activity Detail if available
    if let Some(act) = &detail.activity_detail {
        if let Some(area) = chunks.get(current_chunk) {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled("PID: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(&act.pid, Style::default().fg(Color::White)),
                    Span::raw(" | "),
                    Span::styled("User: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(&act.usename, Style::default().fg(Color::White)),
                    Span::raw(" | "),
                    Span::styled("App: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(&act.application_name, Style::default().fg(Color::White)),
                    Span::raw(" | "),
                    Span::styled("Client: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{}:{}", act.client_addr, act.client_port),
                        Style::default().fg(Color::White),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("State: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        &act.state,
                        if act.state == "active" {
                            Style::default().fg(Color::Green)
                        } else if act.state.contains("idle in transaction") {
                            Style::default().fg(Color::Red)
                        } else {
                            Style::default().fg(Color::White)
                        },
                    ),
                    Span::raw(" | "),
                    Span::styled("Wait: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{} / {}", act.wait_event_type, act.wait_event),
                        if act.wait_event_type == "Lock" {
                            Style::default().fg(Color::Red)
                        } else {
                            Style::default().fg(Color::White)
                        },
                    ),
                ]),
                Line::from(vec![
                    Span::styled("Backend Start: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(&act.backend_start, Style::default().fg(Color::White)),
                    Span::raw(" | "),
                    Span::styled("Xact Start: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(&act.xact_start, Style::default().fg(Color::White)),
                    Span::raw(" | "),
                    Span::styled("State Change: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(&act.state_change, Style::default().fg(Color::White)),
                ]),
                if act.blocking_pids == "{}" {
                    Line::raw("")
                } else {
                    Line::from(vec![
                        Span::styled("Waiting on PIDs: ", Style::default().fg(Color::Red)),
                        Span::styled(&act.blocking_pids, Style::default().fg(Color::White)),
                    ])
                },
            ];
            if act.state.is_empty() {
                lines = vec![Line::styled(
                    "Loading extended activity details...",
                    Style::default().fg(Color::DarkGray),
                )];
            }
            f.render_widget(Paragraph::new(lines), *area);
        }
        current_chunk += 1;

        // 3. Render Blockers
        if !act.blockers.is_empty() {
            if let Some(area) = chunks.get(current_chunk) {
                let header = Row::new(vec![
                    Cell::from("Blocked PID"),
                    Cell::from("User"),
                    Cell::from("State"),
                    Cell::from("Duration"),
                    Cell::from("Query"),
                ])
                .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD));
                let rows: Vec<Row> = act
                    .blockers
                    .iter()
                    .map(|b| {
                        Row::new(vec![
                            Cell::from(b.first().cloned().unwrap_or_default()),
                            Cell::from(b.get(1).cloned().unwrap_or_default()),
                            Cell::from(b.get(2).cloned().unwrap_or_default()),
                            Cell::from(format!("{}s", b.get(3).cloned().unwrap_or_default())),
                            Cell::from(b.get(4).cloned().unwrap_or_default()),
                        ])
                    })
                    .collect();

                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(10),
                        Constraint::Length(15),
                        Constraint::Length(10),
                        Constraint::Length(10),
                        Constraint::Min(0),
                    ],
                )
                .header(header)
                .block(
                    Block::default()
                        .borders(Borders::TOP)
                        .title(" Blocking Sessions "),
                );
                f.render_widget(table, *area);
            }
            current_chunk += 1;
        }

        // 4. Render Locks
        if !act.locks.is_empty() {
            if let Some(area) = chunks.get(current_chunk) {
                let header = Row::new(vec![
                    Cell::from("Lock Type"),
                    Cell::from("Mode"),
                    Cell::from("Granted"),
                    Cell::from("Relation"),
                ])
                .style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                );
                let rows: Vec<Row> = act
                    .locks
                    .iter()
                    .map(|l| {
                        Row::new(vec![
                            Cell::from(l.first().cloned().unwrap_or_default()),
                            Cell::from(l.get(1).cloned().unwrap_or_default()),
                            Cell::from(l.get(2).cloned().unwrap_or_default()),
                            Cell::from(l.get(3).cloned().unwrap_or_default()),
                        ])
                    })
                    .collect();

                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(15),
                        Constraint::Length(25),
                        Constraint::Length(10),
                        Constraint::Min(0),
                    ],
                )
                .header(header)
                .block(Block::default().borders(Borders::TOP).title(" Locks Held "));
                f.render_widget(table, *area);
            }
            current_chunk += 1;
        }
    }

    // 5. Render Stats if available
    if let (Some(stats), Some(area)) = (detail.stats.as_ref(), chunks.get(current_chunk)) {
        let stats_lines = vec![
            Line::from(vec![
                Span::styled("total: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&stats.total_time),
                Span::raw(" ms | "),
                Span::styled("mean: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&stats.mean_time),
                Span::raw(" ms | "),
                Span::styled("calls: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&stats.calls),
            ]),
            Line::from(vec![
                Span::styled("read: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&stats.read_time),
                Span::raw(" ms | "),
                Span::styled("write: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&stats.write_time),
                Span::raw(" ms"),
            ]),
        ];
        f.render_widget(Paragraph::new(stats_lines), *area);
        current_chunk += 1;
    }

    // 6. Render Query
    if let Some(area) = chunks.get(current_chunk) {
        let query_text = if let Some(act) = &detail.activity_detail {
            if act.query.is_empty() {
                detail.query.as_str()
            } else {
                act.query.as_str()
            }
        } else {
            detail.query.as_str()
        };
        f.render_widget(
            Paragraph::new(query_text)
                .wrap(Wrap { trim: true })
                .block(Block::default().borders(Borders::TOP).title(" Query ")),
            *area,
        );
    }

    // 7. Render Footer
    if let Some(area) = chunks.last() {
        let mut footer_spans = if detail.source == QueryDetailSource::Statements {
            Vec::new()
        } else if is_normalized_query(detail.query.as_str()) {
            vec![
                Span::styled(
                    "x",
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    ":Explain unavailable | ",
                    Style::default().fg(Color::DarkGray),
                ),
            ]
        } else {
            vec![
                Span::styled(
                    "x",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(":Explain Analyze | ", Style::default().fg(Color::Cyan)),
                Span::styled(
                    "K",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Span::styled(":Terminate Session | ", Style::default().fg(Color::Cyan)),
            ]
        };
        footer_spans.extend([
            Span::styled(
                "Enter",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(":Save Query | ", Style::default().fg(Color::Cyan)),
            Span::styled(
                "Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(":Close", Style::default().fg(Color::Cyan)),
        ]);
        let footer = Line::from(footer_spans);
        f.render_widget(Paragraph::new(footer).alignment(Alignment::Right), *area);
    }
}

pub fn draw_explain_modal(f: &mut Frame, app: &App) {
    let Some(explain) = app.explain_plan.as_ref() else {
        return;
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let modal_height = (f64::from(f.area().height) * 0.9) as u16;
    let area = centered_rect(f.area(), 95, modal_height);
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::styled(
            " Explain Analyze Plan ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ))
        .border_style(Style::default().fg(Color::Green))
        .style(Style::default().bg(Color::Black));
    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let plan_text: Vec<Line> = explain
        .plan
        .iter()
        .map(|line| Line::from(Span::styled(line, Style::default().fg(Color::White))))
        .collect();

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Min(0),    // Plan
            ratatui::layout::Constraint::Length(1), // Footer
        ])
        .split(inner_area);

    if let (Some(plan_area), Some(footer_area)) = (chunks.first(), chunks.get(1)) {
        f.render_widget(
            Paragraph::new(plan_text).wrap(Wrap { trim: false }),
            *plan_area,
        );

        let footer = Line::from(vec![
            Span::styled(
                "Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(":Close Plan", Style::default().fg(Color::Cyan)),
        ]);
        f.render_widget(
            Paragraph::new(footer).alignment(Alignment::Right),
            *footer_area,
        );
    }
}

pub fn draw_table_definition_modal(f: &mut Frame, app: &App) {
    let Some(detail) = app.table_definition.as_ref() else {
        return;
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let modal_height = (f64::from(f.area().height) * 0.9) as u16;
    let area = centered_rect(f.area(), 95, modal_height);
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::styled(
            format!(" Definition: {}.{} ", detail.schema, detail.name),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Black));
    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1), // Title
            ratatui::layout::Constraint::Min(0),    // Columns Table
            ratatui::layout::Constraint::Length(1), // Indexes Title
            ratatui::layout::Constraint::Length(5), // Indexes List (min)
            ratatui::layout::Constraint::Length(1), // Footer
        ])
        .split(inner_area);

    if let (
        Some(col_title_area),
        Some(col_table_area),
        Some(idx_title_area),
        Some(idx_list_area),
        Some(footer_area),
    ) = (
        chunks.first(),
        chunks.get(1),
        chunks.get(2),
        chunks.get(3),
        chunks.get(4),
    ) {
        draw_table_definition_columns(f, *col_title_area, *col_table_area, detail);
        draw_table_definition_indexes(f, *idx_title_area, *idx_list_area, detail);
        draw_close_footer(f, *footer_area);
    }
}

fn draw_table_definition_columns(
    f: &mut Frame,
    title_area: Rect,
    table_area: Rect,
    detail: &TableDefinitionState,
) {
    f.render_widget(section_title("Columns"), title_area);

    let col_headers = Row::new(vec!["Column", "Type", "Len", "Nullable", "Default"])
        .style(Style::default().fg(Color::Cyan))
        .bottom_margin(1);

    let col_rows = detail.columns.iter().map(|column| {
        Row::new(vec![
            styled_cell(column.first().cloned().unwrap_or_default(), Color::White),
            styled_cell(column.get(1).cloned().unwrap_or_default(), Color::White),
            styled_cell(column.get(2).cloned().unwrap_or_default(), Color::DarkGray),
            styled_cell(column.get(3).cloned().unwrap_or_default(), Color::DarkGray),
            styled_cell(column.get(4).cloned().unwrap_or_default(), Color::DarkGray),
        ])
    });

    let col_table = Table::new(
        col_rows,
        [
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Min(0),
        ],
    )
    .header(col_headers);
    f.render_widget(col_table, table_area);
}

fn draw_table_definition_indexes(
    f: &mut Frame,
    title_area: Rect,
    list_area: Rect,
    detail: &TableDefinitionState,
) {
    f.render_widget(section_title("Indexes"), title_area);

    let idx_text: Vec<Line> = detail
        .indexes
        .iter()
        .map(|index| Line::from(Span::styled(index, Style::default().fg(Color::White))))
        .collect();
    f.render_widget(
        Paragraph::new(idx_text).wrap(Wrap { trim: false }),
        list_area,
    );
}

fn draw_close_footer(f: &mut Frame, footer_area: Rect) {
    let footer = Line::from(vec![
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(":Close", Style::default().fg(Color::Cyan)),
    ]);
    f.render_widget(
        Paragraph::new(footer).alignment(Alignment::Right),
        footer_area,
    );
}

fn section_title(title: &'static str) -> Paragraph<'static> {
    Paragraph::new(Line::styled(
        title,
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    ))
}

fn styled_cell(value: String, color: Color) -> Cell<'static> {
    Cell::from(value).style(Style::default().fg(color))
}

pub fn draw_deadlock_modal(f: &mut Frame, app: &App) {
    let Some(deadlock) = app.deadlock_graph.as_ref() else {
        return;
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let modal_height = (f64::from(f.area().height) * 0.9) as u16;
    let area = centered_rect(f.area(), 95, modal_height);
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::styled(
            " Deadlock Visualizer ",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ))
        .border_style(Style::default().fg(Color::Red))
        .style(Style::default().bg(Color::Black));
    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let graph_text: Vec<Line> = deadlock
        .graph
        .iter()
        .map(|line| {
            let style = if line.contains("BLOCKER") {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else if line.contains("!!!") {
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            Line::from(Span::styled(line, style))
        })
        .collect();

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Min(0),    // Graph
            ratatui::layout::Constraint::Length(1), // Footer
        ])
        .split(inner_area);

    if let (Some(graph_area), Some(footer_area)) = (chunks.first(), chunks.get(1)) {
        f.render_widget(
            Paragraph::new(graph_text).wrap(Wrap { trim: false }),
            *graph_area,
        );

        let footer = Line::from(vec![
            Span::styled(
                "Esc",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(":Close Visualizer", Style::default().fg(Color::Cyan)),
        ]);
        f.render_widget(
            Paragraph::new(footer).alignment(Alignment::Right),
            *footer_area,
        );
    }
}

pub fn draw_error_modal(f: &mut Frame, app: &App) {
    let Some(error_state) = app.error_state.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 80, 12);
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

pub fn draw_confirm_modal(f: &mut Frame, app: &App) {
    let Some(action) = app.confirm_action.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 80, 16);
    let content = vec![
        Line::styled(
            action.title.as_str(),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::styled("COMMAND TO RUN:", Style::default().fg(Color::DarkGray)),
        Line::styled(action.query.as_str(), Style::default().fg(Color::Yellow)),
        Line::raw(""),
        Line::styled(
            action.description.as_str(),
            Style::default().fg(Color::LightBlue),
        ),
        Line::raw(""),
        Line::raw(""),
        Line::styled(
            "Press y to execute or n to cancel.",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    ];

    let modal = Paragraph::new(content)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(Line::styled(
                    " Confirm Action ",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ))
                .border_style(Style::default().fg(Color::Red))
                .style(Style::default().bg(Color::Black)),
        );

    f.render_widget(Clear, area);
    f.render_widget(modal, area);
}

pub fn draw_refresh_modal(f: &mut Frame, app: &App) {
    let Some(modal) = app.refresh_interval_modal.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 40, 12);
    let mut lines = vec![
        Line::styled(
            "Select Refresh Interval",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
    ];

    for (i, &ms) in modal.options.iter().enumerate() {
        let is_selected = i == modal.selected_index;
        let is_current = ms == app.refresh_ms;

        let symbol = if is_selected { "> " } else { "  " };
        let current_tag = if is_current { " (current)" } else { "" };
        #[allow(clippy::cast_precision_loss)]
        let label = format!("{}{}s{}", symbol, ms as f64 / 1000.0, current_tag);

        let style = if is_selected {
            Style::default().fg(Color::Black).bg(Color::White)
        } else if is_current {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        };

        lines.push(Line::styled(label, style));
    }

    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "Enter:Select | Esc/r:Cancel",
        Style::default().fg(Color::DarkGray),
    ));

    let modal_widget = Paragraph::new(lines).alignment(Alignment::Center).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Refresh Settings ")
            .border_style(Style::default().fg(Color::Cyan))
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, area);
    f.render_widget(modal_widget, area);
}

pub fn draw_top_n_modal(f: &mut Frame, app: &App) {
    let Some(modal) = app.top_n_modal.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 40, 10);
    let mut lines = vec![
        Line::styled(
            "Select Display Limit",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
    ];

    for (i, &n) in modal.options.iter().enumerate() {
        let is_selected = i == modal.selected_index;
        let is_current = n == app.top_n;

        let symbol = if is_selected { "> " } else { "  " };
        let current_tag = if is_current { " (current)" } else { "" };
        let label_text = if n == 0 {
            "All".to_string()
        } else {
            n.to_string()
        };
        let label = format!("{symbol}{label_text} {current_tag}");

        let style = if is_selected {
            Style::default().fg(Color::Black).bg(Color::White)
        } else if is_current {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        };

        lines.push(Line::styled(label, style));
    }

    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "Enter:Select | Esc/n:Cancel",
        Style::default().fg(Color::DarkGray),
    ));

    let modal_widget = Paragraph::new(lines).alignment(Alignment::Center).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Top-N Limit ")
            .border_style(Style::default().fg(Color::Cyan))
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, area);
    f.render_widget(modal_widget, area);
}

pub fn draw_theme_modal(f: &mut Frame, app: &App) {
    let Some(modal) = app.theme_modal.as_ref() else {
        return;
    };

    let area = centered_rect(f.area(), 48, 12);
    let mut lines = vec![
        Line::styled(
            "Select Theme",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
    ];

    for (i, name) in modal.options.iter().enumerate() {
        let is_selected = i == modal.selected_index;
        let is_active = app.active_theme_name().is_some_and(|active| active == name);
        let symbol = if is_selected { "> " } else { "  " };
        let active_tag = if is_active { " (current)" } else { "" };
        let label = format!("{symbol}{name}{active_tag}");

        let style = if is_selected {
            Style::default().fg(Color::Black).bg(Color::White)
        } else if is_active {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        };

        lines.push(Line::styled(label, style));
    }

    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "Enter:Apply | Esc/T:Cancel",
        Style::default().fg(Color::DarkGray),
    ));
    lines.push(Line::styled(
        "Applies to the current session.",
        Style::default().fg(Color::DarkGray),
    ));

    let modal_widget = Paragraph::new(lines).alignment(Alignment::Center).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Theme ")
            .border_style(Style::default().fg(Color::Cyan))
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, area);
    f.render_widget(modal_widget, area);
}

pub fn draw_help_modal(f: &mut Frame, app: &App) {
    let Some(modal) = app.help_modal.as_ref() else {
        return;
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let modal_height = (f64::from(f.area().height) * 0.8) as u16;
    let area = centered_rect(f.area(), 82, modal_height);
    let mut lines = Vec::new();

    for section in &modal.sections {
        lines.push(Line::styled(
            section.heading.as_str(),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        for line in &section.lines {
            lines.push(Line::from(vec![
                Span::styled("- ", Style::default().fg(Color::DarkGray)),
                Span::styled(line.as_str(), Style::default().fg(Color::White)),
            ]));
        }
        lines.push(Line::raw(""));
    }

    lines.push(Line::styled(
        "Esc/?:Close",
        Style::default().fg(Color::DarkGray),
    ));

    let modal_widget = Paragraph::new(lines).wrap(Wrap { trim: true }).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" {} ", modal.title))
            .border_style(Style::default().fg(Color::Cyan))
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, area);
    f.render_widget(modal_widget, area);
}

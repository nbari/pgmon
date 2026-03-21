use super::common::centered_rect;
use crate::tui::app::App;
use ratatui::{
    Frame,
    layout::Alignment,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
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

pub fn draw_statement_detail_modal(f: &mut Frame, app: &App) {
    let Some(detail) = app.statement_detail.as_ref() else {
        return;
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let modal_height = (f64::from(f.area().height) * 0.8) as u16;
    let area = centered_rect(f.area(), 90, modal_height);
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::styled(
            " Statement Detail ",
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
            ratatui::layout::Constraint::Length(3), // Stats
            ratatui::layout::Constraint::Min(0),    // Query (Wraps)
            ratatui::layout::Constraint::Length(1), // Footer
        ])
        .split(inner_area);

    if let (Some(title_area), Some(stats_area), Some(query_area), Some(footer_area)) =
        (chunks.first(), chunks.get(1), chunks.get(2), chunks.get(3))
    {
        // Render Title
        f.render_widget(
            Paragraph::new(Line::styled(
                "Statement Detail",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            *title_area,
        );

        // Render Stats
        let stats = vec![
            Line::from(vec![
                Span::styled("total: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&detail.total_time),
                Span::raw(" ms | "),
                Span::styled("mean: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&detail.mean_time),
                Span::raw(" ms | "),
                Span::styled("calls: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&detail.calls),
            ]),
            Line::from(vec![
                Span::styled("read: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&detail.read_time),
                Span::raw(" ms | "),
                Span::styled("write: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&detail.write_time),
                Span::raw(" ms"),
            ]),
        ];
        f.render_widget(Paragraph::new(stats), *stats_area);

        // Render Query (Wrapped)
        f.render_widget(
            Paragraph::new(detail.query.as_str()).wrap(Wrap { trim: true }),
            *query_area,
        );

        // Render Footer
        let footer = Line::from(vec![
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

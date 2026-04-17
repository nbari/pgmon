use crate::pg::client::CapabilityStatus;
use crate::tui::app::{
    App,
    format::{is_fuzzy_match, sender_to_row, slot_to_row},
};
use crate::tui::ui::common::centered_rect;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table},
};

pub fn draw_replication(f: &mut Frame, app: &App, area: Rect) {
    if let Some(reason) = replication_unavailable_reason(app) {
        draw_replication_unavailable(f, app, area, reason);
        return;
    }

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Min(8),
        ])
        .split(area);

    if let Some(summary_area) = sections.first() {
        draw_receiver_summary(f, app, *summary_area);
    }
    if let Some(senders_area) = sections.get(1) {
        draw_senders_table(f, app, *senders_area);
    }
    if let Some(slots_area) = sections.get(2) {
        draw_slots_table(f, app, *slots_area);
    }
}

fn replication_unavailable_reason(app: &App) -> Option<&str> {
    match &app.capabilities.replication {
        CapabilityStatus::Unavailable(reason) => Some(reason.as_str()),
        CapabilityStatus::Available | CapabilityStatus::Unknown => None,
    }
}

fn draw_replication_unavailable(f: &mut Frame, app: &App, area: Rect, reason: &str) {
    let modal_area = centered_rect(area, 70, 7);
    let paragraph = Paragraph::new(vec![
        Line::styled(
            "Replication Unavailable",
            Style::default()
                .fg(app
                    .config
                    .get_view_color("table", "header_fg")
                    .unwrap_or(Color::Yellow))
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::styled(reason, Style::default().fg(Color::White)),
        Line::styled(
            "Enable replication settings or connect to a server with replication configured.",
            Style::default().fg(Color::DarkGray),
        ),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Replication ")
            .style(Style::default().bg(Color::Black)),
    );

    f.render_widget(Clear, modal_area);
    f.render_widget(paragraph, modal_area);
}

fn draw_receiver_summary(f: &mut Frame, app: &App, area: Rect) {
    let summary = app
        .replication
        .receiver_summary
        .as_deref()
        .unwrap_or("Receiver: inactive");
    let paragraph = Paragraph::new(Line::from(vec![
        Span::styled(
            "Standby Receiver ",
            Style::default()
                .fg(app
                    .config
                    .get_view_color("table", "header_fg")
                    .unwrap_or(Color::Yellow))
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(summary, Style::default().fg(Color::White)),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Replication "),
    );
    f.render_widget(paragraph, area);
}

fn draw_senders_table(f: &mut Frame, app: &App, area: Rect) {
    let rows = sender_rows(app)
        .into_iter()
        .map(|row| {
            Row::new(
                row.into_iter()
                    .enumerate()
                    .map(|(column, value)| Cell::from(value).style(sender_cell_style(column))),
            )
        })
        .collect::<Vec<_>>();

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(11),
        ],
    )
    .header(
        Row::new([
            "PID",
            "User",
            "App",
            "Client",
            "State",
            "Sync",
            "Slot",
            "Sent Lag",
            "Write Lag",
            "Flush Lag",
            "Replay Lag",
        ])
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
            .title(" WAL Senders "),
    );

    f.render_widget(table, area);
}

fn draw_slots_table(f: &mut Frame, app: &App, area: Rect) {
    let rows = slot_rows(app)
        .into_iter()
        .map(|row| {
            Row::new(
                row.into_iter()
                    .enumerate()
                    .map(|(column, value)| Cell::from(value).style(slot_cell_style(column))),
            )
        })
        .collect::<Vec<_>>();

    let table = Table::new(
        rows,
        [
            Constraint::Length(18),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(18),
            Constraint::Min(20),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new([
            "Slot",
            "Type",
            "Active",
            "PID",
            "Restart LSN",
            "Confirmed Flush LSN",
            "WAL Status",
        ])
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
            .title(" Replication Slots "),
    );

    f.render_widget(table, area);
}

fn sender_rows(app: &App) -> Vec<Vec<String>> {
    let mut rows = app
        .replication
        .senders
        .iter()
        .map(sender_to_row)
        .collect::<Vec<_>>();
    apply_replication_filters(&mut rows, &app.search_query, app.top_n);

    if rows.is_empty() {
        return vec![vec![
            "No active WAL senders".to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]];
    }

    rows
}

fn slot_rows(app: &App) -> Vec<Vec<String>> {
    let mut rows = app
        .replication
        .slots
        .iter()
        .map(slot_to_row)
        .collect::<Vec<_>>();
    apply_replication_filters(&mut rows, &app.search_query, app.top_n);

    if rows.is_empty() {
        return vec![vec![
            "No replication slots".to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]];
    }

    rows
}

fn apply_replication_filters(rows: &mut Vec<Vec<String>>, search_query: &str, top_n: u32) {
    if !search_query.is_empty() {
        rows.retain(|row| row.iter().any(|cell| is_fuzzy_match(cell, search_query)));
    }

    if top_n > 0 {
        rows.truncate(top_n as usize);
    }
}

fn sender_cell_style(column: usize) -> Style {
    match column {
        0..=6 => Style::default().fg(Color::White),
        _ => Style::default().fg(Color::DarkGray),
    }
}

fn slot_cell_style(column: usize) -> Style {
    match column {
        0..=2 => Style::default().fg(Color::White),
        _ => Style::default().fg(Color::DarkGray),
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_replication_filters, sender_rows, sender_to_row, slot_to_row};
    use crate::pg::client::{ReplicationSender, ReplicationSlot};
    use crate::tui::app::App;

    #[test]
    fn test_apply_replication_filters_respects_search_and_top_n() {
        let mut rows = vec![
            vec!["replica_a".to_string(), "streaming".to_string()],
            vec!["replica_b".to_string(), "sync".to_string()],
        ];

        apply_replication_filters(&mut rows, "rb", 1);

        assert_eq!(
            rows,
            vec![vec!["replica_b".to_string(), "sync".to_string()]]
        );
    }

    #[test]
    fn test_sender_to_row_formats_lag_bytes() {
        let sender = ReplicationSender {
            pid: "1".to_string(),
            user: "postgres".to_string(),
            application: "walreceiver".to_string(),
            client: "127.0.0.1".to_string(),
            state: "streaming".to_string(),
            sync_state: "sync".to_string(),
            slot_name: "slot_a".to_string(),
            sent_lag_bytes: 1024,
            write_lag_bytes: 0,
            flush_lag_bytes: 0,
            replay_lag_bytes: 0,
        };

        let row = sender_to_row(&sender);

        assert_eq!(row.get(7).map(String::as_str), Some("1.0 KiB"));
    }

    #[test]
    fn test_slot_to_row_preserves_slot_metadata() {
        let slot = ReplicationSlot {
            slot_name: "slot_a".to_string(),
            slot_type: "physical".to_string(),
            active: "yes".to_string(),
            active_pid: "42".to_string(),
            restart_lsn: "0/123".to_string(),
            confirmed_flush_lsn: String::new(),
            wal_status: "reserved".to_string(),
        };

        let row = slot_to_row(&slot);

        assert_eq!(row.first().map(String::as_str), Some("slot_a"));
        assert_eq!(row.get(6).map(String::as_str), Some("reserved"));
    }

    #[test]
    fn test_sender_rows_empty_state_uses_first_column() {
        let app = App::new(
            String::new(),
            0,
            None,
            1000,
            10,
            "activity",
            "",
            crate::config::Config::default(),
        );

        let rows = sender_rows(&app);

        assert_eq!(rows.len(), 1);
        let first_row = rows.first();
        assert_eq!(
            first_row.and_then(|row| row.first()).map(String::as_str),
            Some("No active WAL senders")
        );
        assert!(first_row.is_some_and(|row| row.iter().skip(1).all(String::is_empty)));
    }
}

use ratatui::layout::{Constraint, Flex, Layout, Rect};

pub fn centered_rect(area: Rect, width_percentage: u16, height: u16) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(height),
        Constraint::Fill(1),
    ])
    .flex(Flex::Center)
    .split(area);
    let Some(mid_row) = vertical.get(1) else {
        return area;
    };

    let horizontal = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Percentage(width_percentage),
        Constraint::Fill(1),
    ])
    .flex(Flex::Center)
    .split(*mid_row);

    horizontal.get(1).copied().unwrap_or(*mid_row)
}

pub fn truncate_str(text: &str, max_len: usize) -> String {
    if text.len() > max_len {
        format!("{}...", &text[..max_len.saturating_sub(3)])
    } else {
        text.to_string()
    }
}

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

pub fn format_duration_hms(duration_seconds: i64) -> String {
    let duration_seconds = duration_seconds.max(0);
    let hours = duration_seconds / 3600;
    let minutes = (duration_seconds % 3600) / 60;
    let seconds = duration_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

pub fn format_uptime(uptime_seconds: i64) -> String {
    let uptime_seconds = uptime_seconds.max(0);
    let days = uptime_seconds / 86_400;
    let hours = (uptime_seconds % 86_400) / 3600;
    let minutes = (uptime_seconds % 3600) / 60;

    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

pub fn format_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let negative = bytes < 0;
    let absolute = u128::from(bytes.unsigned_abs());
    let mut unit_index = 0usize;
    let mut divisor = 1u128;

    while unit_index + 1 < UNITS.len() && absolute >= divisor * 1024 {
        divisor *= 1024;
        unit_index += 1;
    }

    let unit = UNITS.get(unit_index).copied().unwrap_or("B");
    let sign = if negative { "-" } else { "" };

    if unit_index == 0 {
        format!("{sign}{absolute} {unit}")
    } else {
        let scaled_tenths = absolute.saturating_mul(10) / divisor;
        let whole = scaled_tenths / 10;
        let tenths = scaled_tenths % 10;
        format!("{sign}{whole}.{tenths} {unit}")
    }
}

pub fn truncate_str(text: &str, max_len: usize) -> String {
    if text.len() > max_len {
        format!("{}...", &text[..max_len.saturating_sub(3)])
    } else {
        text.to_string()
    }
}

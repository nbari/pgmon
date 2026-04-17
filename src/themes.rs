//! Built-in color themes shipped with `pgmon`.

use crate::config::{ThemeConfig, ViewConfig};
use std::collections::HashMap;

/// Return all built-in themes keyed by their public theme name.
pub(crate) fn built_in_themes() -> HashMap<String, ThemeConfig> {
    let mut themes = HashMap::new();
    themes.insert("calibrachoa".to_string(), calibrachoa());
    themes.insert("sky".to_string(), sky());
    themes.insert("mint".to_string(), mint());
    themes.insert("retro".to_string(), retro());
    themes
}

fn calibrachoa() -> ThemeConfig {
    ThemeConfig::new(
        Some(("#ff4fa3", "#ff9f1c")),
        &[
            ("settings", &[("value", "#7bff00")]),
            (
                "table",
                &[
                    ("header_fg", "#ff9f1c"),
                    ("highlight_fg", "#000000"),
                    ("highlight_bg", "#ff4fa3"),
                ],
            ),
            (
                "chart",
                &[
                    ("line1", "#ff4fa3"),
                    ("line2", "#ff9f1c"),
                    ("line3", "#7bff00"),
                ],
            ),
        ],
    )
}

fn sky() -> ThemeConfig {
    ThemeConfig::new(
        Some(("#8fa1b3", "#93a8a3")),
        &[
            ("settings", &[("value", "#b7a8ba")]),
            (
                "table",
                &[
                    ("header_fg", "#93a8a3"),
                    ("highlight_fg", "#000000"),
                    ("highlight_bg", "#8fa1b3"),
                ],
            ),
            (
                "chart",
                &[
                    ("line1", "#8fa1b3"),
                    ("line2", "#93a8a3"),
                    ("line3", "#b7a8ba"),
                ],
            ),
        ],
    )
}

fn mint() -> ThemeConfig {
    ThemeConfig::new(
        Some(("#9db39d", "#9eb1ac")),
        &[
            ("settings", &[("value", "#adb4c7")]),
            (
                "table",
                &[
                    ("header_fg", "#9eb1ac"),
                    ("highlight_fg", "#000000"),
                    ("highlight_bg", "#9db39d"),
                ],
            ),
            (
                "chart",
                &[
                    ("line1", "#9db39d"),
                    ("line2", "#9eb1ac"),
                    ("line3", "#adb4c7"),
                ],
            ),
        ],
    )
}

fn retro() -> ThemeConfig {
    ThemeConfig::new(
        Some(("#6f8f73", "#5f7a62")),
        &[
            ("settings", &[("value", "#8ea68c")]),
            (
                "table",
                &[
                    ("header_fg", "#5f7a62"),
                    ("highlight_fg", "#000000"),
                    ("highlight_bg", "#6f8f73"),
                ],
            ),
            (
                "chart",
                &[
                    ("line1", "#6f8f73"),
                    ("line2", "#5f7a62"),
                    ("line3", "#8ea68c"),
                ],
            ),
        ],
    )
}

impl ThemeConfig {
    fn new(border_colors: Option<(&str, &str)>, view_colors: &[(&str, &[(&str, &str)])]) -> Self {
        let ui = border_colors.map(|(header_border_color, footer_border_color)| {
            crate::config::PartialUIConfig::new(header_border_color, footer_border_color)
        });
        let views = view_colors
            .iter()
            .map(|(view_name, colors)| {
                (
                    (*view_name).to_string(),
                    ViewConfig::from_pairs(colors.iter().copied()),
                )
            })
            .collect();

        Self { ui, views }
    }
}

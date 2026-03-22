//! Built-in color themes shipped with `pgmon`.

use crate::config::{ThemeConfig, ViewConfig};
use std::collections::HashMap;

/// Return all built-in themes keyed by their public theme name.
pub(crate) fn built_in_themes() -> HashMap<String, ThemeConfig> {
    let mut themes = HashMap::new();
    themes.insert(
        "calibrachoa".to_string(),
        ThemeConfig::new(
            Some(("#ff4fa3", "#ff9f1c")),
            &[("settings", &[("value", "#7bff00")])],
        ),
    );
    themes.insert(
        "sky".to_string(),
        ThemeConfig::new(
            Some(("#8fa1b3", "#93a8a3")),
            &[("settings", &[("value", "#b7a8ba")])],
        ),
    );
    themes.insert(
        "mint".to_string(),
        ThemeConfig::new(
            Some(("#9db39d", "#9eb1ac")),
            &[("settings", &[("value", "#adb4c7")])],
        ),
    );
    themes.insert(
        "retro".to_string(),
        ThemeConfig::new(
            Some(("#6f8f73", "#5f7a62")),
            &[("settings", &[("value", "#8ea68c")])],
        ),
    );
    themes
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

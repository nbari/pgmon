//! Configuration loading and UI customization support for `pgmon`.

use anyhow::{Context, Result, anyhow};
use ratatui::style::Color;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Runtime configuration loaded from `pgmon.yaml` or `pgmon.yml`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    /// Named `PostgreSQL` connection aliases.
    #[serde(default)]
    pub connections: HashMap<String, ConnectionConfig>,
    /// Default connection alias used when no CLI alias is provided.
    #[serde(default)]
    pub default_connection: Option<String>,
    /// Per-view customization settings keyed by view name.
    #[serde(default)]
    pub views: HashMap<String, ViewConfig>,
    /// Global UI preferences.
    #[serde(default)]
    pub ui: UIConfig,
    #[serde(skip)]
    active_theme: Option<String>,
    #[serde(skip)]
    themes: HashMap<String, ThemeConfig>,
    #[serde(skip)]
    ui_overrides: Option<PartialUIConfig>,
    #[serde(skip)]
    view_overrides: HashMap<String, ViewConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connections: HashMap::new(),
            default_connection: None,
            views: HashMap::new(),
            ui: UIConfig::default(),
            active_theme: None,
            themes: crate::themes::built_in_themes(),
            ui_overrides: None,
            view_overrides: HashMap::new(),
        }
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RawConfig {
    #[serde(default)]
    connections: HashMap<String, ConnectionConfig>,
    #[serde(default)]
    default_connection: Option<String>,
    #[serde(default)]
    theme: Option<String>,
    #[serde(default)]
    themes: HashMap<String, ThemeConfig>,
    #[serde(default)]
    views: HashMap<String, ViewConfig>,
    #[serde(default)]
    ui: Option<PartialUIConfig>,
}

/// Supported output formats for table exports.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    /// Export data as CSV.
    #[default]
    Csv,
    /// Export data as JSON.
    Json,
}

/// Global UI preferences that can be customized via config.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UIConfig {
    /// Whether to render the controls footer.
    #[serde(default = "default_true")]
    pub show_controls: bool,
    /// Border color for the header panel.
    #[serde(default = "default_header_color")]
    pub header_border_color: String,
    /// Border color for the footer panel.
    #[serde(default = "default_footer_color")]
    pub footer_border_color: String,
    /// Default format used when exporting table data.
    #[serde(default)]
    pub default_export_format: ExportFormat,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct PartialUIConfig {
    #[serde(default)]
    show_controls: Option<bool>,
    #[serde(default)]
    header_border_color: Option<String>,
    #[serde(default)]
    footer_border_color: Option<String>,
    #[serde(default)]
    default_export_format: Option<ExportFormat>,
}

impl PartialUIConfig {
    pub(crate) fn new(header_border_color: &str, footer_border_color: &str) -> Self {
        Self {
            show_controls: None,
            header_border_color: Some(header_border_color.to_string()),
            footer_border_color: Some(footer_border_color.to_string()),
            default_export_format: None,
        }
    }
}

impl Default for UIConfig {
    fn default() -> Self {
        Self {
            show_controls: true,
            header_border_color: "cyan".to_string(),
            footer_border_color: "cyan".to_string(),
            default_export_format: ExportFormat::default(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_header_color() -> String {
    "cyan".to_string()
}
fn default_footer_color() -> String {
    "cyan".to_string()
}

/// View-specific customization settings.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ViewConfig {
    /// Arbitrary color overrides keyed by UI element name.
    #[serde(default)]
    pub colors: HashMap<String, String>,
}

impl ViewConfig {
    pub(crate) fn from_pairs<'a>(colors: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self {
            colors: colors
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct ThemeConfig {
    #[serde(default)]
    pub(crate) ui: Option<PartialUIConfig>,
    #[serde(default)]
    pub(crate) views: HashMap<String, ViewConfig>,
}

/// Named connection settings stored in `pgmon.yaml` or `pgmon.yml`.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ConnectionConfig {
    /// `PostgreSQL` DSN associated with the alias.
    pub dsn: String,
}

impl Config {
    /// Load configuration from an explicit path or from the supported search path.
    pub fn load(config_path: Option<&Path>) -> Result<Self> {
        if let Some(path) = resolved_config_path(config_path) {
            return Self::load_path(&path);
        }

        Ok(Self::default())
    }

    /// Resolve a configured color for a given view and key.
    pub fn get_view_color(&self, view: &str, key: &str) -> Option<Color> {
        self.views
            .get(view)
            .and_then(|v| v.colors.get(key))
            .and_then(|c| parse_color(c))
    }

    /// Resolve a configured DSN alias to its DSN string.
    pub fn get_connection_dsn(&self, alias: &str) -> Option<&str> {
        self.connections
            .get(alias)
            .map(|connection| connection.dsn.as_str())
    }

    /// Return the configured default connection alias, if any.
    pub fn default_connection_alias(&self) -> Option<&str> {
        self.default_connection
            .as_deref()
            .map(str::trim)
            .filter(|alias| !alias.is_empty())
    }

    /// Return all configured connection alias names in sorted order.
    pub fn connection_aliases(&self) -> Vec<&str> {
        let mut aliases = self
            .connections
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        aliases.sort_unstable();
        aliases
    }

    /// Return all configured theme names in sorted order.
    pub fn theme_names(&self) -> Vec<&str> {
        let mut names = self.themes.keys().map(String::as_str).collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    /// Return the currently selected theme name, if any.
    pub fn active_theme_name(&self) -> Option<&str> {
        self.active_theme
            .as_deref()
            .map(str::trim)
            .filter(|theme| !theme.is_empty())
    }

    /// Whether the config defines any named themes.
    pub fn has_themes(&self) -> bool {
        !self.themes.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let mut issues = Vec::new();

        if let Some(alias) = self.default_connection_alias()
            && !self.connections.contains_key(alias)
        {
            issues.push(format!(
                "default_connection '{alias}' does not match any configured connection alias."
            ));
        }

        for (alias, connection) in &self.connections {
            if connection.dsn.trim().is_empty() {
                issues.push(format!("connection '{alias}' has an empty dsn value."));
            }
        }

        validate_color_setting(
            "ui.header_border_color",
            &self.ui.header_border_color,
            &mut issues,
        );
        validate_color_setting(
            "ui.footer_border_color",
            &self.ui.footer_border_color,
            &mut issues,
        );

        for (view_name, view_config) in &self.views {
            for (key, value) in &view_config.colors {
                validate_color_setting(
                    &format!("views.{view_name}.colors.{key}"),
                    value,
                    &mut issues,
                );
            }
        }

        if issues.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(issues.join("\n")))
        }
    }

    /// Apply a configured theme by name and rebuild the effective runtime colors.
    pub fn apply_theme_by_name(&mut self, theme_name: &str) -> Result<()> {
        let trimmed = theme_name.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("Theme name cannot be empty."));
        }

        self.ensure_theme_exists(trimmed)?;
        self.active_theme = Some(trimmed.to_string());
        self.rebuild_effective_theme()
    }

    fn load_path(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let raw: RawConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        raw.into_config()
    }
}

/// Parse a configured color name or `#RRGGBB` value into a `ratatui` color.
pub fn parse_color(value: &str) -> Option<Color> {
    let trimmed = value.trim();
    Color::from_str(trimmed)
        .ok()
        .or_else(|| parse_hex_color(trimmed))
}

impl RawConfig {
    fn into_config(self) -> Result<Config> {
        let mut themes = crate::themes::built_in_themes();
        themes.extend(self.themes);

        let mut config = Config {
            connections: self.connections,
            default_connection: self.default_connection,
            views: HashMap::new(),
            ui: UIConfig::default(),
            active_theme: self
                .theme
                .as_deref()
                .map(str::trim)
                .filter(|theme| !theme.is_empty())
                .map(str::to_string),
            themes,
            ui_overrides: self.ui,
            view_overrides: self.views,
        };
        config.rebuild_effective_theme()?;
        Ok(config)
    }
}

impl Config {
    fn rebuild_effective_theme(&mut self) -> Result<()> {
        self.ui = UIConfig::default();
        self.views.clear();

        if let Some(theme_name) = self.active_theme.as_deref() {
            let theme = self.ensure_theme_exists(theme_name)?.clone();
            apply_theme(self, &theme);
        }

        if let Some(ui) = self.ui_overrides.as_ref() {
            apply_ui_overrides(&mut self.ui, ui);
        }
        merge_view_overrides(&mut self.views, &self.view_overrides);

        Ok(())
    }

    fn ensure_theme_exists(&self, theme_name: &str) -> Result<&ThemeConfig> {
        self.themes
            .get(theme_name)
            .ok_or_else(|| unknown_theme_error(theme_name, &self.themes))
    }
}

fn apply_theme(config: &mut Config, theme: &ThemeConfig) {
    if let Some(ui) = theme.ui.as_ref() {
        apply_ui_overrides(&mut config.ui, ui);
    }
    merge_view_overrides(&mut config.views, &theme.views);
}

fn unknown_theme_error(theme_name: &str, themes: &HashMap<String, ThemeConfig>) -> anyhow::Error {
    let mut available = themes.keys().map(String::as_str).collect::<Vec<_>>();
    available.sort_unstable();
    if available.is_empty() {
        anyhow!("Unknown theme '{theme_name}'. No themes are defined in pgmon.yaml or pgmon.yml.")
    } else {
        anyhow!(
            "Unknown theme '{theme_name}'. Available themes: {}.",
            available.join(", ")
        )
    }
}

fn apply_ui_overrides(target: &mut UIConfig, source: &PartialUIConfig) {
    if let Some(show_controls) = source.show_controls {
        target.show_controls = show_controls;
    }
    if let Some(color) = source.header_border_color.as_ref() {
        target.header_border_color.clone_from(color);
    }
    if let Some(color) = source.footer_border_color.as_ref() {
        target.footer_border_color.clone_from(color);
    }
    if let Some(format) = source.default_export_format {
        target.default_export_format = format;
    }
}

fn merge_view_overrides(
    target: &mut HashMap<String, ViewConfig>,
    source: &HashMap<String, ViewConfig>,
) {
    for (view_name, view_config) in source {
        target
            .entry(view_name.clone())
            .or_default()
            .colors
            .extend(view_config.colors.clone());
    }
}

fn parse_hex_color(value: &str) -> Option<Color> {
    let hex = value.strip_prefix('#')?;
    if hex.len() != 6 {
        return None;
    }

    let red = u8::from_str_radix(&hex[0..2], 16).ok()?;
    let green = u8::from_str_radix(&hex[2..4], 16).ok()?;
    let blue = u8::from_str_radix(&hex[4..6], 16).ok()?;
    Some(Color::Rgb(red, green, blue))
}

pub(crate) fn resolved_config_path(config_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = config_path {
        return Some(path.to_path_buf());
    }

    default_config_paths()
        .into_iter()
        .find(|path| path.exists())
}

fn default_config_paths() -> Vec<PathBuf> {
    vec![
        PathBuf::from("pgmon.yaml"),
        PathBuf::from("pgmon.yml"),
        dirs::home_dir()
            .map(|h| h.join(".config/pgmon/pgmon.yaml"))
            .unwrap_or_default(),
        dirs::home_dir()
            .map(|h| h.join(".config/pgmon/pgmon.yml"))
            .unwrap_or_default(),
    ]
}

fn validate_color_setting(name: &str, value: &str, issues: &mut Vec<String>) {
    if parse_color(value).is_none() {
        issues.push(format!(
            "{name} uses unsupported color '{value}'. Use a named color or #RRGGBB."
        ));
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{Config, ExportFormat, RawConfig, parse_color};
    use ratatui::style::Color;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn test_config_deserialization_applies_defaults() {
        let raw_result = serde_yaml::from_str("ui:\n  show_controls: false\n");
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        assert!(!config.ui.show_controls);
        assert_eq!(config.ui.header_border_color, "cyan");
        assert_eq!(config.ui.footer_border_color, "cyan");
        assert_eq!(config.ui.default_export_format, ExportFormat::Csv);
        assert!(config.connections.is_empty());
        assert_eq!(config.default_connection_alias(), None);
        assert!(config.has_themes());
        assert_eq!(
            config.theme_names(),
            vec!["calibrachoa", "mint", "retro", "sky"]
        );
    }

    #[test]
    fn test_get_view_color_returns_configured_color() {
        let raw_result =
            serde_yaml::from_str("views:\n  settings:\n    colors:\n      value: blue\n");
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        assert_eq!(
            config.get_view_color("settings", "value"),
            Some(Color::Blue)
        );
        assert_eq!(config.get_view_color("settings", "missing"), None);
    }

    #[test]
    fn test_config_deserialization_supports_connection_aliases() {
        let raw_result = serde_yaml::from_str(
            "connections:\n  prod:\n    dsn: postgresql://prod.example.com/postgres\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        assert_eq!(
            config.get_connection_dsn("prod"),
            Some("postgresql://prod.example.com/postgres")
        );
    }

    #[test]
    fn test_connection_aliases_returns_sorted_aliases() {
        let raw_result = serde_yaml::from_str(
            "connections:\n  staging:\n    dsn: postgres://staging\n  prod:\n    dsn: postgres://prod\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        assert_eq!(config.connection_aliases(), vec!["prod", "staging"]);
    }

    #[test]
    fn test_config_deserialization_supports_default_connection() {
        let raw_result = serde_yaml::from_str(
            "default_connection: local\nconnections:\n  local:\n    dsn: postgresql://localhost/postgres\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        assert_eq!(config.default_connection_alias(), Some("local"));
        assert_eq!(
            config.get_connection_dsn("local"),
            Some("postgresql://localhost/postgres")
        );
    }

    #[test]
    fn test_load_reads_explicit_config_path() {
        let path = temp_config_path("explicit");
        let write_result = fs::write(
            &path,
            "default_connection: prod\nconnections:\n  prod:\n    dsn: postgresql://prod.example.com/postgres\n",
        );
        assert!(write_result.is_ok());

        let config_result = Config::load(Some(&path));
        let _ = fs::remove_file(&path);

        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load");
        };
        assert_eq!(
            config.get_connection_dsn("prod"),
            Some("postgresql://prod.example.com/postgres")
        );
        assert_eq!(config.default_connection_alias(), Some("prod"));
    }

    #[test]
    fn test_load_reads_default_yml_config_path() {
        let temp_dir = temp_config_dir("default-yml");
        let previous_dir = std::env::current_dir();
        assert!(previous_dir.is_ok());
        let Ok(previous_dir) = previous_dir else {
            panic!("current dir should resolve");
        };

        let create_dir_result = fs::create_dir_all(&temp_dir);
        assert!(create_dir_result.is_ok());
        let path = temp_dir.join("pgmon.yml");
        let write_result = fs::write(
            &path,
            "connections:\n  prod:\n    dsn: postgresql://prod.example.com/postgres\n",
        );
        assert!(write_result.is_ok());

        let set_dir_result = std::env::set_current_dir(&temp_dir);
        assert!(set_dir_result.is_ok());
        let config_result = Config::load(None);
        let reset_dir_result = std::env::set_current_dir(&previous_dir);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&temp_dir);
        assert!(reset_dir_result.is_ok());

        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load from pgmon.yml");
        };
        assert_eq!(
            config.get_connection_dsn("prod"),
            Some("postgresql://prod.example.com/postgres")
        );
    }

    #[test]
    fn test_theme_merges_muted_hex_colors() {
        let raw_result = serde_yaml::from_str(
            "theme: sky\nthemes:\n  sky:\n    ui:\n      header_border_color: \"#8fa1b3\"\n      footer_border_color: \"#93a8a3\"\n    views:\n      settings:\n        colors:\n          value: \"#b7a8ba\"\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load");
        };

        assert_eq!(config.ui.header_border_color, "#8fa1b3");
        assert_eq!(config.ui.footer_border_color, "#93a8a3");
        assert_eq!(
            config.get_view_color("settings", "value"),
            Some(Color::Rgb(183, 168, 186))
        );
    }

    #[test]
    fn test_top_level_ui_and_view_colors_override_theme() {
        let raw_result = serde_yaml::from_str(
            "theme: sky\nthemes:\n  sky:\n    ui:\n      header_border_color: \"#8fa1b3\"\n    views:\n      settings:\n        colors:\n          value: \"#b7a8ba\"\nui:\n  header_border_color: yellow\nviews:\n  settings:\n    colors:\n      value: blue\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load");
        };

        assert_eq!(config.ui.header_border_color, "yellow");
        assert_eq!(
            config.get_view_color("settings", "value"),
            Some(Color::Blue)
        );
    }

    #[test]
    fn test_theme_names_returns_sorted_theme_names() {
        let raw_result = serde_yaml::from_str(
            "themes:\n  mint:\n    ui:\n      header_border_color: \"#9db39d\"\n  sky:\n    ui:\n      header_border_color: \"#8fa1b3\"\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load");
        };

        assert_eq!(
            config.theme_names(),
            vec!["calibrachoa", "mint", "retro", "sky"]
        );
    }

    #[test]
    fn test_apply_theme_by_name_reapplies_overrides() {
        let raw_result = serde_yaml::from_str(
            "theme: sky\nthemes:\n  sky:\n    ui:\n      header_border_color: \"#8fa1b3\"\n      footer_border_color: \"#93a8a3\"\n    views:\n      settings:\n        colors:\n          value: \"#b7a8ba\"\n  mint:\n    ui:\n      header_border_color: \"#9db39d\"\n      footer_border_color: \"#9eb1ac\"\n    views:\n      settings:\n        colors:\n          value: \"#adb4c7\"\nui:\n  footer_border_color: yellow\nviews:\n  settings:\n    colors:\n      value: blue\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(mut config) = config_result else {
            panic!("config should load");
        };

        let apply_result = config.apply_theme_by_name("mint");
        assert!(apply_result.is_ok());

        assert_eq!(config.active_theme_name(), Some("mint"));
        assert_eq!(config.ui.header_border_color, "#9db39d");
        assert_eq!(config.ui.footer_border_color, "yellow");
        assert_eq!(
            config.get_view_color("settings", "value"),
            Some(Color::Blue)
        );
    }

    #[test]
    fn test_builtin_theme_available_without_yaml_theme_block() {
        let raw_result = serde_yaml::from_str("theme: mint\n");
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load");
        };

        assert_eq!(config.active_theme_name(), Some("mint"));
        assert_eq!(config.ui.header_border_color, "#9db39d");
        assert_eq!(config.ui.footer_border_color, "#9eb1ac");
    }

    #[test]
    fn test_yaml_theme_overrides_builtin_theme_name() {
        let raw_result = serde_yaml::from_str(
            "theme: mint\nthemes:\n  mint:\n    ui:\n      header_border_color: \"#111111\"\n      footer_border_color: \"#222222\"\n",
        );
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should load");
        };

        assert_eq!(config.active_theme_name(), Some("mint"));
        assert_eq!(config.ui.header_border_color, "#111111");
        assert_eq!(config.ui.footer_border_color, "#222222");
    }

    #[test]
    fn test_unknown_theme_returns_error() {
        let raw_result = serde_yaml::from_str("theme: missing\n");
        assert!(raw_result.is_ok());
        let Ok(raw): Result<RawConfig, _> = raw_result else {
            panic!("raw config should parse");
        };

        let config_result = raw.into_config();
        assert!(config_result.is_err());
        let Err(error) = config_result else {
            panic!("missing theme should fail");
        };
        assert!(
            error
                .to_string()
                .contains("Available themes: calibrachoa, mint, retro, sky.")
        );
    }

    #[test]
    fn test_parse_color_supports_hex_rgb_values() {
        assert_eq!(parse_color("#89b4fa"), Some(Color::Rgb(137, 180, 250)));
        assert_eq!(parse_color("cyan"), Some(Color::Cyan));
        assert_eq!(parse_color("not-a-color"), None);
    }

    fn temp_config_path(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("pgmon-config-{suffix}-{nanos}.yaml"))
    }

    fn temp_config_dir(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("pgmon-config-dir-{suffix}-{nanos}"))
    }
}

use crate::{cli::actions::Action, config::Config, pg::conninfo};
use anyhow::{Result, anyhow};
use clap::{ArgMatches, parser::ValueSource};
use std::path::{Path, PathBuf};

pub fn handler(matches: &ArgMatches, config: Config, config_path: Option<&Path>) -> Result<Action> {
    if let Some(("check-config", subcommand_matches)) = matches.subcommand() {
        return Ok(build_check_config_action(
            subcommand_matches,
            &config,
            config_path,
        ));
    }

    let dsn_value = matches.get_one::<String>("dsn").map(String::as_str);
    let (explicit_dsn, env_dsn) = match matches.value_source("dsn") {
        Some(ValueSource::CommandLine) => (dsn_value, None),
        Some(ValueSource::EnvVariable) => (None, dsn_value),
        _ => (None, None),
    };
    let alias_dsn = if explicit_dsn.is_some() {
        None
    } else {
        let alias = selected_alias(
            matches.get_one::<String>("alias").map(String::as_str),
            &config,
        );
        resolve_alias_dsn(alias, &config)?
    };
    let dsn = conninfo::resolve_dsn(explicit_dsn, alias_dsn.as_deref(), env_dsn)?;
    let connect_timeout_ms = *matches
        .get_one::<u64>("connect-timeout-ms")
        .unwrap_or(&3000);
    let query_output_dir = matches
        .get_one::<String>("query-output-dir")
        .map(PathBuf::from);
    let refresh_ms = *matches.get_one::<u64>("refresh-ms").unwrap_or(&1000);
    let top_n = *matches.get_one::<u32>("top-n").unwrap_or(&10);
    let home_view = matches
        .get_one::<String>("home-view")
        .cloned()
        .unwrap_or_else(|| "activity".into());
    let sort = matches
        .get_one::<String>("sort")
        .cloned()
        .unwrap_or_else(|| "total_time".into());

    Ok(Action::StartTui {
        dsn,
        connect_timeout_ms,
        query_output_dir,
        refresh_ms,
        top_n,
        home_view,
        sort,
        config: Box::new(config),
    })
}

fn build_check_config_action(
    matches: &ArgMatches,
    config: &Config,
    config_path: Option<&Path>,
) -> Action {
    let config_validation = config.validate();
    let connection_check = check_connection_resolution(matches, config);
    let success = config_validation.is_ok() && connection_check.is_ok();

    let mut lines = vec![
        "pgmon configuration check".to_string(),
        String::new(),
        "Configuration".to_string(),
        format!(
            "- Source: {}",
            config_path.map_or_else(
                || "none found; using built-in defaults".to_string(),
                |path| path.display().to_string()
            )
        ),
    ];

    let aliases = config.connection_aliases();
    lines.push(format!(
        "- Connection aliases: {}",
        if aliases.is_empty() {
            "none".to_string()
        } else {
            aliases.join(", ")
        }
    ));
    lines.push(format!(
        "- Default connection: {}",
        config.default_connection_alias().unwrap_or("none")
    ));
    lines.push(format!(
        "- Active theme: {}",
        config.active_theme_name().unwrap_or("none")
    ));

    match config_validation {
        Ok(()) => lines.push("- Validation: ok".to_string()),
        Err(error) => {
            lines.push("- Validation: failed".to_string());
            for detail in error.to_string().lines() {
                lines.push(format!("  {detail}"));
            }
        }
    }

    lines.push(String::new());
    lines.push("Connection resolution".to_string());

    match connection_check {
        Ok(connection_lines) => lines.extend(connection_lines),
        Err(error) => {
            lines.push("- Resolution: failed".to_string());
            for detail in error.to_string().lines() {
                lines.push(format!("  {detail}"));
            }
        }
    }

    lines.push(String::new());
    lines.push(format!("Result: {}", if success { "ok" } else { "failed" }));

    Action::CheckConfig {
        report: lines.join("\n"),
        success,
    }
}

fn check_connection_resolution(matches: &ArgMatches, config: &Config) -> Result<Vec<String>> {
    let dsn_value = matches.get_one::<String>("dsn").map(String::as_str);
    let (explicit_dsn, env_dsn) = match matches.value_source("dsn") {
        Some(ValueSource::CommandLine) => (dsn_value, None),
        Some(ValueSource::EnvVariable) => (None, dsn_value),
        _ => (None, None),
    };

    let alias_selection = selected_alias(
        matches.get_one::<String>("alias").map(String::as_str),
        config,
    );
    let alias_dsn = if explicit_dsn.is_some() {
        None
    } else {
        resolve_alias_dsn(alias_selection, config)?
    };
    let pgpass_path = conninfo::candidate_pgpass_path();
    let resolved_dsn = conninfo::resolve_dsn(explicit_dsn, alias_dsn.as_deref(), env_dsn)?;

    let mut lines = Vec::new();
    lines.push(format!(
        "- Explicit --dsn: {}",
        if explicit_dsn.is_some() {
            "set"
        } else {
            "not set"
        }
    ));
    lines.push(format!(
        "- PGMON_DSN: {}",
        if env_dsn.is_some() { "set" } else { "not set" }
    ));
    lines.push(format!(
        "- Alias input: {}",
        alias_selection.map_or_else(
            || "none".to_string(),
            |(alias, source)| match source {
                AliasSource::Alias => format!("{alias} (from CLI alias)"),
                AliasSource::DefaultConnection => {
                    format!("{alias} (from default_connection)")
                }
            }
        )
    ));
    lines.push(format!(
        "- .pgpass: {}",
        describe_pgpass_status(pgpass_path.as_deref())
    ));
    lines.push(format!(
        "- Effective source: {}",
        describe_effective_source(
            explicit_dsn,
            alias_selection,
            env_dsn,
            pgpass_path.as_deref()
        )
    ));
    lines.push(format!(
        "- Effective target: {}",
        conninfo::describe_connection_target(&resolved_dsn)
    ));

    if explicit_dsn.is_some()
        && let Some((alias, source)) = alias_selection
    {
        let source_name = match source {
            AliasSource::Alias => "CLI alias",
            AliasSource::DefaultConnection => "default_connection",
        };
        lines.push(format!(
            "- Note: {source_name} '{alias}' is ignored because --dsn takes precedence."
        ));
    }

    Ok(lines)
}

#[derive(Clone, Copy)]
enum AliasSource {
    Alias,
    DefaultConnection,
}

fn selected_alias<'a>(
    alias: Option<&'a str>,
    config: &'a Config,
) -> Option<(&'a str, AliasSource)> {
    match alias.filter(|alias| !alias.trim().is_empty()) {
        Some(alias) => Some((alias, AliasSource::Alias)),
        None => config
            .default_connection_alias()
            .map(|alias| (alias, AliasSource::DefaultConnection)),
    }
}

fn resolve_alias_dsn(
    selected_alias: Option<(&str, AliasSource)>,
    config: &Config,
) -> Result<Option<String>> {
    let Some((alias, source)) = selected_alias else {
        return Ok(None);
    };

    let source = match source {
        AliasSource::Alias => "alias",
        AliasSource::DefaultConnection => "default_connection",
    };

    if let Some(dsn) = config.get_connection_dsn(alias) {
        return Ok(Some(dsn.to_string()));
    }

    let aliases = config.connection_aliases();
    let known_aliases = if aliases.is_empty() {
        "No aliases are configured in pgmon.yaml.".to_string()
    } else {
        format!("Available aliases: {}.", aliases.join(", "))
    };

    Err(anyhow!(
        "Unknown connection {source} '{alias}'. {known_aliases}"
    ))
}

fn describe_pgpass_status(path: Option<&Path>) -> String {
    match path {
        Some(path) if path.exists() => format!("available at {}", path.display()),
        Some(path) => format!("not found at {}", path.display()),
        None => "unavailable (HOME/PGPASSFILE not set)".to_string(),
    }
}

fn describe_effective_source(
    explicit_dsn: Option<&str>,
    alias_selection: Option<(&str, AliasSource)>,
    env_dsn: Option<&str>,
    pgpass_path: Option<&Path>,
) -> String {
    if explicit_dsn.is_some() {
        return "--dsn".to_string();
    }
    if let Some((alias, source)) = alias_selection {
        return match source {
            AliasSource::Alias => format!("alias '{alias}'"),
            AliasSource::DefaultConnection => format!("default_connection '{alias}'"),
        };
    }
    if env_dsn.is_some() {
        return "PGMON_DSN".to_string();
    }
    if let Some(path) = pgpass_path {
        return format!(".pgpass ({})", path.display());
    }
    "unresolved".to_string()
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::handler;
    use crate::{cli::actions::Action, cli::commands, config::Config};

    #[test]
    fn test_handler_resolves_alias_from_config() {
        let matches = commands::new().get_matches_from(vec!["pgmon", "prod"]);
        let config_result: Result<Config, _> = serde_yaml::from_str(
            "connections:\n  prod:\n    dsn: postgresql://prod.example.com/postgres\n",
        );
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        let action = handler(&matches, config, None);

        assert!(action.is_ok());
    }

    #[test]
    fn test_handler_rejects_unknown_alias() {
        let matches = commands::new().get_matches_from(vec!["pgmon", "missing"]);
        let result = handler(&matches, Config::default(), None);
        assert!(result.is_err());
        let Err(error) = result else {
            panic!("alias should fail");
        };

        assert!(
            error
                .to_string()
                .contains("Unknown connection alias 'missing'")
        );
    }

    #[test]
    fn test_handler_uses_default_connection_when_alias_is_missing() {
        let matches = commands::new().get_matches_from(vec!["pgmon"]);
        let config_result: Result<Config, _> = serde_yaml::from_str(
            "default_connection: local\nconnections:\n  local:\n    dsn: postgresql://localhost/postgres\n",
        );
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        let action = handler(&matches, config, None);

        assert!(action.is_ok());
    }

    #[test]
    fn test_handler_rejects_unknown_default_connection() {
        let matches = commands::new().get_matches_from(vec!["pgmon"]);
        let config_result: Result<Config, _> =
            serde_yaml::from_str("default_connection: missing\n");
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        let result = handler(&matches, config, None);

        assert!(result.is_err());
        let Err(error) = result else {
            panic!("default connection should fail");
        };
        assert!(
            error
                .to_string()
                .contains("Unknown connection default_connection 'missing'")
        );
    }

    #[test]
    fn test_handler_explicit_dsn_ignores_invalid_default_connection() {
        let matches = commands::new().get_matches_from(vec![
            "pgmon",
            "--dsn",
            "postgresql://localhost/postgres",
        ]);
        let config_result: Result<Config, _> =
            serde_yaml::from_str("default_connection: missing\n");
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        let action = handler(&matches, config, None);

        assert!(action.is_ok());
    }

    #[test]
    fn test_handler_builds_check_config_action() {
        let matches = commands::new().get_matches_from(vec![
            "pgmon",
            "check-config",
            "--dsn",
            "postgresql://localhost/postgres",
        ]);

        let action = handler(&matches, Config::default(), None);

        assert!(action.is_ok());
        let Ok(Action::CheckConfig { success, report }) = action else {
            panic!("check-config action should be returned");
        };
        assert!(success);
        assert!(report.contains("Effective source: --dsn"));
    }

    #[test]
    fn test_handler_reports_invalid_config_in_check_config() {
        let matches = commands::new().get_matches_from(vec![
            "pgmon",
            "check-config",
            "--dsn",
            "postgresql://localhost/postgres",
        ]);
        let config_result: Result<Config, _> =
            serde_yaml::from_str("ui:\n  header_border_color: not-a-color\n");
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("config should parse");
        };

        let action = handler(&matches, config, None);

        assert!(action.is_ok());
        let Ok(Action::CheckConfig { success, report }) = action else {
            panic!("check-config action should be returned");
        };
        assert!(!success);
        assert!(report.contains("ui.header_border_color"));
    }
}

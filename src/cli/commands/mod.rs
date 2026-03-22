use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::styling::{AnsiColor, Effects, Styles},
};

pub mod built_info {
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn new() -> Command {
    base_command()
        .arg(dsn_arg())
        .arg(alias_arg())
        .arg(config_arg())
        .arg(connect_timeout_arg())
        .arg(query_output_dir_arg())
        .arg(refresh_ms_arg())
        .arg(top_n_arg())
        .arg(home_view_arg())
        .arg(sort_arg())
        .arg(verbose_arg())
        .subcommand(check_config_command())
}

fn base_command() -> Command {
    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    let git_hash = built_info::GIT_COMMIT_HASH.unwrap_or("unknown");
    let long_version: &'static str =
        Box::leak(format!("{} - {}", env!("CARGO_PKG_VERSION"), git_hash).into_boxed_str());

    Command::new("pgmon")
        .about("A PostgreSQL monitoring TUI")
        .long_about("pgmon is a terminal-based monitoring tool for PostgreSQL, providing real-time insights into database activity, statistics, and performance metrics.")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(long_version)
        .color(ColorChoice::Auto)
        .styles(styles)
        .override_usage("pgmon [OPTIONS] [ALIAS]")
        .after_help("For more information, visit: https://github.com/nbari/pgmon")
}

fn dsn_arg() -> Arg {
    Arg::new("dsn")
        .short('d')
        .long("dsn")
        .aliases(["url"])
        .value_name("STRING")
        .help("PostgreSQL DSN (connection string)")
        .long_help("The PostgreSQL connection string (DSN). It can be a URI-style string (e.g., postgresql://user:password@host:port/dbname) or a key=value pair string (e.g., host=localhost user=postgres). If omitted, pgmon falls back to PGMON_DSN and then to the first usable entry in PGPASSFILE or ~/.pgpass.")
        .env("PGMON_DSN")
}

fn alias_arg() -> Arg {
    Arg::new("alias")
        .value_name("ALIAS")
        .help("Connection alias from pgmon.yaml or pgmon.yml")
        .long_help("Named PostgreSQL connection alias loaded from pgmon.yaml or pgmon.yml. Use this to connect with `pgmon <alias>`. If --dsn is also provided, the explicit DSN takes precedence.")
}

fn config_arg() -> Arg {
    Arg::new("config")
        .short('c')
        .long("config")
        .value_name("PATH")
        .help("Path to pgmon.yaml or pgmon.yml")
        .long_help("Load configuration from the provided pgmon.yaml or pgmon.yml path instead of searching the default locations.")
}

fn connect_timeout_arg() -> Arg {
    Arg::new("connect-timeout-ms")
        .long("connect-timeout-ms")
        .value_name("u64")
        .help("Connection timeout in milliseconds")
        .long_help("Set how long pgmon waits for each PostgreSQL connection attempt before failing. This applies at startup and on refresh reconnects. The value must be between 250ms and 60,000ms (1 minute).")
        .value_parser(clap::value_parser!(u64).range(250..=60000))
        .default_value("3000")
}

fn query_output_dir_arg() -> Arg {
    Arg::new("query-output-dir")
        .long("query-output-dir")
        .value_name("PATH")
        .help("Directory used when saving queries with Enter")
        .long_help("Set the directory used when saving a selected query from the Activity or Statements view. If omitted, pgmon writes query files into the system temporary directory.")
        .env("PGMON_QUERY_OUTPUT_DIR")
}

fn refresh_ms_arg() -> Arg {
    Arg::new("refresh-ms")
        .short('r')
        .long("refresh-ms")
        .value_name("u64")
        .help("Refresh interval in milliseconds")
        .long_help("Set the interval at which the UI polls the database for new statistics. The value must be between 200ms and 60,000ms (1 minute).")
        .value_parser(clap::value_parser!(u64).range(200..=60000))
        .default_value("1000")
}

fn top_n_arg() -> Arg {
    Arg::new("top-n")
        .short('n')
        .long("top-n")
        .value_name("u32")
        .help("Number of rows to show in top views")
        .long_help("Specifies the maximum number of rows to display in views like 'Top Queries' or 'Activity'. This helps manage terminal space and performance.")
        .value_parser(clap::value_parser!(u32).range(1..=100))
        .default_value("10")
}

fn home_view_arg() -> Arg {
    Arg::new("home-view")
        .long("home-view")
        .value_name("VIEW")
        .help("Initial view on startup")
        .long_help("Select which tab should be displayed when pgmon starts. Options are 'activity' (default) for current processes or 'statements' for cumulative statistics.")
        .value_parser(["activity", "statements"])
        .default_value("activity")
}

fn sort_arg() -> Arg {
    Arg::new("sort")
        .short('s')
        .long("sort")
        .value_name("COLUMN")
        .help("Default sort column")
        .long_help("Define the default sorting criteria for the Statements view.")
        .value_parser(["total_time", "mean_time", "calls"])
        .default_value("total_time")
}

fn verbose_arg() -> Arg {
    Arg::new("verbose")
        .short('v')
        .long("verbose")
        .action(ArgAction::Count)
        .help("Increase logging verbosity")
        .long_help("Increase the level of detail in the logs. Can be used multiple times (-vv, -vvv) to increase verbosity from INFO to DEBUG or TRACE.")
}

fn check_config_command() -> Command {
    Command::new("check-config")
        .about("Validate configuration and connection resolution")
        .long_about("Validate pgmon.yaml/pgmon.yml loading, aliases, themes, colors, and the effective PostgreSQL connection resolution without starting the TUI.")
        .override_usage("pgmon check-config [OPTIONS] [ALIAS]")
        .arg(dsn_arg())
        .arg(alias_arg())
        .arg(config_arg())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_accepts_pgpass_fallback_without_dsn() {
        let cmd = new();
        let matches = cmd.try_get_matches_from(vec!["pgmon"]);
        assert!(matches.is_ok());
    }

    #[test]
    fn test_cli_default_values() {
        let cmd = new();
        let matches = cmd.get_matches_from(vec!["pgmon", "--dsn", "postgres://localhost"]);
        assert_eq!(matches.get_one::<u64>("connect-timeout-ms"), Some(&3000));
        assert_eq!(matches.get_one::<String>("config"), None);
        assert_eq!(matches.get_one::<String>("query-output-dir"), None);
        assert_eq!(matches.get_one::<u64>("refresh-ms"), Some(&1000));
        assert_eq!(matches.get_one::<u32>("top-n"), Some(&10));
        assert_eq!(
            matches.get_one::<String>("home-view"),
            Some(&"activity".to_string())
        );
        assert_eq!(
            matches.get_one::<String>("sort"),
            Some(&"total_time".to_string())
        );
        assert_eq!(matches.get_one::<String>("alias"), None);
    }

    #[test]
    fn test_cli_range_validation() {
        let matches =
            new().try_get_matches_from(vec!["pgmon", "--dsn", "x", "--connect-timeout-ms", "100"]);
        assert!(matches.is_err());

        let matches =
            new().try_get_matches_from(vec!["pgmon", "--dsn", "x", "--refresh-ms", "100"]);
        assert!(matches.is_err());

        let matches =
            new().try_get_matches_from(vec!["pgmon", "--dsn", "x", "--sort", "longest_running"]);
        assert!(matches.is_err());
    }

    #[test]
    fn test_cli_accepts_positional_alias() {
        let matches = new().get_matches_from(vec!["pgmon", "prod"]);
        assert_eq!(
            matches.get_one::<String>("alias"),
            Some(&"prod".to_string())
        );
    }

    #[test]
    fn test_cli_accepts_alias_with_explicit_dsn() {
        let matches =
            new().get_matches_from(vec!["pgmon", "prod", "--dsn", "postgres://localhost"]);
        assert_eq!(
            matches.get_one::<String>("alias"),
            Some(&"prod".to_string())
        );
        assert_eq!(
            matches.get_one::<String>("dsn"),
            Some(&"postgres://localhost".to_string())
        );
    }

    #[test]
    fn test_cli_accepts_explicit_config_path() {
        let matches = new().get_matches_from(vec!["pgmon", "--config", "/tmp/pgmon.yaml"]);
        assert_eq!(
            matches.get_one::<String>("config"),
            Some(&"/tmp/pgmon.yaml".to_string())
        );
    }

    #[test]
    fn test_cli_accepts_check_config_subcommand() {
        let matches =
            new().get_matches_from(vec!["pgmon", "check-config", "--config", "/tmp/pgmon.yml"]);

        let Some(("check-config", subcommand)) = matches.subcommand() else {
            panic!("check-config subcommand should parse");
        };

        assert_eq!(
            subcommand.get_one::<String>("config"),
            Some(&"/tmp/pgmon.yml".to_string())
        );
    }

    #[test]
    fn test_cli_accepts_check_config_alias() {
        let matches = new().get_matches_from(vec!["pgmon", "check-config", "prod"]);

        let Some(("check-config", subcommand)) = matches.subcommand() else {
            panic!("check-config subcommand should parse");
        };

        assert_eq!(
            subcommand.get_one::<String>("alias"),
            Some(&"prod".to_string())
        );
    }
}

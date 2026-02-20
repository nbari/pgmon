use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::styling::{AnsiColor, Effects, Styles},
};

pub mod built_info {
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn new() -> Command {
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
        .override_usage("pgmon [OPTIONS] --dsn <DSN>")
        .after_help("For more information, visit: https://github.com/nbari/pgmon")
        .arg(
            Arg::new("dsn")
                .short('d')
                .long("dsn")
                .aliases(["url"])
                .value_name("STRING")
                .help("PostgreSQL DSN (connection string)")
                .long_help("The PostgreSQL connection string (DSN). It can be a URI-style string (e.g., postgresql://user:password@host:port/dbname) or a key=value pair string (e.g., host=localhost user=postgres).")
                .required(true)
                .env("PGMON_DSN"),
        )
        .arg(
            Arg::new("refresh-ms")
                .short('r')
                .long("refresh-ms")
                .value_name("u64")
                .help("Refresh interval in milliseconds")
                .long_help("Set the interval at which the UI polls the database for new statistics. The value must be between 200ms and 60,000ms (1 minute).")
                .value_parser(clap::value_parser!(u64).range(200..=60000))
                .default_value("1000"),
        )
        .arg(
            Arg::new("top-n")
                .short('n')
                .long("top-n")
                .value_name("u32")
                .help("Number of rows to show in top views")
                .long_help("Specifies the maximum number of rows to display in views like 'Top Queries' or 'Activity'. This helps manage terminal space and performance.")
                .value_parser(clap::value_parser!(u32).range(1..=100))
                .default_value("10"),
        )
        .arg(
            Arg::new("home-view")
                .long("home-view")
                .value_name("VIEW")
                .help("Initial view on startup")
                .long_help("Select which tab should be displayed when pgmon starts. Options are 'activity' (default) for current processes or 'statements' for cumulative statistics.")
                .value_parser(["activity", "statements"])
                .default_value("activity"),
        )
        .arg(
            Arg::new("sort")
                .short('s')
                .long("sort")
                .value_name("COLUMN")
                .help("Default sort column")
                .long_help("Define the default sorting criteria for the views. This is particularly useful for the 'statements' view.")
                .value_parser(["total_time", "mean_time", "calls", "longest_running"])
                .default_value("longest_running"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .action(ArgAction::Count)
                .help("Increase logging verbosity")
                .long_help("Increase the level of detail in the logs. Can be used multiple times (-vv, -vvv) to increase verbosity from INFO to DEBUG or TRACE."),
        )
        .arg(
            Arg::new("color")
                .long("color")
                .action(ArgAction::SetTrue)
                .help("Enable colored help output")
                .long_help("Forces the use of ANSI colors in the help output and UI, even if the terminal doesn't explicitly report support."),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_dsn_required() {
        // SAFETY: This is a test environment and we're ensuring PGMON_DSN is cleared.
        unsafe {
            std::env::remove_var("PGMON_DSN");
        }
        let cmd = new();
        let matches = cmd.try_get_matches_from(vec!["pgmon"]);
        assert!(matches.is_err());
    }

    #[test]
    fn test_cli_default_values() {
        let cmd = new();
        let matches = cmd.get_matches_from(vec!["pgmon", "--dsn", "postgres://localhost"]);
        assert_eq!(matches.get_one::<u64>("refresh-ms"), Some(&1000));
        assert_eq!(matches.get_one::<u32>("top-n"), Some(&10));
        assert_eq!(
            matches.get_one::<String>("home-view"),
            Some(&"activity".to_string())
        );
    }

    #[test]
    fn test_cli_range_validation() {
        let cmd = new();
        let matches = cmd.try_get_matches_from(vec!["pgmon", "--dsn", "x", "--refresh-ms", "100"]);
        assert!(matches.is_err());
    }
}

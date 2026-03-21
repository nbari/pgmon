use crate::{cli::actions::Action, pg::conninfo};
use anyhow::Result;
use clap::ArgMatches;
use std::path::PathBuf;

pub fn handler(matches: &ArgMatches) -> Result<Action> {
    let dsn = conninfo::resolve_dsn(matches.get_one::<String>("dsn").map(String::as_str))?;
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
        .unwrap_or_else(|| "longest_running".into());

    Ok(Action::StartTui {
        dsn,
        connect_timeout_ms,
        query_output_dir,
        refresh_ms,
        top_n,
        home_view,
        sort,
    })
}

use crate::cli::actions::Action;
use clap::ArgMatches;

pub fn handler(matches: &ArgMatches) -> Action {
    let dsn = matches.get_one::<String>("dsn").expect("required").clone();
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

    Action::StartTui {
        dsn,
        refresh_ms,
        top_n,
        home_view,
        sort,
    }
}

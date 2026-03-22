use crate::cli::{commands, dispatch, telemetry};
use crate::config::Config;
use anyhow::Result;
use clap::ArgMatches;
use std::path::Path;

pub fn run() -> Result<()> {
    let matches = commands::new().get_matches();

    let verbosity = matches.get_count("verbose");
    telemetry::init(verbosity as u8);

    let config_path = selected_config_path(&matches);
    let resolved_config_path = crate::config::resolved_config_path(config_path);
    let config = Config::load(config_path)?;
    let action = dispatch::handler(&matches, config, resolved_config_path.as_deref())?;
    action.execute()
}

fn selected_config_path(matches: &ArgMatches) -> Option<&Path> {
    matches
        .get_one::<String>("config")
        .or_else(|| {
            matches
                .subcommand()
                .and_then(|(_, subcommand)| subcommand.get_one::<String>("config"))
        })
        .map(Path::new)
}

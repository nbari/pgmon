use crate::cli::{commands, dispatch, telemetry};
use anyhow::Result;

pub fn run() -> Result<()> {
    let matches = commands::new().get_matches();

    let verbosity = matches.get_count("verbose");
    telemetry::init(verbosity as u8);

    let action = dispatch::handler(&matches);
    action.execute()
}

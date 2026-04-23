use crate::config::Config;
use crate::tui::app::App;
use anyhow::{Result, anyhow};
use std::path::PathBuf;

#[derive(Debug)]
pub enum Action {
    StartTui {
        dsn: String,
        connect_timeout_ms: u64,
        query_output_dir: Option<PathBuf>,
        refresh_ms: u64,
        top_n: u32,
        home_view: String,
        sort: String,
        config: Box<Config>,
    },
    CheckConfig {
        report: String,
        success: bool,
    },
}

impl Action {
    pub fn execute(self) -> Result<()> {
        match self {
            Action::StartTui {
                dsn,
                connect_timeout_ms,
                query_output_dir,
                refresh_ms,
                top_n,
                home_view,
                sort,
                config,
            } => {
                let mut app = App::new(
                    dsn,
                    connect_timeout_ms,
                    query_output_dir,
                    refresh_ms,
                    top_n,
                    &home_view,
                    &sort,
                    *config,
                )?;
                app.run()
            }
            Action::CheckConfig { report, success } => {
                if success {
                    println!("{report}");
                    Ok(())
                } else {
                    eprintln!("{report}");
                    Err(anyhow!("Configuration check failed."))
                }
            }
        }
    }
}

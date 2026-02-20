use crate::tui::app::App;
use anyhow::Result;

#[derive(Debug)]
pub enum Action {
    StartTui {
        dsn: String,
        refresh_ms: u64,
        top_n: u32,
        home_view: String,
        sort: String,
    },
}

impl Action {
    pub fn execute(self) -> Result<()> {
        match self {
            Action::StartTui {
                dsn,
                refresh_ms,
                top_n,
                home_view,
                sort,
            } => {
                let mut app = App::new(dsn, refresh_ms, top_n, &home_view, &sort);
                app.run()
            }
        }
    }
}

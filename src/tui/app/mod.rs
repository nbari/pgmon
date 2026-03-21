pub mod fetch;
pub mod format;
pub mod state;
pub mod ui_state;

#[cfg(test)]
mod tests;

use crate::pg::{
    client::{ActivitySnapshot, PgClient},
    conninfo::describe_connection_target,
};
use anyhow::{Context, Result};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend, widgets::TableState};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::pg::client::ActivitySession;
use fetch::{RefreshPayload, load_refresh_payload};
use format::{
    ActivityCounterSample, build_activity_summary, count_sessions, filter_activity_sessions,
    is_fuzzy_match, limit_activity_sessions, limit_rows, sort_statement_rows,
    sorted_activity_sessions,
};
pub use state::{
    ActivitySubview, ConfirmActionState, DashboardStats, DatabaseView, ErrorState, InputMode,
    LoadingState, NoticeState, RefreshIntervalState, StatementDetailState, Tab, TopNState,
};
pub use ui_state::{ActivitySummaryMetric, ActivitySummarySection};

const CONN_HISTORY_SIZE: usize = 600;
const NOTICE_DURATION: Duration = Duration::from_secs(6);

pub struct App {
    pub dsn: String,
    pub connect_timeout_ms: u64,
    pub query_output_dir: PathBuf,
    pub refresh_ms: u64,
    pub top_n: u32,
    pub sort: String,
    pub current_tab: Tab,
    pub activity_subview: ActivitySubview,
    pub database_view: DatabaseView,
    pub data: Vec<Vec<String>>,
    pub dashboard: DashboardStats,
    pub should_quit: bool,
    pub last_refresh: Instant,
    pub selected_row: Option<usize>,
    pub table_state: TableState,
    pub loading_state: Option<LoadingState>,
    pub error_state: Option<ErrorState>,
    pub notice_state: Option<NoticeState>,
    pub statement_detail: Option<StatementDetailState>,
    pub confirm_action: Option<ConfirmActionState>,
    pub refresh_interval_modal: Option<RefreshIntervalState>,
    pub top_n_modal: Option<TopNState>,
    pub input_mode: InputMode,
    pub search_query: String,
    pub has_loaded_once: bool,
    pg_client: Option<PgClient>,
    activity_sessions: Vec<ActivitySession>,
    previous_activity_sample: Option<ActivityCounterSample>,
    refresh_rx: Option<mpsc::Receiver<Result<RefreshPayload>>>,
}

impl App {
    pub fn new(
        dsn: String,
        connect_timeout_ms: u64,
        query_output_dir: Option<PathBuf>,
        refresh_ms: u64,
        top_n: u32,
        home_view: &str,
        sort: &str,
    ) -> Self {
        let current_tab = match home_view {
            "statements" => Tab::Statements,
            _ => Tab::Activity,
        };

        Self {
            dsn,
            connect_timeout_ms,
            query_output_dir: query_output_dir.unwrap_or_else(std::env::temp_dir),
            refresh_ms,
            top_n,
            sort: sort.to_string(),
            current_tab,
            activity_subview: ActivitySubview::Active,
            database_view: DatabaseView::Summary,
            data: Vec::new(),
            dashboard: DashboardStats::default(),
            should_quit: false,
            last_refresh: Instant::now(),
            selected_row: None,
            table_state: TableState::default(),
            loading_state: None,
            error_state: None,
            notice_state: None,
            statement_detail: None,
            confirm_action: None,
            refresh_interval_modal: None,
            top_n_modal: None,
            input_mode: InputMode::Normal,
            search_query: String::new(),
            has_loaded_once: false,
            pg_client: None,
            activity_sessions: Vec::new(),
            previous_activity_sample: None,
            refresh_rx: None,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;

        self.request_refresh();

        let run_result = self.run_loop(&mut terminal);

        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        run_result
    }

    fn run_loop(&mut self, terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
        while !self.should_quit {
            self.prune_notice();
            self.handle_refresh_result();
            terminal.draw(|f| crate::tui::ui::draw(f, self))?;

            if let Ok(true) = event::poll(Duration::from_millis(50))
                && let Event::Key(key) = event::read()?
            {
                self.handle_key_event(key);
            }

            if self.refresh_rx.is_none()
                && self.error_state.is_none()
                && self.last_refresh.elapsed() >= Duration::from_millis(self.refresh_ms)
            {
                self.request_refresh();
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn handle_key_event(&mut self, key: crossterm::event::KeyEvent) {
        match self.input_mode {
            InputMode::Normal => match key.code {
                KeyCode::Char('q') => {
                    if self.confirm_action.is_some() {
                        self.confirm_action = None;
                    } else if self.refresh_interval_modal.is_some() {
                        self.refresh_interval_modal = None;
                    } else if self.top_n_modal.is_some() {
                        self.top_n_modal = None;
                    } else {
                        self.should_quit = true;
                    }
                }
                KeyCode::Char('y') => {
                    if self.confirm_action.is_some() {
                        self.execute_confirmed_action();
                    }
                }
                KeyCode::Char('n') => {
                    if self.confirm_action.is_some() {
                        self.confirm_action = None;
                    } else {
                        self.toggle_top_n_modal();
                    }
                }
                KeyCode::Esc => {
                    if self.confirm_action.is_some() {
                        self.confirm_action = None;
                    } else if self.refresh_interval_modal.is_some() {
                        self.refresh_interval_modal = None;
                    } else if self.top_n_modal.is_some() {
                        self.top_n_modal = None;
                    } else {
                        self.search_query.clear();
                        self.handle_escape_key();
                    }
                }
                KeyCode::Char('r') => {
                    if self.error_state.is_some() {
                        self.error_state = None;
                        self.request_refresh();
                    } else {
                        self.toggle_refresh_modal();
                    }
                }
                KeyCode::Char('/') => {
                    self.input_mode = InputMode::Search;
                    self.search_query.clear();
                }
                KeyCode::Char('a') => self.set_activity_subview(ActivitySubview::Active),
                KeyCode::Char('w') => self.set_activity_subview(ActivitySubview::Waiting),
                KeyCode::Char('b') => self.set_activity_subview(ActivitySubview::Blocking),
                KeyCode::Char('t') => {
                    self.set_activity_subview(ActivitySubview::IdleInTransaction);
                }
                KeyCode::Char('i') => self.toggle_statement_detail(),
                KeyCode::Enter => {
                    if let Some(modal) = self.refresh_interval_modal.take() {
                        if let Some(&ms) = modal.options.get(modal.selected_index) {
                            self.refresh_ms = ms;
                            self.request_refresh();
                        }
                    } else if let Some(modal) = self.top_n_modal.take() {
                        if let Some(&n) = modal.options.get(modal.selected_index) {
                            self.top_n = n;
                            self.request_refresh();
                        }
                    } else {
                        self.handle_enter_key();
                    }
                }
                KeyCode::Char('1') => self.set_tab(Tab::Activity),
                KeyCode::Char('2') => self.set_tab(Tab::Database),
                KeyCode::Char('3') => self.set_tab(Tab::Locks),
                KeyCode::Char('4') => self.set_tab(Tab::IO),
                KeyCode::Char('5') => self.set_tab(Tab::Statements),
                KeyCode::Char('6') => self.set_tab(Tab::Tools),
                KeyCode::Char('7') => self.set_tab(Tab::Settings),
                KeyCode::Down | KeyCode::Char('j') => {
                    if let Some(ref mut modal) = self.refresh_interval_modal {
                        if modal.selected_index < modal.options.len() - 1 {
                            modal.selected_index += 1;
                        }
                    } else if let Some(ref mut modal) = self.top_n_modal {
                        if modal.selected_index < modal.options.len() - 1 {
                            modal.selected_index += 1;
                        }
                    } else {
                        let len = self.current_row_count();
                        if len > 0 {
                            let next = match self.selected_row {
                                Some(selected_row) if selected_row < len - 1 => selected_row + 1,
                                Some(selected_row) => selected_row,
                                None => 0,
                            };
                            self.selected_row = Some(next);
                            self.table_state.select(Some(next));
                        }
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if let Some(ref mut modal) = self.refresh_interval_modal {
                        if modal.selected_index > 0 {
                            modal.selected_index -= 1;
                        }
                    } else if let Some(ref mut modal) = self.top_n_modal {
                        if modal.selected_index > 0 {
                            modal.selected_index -= 1;
                        }
                    } else if self.current_row_count() > 0 {
                        let next = match self.selected_row {
                            Some(selected_row) if selected_row > 0 => selected_row - 1,
                            Some(selected_row) => selected_row,
                            None => 0,
                        };
                        self.selected_row = Some(next);
                        self.table_state.select(Some(next));
                    }
                }
                _ => {}
            },
            InputMode::Search => match key.code {
                KeyCode::Char(c) => {
                    self.search_query.push(c);
                    self.request_refresh();
                }
                KeyCode::Backspace => {
                    self.search_query.pop();
                    self.request_refresh();
                }
                KeyCode::Esc => {
                    self.input_mode = InputMode::Normal;
                    self.search_query.clear();
                    self.request_refresh();
                }
                KeyCode::Enter => {
                    self.input_mode = InputMode::Normal;
                }
                _ => {}
            },
        }
    }
    fn set_tab(&mut self, tab: Tab) {
        self.current_tab = tab;
        self.selected_row = None;
        self.database_view = DatabaseView::Summary;
        self.statement_detail = None;
        self.confirm_action = None;
        self.refresh_interval_modal = None;
        self.top_n_modal = None;
        self.search_query.clear();
        self.input_mode = InputMode::Normal;
        self.request_refresh();
    }

    fn set_activity_subview(&mut self, subview: ActivitySubview) {
        if self.current_tab != Tab::Activity || self.activity_subview == subview {
            return;
        }

        self.activity_subview = subview;
        self.selected_row = None;
        self.apply_activity_session_view();
    }

    fn request_refresh(&mut self) {
        let (tx, rx) = mpsc::channel();
        let dsn = self.dsn.clone();
        let connect_timeout_ms = self.connect_timeout_ms;
        let tab = self.current_tab;
        let database_view = self.database_view.clone();
        let should_show_loading = !self.has_loaded_once || self.error_state.is_some();
        let pg_client = self.pg_client.take();

        self.error_state = None;
        self.loading_state = should_show_loading.then(|| LoadingState {
            message: format!(
                "Connecting to {} (timeout: {} ms)...",
                describe_connection_target(&self.dsn),
                self.connect_timeout_ms
            ),
            started_at: Instant::now(),
        });
        self.refresh_rx = Some(rx);

        thread::spawn(move || {
            let result =
                load_refresh_payload(pg_client, &dsn, connect_timeout_ms, tab, &database_view);
            let _ = tx.send(result);
        });
    }

    fn handle_enter_key(&mut self) {
        match self.current_tab {
            Tab::Activity | Tab::Statements => self.save_selected_query(),
            Tab::Database => self.open_selected_database_tree(),
            Tab::Tools => self.prompt_tool_action(),
            Tab::Locks | Tab::IO | Tab::Settings => {}
        }
    }

    fn prompt_tool_action(&mut self) {
        if self.current_tab != Tab::Tools {
            return;
        }

        if let Some(row_data) = self.selected_row.and_then(|row| self.data.get(row)) {
            let title = row_data.first().map_or("", String::as_str).to_string();
            let description = row_data.get(1).map_or("", String::as_str).to_string();
            let query = row_data.get(2).map_or("", String::as_str).to_string();

            self.confirm_action = Some(ConfirmActionState {
                title,
                description,
                query,
            });
        }
    }

    fn execute_confirmed_action(&mut self) {
        let Some(action) = self.confirm_action.take() else {
            return;
        };

        let Some(mut client) = self.pg_client.take() else {
            self.error_state = Some(ErrorState {
                title: "Execution Failed".to_string(),
                details: "No active connection".to_string(),
            });
            return;
        };

        match client.execute_query(&action.query) {
            Ok(rows_affected) => {
                self.notice_state = Some(NoticeState {
                    message: format!("Success: {rows_affected} rows affected"),
                    created_at: Instant::now(),
                });
                self.pg_client = Some(client);
                self.request_refresh();
            }
            Err(e) => {
                self.error_state = Some(ErrorState {
                    title: "Execution Failed".to_string(),
                    details: e.to_string(),
                });
                self.pg_client = Some(client);
            }
        }
    }

    fn handle_escape_key(&mut self) {
        if self.statement_detail.is_some() {
            self.statement_detail = None;
        } else if self.current_tab == Tab::Database && self.database_view != DatabaseView::Summary {
            self.database_view = DatabaseView::Summary;
            self.selected_row = None;
            self.request_refresh();
        }
    }

    fn toggle_refresh_modal(&mut self) {
        if self.refresh_interval_modal.is_some() {
            self.refresh_interval_modal = None;
        } else {
            let options = vec![500, 1000, 2000, 3000, 4000, 5000, 10000];
            let selected_index = options
                .iter()
                .position(|&ms| ms == self.refresh_ms)
                .unwrap_or(1); // Default to 1s if not found
            self.refresh_interval_modal = Some(RefreshIntervalState {
                options,
                selected_index,
            });
        }
    }

    fn toggle_top_n_modal(&mut self) {
        if self.top_n_modal.is_some() {
            self.top_n_modal = None;
        } else {
            let options = vec![10, 20, 50, 100, 0]; // 0 means All
            let selected_index = options.iter().position(|&n| n == self.top_n).unwrap_or(0); // Default to 10
            self.top_n_modal = Some(TopNState {
                options,
                selected_index,
            });
        }
    }

    fn toggle_statement_detail(&mut self) {
        if self.current_tab != Tab::Statements {
            return;
        }

        self.statement_detail = if self.statement_detail.is_some() {
            None
        } else {
            self.selected_statement_detail()
        };
    }

    fn prune_notice(&mut self) {
        if self
            .notice_state
            .as_ref()
            .is_some_and(|notice| notice.created_at.elapsed() >= NOTICE_DURATION)
        {
            self.notice_state = None;
        }
    }

    fn save_selected_query(&mut self) {
        let Some(query) = self.selected_query() else {
            return;
        };

        match save_query_to_directory(self.current_tab, query, &self.query_output_dir) {
            Ok(path) => {
                self.notice_state = Some(NoticeState {
                    message: format!("Saved query to {}", path.display()),
                    created_at: Instant::now(),
                });
            }
            Err(error) => {
                self.error_state = Some(ErrorState {
                    title: "Save Failed".to_string(),
                    details: format!("{error:#}"),
                });
            }
        }
    }

    fn selected_query(&self) -> Option<&str> {
        match self.current_tab {
            Tab::Activity => self
                .selected_row
                .and_then(|selected_row| self.dashboard.sessions.get(selected_row))
                .map(|session| session.query.as_str()),
            Tab::Statements => self
                .selected_row
                .and_then(|selected_row| self.data.get(selected_row))
                .and_then(|row| row.first())
                .map(String::as_str),
            _ => None,
        }
    }

    fn selected_statement_detail(&self) -> Option<StatementDetailState> {
        let row = self
            .selected_row
            .and_then(|selected_row| self.data.get(selected_row))?;
        Some(StatementDetailState {
            query: row.first()?.clone(),
            total_time: row.get(1)?.clone(),
            mean_time: row.get(2)?.clone(),
            calls: row.get(3)?.clone(),
            read_time: row.get(4)?.clone(),
            write_time: row.get(5)?.clone(),
        })
    }

    fn open_selected_database_tree(&mut self) {
        if self.database_view != DatabaseView::Summary {
            return;
        }

        let Some(database) = self
            .selected_row
            .and_then(|selected_row| self.data.get(selected_row))
            .and_then(|row| row.first())
            .filter(|database| !database.is_empty())
            .cloned()
        else {
            return;
        };

        self.database_view = DatabaseView::Tables { database };
        self.selected_row = None;
        self.request_refresh();
    }

    fn current_row_count(&self) -> usize {
        if self.current_tab == Tab::Activity {
            self.dashboard.sessions.len()
        } else {
            self.data.len()
        }
    }

    fn handle_refresh_result(&mut self) {
        let Some(rx) = self.refresh_rx.as_ref() else {
            return;
        };

        match rx.try_recv() {
            Ok(result) => {
                self.refresh_rx = None;
                self.loading_state = None;
                match result {
                    Ok(payload) => {
                        self.last_refresh = Instant::now();
                        self.error_state = None;
                        self.apply_refresh_payload(payload);
                        self.has_loaded_once = true;
                    }
                    Err(error) => {
                        self.error_state = Some(ErrorState {
                            title: "Connection Failed".to_string(),
                            details: format!("{error:#}"),
                        });
                    }
                }
            }
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                self.refresh_rx = None;
                self.loading_state = None;
                self.error_state = Some(ErrorState {
                    title: "Refresh Failed".to_string(),
                    details: "Background refresh worker disconnected".to_string(),
                });
            }
        }
    }

    fn apply_refresh_payload(&mut self, payload: RefreshPayload) {
        match payload {
            RefreshPayload::Activity(activity, client) => {
                self.pg_client = Some(client);
                self.apply_activity_snapshot(*activity);
            }
            RefreshPayload::Table(data, client) => {
                self.pg_client = Some(client);
                self.data = self.prepare_table_data(data);
            }
        }

        let len = self.current_row_count();
        self.selected_row = clamp_selected_row(self.selected_row, len);
        self.table_state.select(self.selected_row);

        if self.current_tab == Tab::Statements && self.statement_detail.is_some() {
            self.statement_detail = self.selected_statement_detail();
        }
    }

    fn prepare_table_data(&self, data: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut rows = match self.current_tab {
            Tab::Statements => sort_statement_rows(data, &self.sort),
            _ => data,
        };

        if !self.search_query.is_empty() {
            rows.retain(|row| {
                row.iter()
                    .any(|cell| is_fuzzy_match(cell, &self.search_query))
            });
        }

        match self.current_tab {
            Tab::Settings | Tab::Tools => rows,
            Tab::Database if self.database_view != DatabaseView::Summary => rows,
            _ => {
                if self.top_n == 0 {
                    rows
                } else {
                    limit_rows(rows, self.top_n)
                }
            }
        }
    }

    fn apply_activity_snapshot(&mut self, snapshot: ActivitySnapshot) {
        let session_counts = count_sessions(&snapshot.sessions);
        self.dashboard.summary = build_activity_summary(
            &snapshot.summary,
            &snapshot.process,
            session_counts,
            self.previous_activity_sample.as_ref(),
        );
        self.previous_activity_sample = Some(ActivityCounterSample::from(&snapshot.summary));
        self.activity_sessions = snapshot.sessions;
        self.apply_activity_session_view();
        self.push_conn_history();
        self.data.clear();
    }

    fn apply_activity_session_view(&mut self) {
        let mut sorted = sorted_activity_sessions(
            filter_activity_sessions(&self.activity_sessions, self.activity_subview),
            self.activity_subview,
        );
        if self.top_n > 0 {
            sorted = limit_activity_sessions(sorted, self.top_n);
        }
        self.dashboard.sessions = sorted;
        self.selected_row = clamp_selected_row(self.selected_row, self.dashboard.sessions.len());
    }

    fn push_conn_history(&mut self) {
        let session_counts = &self.dashboard.summary.session_counts;
        if self.dashboard.conn_history.len() >= CONN_HISTORY_SIZE {
            let _ = self.dashboard.conn_history.pop_front();
        }
        self.dashboard.conn_history.push_back((
            session_counts.active,
            session_counts.idle,
            session_counts.total,
        ));
    }
}

fn clamp_selected_row(selected_row: Option<usize>, len: usize) -> Option<usize> {
    if len == 0 {
        None
    } else {
        selected_row.map(|selected_row| selected_row.min(len - 1))
    }
}

fn save_query_to_directory(tab: Tab, query: &str, directory: &Path) -> Result<PathBuf> {
    fs::create_dir_all(directory).with_context(|| {
        format!(
            "Failed to create query output directory {}",
            directory.display()
        )
    })?;
    let path = query_output_path(tab, directory)?;
    let mut contents = query.to_string();
    if !contents.ends_with('\n') {
        contents.push('\n');
    }

    fs::write(&path, contents)
        .with_context(|| format!("Failed to write query file at {}", path.display()))?;
    Ok(path)
}

fn query_output_path(tab: Tab, directory: &Path) -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before the Unix epoch")?
        .as_millis();
    let tab_name = match tab {
        Tab::Activity => "activity",
        Tab::Database => "database",
        Tab::Locks => "locks",
        Tab::IO => "io",
        Tab::Statements => "statements",
        Tab::Tools => "tools",
        Tab::Settings => "settings",
    };

    Ok(directory.join(format!("pgmon-query-{tab_name}-{timestamp}.sql")))
}

use crate::pg::{
    client::{
        ActivityProcessSnapshot, ActivitySession, ActivitySnapshot, ActivitySummarySnapshot,
        PgClient,
    },
    conninfo::describe_connection_target,
};
use anyhow::{Context, Result};
use chrono::Utc;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    collections::VecDeque,
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CONN_HISTORY_SIZE: usize = 600;
const NOTICE_DURATION: Duration = Duration::from_secs(6);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Tab {
    Activity,
    Database,
    Locks,
    IO,
    Statements,
}

#[derive(Clone, Default)]
pub struct DashboardStats {
    pub summary: ActivityDisplaySummary,
    pub sessions: Vec<ActivitySession>,
    /// Ring buffer: `(active, idle, total)` per refresh tick
    pub conn_history: VecDeque<(i64, i64, i64)>,
}

#[derive(Debug, Clone)]
enum RefreshPayload {
    Activity(Box<ActivitySnapshot>),
    Table(Vec<Vec<String>>),
}

#[derive(Debug, Clone)]
pub struct LoadingState {
    pub message: String,
    pub started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ErrorState {
    pub title: String,
    pub details: String,
}

/// Browsable session subsets within the `Activity` tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivitySubview {
    Active,
    Waiting,
    Blocking,
    IdleInTransaction,
}

#[derive(Debug, Clone, Default)]
pub struct SessionCounts {
    pub total: i64,
    pub active: i64,
    pub idle: i64,
    pub idle_in_transaction: i64,
    pub idle_in_transaction_aborted: i64,
    pub waiting: i64,
}

#[derive(Debug, Clone)]
pub struct ActivityRates {
    pub tps: String,
    pub inserts_per_sec: String,
    pub updates_per_sec: String,
    pub deletes_per_sec: String,
    pub tuples_returned_per_sec: String,
    pub temp_files_per_sec: String,
    pub temp_bytes_per_sec: String,
    pub growth_bytes_per_sec: String,
}

impl Default for ActivityRates {
    fn default() -> Self {
        Self {
            tps: "-".to_string(),
            inserts_per_sec: "-".to_string(),
            updates_per_sec: "-".to_string(),
            deletes_per_sec: "-".to_string(),
            tuples_returned_per_sec: "-".to_string(),
            temp_files_per_sec: "-".to_string(),
            temp_bytes_per_sec: "-".to_string(),
            growth_bytes_per_sec: "-".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityDisplaySummary {
    pub server_version: String,
    pub uptime_seconds: i64,
    pub database_count: i64,
    pub total_database_bytes: i64,
    pub cache_hit_pct: f64,
    pub rollback_pct: f64,
    pub session_counts: SessionCounts,
    pub rates: ActivityRates,
    pub process: ActivityProcessSnapshot,
    pub max_connections: i64,
}

impl Default for ActivityDisplaySummary {
    fn default() -> Self {
        Self {
            server_version: String::new(),
            uptime_seconds: 0,
            database_count: 0,
            total_database_bytes: 0,
            cache_hit_pct: 0.0,
            rollback_pct: 0.0,
            session_counts: SessionCounts::default(),
            rates: ActivityRates::default(),
            process: ActivityProcessSnapshot::default(),
            max_connections: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseView {
    Summary,
    Tables { database: String },
}

#[derive(Debug, Clone)]
pub struct NoticeState {
    pub message: String,
    pub created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct StatementDetailState {
    pub query: String,
    pub total_time: String,
    pub mean_time: String,
    pub calls: String,
    pub read_time: String,
    pub write_time: String,
}

#[derive(Debug, Clone)]
struct ActivityCounterSample {
    sampled_at: Instant,
    total_database_bytes: i64,
    total_xacts: i64,
    total_inserts: i64,
    total_updates: i64,
    total_deletes: i64,
    total_returned: i64,
    total_temp_files: i64,
    total_temp_bytes: i64,
}

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
    pub loading_state: Option<LoadingState>,
    pub error_state: Option<ErrorState>,
    pub notice_state: Option<NoticeState>,
    pub statement_detail: Option<StatementDetailState>,
    pub has_loaded_once: bool,
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
            loading_state: None,
            error_state: None,
            notice_state: None,
            statement_detail: None,
            has_loaded_once: false,
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
                match key.code {
                    KeyCode::Char('q') => self.should_quit = true,
                    KeyCode::Char('r') => {
                        self.error_state = None;
                        self.request_refresh();
                    }
                    KeyCode::Char('a') => self.set_activity_subview(ActivitySubview::Active),
                    KeyCode::Char('w') => self.set_activity_subview(ActivitySubview::Waiting),
                    KeyCode::Char('b') => self.set_activity_subview(ActivitySubview::Blocking),
                    KeyCode::Char('t') => {
                        self.set_activity_subview(ActivitySubview::IdleInTransaction);
                    }
                    KeyCode::Char('i') => self.toggle_statement_detail(),
                    KeyCode::Enter => self.handle_enter_key(),
                    KeyCode::Esc => self.handle_escape_key(),
                    KeyCode::Char('1') => self.set_tab(Tab::Activity),
                    KeyCode::Char('2') => self.set_tab(Tab::Database),
                    KeyCode::Char('3') => self.set_tab(Tab::Locks),
                    KeyCode::Char('4') => self.set_tab(Tab::IO),
                    KeyCode::Char('5') => self.set_tab(Tab::Statements),
                    KeyCode::Down => {
                        let len = if self.current_tab == Tab::Activity {
                            self.dashboard.sessions.len()
                        } else {
                            self.data.len()
                        };
                        if len > 0 {
                            self.selected_row = Some(match self.selected_row {
                                Some(selected_row) if selected_row < len - 1 => selected_row + 1,
                                Some(selected_row) => selected_row,
                                None => 0,
                            });
                        }
                    }
                    KeyCode::Up => {
                        if !self.current_rows().is_empty() {
                            self.selected_row = Some(match self.selected_row {
                                Some(selected_row) if selected_row > 0 => selected_row - 1,
                                Some(selected_row) => selected_row,
                                None => 0,
                            });
                        }
                    }
                    _ => {}
                }
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

    fn set_tab(&mut self, tab: Tab) {
        self.current_tab = tab;
        self.selected_row = None;
        self.database_view = DatabaseView::Summary;
        self.statement_detail = None;
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
            let result = load_refresh_payload(&dsn, connect_timeout_ms, tab, &database_view);
            let _ = tx.send(result);
        });
    }

    fn handle_enter_key(&mut self) {
        match self.current_tab {
            Tab::Activity | Tab::Statements => self.save_selected_query(),
            Tab::Database => self.open_selected_database_tree(),
            Tab::Locks | Tab::IO => {}
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

    fn current_rows(&self) -> &[Vec<String>] {
        &self.data
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
            RefreshPayload::Activity(activity) => self.apply_activity_snapshot(*activity),
            RefreshPayload::Table(data) => {
                self.data = self.prepare_table_data(data);
            }
        }

        self.selected_row = clamp_selected_row(self.selected_row, self.current_row_count());
        if self.current_tab == Tab::Statements && self.statement_detail.is_some() {
            self.statement_detail = self.selected_statement_detail();
        }
    }

    fn prepare_table_data(&self, data: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let rows = match self.current_tab {
            Tab::Statements => sort_statement_rows(data, &self.sort),
            _ => data,
        };
        if self.current_tab == Tab::Database && self.database_view != DatabaseView::Summary {
            rows
        } else {
            limit_rows(rows, self.top_n)
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
        self.dashboard.sessions = limit_activity_sessions(
            sorted_activity_sessions(
                filter_activity_sessions(&self.activity_sessions, self.activity_subview),
                self.activity_subview,
            ),
            self.top_n,
        );
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

fn load_refresh_payload(
    dsn: &str,
    connect_timeout_ms: u64,
    tab: Tab,
    database_view: &DatabaseView,
) -> Result<RefreshPayload> {
    match tab {
        Tab::Activity => PgClient::new(dsn, connect_timeout_ms)?
            .fetch_activity_snapshot()
            .map(|snapshot| RefreshPayload::Activity(Box::new(snapshot))),
        Tab::Database => match database_view {
            DatabaseView::Summary => PgClient::new(dsn, connect_timeout_ms)?
                .fetch_database_stats()
                .map(RefreshPayload::Table),
            DatabaseView::Tables { database } => {
                PgClient::for_database(dsn, connect_timeout_ms, database)?
                    .fetch_database_tree()
                    .map(RefreshPayload::Table)
            }
        },
        Tab::Locks => PgClient::new(dsn, connect_timeout_ms)?
            .fetch_locks()
            .map(RefreshPayload::Table),
        Tab::IO => PgClient::new(dsn, connect_timeout_ms)?
            .fetch_io_stats()
            .map(RefreshPayload::Table),
        Tab::Statements => PgClient::new(dsn, connect_timeout_ms)?
            .fetch_statements()
            .map(RefreshPayload::Table),
    }
}

fn sort_statement_rows(mut rows: Vec<Vec<String>>, sort: &str) -> Vec<Vec<String>> {
    let column = match sort {
        "mean_time" => Some(2usize),
        "calls" => Some(3usize),
        "total_time" => Some(1usize),
        _ => None,
    };

    if let Some(index) = column {
        rows.sort_by(|left, right| match sort {
            "calls" => parse_i64_field(right.get(index)).cmp(&parse_i64_field(left.get(index))),
            _ => parse_f64_field(right.get(index)).total_cmp(&parse_f64_field(left.get(index))),
        });
    }

    rows
}

fn limit_rows(mut rows: Vec<Vec<String>>, top_n: u32) -> Vec<Vec<String>> {
    rows.truncate(top_n as usize);
    rows
}

fn limit_activity_sessions(mut rows: Vec<ActivitySession>, top_n: u32) -> Vec<ActivitySession> {
    rows.truncate(top_n as usize);
    rows
}

fn clamp_selected_row(selected_row: Option<usize>, len: usize) -> Option<usize> {
    if len == 0 {
        None
    } else {
        selected_row.map(|selected_row| selected_row.min(len - 1))
    }
}

fn parse_i64_field(value: Option<&String>) -> i64 {
    value.and_then(|item| item.parse::<i64>().ok()).unwrap_or(0)
}

fn parse_f64_field(value: Option<&String>) -> f64 {
    value
        .and_then(|item| item.parse::<f64>().ok())
        .unwrap_or(0.0)
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
    };

    Ok(directory.join(format!("pgmon-query-{tab_name}-{timestamp}.sql")))
}

impl ActivitySubview {
    pub fn label(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Waiting => "Waiting",
            Self::Blocking => "Blocking",
            Self::IdleInTransaction => "Idle in txn",
        }
    }
}

impl From<&ActivitySummarySnapshot> for ActivityCounterSample {
    fn from(summary: &ActivitySummarySnapshot) -> Self {
        Self {
            sampled_at: Instant::now(),
            total_database_bytes: summary.total_database_bytes,
            total_xacts: summary.total_xacts,
            total_inserts: summary.total_inserts,
            total_updates: summary.total_updates,
            total_deletes: summary.total_deletes,
            total_returned: summary.total_returned,
            total_temp_files: summary.total_temp_files,
            total_temp_bytes: summary.total_temp_bytes,
        }
    }
}

fn build_activity_summary(
    summary: &ActivitySummarySnapshot,
    process: &ActivityProcessSnapshot,
    session_counts: SessionCounts,
    previous_sample: Option<&ActivityCounterSample>,
) -> ActivityDisplaySummary {
    let uptime_seconds = Utc::now()
        .signed_duration_since(summary.postmaster_start)
        .num_seconds()
        .max(0);

    ActivityDisplaySummary {
        server_version: summary.server_version.clone(),
        uptime_seconds,
        database_count: summary.database_count,
        total_database_bytes: summary.total_database_bytes,
        cache_hit_pct: summary.cache_hit_pct,
        rollback_pct: summary.rollback_pct,
        session_counts,
        rates: compute_activity_rates(previous_sample, summary),
        process: process.clone(),
        max_connections: summary.max_connections,
    }
}

fn compute_activity_rates(
    previous_sample: Option<&ActivityCounterSample>,
    current: &ActivitySummarySnapshot,
) -> ActivityRates {
    let Some(previous) = previous_sample else {
        return ActivityRates::default();
    };

    let elapsed_millis = previous.sampled_at.elapsed().as_millis();
    if elapsed_millis == 0 {
        return ActivityRates::default();
    }

    ActivityRates {
        tps: format_counter_rate(current.total_xacts, previous.total_xacts, elapsed_millis),
        inserts_per_sec: format_counter_rate(
            current.total_inserts,
            previous.total_inserts,
            elapsed_millis,
        ),
        updates_per_sec: format_counter_rate(
            current.total_updates,
            previous.total_updates,
            elapsed_millis,
        ),
        deletes_per_sec: format_counter_rate(
            current.total_deletes,
            previous.total_deletes,
            elapsed_millis,
        ),
        tuples_returned_per_sec: format_counter_rate(
            current.total_returned,
            previous.total_returned,
            elapsed_millis,
        ),
        temp_files_per_sec: format_counter_rate(
            current.total_temp_files,
            previous.total_temp_files,
            elapsed_millis,
        ),
        temp_bytes_per_sec: format_byte_rate(
            current.total_temp_bytes,
            previous.total_temp_bytes,
            elapsed_millis,
        ),
        growth_bytes_per_sec: format_signed_byte_rate(
            current.total_database_bytes,
            previous.total_database_bytes,
            elapsed_millis,
        ),
    }
}

fn format_counter_rate(current: i64, previous: i64, elapsed_millis: u128) -> String {
    if current < previous {
        return "-".to_string();
    }

    format_tenths_rate(i128::from(current - previous), elapsed_millis)
}

fn format_byte_rate(current: i64, previous: i64, elapsed_millis: u128) -> String {
    if current < previous {
        return "-".to_string();
    }

    format_signed_bytes_rate(i128::from(current - previous), elapsed_millis)
}

fn format_signed_byte_rate(current: i64, previous: i64, elapsed_millis: u128) -> String {
    format_signed_bytes_rate(i128::from(current - previous), elapsed_millis)
}

fn format_tenths_rate(delta: i128, elapsed_millis: u128) -> String {
    if elapsed_millis == 0 {
        return "-".to_string();
    }

    let Ok(elapsed_millis) = i128::try_from(elapsed_millis) else {
        return "-".to_string();
    };
    let scaled_tenths = delta.saturating_mul(10_000) / elapsed_millis;
    let sign = if scaled_tenths < 0 { "-" } else { "" };
    let absolute = scaled_tenths.abs();
    let whole = absolute / 10;
    let tenths = absolute % 10;
    format!("{sign}{whole}.{tenths}")
}

fn format_signed_bytes_rate(delta: i128, elapsed_millis: u128) -> String {
    if elapsed_millis == 0 {
        return "-".to_string();
    }

    let Ok(elapsed_millis) = i128::try_from(elapsed_millis) else {
        return "-".to_string();
    };
    let per_second = delta.saturating_mul(1000) / elapsed_millis;
    format_signed_bytes(per_second)
}

fn format_signed_bytes(bytes: i128) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let negative = bytes < 0;
    let absolute = bytes.unsigned_abs();
    let mut unit_index = 0usize;
    let mut divisor = 1u128;

    while unit_index + 1 < UNITS.len() && absolute >= divisor.saturating_mul(1024) {
        divisor = divisor.saturating_mul(1024);
        unit_index += 1;
    }

    let unit = UNITS.get(unit_index).copied().unwrap_or("B");
    let sign = if negative { "-" } else { "" };

    if unit_index == 0 {
        format!("{sign}{absolute} {unit}")
    } else {
        let scaled_tenths = absolute.saturating_mul(10) / divisor;
        let whole = scaled_tenths / 10;
        let tenths = scaled_tenths % 10;
        format!("{sign}{whole}.{tenths} {unit}")
    }
}

fn count_sessions(sessions: &[ActivitySession]) -> SessionCounts {
    SessionCounts {
        total: count_as_i64(sessions.len()),
        active: count_as_i64(
            sessions
                .iter()
                .filter(|session| session.state == "active")
                .count(),
        ),
        idle: count_as_i64(
            sessions
                .iter()
                .filter(|session| session.state == "idle")
                .count(),
        ),
        idle_in_transaction: sessions
            .iter()
            .filter(|session| session.state == "idle in transaction")
            .count()
            .try_into()
            .unwrap_or(i64::MAX),
        idle_in_transaction_aborted: sessions
            .iter()
            .filter(|session| session.state == "idle in transaction (aborted)")
            .count()
            .try_into()
            .unwrap_or(i64::MAX),
        waiting: sessions
            .iter()
            .filter(|session| session.blocked_by_count > 0)
            .count()
            .try_into()
            .unwrap_or(i64::MAX),
    }
}

fn count_as_i64(count: usize) -> i64 {
    count.try_into().unwrap_or(i64::MAX)
}

fn filter_activity_sessions(
    sessions: &[ActivitySession],
    subview: ActivitySubview,
) -> Vec<ActivitySession> {
    sessions
        .iter()
        .filter(|session| match subview {
            ActivitySubview::Active => session.state == "active",
            ActivitySubview::Waiting => session.blocked_by_count > 0,
            ActivitySubview::Blocking => session.blocked_count > 0,
            ActivitySubview::IdleInTransaction => is_idle_in_transaction_state(&session.state),
        })
        .cloned()
        .collect()
}

fn sorted_activity_sessions(
    mut sessions: Vec<ActivitySession>,
    subview: ActivitySubview,
) -> Vec<ActivitySession> {
    sessions.sort_by(|left, right| match subview {
        ActivitySubview::Blocking => right
            .blocked_count
            .cmp(&left.blocked_count)
            .then_with(|| right.duration_seconds.cmp(&left.duration_seconds))
            .then_with(|| left.pid.cmp(&right.pid)),
        _ => right
            .duration_seconds
            .cmp(&left.duration_seconds)
            .then_with(|| right.blocked_by_count.cmp(&left.blocked_by_count))
            .then_with(|| left.pid.cmp(&right.pid)),
    });
    sessions
}

fn is_idle_in_transaction_state(state: &str) -> bool {
    matches!(
        state,
        "idle in transaction" | "idle in transaction (aborted)"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ActivitySession, ActivitySubview, Tab, clamp_selected_row, count_sessions,
        filter_activity_sessions, format_counter_rate, limit_rows, query_output_path,
        save_query_to_directory, sort_statement_rows, sorted_activity_sessions,
    };
    use std::{error::Error, fs, path::Path};

    #[test]
    fn test_sort_statement_rows_by_calls_desc() {
        let rows = vec![
            vec![
                "q1".to_string(),
                "10.0".to_string(),
                "1.0".to_string(),
                "5".to_string(),
            ],
            vec![
                "q2".to_string(),
                "5.0".to_string(),
                "0.5".to_string(),
                "10".to_string(),
            ],
        ];

        let sorted = sort_statement_rows(rows, "calls");
        assert_eq!(
            sorted.first().and_then(|row| row.first()),
            Some(&"q2".to_string())
        );
    }

    #[test]
    fn test_limit_rows_truncates_to_top_n() {
        let rows = vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ];
        let limited = limit_rows(rows, 2);
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_clamp_selected_row_handles_shorter_data() {
        assert_eq!(clamp_selected_row(Some(4), 2), Some(1));
        assert_eq!(clamp_selected_row(Some(4), 0), None);
        assert_eq!(clamp_selected_row(None, 2), None);
    }

    #[test]
    fn test_filter_activity_sessions_active_includes_only_active_rows() {
        let sessions = vec![
            session_with_state("1", "active", 0, 0, 5),
            session_with_state("2", "idle in transaction", 0, 0, 30),
            session_with_state("3", "idle", 1, 0, 15),
            session_with_state("4", "idle", 0, 0, 20),
        ];

        let filtered = filter_activity_sessions(&sessions, ActivitySubview::Active);
        let pids: Vec<&str> = filtered
            .iter()
            .map(|session| session.pid.as_str())
            .collect();

        assert_eq!(pids, vec!["1"]);
    }

    #[test]
    fn test_sorted_activity_sessions_prefers_blocker_count_in_blocking_view() {
        let sessions = vec![
            session_with_state("1", "idle", 0, 1, 30),
            session_with_state("2", "active", 0, 3, 10),
        ];

        let sorted = sorted_activity_sessions(sessions, ActivitySubview::Blocking);
        assert_eq!(
            sorted.first().map(|session| session.pid.as_str()),
            Some("2")
        );
    }

    #[test]
    fn test_count_sessions_tracks_waiting_and_idle_states() {
        let sessions = vec![
            session_with_state("1", "active", 0, 0, 5),
            session_with_state("2", "idle", 0, 0, 5),
            session_with_state("3", "idle in transaction", 0, 0, 5),
            session_with_state("4", "idle in transaction (aborted)", 0, 0, 5),
            session_with_state("5", "active", 1, 0, 5),
        ];

        let counts = count_sessions(&sessions);

        assert_eq!(counts.total, 5);
        assert_eq!(counts.active, 2);
        assert_eq!(counts.idle, 1);
        assert_eq!(counts.idle_in_transaction, 1);
        assert_eq!(counts.idle_in_transaction_aborted, 1);
        assert_eq!(counts.waiting, 1);
    }

    #[test]
    fn test_format_counter_rate_formats_tenths() {
        assert_eq!(format_counter_rate(110, 100, 2_000), "5.0");
    }

    #[test]
    fn test_query_output_path_uses_tab_name_and_sql_extension() -> Result<(), Box<dyn Error>> {
        let path = query_output_path(Tab::Statements, Path::new("/tmp"))?;
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("missing filename")?;

        assert!(filename.starts_with("pgmon-query-statements-"));
        assert!(
            path.extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
        );
        Ok(())
    }

    #[test]
    fn test_save_query_to_directory_writes_query() -> Result<(), Box<dyn Error>> {
        let path = save_query_to_directory(Tab::Activity, "select 1", Path::new("/tmp"))?;
        let content = fs::read_to_string(&path)?;

        assert_eq!(content, "select 1\n");

        fs::remove_file(path)?;
        Ok(())
    }

    fn session_with_state(
        pid: &str,
        state: &str,
        blocked_by_count: i64,
        blocked_count: i64,
        duration_seconds: i64,
    ) -> ActivitySession {
        ActivitySession {
            pid: pid.to_string(),
            xmin: String::new(),
            database: "postgres".to_string(),
            application: "app".to_string(),
            user: "postgres".to_string(),
            client: "[local]".to_string(),
            duration_seconds,
            wait_info: String::new(),
            state: state.to_string(),
            query: "select 1".to_string(),
            blocked_by_count,
            blocked_count,
        }
    }
}

pub mod fetch;
pub mod format;
pub mod state;
pub mod ui_state;

#[cfg(test)]
mod tests;

use crate::config::{Config, ExportFormat};
use crate::pg::{
    client::{ActivitySnapshot, CapabilityStatus, PgClient, ReplicationSnapshot},
    conninfo::{describe_connection_target, describe_host},
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
    ActivityCounterSample, activity_state_cell, activity_wait_cell, build_activity_summary,
    count_sessions, filter_activity_sessions, format_activity_query, format_duration_hms,
    is_fuzzy_match, is_normalized_query, limit_activity_sessions, limit_rows, sender_to_row,
    sort_statement_rows, sorted_activity_sessions, table_header_cells,
};
pub use state::{
    ActivityChartMetric, ActivitySubview, CapabilitySummary, ConfirmActionState,
    ConnectionHealthState, ConnectionStatus, DashboardStats, DatabaseView, DeadlockGraphState,
    ErrorState, ExplainPlanState, HelpSection, HelpState, InputMode, LoadingState, NoticeState,
    OfflineState, QueryDetailSource, QueryDetailState, QueryStats, RefreshIntervalState, Tab,
    TableDefinitionState, ThemeState, TopNState,
};
pub use ui_state::{ActivitySummaryMetric, ActivitySummarySection};

const CONN_HISTORY_SIZE: usize = 600;
const NOTICE_DURATION: Duration = Duration::from_secs(6);
const RECONNECT_BACKOFF_STEPS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(30),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingRequestKind {
    Refresh,
    Explain,
    AdminAction,
    TableDefinition,
}

struct PendingRequest {
    kind: PendingRequestKind,
    rx: mpsc::Receiver<Result<RefreshPayload>>,
    started_at: Instant,
}

pub struct App {
    pub dsn: String,
    pub connect_timeout_ms: u64,
    pub query_output_dir: PathBuf,
    pub refresh_ms: u64,
    pub top_n: u32,
    pub sort: String,
    pub current_tab: Tab,
    pub activity_chart_metric: ActivityChartMetric,
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
    pub query_detail: Option<QueryDetailState>,
    pub explain_plan: Option<ExplainPlanState>,
    pub deadlock_graph: Option<DeadlockGraphState>,
    pub table_definition: Option<TableDefinitionState>,
    pub confirm_action: Option<ConfirmActionState>,
    pub refresh_interval_modal: Option<RefreshIntervalState>,
    pub top_n_modal: Option<TopNState>,
    pub theme_modal: Option<ThemeState>,
    pub help_modal: Option<HelpState>,
    pub input_mode: InputMode,
    pub search_query: String,
    pub has_loaded_once: bool,
    pub(crate) replication: ReplicationSnapshot,
    pub(crate) capabilities: CapabilitySummary,
    pub(crate) connection_health: ConnectionHealthState,
    pub(crate) config: Config,
    pub(crate) connection_status: ConnectionStatus,
    pg_client: Option<PgClient>,
    activity_sessions: Vec<ActivitySession>,
    previous_activity_sample: Option<ActivityCounterSample>,
    pending_request: Option<PendingRequest>,
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dsn: String,
        connect_timeout_ms: u64,
        query_output_dir: Option<PathBuf>,
        refresh_ms: u64,
        top_n: u32,
        home_view: &str,
        sort: &str,
        config: Config,
    ) -> Self {
        let current_tab = match home_view {
            "statements" => Tab::Statements,
            _ => Tab::Activity,
        };
        let connection_target = describe_host(&dsn);

        Self {
            dsn,
            connect_timeout_ms,
            query_output_dir: query_output_dir.unwrap_or_else(std::env::temp_dir),
            refresh_ms,
            top_n,
            sort: sort.to_string(),
            current_tab,
            activity_chart_metric: ActivityChartMetric::default(),
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
            query_detail: None,
            explain_plan: None,
            deadlock_graph: None,
            table_definition: None,
            confirm_action: None,
            refresh_interval_modal: None,
            top_n_modal: None,
            theme_modal: None,
            help_modal: None,
            input_mode: InputMode::Normal,
            search_query: String::new(),
            has_loaded_once: false,
            replication: ReplicationSnapshot::default(),
            capabilities: CapabilitySummary::default(),
            connection_health: ConnectionHealthState {
                target: connection_target,
                last_refresh_duration: None,
                last_refresh_at: None,
                last_error: None,
                last_error_at: None,
            },
            config,
            connection_status: ConnectionStatus::Online,
            pg_client: None,
            activity_sessions: Vec::new(),
            previous_activity_sample: None,
            pending_request: None,
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

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
            {
                self.handle_key_event(key);
            }

            if self.should_request_background_refresh() {
                self.request_refresh();
            }
        }

        Ok(())
    }

    fn can_request_refresh(&self) -> bool {
        self.pending_request.is_none() && !self.has_blocking_modal_open()
    }

    fn should_request_background_refresh(&self) -> bool {
        if !self.has_loaded_once || !self.can_request_refresh() {
            return false;
        }

        match self.offline_state() {
            Some(state) => Instant::now() >= state.next_retry_at,
            None => self.last_refresh.elapsed() >= self.effective_refresh_interval(),
        }
    }

    fn has_blocking_modal_open(&self) -> bool {
        self.error_state.is_some()
            || self.confirm_action.is_some()
            || self.query_detail.is_some()
            || self.explain_plan.is_some()
            || self.deadlock_graph.is_some()
            || self.table_definition.is_some()
    }

    #[allow(clippy::too_many_lines)]
    fn handle_key_event(&mut self, key: crossterm::event::KeyEvent) {
        // 1. Error Modal (Highest Priority)
        if self.error_state.is_some() {
            match key.code {
                KeyCode::Char('q') => self.should_quit = true,
                KeyCode::Char('r') | KeyCode::Esc => {
                    self.error_state = None;
                    if key.code == KeyCode::Char('r') {
                        self.request_refresh();
                    }
                }
                _ => {}
            }
            return;
        }

        // 2. Confirm Action Modal
        if let Some(action) = self.confirm_action.clone() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Enter => {
                    self.confirm_action = None;
                    self.execute_confirmed_action(&action.query);
                }
                KeyCode::Char('n' | 'q') | KeyCode::Esc => {
                    self.confirm_action = None;
                }
                _ => {}
            }
            return;
        }

        // 3. Explain Plan Modal
        if self.explain_plan.is_some() {
            if key.code == KeyCode::Esc || key.code == KeyCode::Char('q') {
                self.explain_plan = None;
            }
            return;
        }

        // 3.1 Deadlock Graph Modal
        if self.deadlock_graph.is_some() {
            if key.code == KeyCode::Esc || key.code == KeyCode::Char('q') {
                self.deadlock_graph = None;
            }
            return;
        }

        // 3.2 Table Definition Modal
        if self.table_definition.is_some() {
            if key.code == KeyCode::Esc || key.code == KeyCode::Char('q') {
                self.table_definition = None;
            }
            return;
        }

        // 4. Query Detail Modal
        if let Some(detail) = self.query_detail.clone() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    self.query_detail = None;
                }
                KeyCode::Enter => {
                    self.save_selected_query();
                }
                KeyCode::Char('x') => match detail.source {
                    QueryDetailSource::Statements => {
                        self.notice_state = Some(NoticeState {
                                message: "Explain Analyze is only available from the Activity view. Statements uses normalized pg_stat_statements SQL.".to_string(),
                                created_at: Instant::now(),
                            });
                    }
                    QueryDetailSource::Activity => {
                        if is_normalized_query(detail.query.as_str()) {
                            self.notice_state = Some(NoticeState {
                                    message: "Explain Analyze is unavailable for normalized queries. Replace $1, $2, ... with literal values first.".to_string(),
                                    created_at: Instant::now(),
                                });
                        } else {
                            self.handle_explain_analyze(&detail);
                        }
                    }
                },
                _ => {}
            }
            return;
        }

        // 5. Refresh Interval Modal
        if let Some(modal) = self.refresh_interval_modal.as_mut() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q' | 'r') => {
                    self.refresh_interval_modal = None;
                }
                KeyCode::Enter => {
                    if let Some(&ms) = modal.options.get(modal.selected_index) {
                        self.refresh_ms = ms;
                        self.refresh_interval_modal = None;
                        self.request_refresh();
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if modal.selected_index < modal.options.len() - 1 {
                        modal.selected_index += 1;
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if modal.selected_index > 0 {
                        modal.selected_index -= 1;
                    }
                }
                _ => {}
            }
            return;
        }

        // 6. Top-N Modal
        if let Some(modal) = self.top_n_modal.as_mut() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q' | 'n') => {
                    self.top_n_modal = None;
                }
                KeyCode::Enter => {
                    if let Some(&n) = modal.options.get(modal.selected_index) {
                        self.top_n = n;
                        self.top_n_modal = None;
                        self.request_refresh();
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if modal.selected_index < modal.options.len() - 1 {
                        modal.selected_index += 1;
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if modal.selected_index > 0 {
                        modal.selected_index -= 1;
                    }
                }
                _ => {}
            }
            return;
        }

        // 6.1 Theme Modal
        if self.theme_modal.is_some() {
            let mut close_modal = false;
            let mut selected_theme = None;
            if let Some(modal) = self.theme_modal.as_mut() {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('q' | 'T') => {
                        close_modal = true;
                    }
                    KeyCode::Enter => {
                        selected_theme = modal.options.get(modal.selected_index).cloned();
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if modal.selected_index < modal.options.len().saturating_sub(1) {
                            modal.selected_index += 1;
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if modal.selected_index > 0 {
                            modal.selected_index -= 1;
                        }
                    }
                    _ => {}
                }
            }
            if close_modal {
                self.theme_modal = None;
            }
            if let Some(theme_name) = selected_theme.as_deref() {
                self.apply_theme_selection(theme_name);
            }
            return;
        }

        // 6.2 Help Modal
        if self.help_modal.is_some() {
            if matches!(key.code, KeyCode::Esc | KeyCode::Char('q' | '?')) {
                self.help_modal = None;
            }
            return;
        }

        // 7. Input Mode handling
        match self.input_mode {
            InputMode::Normal => match key.code {
                KeyCode::Char('q') => {
                    self.should_quit = true;
                }
                KeyCode::Char('1') => self.set_tab(Tab::Activity),
                KeyCode::Char('2') => self.set_tab(Tab::Database),
                KeyCode::Char('3') => self.set_tab(Tab::Locks),
                KeyCode::Char('4') => self.set_tab(Tab::IO),
                KeyCode::Char('5') => self.set_tab(Tab::Statements),
                KeyCode::Char('6') => self.set_tab(Tab::Replication),
                KeyCode::Char('7') => self.set_tab(Tab::Settings),
                KeyCode::Char('8') => self.set_tab(Tab::Tools),
                KeyCode::Char('e') => {
                    if self.can_export_current_view() {
                        self.export_current_table();
                    }
                }
                KeyCode::Char('s') => {
                    if let (Tab::Database, DatabaseView::Tables { .. }) =
                        (self.current_tab, &self.database_view)
                    {
                        self.handle_show_table_definition();
                    }
                }
                KeyCode::Char('i') => match self.current_tab {
                    Tab::Activity => {
                        self.query_detail = self.selected_activity_detail();
                    }
                    Tab::Statements => {
                        self.query_detail = self.selected_statement_detail();
                    }
                    Tab::Locks => {
                        self.generate_deadlock_graph();
                    }
                    _ => {}
                },
                KeyCode::Char('v') => {
                    if let (Tab::Database, DatabaseView::Tables { .. }) =
                        (self.current_tab, &self.database_view)
                    {
                        self.prompt_vacuum_analyze();
                    }
                }
                KeyCode::Char('R') => {
                    if let (Tab::Database, DatabaseView::Tables { .. }) =
                        (self.current_tab, &self.database_view)
                    {
                        self.prompt_reindex();
                    }
                }
                KeyCode::Char('h') => self.set_tab(self.current_tab.prev()),
                KeyCode::Char('l') => self.set_tab(self.current_tab.next()),
                KeyCode::Down | KeyCode::Char('j') => {
                    let len = self.current_row_count();
                    if let Some(selected_row) = self.selected_row {
                        if selected_row < len.saturating_sub(1) {
                            self.selected_row = Some(selected_row + 1);
                        }
                    } else if len > 0 {
                        self.selected_row = Some(0);
                    }
                    self.table_state.select(self.selected_row);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if let Some(selected_row) = self.selected_row {
                        if selected_row > 0 {
                            self.selected_row = Some(selected_row - 1);
                        }
                    } else {
                        let len = self.current_row_count();
                        if len > 0 {
                            self.selected_row = Some(len - 1);
                        }
                    }
                    self.table_state.select(self.selected_row);
                }
                KeyCode::Char('n') => self.toggle_top_n_modal(),
                KeyCode::Char('m') => {
                    if self.current_tab == Tab::Activity {
                        self.cycle_activity_chart_metric();
                    }
                }
                KeyCode::Char('?') => self.toggle_help_modal(),
                KeyCode::Char('T') => self.toggle_theme_modal(),
                KeyCode::Char('c') => {
                    if self.is_offline() {
                        self.request_refresh();
                    }
                }
                KeyCode::Esc => {
                    self.search_query.clear();
                    self.handle_escape_key();
                }
                KeyCode::Char('r') => self.toggle_refresh_modal(),
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
                KeyCode::Enter => {
                    self.handle_enter_key();
                }
                _ => {}
            },
            InputMode::Search => match key.code {
                KeyCode::Enter => {
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Char(c) => {
                    self.search_query.push(c);
                    if self.current_tab == Tab::Activity {
                        self.apply_activity_session_view();
                    }
                }
                KeyCode::Backspace => {
                    self.search_query.pop();
                    if self.current_tab == Tab::Activity {
                        self.apply_activity_session_view();
                    }
                }
                KeyCode::Esc => {
                    self.input_mode = InputMode::Normal;
                    self.search_query.clear();
                    if self.current_tab == Tab::Activity {
                        self.apply_activity_session_view();
                    }
                }
                _ => {}
            },
        }
    }

    fn handle_escape_key(&mut self) {
        if self.current_tab == Tab::Database && self.database_view != DatabaseView::Summary {
            self.database_view = DatabaseView::Summary;
            self.selected_row = None;
            self.request_refresh();
        }
    }

    fn handle_enter_key(&mut self) {
        match self.current_tab {
            Tab::Activity | Tab::Statements => {
                self.save_selected_query();
            }
            Tab::Database => {
                self.open_selected_database_tree();
            }
            Tab::Tools => {
                self.prompt_tool_action();
            }
            _ => {}
        }
    }

    fn set_tab(&mut self, tab: Tab) {
        if self.current_tab == tab {
            return;
        }
        self.current_tab = tab;
        self.selected_row = None;
        self.table_state.select(None);
        self.search_query.clear();
        self.query_detail = None;
        self.request_refresh();
    }

    fn set_activity_subview(&mut self, subview: ActivitySubview) {
        if self.current_tab != Tab::Activity || self.activity_subview == subview {
            return;
        }
        self.activity_subview = subview;
        self.apply_activity_session_view();
    }

    fn request_refresh(&mut self) {
        if self.pending_request.is_some() {
            return;
        }

        if !self.has_loaded_once {
            self.loading_state = Some(LoadingState {
                message: format!("Connecting to {}...", describe_connection_target(&self.dsn)),
                started_at: Instant::now(),
            });
        }

        let (tx, rx) = mpsc::channel();
        self.pending_request = Some(PendingRequest {
            kind: PendingRequestKind::Refresh,
            rx,
            started_at: Instant::now(),
        });

        let dsn = self.dsn.clone();
        let timeout = self.connect_timeout_ms;
        let tab = self.current_tab;
        let database_view = self.database_view.clone();
        let pg_client = if self.is_offline() {
            self.pg_client = None;
            None
        } else {
            self.pg_client.take()
        };

        thread::spawn(move || {
            let result = load_refresh_payload(pg_client, &dsn, timeout, tab, &database_view);
            let _ = tx.send(result);
        });
    }

    fn toggle_refresh_modal(&mut self) {
        if self.refresh_interval_modal.is_some() {
            self.refresh_interval_modal = None;
        } else {
            let options = vec![500, 1000, 2000, 3000, 4000, 5000, 10000];
            let selected_index = options
                .iter()
                .position(|&ms| ms == self.refresh_ms)
                .unwrap_or(0);
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
            let options = vec![5, 10, 20, 50, 0];
            let selected_index = options.iter().position(|&n| n == self.top_n).unwrap_or(1);
            self.top_n_modal = Some(TopNState {
                options,
                selected_index,
            });
        }
    }

    fn toggle_theme_modal(&mut self) {
        if self.theme_modal.is_some() {
            self.theme_modal = None;
            return;
        }

        let options = self
            .config
            .theme_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        if options.is_empty() {
            self.notice_state = Some(NoticeState {
                message: "No named themes are configured in pgmon.yaml or pgmon.yml.".to_string(),
                created_at: Instant::now(),
            });
            return;
        }

        let selected_index = self
            .config
            .active_theme_name()
            .and_then(|theme| options.iter().position(|option| option == theme))
            .unwrap_or(0);
        self.theme_modal = Some(ThemeState {
            options,
            selected_index,
        });
    }

    fn toggle_help_modal(&mut self) {
        if self.help_modal.is_some() {
            self.help_modal = None;
            return;
        }

        self.help_modal = Some(self.build_help_state());
    }

    fn build_help_state(&self) -> HelpState {
        let mut sections = Self::global_help_sections();
        sections.extend(self.current_view_help_sections());
        if let Some(section) = self.capability_help_section() {
            sections.push(section);
        }
        if let Some(section) = self.offline_help_section() {
            sections.push(section);
        }

        HelpState {
            title: format!("Help: {}", self.current_tab.label()),
            sections,
        }
    }

    fn global_help_sections() -> Vec<HelpSection> {
        vec![HelpSection {
            heading: "Global".to_string(),
            lines: vec![
                "1-8 switch tabs; h/l move between tabs; j/k move the current selection."
                    .to_string(),
                "/ enters search for the current view; Esc closes search or the current modal."
                    .to_string(),
                "r opens refresh interval choices; n opens the Top-N picker; T opens the theme picker when themes exist."
                    .to_string(),
            ],
        }]
    }

    fn current_view_help_sections(&self) -> Vec<HelpSection> {
        match self.current_tab {
            Tab::Activity => Self::activity_help_sections(),
            Tab::Statements => vec![Self::statements_help_section()],
            Tab::Database => vec![self.database_help_section()],
            Tab::Locks => vec![Self::locks_help_section()],
            Tab::IO => vec![Self::io_help_section()],
            Tab::Replication => vec![Self::replication_help_section()],
            Tab::Settings => vec![Self::settings_help_section()],
            Tab::Tools => vec![Self::tools_help_section()],
        }
    }

    fn activity_help_sections() -> Vec<HelpSection> {
        vec![
            HelpSection {
                heading: "Activity".to_string(),
                lines: vec![
                    "a/w/b/t switch between active, waiting, blocking, and idle-in-transaction sessions.".to_string(),
                    "i inspects the selected backend query; x runs Explain only from the Activity query detail when the SQL is executable.".to_string(),
                    "Enter saves the selected query; e exports the current Activity table; m cycles the main chart metric.".to_string(),
                ],
            },
            HelpSection {
                heading: "Metrics".to_string(),
                lines: vec![
                    "TPS is transactions per second (commits plus rollbacks).".to_string(),
                    "DML/s shows INSERT, UPDATE, and DELETE rates per second.".to_string(),
                    "Temp Bytes/s tracks temporary file bytes written per second when queries spill to disk.".to_string(),
                    "Growth Bytes/s estimates database size growth per second across sampled refreshes.".to_string(),
                ],
            },
        ]
    }

    fn statements_help_section() -> HelpSection {
        HelpSection {
            heading: "Statements".to_string(),
            lines: vec![
                "i opens the selected pg_stat_statements entry with aggregated timing details."
                    .to_string(),
                "Enter saves the selected SQL text; e exports the current Statements table."
                    .to_string(),
                "Statements uses normalized pg_stat_statements SQL, so Explain is intentionally unavailable here."
                    .to_string(),
            ],
        }
    }

    fn database_help_section(&self) -> HelpSection {
        let mut lines = vec![
            "Enter opens the selected database and browses schemas/tables.".to_string(),
            "Esc returns from the table tree to the database summary.".to_string(),
        ];
        if matches!(self.database_view, DatabaseView::Tables { .. }) {
            lines.push(
                "s shows the selected table definition; v runs VACUUM ANALYZE; R runs REINDEX after confirmation."
                    .to_string(),
            );
        }

        HelpSection {
            heading: "Database".to_string(),
            lines,
        }
    }

    fn locks_help_section() -> HelpSection {
        HelpSection {
            heading: "Locks".to_string(),
            lines: vec![
                "i opens the deadlock/blocking visualizer for the current lock snapshot."
                    .to_string(),
                "Use this view to find blockers and blocked sessions quickly before drilling into Activity."
                    .to_string(),
            ],
        }
    }

    fn io_help_section() -> HelpSection {
        HelpSection {
            heading: "IO".to_string(),
            lines: vec![
                "This view summarizes pg_stat_io metrics when PostgreSQL exposes them."
                    .to_string(),
                "If the view is unavailable, pgmon will say so explicitly; pg_stat_io requires PostgreSQL 16+."
                    .to_string(),
            ],
        }
    }

    fn replication_help_section() -> HelpSection {
        HelpSection {
            heading: "Replication".to_string(),
            lines: vec![
                "Shows WAL senders, standby receiver status, and replication slots when replication is enabled."
                    .to_string(),
                "If replication is not configured on the server, the view shows a capability message instead of empty panels."
                    .to_string(),
            ],
        }
    }

    fn settings_help_section() -> HelpSection {
        HelpSection {
            heading: "Settings".to_string(),
            lines: vec![
                "Use / to filter configuration rows and inspect server settings quickly."
                    .to_string(),
            ],
        }
    }

    fn tools_help_section() -> HelpSection {
        HelpSection {
            heading: "Tools".to_string(),
            lines: vec![
                "Enter opens a confirmation prompt for the selected administrative statement."
                    .to_string(),
                "Use y to confirm or n/Esc to cancel once the confirmation modal is open."
                    .to_string(),
            ],
        }
    }

    fn capability_help_section(&self) -> Option<HelpSection> {
        match self.current_view_capability() {
            Some(CapabilityStatus::Unavailable(reason)) => Some(HelpSection {
                heading: "Capability".to_string(),
                lines: vec![reason.clone()],
            }),
            _ => None,
        }
    }

    fn offline_help_section(&self) -> Option<HelpSection> {
        if !self.is_offline() {
            return None;
        }

        Some(HelpSection {
            heading: "Connection".to_string(),
            lines: vec![
                "The app keeps the last successful data on screen while reconnecting.".to_string(),
                "Use c to trigger an immediate reconnect attempt when the footer shows OFFLINE."
                    .to_string(),
            ],
        })
    }

    fn apply_theme_selection(&mut self, theme_name: &str) {
        match self.config.apply_theme_by_name(theme_name) {
            Ok(()) => {
                self.theme_modal = None;
                self.notice_state = Some(NoticeState {
                    message: format!("Theme switched to {theme_name} for this session."),
                    created_at: Instant::now(),
                });
            }
            Err(error) => {
                self.theme_modal = None;
                self.error_state = Some(ErrorState {
                    title: "Theme Change Failed".to_string(),
                    details: format!("{error:#}"),
                });
            }
        }
    }

    fn handle_explain_analyze(&mut self, detail: &QueryDetailState) {
        let dsn = self.dsn.clone();
        let timeout = self.connect_timeout_ms;
        let database = detail.database.clone();
        let query = detail.query.clone();

        self.query_detail = None;
        self.loading_state = Some(LoadingState {
            message: "Running EXPLAIN ANALYZE...".to_string(),
            started_at: Instant::now(),
        });

        self.start_explain_request(dsn, timeout, database, query);
    }

    fn selected_activity_detail(&self) -> Option<QueryDetailState> {
        let session = self
            .selected_row
            .and_then(|selected_row| self.dashboard.sessions.get(selected_row))?;
        Some(QueryDetailState {
            query: session.query.clone(),
            database: session.database.clone(),
            source: QueryDetailSource::Activity,
            stats: None,
        })
    }

    fn selected_statement_detail(&self) -> Option<QueryDetailState> {
        let row = self
            .selected_row
            .and_then(|selected_row| self.data.get(selected_row))?;

        let db = row.first()?.clone();
        let query = row.get(1)?.clone();

        Some(QueryDetailState {
            query,
            database: db,
            source: QueryDetailSource::Statements,
            stats: Some(QueryStats {
                total_time: row.get(2)?.clone(),
                mean_time: row.get(3)?.clone(),
                calls: row.get(4)?.clone(),
                read_time: row.get(5)?.clone(),
                write_time: row.get(6)?.clone(),
            }),
        })
    }

    fn selected_query(&self) -> Option<&str> {
        if let Some(detail) = self.query_detail.as_ref() {
            return Some(&detail.query);
        }

        match self.current_tab {
            Tab::Activity => self
                .selected_row
                .and_then(|selected_row| self.dashboard.sessions.get(selected_row))
                .map(|session| session.query.as_str()),
            Tab::Statements => self
                .selected_row
                .and_then(|selected_row| self.data.get(selected_row))
                .and_then(|row| row.get(1))
                .map(String::as_str),
            _ => None,
        }
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

    fn can_export_current_view(&self) -> bool {
        matches!(self.current_tab, Tab::Activity | Tab::Statements)
    }

    fn export_current_table(&mut self) {
        let tab = self.current_tab;
        if !self.can_export_current_view() {
            return;
        }
        let format = self.config.ui.default_export_format;

        let headers = table_header_cells(tab, &self.database_view);
        let data = if tab == Tab::Activity {
            self.dashboard
                .sessions
                .iter()
                .map(|s| {
                    vec![
                        s.pid.clone(),
                        s.xmin.clone(),
                        s.database.clone(),
                        s.application.clone(),
                        s.user.clone(),
                        s.client.clone(),
                        format_duration_hms(s.duration_seconds),
                        activity_wait_cell(s),
                        activity_state_cell(s),
                        format_activity_query(s),
                    ]
                })
                .collect()
        } else if tab == Tab::Replication {
            self.replication.senders.iter().map(sender_to_row).collect()
        } else {
            self.data.clone()
        };

        if data.is_empty() {
            return;
        }

        match self.save_export(&headers, &data, format) {
            Ok(path) => {
                self.notice_state = Some(NoticeState {
                    message: format!("Exported {} rows to {}", data.len(), path.display()),
                    created_at: Instant::now(),
                });
            }
            Err(e) => {
                self.error_state = Some(ErrorState {
                    title: "Export Failed".to_string(),
                    details: format!("{e:#}"),
                });
            }
        }
    }

    fn save_export(
        &self,
        headers: &[&str],
        data: &[Vec<String>],
        format: ExportFormat,
    ) -> Result<PathBuf> {
        let path = export_output_path(self.current_tab, &self.query_output_dir, format)?;

        match format {
            ExportFormat::Csv => {
                let mut wtr = csv::Writer::from_path(&path)?;
                wtr.write_record(headers)?;
                for row in data {
                    wtr.write_record(row)?;
                }
                wtr.flush()?;
            }
            ExportFormat::Json => {
                let mut json_data = Vec::new();
                for row in data {
                    let mut map = serde_json::Map::new();
                    for (i, header) in headers.iter().enumerate() {
                        let value = row.get(i).cloned().unwrap_or_default();
                        map.insert((*header).to_string(), serde_json::Value::String(value));
                    }
                    json_data.push(serde_json::Value::Object(map));
                }
                let file = fs::File::create(&path)?;
                serde_json::to_writer_pretty(file, &json_data)?;
            }
        }

        Ok(path)
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
        let Some(pending_kind) = self.pending_request.as_ref().map(|pending| pending.kind) else {
            return;
        };
        let request_started_at = self
            .pending_request
            .as_ref()
            .map_or_else(Instant::now, |pending| pending.started_at);

        match self
            .pending_request
            .as_ref()
            .map(|pending| pending.rx.try_recv())
        {
            Some(Ok(result)) => {
                self.pending_request = None;
                self.loading_state = None;
                match result {
                    Ok(payload) => {
                        let completed_at = Instant::now();
                        self.last_refresh = completed_at;
                        if pending_kind == PendingRequestKind::Refresh {
                            self.connection_health.last_refresh_at = Some(completed_at);
                            self.connection_health.last_refresh_duration =
                                Some(completed_at.saturating_duration_since(request_started_at));
                        }
                        self.error_state = None;
                        self.clear_offline_state();
                        self.apply_refresh_payload(payload);
                        self.has_loaded_once = true;
                    }
                    Err(error) => {
                        self.handle_request_failure(pending_kind, format!("{error:#}"));
                    }
                }
            }
            Some(Err(mpsc::TryRecvError::Empty)) | None => {}
            Some(Err(mpsc::TryRecvError::Disconnected)) => {
                self.pending_request = None;
                self.loading_state = None;
                self.handle_request_failure(
                    pending_kind,
                    "Background refresh worker disconnected".to_string(),
                );
            }
        }
    }

    fn handle_request_failure(&mut self, kind: PendingRequestKind, details: String) {
        if kind == PendingRequestKind::Refresh {
            let summary = summarize_error(&details);
            self.connection_health.last_error = Some(summary.clone());
            self.connection_health.last_error_at = Some(Instant::now());
            self.pg_client = None;
            if self.has_loaded_once {
                let failed_attempts = self
                    .offline_state()
                    .map_or(1, |state| state.failed_attempts.saturating_add(1));

                self.connection_status = ConnectionStatus::Offline(OfflineState {
                    last_error: summary,
                    failed_attempts,
                    next_retry_at: Instant::now() + reconnect_delay(failed_attempts),
                    last_successful_refresh_at: Some(self.last_refresh),
                });
                return;
            }
        }

        self.error_state = Some(ErrorState {
            title: request_error_title(kind).to_string(),
            details,
        });
    }

    pub(crate) fn is_offline(&self) -> bool {
        matches!(self.connection_status, ConnectionStatus::Offline(_))
    }

    pub(crate) fn offline_state(&self) -> Option<&OfflineState> {
        match &self.connection_status {
            ConnectionStatus::Offline(state) => Some(state),
            ConnectionStatus::Online => None,
        }
    }

    pub(crate) fn is_reconnecting(&self) -> bool {
        self.is_offline()
            && self
                .pending_request
                .as_ref()
                .is_some_and(|pending| pending.kind == PendingRequestKind::Refresh)
    }

    pub(crate) fn can_manual_reconnect(&self) -> bool {
        self.is_offline() && self.pending_request.is_none() && self.input_mode == InputMode::Normal
    }

    pub(crate) fn has_theme_options(&self) -> bool {
        self.config.has_themes()
    }

    pub(crate) fn active_theme_name(&self) -> Option<&str> {
        self.config.active_theme_name()
    }

    pub(crate) fn offline_retry_remaining(&self) -> Option<Duration> {
        self.offline_state().map(|state| {
            state
                .next_retry_at
                .saturating_duration_since(Instant::now())
        })
    }

    pub(crate) fn offline_last_success_elapsed(&self) -> Option<Duration> {
        self.offline_state().and_then(|state| {
            state
                .last_successful_refresh_at
                .map(|instant| instant.elapsed())
        })
    }

    pub(crate) fn offline_error_summary(&self) -> Option<&str> {
        self.offline_state().map(|state| state.last_error.as_str())
    }

    pub(crate) fn current_view_capability(&self) -> Option<&CapabilityStatus> {
        match self.current_tab {
            Tab::Statements => Some(&self.capabilities.statements),
            Tab::IO => Some(&self.capabilities.io),
            Tab::Replication => Some(&self.capabilities.replication),
            _ => None,
        }
    }

    pub(crate) fn connection_target(&self) -> &str {
        &self.connection_health.target
    }

    pub(crate) fn connection_last_refresh_duration(&self) -> Option<Duration> {
        self.connection_health.last_refresh_duration
    }

    pub(crate) fn connection_last_refresh_elapsed(&self) -> Option<Duration> {
        self.connection_health
            .last_refresh_at
            .map(|instant| instant.elapsed())
    }

    pub(crate) fn high_latency_detected(&self) -> bool {
        self.connection_last_refresh_duration()
            .is_some_and(|duration| duration > Duration::from_millis(self.refresh_ms))
    }

    fn clear_offline_state(&mut self) {
        self.connection_status = ConnectionStatus::Online;
    }

    fn request_refresh_worker(
        &mut self,
        kind: PendingRequestKind,
        worker: impl FnOnce(mpsc::Sender<Result<RefreshPayload>>) + Send + 'static,
    ) {
        let (tx, rx) = mpsc::channel();
        self.pending_request = Some(PendingRequest {
            kind,
            rx,
            started_at: Instant::now(),
        });
        thread::spawn(move || worker(tx));
    }

    fn start_explain_request(
        &mut self,
        dsn: String,
        timeout: u64,
        database: String,
        query: String,
    ) {
        self.request_refresh_worker(PendingRequestKind::Explain, move |tx| {
            let result = (|| -> Result<RefreshPayload> {
                let mut client = if database.is_empty() {
                    PgClient::new(&dsn, timeout)?
                } else {
                    PgClient::for_database(&dsn, timeout, &database)?
                };
                let plan = client.fetch_explain_plan(&query).context(
                    "Explain Analyze failed. Note: normalized queries (with $1, $2) cannot be analyzed without actual values."
                )?;
                Ok(RefreshPayload::Explain(plan))
            })();
            let _ = tx.send(result);
        });
    }

    fn start_admin_action_request(&mut self, dsn: String, timeout: u64, query: String) {
        self.request_refresh_worker(PendingRequestKind::AdminAction, move |tx| {
            let result = (|| -> Result<RefreshPayload> {
                let mut client = PgClient::new(&dsn, timeout)?;
                client.execute_admin_action(&query)?;
                Ok(RefreshPayload::Table(Vec::new(), client))
            })();
            let _ = tx.send(result);
        });
    }

    fn start_table_definition_request(
        &mut self,
        dsn: String,
        timeout: u64,
        schema: String,
        table: String,
    ) {
        self.request_refresh_worker(PendingRequestKind::TableDefinition, move |tx| {
            let result = (|| -> Result<RefreshPayload> {
                let mut client = PgClient::new(&dsn, timeout)?;
                let (columns, indexes) = client.fetch_table_definition(&schema, &table)?;
                Ok(RefreshPayload::TableDefinition(
                    schema, table, columns, indexes,
                ))
            })();
            let _ = tx.send(result);
        });
    }

    fn apply_refresh_payload(&mut self, payload: RefreshPayload) {
        match payload {
            RefreshPayload::Activity(activity, client) => {
                self.sync_capabilities_from_client(&client);
                self.pg_client = Some(client);
                self.apply_activity_snapshot(*activity);
            }
            RefreshPayload::Replication(replication, client) => {
                self.sync_capabilities_from_client(&client);
                self.pg_client = Some(client);
                self.apply_replication_snapshot(*replication);
            }
            RefreshPayload::Table(data, client) => {
                self.sync_capabilities_from_client(&client);
                self.pg_client = Some(client);
                self.data = self.prepare_table_data(data);
            }
            RefreshPayload::Explain(plan) => {
                self.explain_plan = Some(ExplainPlanState { plan });
            }
            RefreshPayload::TableDefinition(schema, name, columns, indexes) => {
                self.table_definition = Some(TableDefinitionState {
                    schema,
                    name,
                    columns,
                    indexes,
                });
            }
        }

        let len = self.current_row_count();
        self.selected_row = clamp_selected_row(self.selected_row, len);
        self.table_state.select(self.selected_row);

        if self.current_tab == Tab::Statements && self.query_detail.is_some() {
            self.query_detail = self.selected_statement_detail();
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
        self.push_activity_chart_history();
        self.data.clear();
    }

    fn apply_replication_snapshot(&mut self, snapshot: ReplicationSnapshot) {
        self.capabilities.replication = snapshot.capability.clone();
        self.replication = snapshot;
        self.selected_row = None;
        self.data.clear();
    }

    fn sync_capabilities_from_client(&mut self, client: &PgClient) {
        merge_capability_status(&mut self.capabilities.io, client.io_capability());
        merge_capability_status(
            &mut self.capabilities.statements,
            client.statements_capability(),
        );
        merge_capability_status(
            &mut self.capabilities.replication,
            client.replication_capability(),
        );
    }

    fn effective_refresh_interval(&self) -> Duration {
        let base = Duration::from_millis(self.refresh_ms);
        let Some(last_duration) = self.connection_last_refresh_duration() else {
            return base;
        };
        if last_duration <= base {
            return base;
        }

        let delay_ms = last_duration.as_millis().min(30_000);
        Duration::from_millis(u64::try_from(delay_ms).unwrap_or(30_000))
    }

    fn apply_activity_session_view(&mut self) {
        let mut filtered = filter_activity_sessions(&self.activity_sessions, self.activity_subview);

        if !self.search_query.is_empty() {
            filtered.retain(|s| {
                is_fuzzy_match(&s.pid, &self.search_query)
                    || is_fuzzy_match(&s.database, &self.search_query)
                    || is_fuzzy_match(&s.application, &self.search_query)
                    || is_fuzzy_match(&s.user, &self.search_query)
                    || is_fuzzy_match(&s.client, &self.search_query)
                    || is_fuzzy_match(&s.state, &self.search_query)
                    || is_fuzzy_match(&format_activity_query(s), &self.search_query)
            });
        }

        let mut sorted = sorted_activity_sessions(filtered, self.activity_subview);
        if self.top_n > 0 {
            sorted = limit_activity_sessions(sorted, self.top_n);
        }
        self.dashboard.sessions = sorted;
        self.selected_row = clamp_selected_row(self.selected_row, self.dashboard.sessions.len());
    }

    fn push_activity_chart_history(&mut self) {
        let session_counts = &self.dashboard.summary.session_counts;
        let history = &mut self.dashboard.chart_history;
        if history.connections.len() >= CONN_HISTORY_SIZE {
            let _ = history.connections.pop_front();
            let _ = history.tps.pop_front();
            let _ = history.inserts_per_sec.pop_front();
            let _ = history.updates_per_sec.pop_front();
            let _ = history.deletes_per_sec.pop_front();
            let _ = history.temp_bytes_per_sec.pop_front();
            let _ = history.growth_bytes_per_sec.pop_front();
        }
        history.connections.push_back((
            session_counts.active,
            session_counts.idle,
            session_counts.total,
        ));
        history
            .tps
            .push_back(parse_rate_value(&self.dashboard.summary.rates.tps));
        history.inserts_per_sec.push_back(parse_rate_value(
            &self.dashboard.summary.rates.inserts_per_sec,
        ));
        history.updates_per_sec.push_back(parse_rate_value(
            &self.dashboard.summary.rates.updates_per_sec,
        ));
        history.deletes_per_sec.push_back(parse_rate_value(
            &self.dashboard.summary.rates.deletes_per_sec,
        ));
        history.temp_bytes_per_sec.push_back(parse_rate_value(
            &self.dashboard.summary.rates.temp_bytes_per_sec,
        ));
        history.growth_bytes_per_sec.push_back(parse_rate_value(
            &self.dashboard.summary.rates.growth_bytes_per_sec,
        ));
    }

    fn cycle_activity_chart_metric(&mut self) {
        self.activity_chart_metric = self.activity_chart_metric.next();
    }

    fn prompt_tool_action(&mut self) {
        let Some(selected_row) = self.selected_row else {
            return;
        };
        let Some(row) = self.data.get(selected_row) else {
            return;
        };

        let title = row.first().cloned().unwrap_or_default();
        let description = row.get(1).cloned().unwrap_or_default();
        let query = row.get(2).cloned().unwrap_or_default();

        self.confirm_action = Some(ConfirmActionState {
            title,
            description,
            query,
        });
    }

    fn prompt_vacuum_analyze(&mut self) {
        if let Some(row) = self.selected_row.and_then(|idx| self.data.get(idx)) {
            let schema = row.get(4).cloned().unwrap_or_default();
            let table = row.get(5).cloned().unwrap_or_default();
            let depth = row.get(6).and_then(|d| d.parse::<i32>().ok()).unwrap_or(0);

            if depth == 1 {
                let query = format!("VACUUM ANALYZE \"{schema}\".\"{table}\"");
                self.confirm_action = Some(ConfirmActionState {
                    title: "Vacuum Analyze".to_string(),
                    description: format!("Run VACUUM ANALYZE on {schema}.{table}?"),
                    query,
                });
            }
        }
    }

    fn prompt_reindex(&mut self) {
        if let Some(row) = self.selected_row.and_then(|idx| self.data.get(idx)) {
            let schema = row.get(4).cloned().unwrap_or_default();
            let table = row.get(5).cloned().unwrap_or_default();
            let depth = row.get(6).and_then(|d| d.parse::<i32>().ok()).unwrap_or(0);

            if depth == 1 {
                let query = format!("REINDEX TABLE \"{schema}\".\"{table}\"");
                self.confirm_action = Some(ConfirmActionState {
                    title: "Reindex Table".to_string(),
                    description: format!("Run REINDEX TABLE on {schema}.{table}?"),
                    query,
                });
            }
        }
    }

    fn generate_deadlock_graph(&mut self) {
        if self.current_tab != Tab::Locks || self.data.is_empty() {
            return;
        }

        let mut blocking_map: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut pid_info: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        for row in &self.data {
            let blocking = row.first().cloned().unwrap_or_default();
            let blocked = row.get(1).cloned().unwrap_or_default();
            let target = row.get(3).cloned().unwrap_or_default();
            let mode = row.get(4).cloned().unwrap_or_default();

            if !blocking.is_empty() {
                blocking_map
                    .entry(blocking.clone())
                    .or_default()
                    .push(blocked.clone());
                pid_info.insert(
                    blocked.clone(),
                    format!("waiting for {blocking} on {target} ({mode})"),
                );
            }
        }

        if blocking_map.is_empty() {
            self.notice_state = Some(NoticeState {
                message: "No active blocking relationships found.".to_string(),
                created_at: Instant::now(),
            });
            return;
        }

        let mut graph = Vec::new();
        graph.push("Dependency Graph (Who is blocking whom):".to_string());
        graph.push(String::new());

        let all_blocked: std::collections::HashSet<_> = pid_info.keys().cloned().collect();
        let mut roots: Vec<_> = blocking_map
            .keys()
            .filter(|p| !all_blocked.contains(*p))
            .cloned()
            .collect();
        roots.sort();

        if roots.is_empty() {
            graph.push("WARNING: Potential DEADLOCK detected (circular dependencies)!".to_string());
            let mut pids: Vec<_> = blocking_map.keys().cloned().collect();
            pids.sort();
            for pid in pids {
                render_dependency_tree(
                    &mut graph,
                    &blocking_map,
                    &pid_info,
                    &pid,
                    0,
                    &mut std::collections::HashSet::new(),
                );
            }
        } else {
            for root in roots {
                render_dependency_tree(
                    &mut graph,
                    &blocking_map,
                    &pid_info,
                    &root,
                    0,
                    &mut std::collections::HashSet::new(),
                );
            }
        }

        self.deadlock_graph = Some(DeadlockGraphState { graph });
    }

    fn execute_confirmed_action(&mut self, query: &str) {
        let dsn = self.dsn.clone();
        let timeout = self.connect_timeout_ms;
        let query = query.to_string();

        self.loading_state = Some(LoadingState {
            message: "Executing administrative action...".to_string(),
            started_at: Instant::now(),
        });

        self.start_admin_action_request(dsn, timeout, query);
    }

    fn handle_show_table_definition(&mut self) {
        if let Some(row) = self.selected_row.and_then(|idx| self.data.get(idx)) {
            let schema = row.get(4).cloned().unwrap_or_default();
            let table = row.get(5).cloned().unwrap_or_default();
            let depth = row.get(6).and_then(|d| d.parse::<i32>().ok()).unwrap_or(0);

            if depth == 1 {
                let dsn = self.dsn.clone();
                let timeout = self.connect_timeout_ms;

                self.loading_state = Some(LoadingState {
                    message: format!("Fetching definition for {schema}.{table}..."),
                    started_at: Instant::now(),
                });

                self.start_table_definition_request(dsn, timeout, schema, table);
            }
        }
    }
}

fn reconnect_delay(failed_attempts: u32) -> Duration {
    let index = failed_attempts.saturating_sub(1) as usize;
    RECONNECT_BACKOFF_STEPS
        .get(index)
        .copied()
        .unwrap_or(Duration::from_secs(30))
}

fn request_error_title(_kind: PendingRequestKind) -> &'static str {
    "Action Failed"
}

fn summarize_error(details: &str) -> String {
    details.lines().next().unwrap_or(details).trim().to_string()
}

fn parse_rate_value(value: &str) -> f64 {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return 0.0;
    }

    trimmed.parse::<f64>().unwrap_or(0.0)
}

fn merge_capability_status(current: &mut CapabilityStatus, incoming: CapabilityStatus) {
    if !matches!(incoming, CapabilityStatus::Unknown) {
        *current = incoming;
    }
}

fn clamp_selected_row(selected_row: Option<usize>, len: usize) -> Option<usize> {
    if len == 0 {
        None
    } else {
        selected_row.map(|selected_row| selected_row.min(len - 1))
    }
}

fn render_dependency_tree(
    graph: &mut Vec<String>,
    map: &std::collections::HashMap<String, Vec<String>>,
    info: &std::collections::HashMap<String, String>,
    pid: &str,
    depth: usize,
    visited: &mut std::collections::HashSet<String>,
) {
    let indent = "  ".repeat(depth);
    let prefix = if depth == 0 { "" } else { "└─ " };
    let node_info = info
        .get(pid)
        .cloned()
        .unwrap_or_else(|| "active (blocking others)".to_string());

    graph.push(format!(
        "{}{}{} PID {}: {}",
        indent,
        prefix,
        if depth == 0 { "BLOCKER" } else { "BLOCKED" },
        pid,
        node_info
    ));

    if visited.contains(pid) {
        graph.push(format!("  {indent}└─ !!! CIRCULAR DEPENDENCY DETECTED !!!"));
        return;
    }

    visited.insert(pid.to_string());

    if let Some(blocked_pids) = map.get(pid) {
        let mut sorted_blocked = blocked_pids.clone();
        sorted_blocked.sort();
        for next_pid in sorted_blocked {
            render_dependency_tree(graph, map, info, &next_pid, depth + 1, visited);
        }
    }

    visited.remove(pid);
}

pub fn save_query_to_directory(tab: Tab, query: &str, directory: &Path) -> Result<PathBuf> {
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
        Tab::Replication => "replication",
        Tab::Settings => "settings",
        Tab::Tools => "tools",
    };

    Ok(directory.join(format!("pgmon-query-{tab_name}-{timestamp}.sql")))
}

fn export_output_path(tab: Tab, directory: &Path, format: ExportFormat) -> Result<PathBuf> {
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
        Tab::Replication => "replication",
        Tab::Settings => "settings",
        Tab::Tools => "tools",
    };

    let extension = match format {
        ExportFormat::Csv => "csv",
        ExportFormat::Json => "json",
    };

    Ok(directory.join(format!("pgmon-export-{tab_name}-{timestamp}.{extension}")))
}

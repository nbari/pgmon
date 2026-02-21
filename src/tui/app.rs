use crate::pg::client::PgClient;
use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::{
    collections::VecDeque,
    io,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const CONN_HISTORY_SIZE: usize = 600;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Tab {
    Activity,
    Database,
    Locks,
    IO,
    Statements,
}

#[derive(Clone)]
pub struct DashboardStats {
    pub conn_by_state: Vec<(String, i64)>,
    pub active_queries: Vec<Vec<String>>,
    pub cache_hit_pct: f64,
    pub total_commits: i64,
    pub total_rollbacks: i64,
    pub total_backends: i64,
    pub max_connections: i64,
    /// Ring buffer: `(active, idle, total)` per refresh tick
    pub conn_history: VecDeque<(i64, i64, i64)>,
}

impl Default for DashboardStats {
    fn default() -> Self {
        Self {
            conn_by_state: Vec::new(),
            active_queries: Vec::new(),
            cache_hit_pct: 0.0,
            total_commits: 0,
            total_rollbacks: 0,
            total_backends: 0,
            max_connections: 0,
            conn_history: VecDeque::new(),
        }
    }
}

pub struct App {
    pub dsn: String,
    pub refresh_ms: u64,
    pub top_n: u32,
    pub current_tab: Tab,
    pub data: Vec<Vec<String>>,
    pub dashboard: DashboardStats,
    pub should_quit: bool,
    pub last_refresh: Instant,
    pub selected_row: usize,
}

impl App {
    pub fn new(dsn: String, refresh_ms: u64, top_n: u32, home_view: &str, _sort: &str) -> Self {
        let current_tab = match home_view {
            "statements" => Tab::Statements,
            _ => Tab::Activity,
        };

        Self {
            dsn,
            refresh_ms,
            top_n,
            current_tab,
            data: Vec::new(),
            dashboard: DashboardStats::default(),
            should_quit: false,
            last_refresh: Instant::now(),
            selected_row: 0,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;

        let (tx, _rx) = mpsc::channel();
        let dsn = self.dsn.clone();

        // Polling thread
        thread::spawn(move || {
            let _client = match PgClient::new(&dsn) {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };

            loop {
                thread::sleep(Duration::from_millis(100));
                let _ = tx.send(Ok(Tab::Activity));
            }
        });

        self.refresh_data()?;

        while !self.should_quit {
            terminal.draw(|f| crate::tui::ui::draw(f, self))?;

            if let Ok(true) = event::poll(Duration::from_millis(50))
                && let Event::Key(key) = event::read()?
            {
                match key.code {
                    KeyCode::Char('q') => self.should_quit = true,
                    KeyCode::Char('1') => self.set_tab(Tab::Activity),
                    KeyCode::Char('2') => self.set_tab(Tab::Database),
                    KeyCode::Char('3') => self.set_tab(Tab::Locks),
                    KeyCode::Char('4') => self.set_tab(Tab::IO),
                    KeyCode::Char('5') => self.set_tab(Tab::Statements),
                    KeyCode::Down => {
                        let len = if self.current_tab == Tab::Activity {
                            self.dashboard.active_queries.len()
                        } else {
                            self.data.len()
                        };
                        if len > 0 && self.selected_row < len - 1 {
                            self.selected_row += 1;
                        }
                    }
                    KeyCode::Up => {
                        if self.selected_row > 0 {
                            self.selected_row -= 1;
                        }
                    }
                    _ => {}
                }
            }

            if self.last_refresh.elapsed() >= Duration::from_millis(self.refresh_ms) {
                self.refresh_data()?;
                self.last_refresh = Instant::now();
            }
        }

        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        Ok(())
    }

    fn set_tab(&mut self, tab: Tab) {
        self.current_tab = tab;
        self.selected_row = 0;
        let _ = self.refresh_data();
    }

    fn refresh_data(&mut self) -> Result<()> {
        let mut client = PgClient::new(&self.dsn)?;
        match self.current_tab {
            Tab::Activity => {
                self.dashboard.conn_by_state = client.fetch_conn_stats()?;
                self.dashboard.active_queries = client.fetch_active_queries()?;
                let (cache_hit, commits, rollbacks, backends, max_conn) =
                    client.fetch_perf_stats()?;
                self.dashboard.cache_hit_pct = cache_hit;
                self.dashboard.total_commits = commits;
                self.dashboard.total_rollbacks = rollbacks;
                self.dashboard.total_backends = backends;
                self.dashboard.max_connections = max_conn;

                let active_count = self
                    .dashboard
                    .conn_by_state
                    .iter()
                    .find(|(s, _)| s == "active")
                    .map_or(0, |(_, c)| *c);
                let idle_count = self
                    .dashboard
                    .conn_by_state
                    .iter()
                    .find(|(s, _)| s == "idle")
                    .map_or(0, |(_, c)| *c);
                let total_count: i64 = self.dashboard.conn_by_state.iter().map(|(_, c)| c).sum();
                if self.dashboard.conn_history.len() >= CONN_HISTORY_SIZE {
                    self.dashboard.conn_history.pop_front();
                }
                self.dashboard
                    .conn_history
                    .push_back((active_count, idle_count, total_count));

                self.data = Vec::new();
            }
            Tab::Database => self.data = client.fetch_database_stats()?,
            Tab::Locks => self.data = client.fetch_locks()?,
            Tab::IO => self.data = client.fetch_io_stats()?,
            Tab::Statements => self.data = client.fetch_statements()?,
        }
        Ok(())
    }
}

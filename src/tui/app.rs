use crate::pg::client::PgClient;
use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{
    io,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Tab {
    Activity,
    Database,
    Locks,
    IO,
    Statements,
}

pub struct App {
    pub dsn: String,
    pub refresh_ms: u64,
    pub top_n: u32,
    pub current_tab: Tab,
    pub data: Vec<Vec<String>>,
    pub should_quit: bool,
    pub last_refresh: Instant,
    pub selected_row: usize,
}

impl App {
    pub fn new(
        dsn: String,
        refresh_ms: u64,
        top_n: u32,
        home_view: String,
        _sort: String,
    ) -> Result<Self> {
        let current_tab = match home_view.as_str() {
            "statements" => Tab::Statements,
            _ => Tab::Activity,
        };

        Ok(Self {
            dsn,
            refresh_ms,
            top_n,
            current_tab,
            data: Vec::new(),
            should_quit: false,
            last_refresh: Instant::now(),
            selected_row: 0,
        })
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

            if let Ok(true) = event::poll(Duration::from_millis(50)) {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') => self.should_quit = true,
                        KeyCode::Char('1') => self.set_tab(Tab::Activity),
                        KeyCode::Char('2') => self.set_tab(Tab::Database),
                        KeyCode::Char('3') => self.set_tab(Tab::Locks),
                        KeyCode::Char('4') => self.set_tab(Tab::IO),
                        KeyCode::Char('5') => self.set_tab(Tab::Statements),
                        KeyCode::Down => {
                            if !self.data.is_empty() && self.selected_row < self.data.len() - 1 {
                                self.selected_row += 1;
                            }
                        }
                        KeyCode::Up => {
                            if self.selected_row > 0 {
                                self.selected_row -= 1;
                            }
                        }
                        KeyCode::Char('/') => {
                            // Basic filter mode could be added here
                        }
                        _ => {}
                    }
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
        self.data = match self.current_tab {
            Tab::Activity => client.fetch_activity()?,
            Tab::Database => client.fetch_database_stats()?,
            Tab::Locks => client.fetch_locks()?,
            Tab::IO => client.fetch_io_stats()?,
            Tab::Statements => client.fetch_statements()?,
        };
        Ok(())
    }
}

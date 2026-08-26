mod cli;
mod config;
mod pg;
mod themes;
mod tls;
mod tui;

fn main() -> anyhow::Result<()> {
    cli::start::run()
}

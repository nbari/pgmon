mod cli;
mod pg;
mod tui;

fn main() -> anyhow::Result<()> {
    cli::start::run()
}

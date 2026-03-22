# Contributing to `pgmon`

Thanks for contributing to `pgmon`.

This project is a small Rust CLI/TUI for PostgreSQL monitoring. The goal for
contributions is to keep changes easy to review, safe to ship, and aligned with
the existing operator-focused workflow.

If you are contributing with an AI coding agent, read
[`AGENTS.md`](./AGENTS.md) first. That file contains repository-specific rules
that AI contributors are expected to follow strictly.

## Before You Start

- Keep diffs focused. Avoid unrelated refactors, file moves, or broad cleanup.
- Do not weaken validation, connection handling, or safety-related behavior.
- Do not commit real DSNs, passwords, or local environment details.
- Prefer using `PGMON_DSN`, `.pgpass`, or a local `pgmon.yaml` that is not
  committed with secrets.

## Quick Start

If you want the shortest path to a local contribution cycle:

```bash
cargo build
cargo test --all-features
cargo run -- --config ./pgmon.yaml
```

Then make your change and finish with:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

## Development Setup

Common commands:

```bash
cargo build
cargo build --release
cargo run -- --dsn "postgresql://user:pass@localhost:5432/postgres"
cargo test
cargo fmt --check
cargo clippy --all-targets --all-features
```

Useful local examples:

```bash
pgmon check-config --config ./pgmon.yaml
cargo run -- --config ./pgmon.yaml
```

## Coding Guidelines

- Follow standard Rust 2024 style and `rustfmt`.
- Use:
  - `snake_case` for functions, modules, and variables
  - `PascalCase` for structs and enums
  - `UPPER_SNAKE_CASE` for SQL constants
- Document production code, especially public modules, types, functions, and
  non-obvious logic.
- Keep functions small and behavior-specific where practical.
- Propagate errors with `?` instead of panicking.
- Do not add `unwrap`, `expect`, or `panic!` in production code.
- Do not add `#[allow(...)]` in production code. Keep lint exceptions limited
  to tests when truly needed.

## Tests

Tests are currently inline with the code under `#[cfg(test)]`.

When changing behavior, add or update tests close to the affected code. Prefer
coverage for:

- CLI parsing and validation
- config loading and precedence
- data shaping and formatting helpers
- TUI state transitions and regression-prone interactions

Use descriptive test names with the existing `test_*` pattern.

## Required Verification

Before opening a PR, run:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

If your change affects docs only, note that in the PR.

## Pull Requests

Please include:

- a short summary of the behavior change
- verification notes
- linked issues when applicable
- screenshots or terminal captures for TUI/UI changes

Prefer short, imperative commit messages focused on one change, for example:

```text
fix refresh interval regression
```

## Project Layout

- `src/main.rs`: entry point
- `src/cli/`: CLI parsing, dispatch, and command actions
- `src/pg/`: PostgreSQL client logic and SQL queries
- `src/tui/`: app state and Ratatui rendering
- `README.md`: usage, config, and feature documentation

## Security Notes

- Never commit credentials or production connection strings.
- Keep password-safe output behavior intact.
- If a change affects connection resolution, config loading, or displayed query
  content, review it carefully for accidental leakage or behavior regressions.

export PGMON_DSN := "postgresql://postgres:postgres@localhost:5432/postgres"

default: test
  @just --list

test: clippy fmt
  cargo test --all-features

clippy:
    cargo clippy --all-targets --all-features

fmt:
    cargo fmt --all --check

# Start a PostgreSQL 18 container (includes pg_stat_io)
up:
    podman run --name pgmon-test -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18 postgres -c 'shared_preload_libraries=pg_stat_statements'
    @echo "Waiting for Postgres to start..."
    @sleep 5
    podman exec pgmon-test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
    @echo "Postgres is ready at {{PGMON_DSN}}"

# Stop and remove the PostgreSQL container
down:
    podman rm -f pgmon-test

# Show logs from the Postgres container
logs:
    podman logs -f pgmon-test

# Connect to the local Postgres using psql
psql:
    podman exec -it pgmon-test psql -U postgres

# Run pgmon against the local Postgres
run:
    cargo run -- --dsn "{{PGMON_DSN}}"

# Run pgmon with pg_stat_statements view
run-statements:
    cargo run -- --dsn "{{PGMON_DSN}}" --home-view statements

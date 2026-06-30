#!/usr/bin/env bash
set -uo pipefail

# Runs on every container start (DevPod postStartCommand). Best-effort: it must not
# fail `devpod up`. Re-applies optional git identity/signing and waits for the
# postgres sibling to be ready so `just test` / `cargo run` work right away.

export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$PATH"
cd /workspaces/pgmon 2>/dev/null || exit 0

# Re-apply optional git identity/signing on every start so updates to forwarded
# DevPod workspace env are reflected without rebuilding the container.
sh .devcontainer/configure-git.sh || true

PG_HOST="postgres"
PG_PORT="5432"

# Wait for PostgreSQL (compose healthcheck usually has it ready already).
for _ in $(seq 1 30); do
    if PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d postgres \
        -c "SELECT 1" >/dev/null 2>&1; then
        echo "✓ Workspace ready. PostgreSQL is up at ${PG_HOST}:${PG_PORT}."
        echo "  PGMON_DSN=${PGMON_DSN:-postgresql://postgres:postgres@postgres:5432/postgres}"
        echo "  Run: just test   # or: cargo run"
        exit 0
    fi
    sleep 1
done

echo "post-start: PostgreSQL not reachable yet at ${PG_HOST}:${PG_PORT} (continuing)." >&2
exit 0

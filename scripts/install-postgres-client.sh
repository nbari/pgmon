#!/usr/bin/env bash
set -euo pipefail

# Install the PostgreSQL client (psql) matching the server version used by the
# devcontainer. pgmon only needs `psql` for manual inspection — the application
# itself talks to PostgreSQL over the wire via sqlx and needs no client binaries.
#
# We add the PGDG apt repo (to match the postgres:18 service in
# .devcontainer/compose.yaml) and install just the client package. Installing the
# matching client avoids version-skew warnings against a newer server.
#
# PG_MAJOR can override the version (defaults to 18 to match compose.yaml).

PG_MAJOR="${PG_MAJOR:-18}"

# Already have psql? Nothing to do (idempotent).
if command -v psql >/dev/null 2>&1 && psql --version >/dev/null 2>&1; then
    echo "psql already installed: $(psql --version)"
    exit 0
fi

sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release >/dev/null

# Add the PostgreSQL APT (PGDG) repository if it is not already configured.
if [ ! -f /etc/apt/sources.list.d/pgdg.list ]; then
    sudo install -d /usr/share/postgresql-common/pgdg
    sudo curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
    codename="$(. /etc/os-release && echo "${VERSION_CODENAME}")"
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${codename}-pgdg main" |
        sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
    sudo apt-get update -qq
fi

# psql + libpq from the client package only (no server / pgbench needed).
sudo apt-get install -y -qq "postgresql-client-${PG_MAJOR}" >/dev/null

echo "✓ Installed: $(psql --version)"

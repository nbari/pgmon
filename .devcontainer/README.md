# pgmon devcontainer

A **compose-based** dev environment for pgmon. A single compose project brings up
two containers together:

- **`app`** — the dev container (`mcr.microsoft.com/devcontainers/rust:trixie`)
  where you build, test, and run pgmon.
- **`postgres`** — a throwaway PostgreSQL 18 instance, **always up**, that pgmon and
  the tests connect to.

The app reaches the database container-to-container at `postgres:5432`. `PGMON_DSN`
and `PGMON_TEST_DSN` are pre-set to
`postgresql://postgres:postgres@postgres:5432/postgres`, so `just test` and
`cargo run` work with no extra setup. Port `5432` is also forwarded to the host for
ad-hoc `psql`/GUI access.

> **Trust boundary:** local/dev only. PostgreSQL uses `trust` auth and the only
> credentials are the disposable `postgres:postgres` dev defaults. Never reuse this
> setup outside a dev machine.

## Files

| File | Purpose |
| --- | --- |
| `compose.yaml` | `app` + `postgres` services, volumes, network. |
| `compose.podman.yaml` | Rootless-podman override (`keep-id`, 1Password SSH agent). |
| `devcontainer.json` | Local flow (compose.yaml + compose.podman.yaml). |
| `devcontainer.portable.json` | Remote/docker flow (compose.yaml only). |
| `init-db.sql` | Creates `pg_stat_statements` on first DB init. |
| `postcreate.sh` | One-time provisioning: mise toolchain (just, slick, nvim tooling), rust components, `psql`, chezmoi dotfiles. |
| `post-start.sh` | Waits for postgres on every start; re-applies git config. |
| `configure-git.sh` | Optional git identity / SSH commit signing from env. |

The toolchain is managed by [`mise`](https://mise.jdx.dev) via the repo-root
`mise.toml` (just, slick prompt, ripgrep/bat/eza, tree-sitter, tmux, uv/python,
pgcli, etc.), and the shell/nvim/chezmoi dotfiles are applied from
`DEVPOD_DOTFILES` (defaults to the personal devpod dotfiles repo). Host users who
don't use mise are unaffected — pgmon still builds with a plain `cargo` + `just`.

## Quick start (DevPod)

From the repo root on the host:

```sh
scripts/dev-up      # builds & starts app + postgres via DevPod
scripts/dev-ssh     # open a shell inside the workspace
```

Inside the container:

```sh
just test           # clippy + fmt + cargo test (uses the postgres sibling)
cargo run           # launch the TUI against PGMON_DSN
just psql           # note: the .justfile psql/up/down target podman on the HOST
```

### Forwarding git identity / signing (optional)

`scripts/dev-up` forwards these host environment variables when set, so commits and
SSH signing work inside the container:

```sh
GIT_USER_NAME="Your Name" \
GIT_USER_EMAIL="you@example.com" \
GIT_SIGNING_KEY="ssh-ed25519 AAAA..." \
  scripts/dev-up
```

If a host 1Password SSH agent socket exists at `~/.1password/agent.sock`, it is
mounted into the container (via `compose.podman.yaml`) and used for signing.

## VS Code / other DevContainer tools

Open the folder in VS Code with the Dev Containers extension and choose **Reopen in
Container**. It uses `devcontainer.json` (local podman/docker). For remote providers
that cannot apply local overrides, point the tool at `devcontainer.portable.json`.

## Host-only alternative (no devcontainer)

The repo `.justfile` also has standalone targets that run PostgreSQL directly with
podman on the host (not part of the compose devcontainer):

```sh
just up      # start a postgres:18 container on the host
just run     # cargo run against localhost:5432
just down    # stop & remove it
```

Use the devcontainer for a fully reproducible environment, or the `.justfile`
targets for a quick host-side database.

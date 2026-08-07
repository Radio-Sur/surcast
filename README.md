# Surcast

Internet radio broadcast management software.

> [!WARNING]  
> ⚠️ Project is in early development. Expect breaking changes and incomplete features.

## Requirements

- **Docker** or
- **Nix** (for the dev shell) or 
- Running the pieces manually:
  - Rust (stable) + `sqlx-cli`, `cargo-watch`
  - [bun](https://bun.sh)
  - PostgreSQL 16
  - Icecast 2 and `ffmpeg` (with the `ebur128` filter) on `$PATH`
  - Playwright / Chromium (for E2E tests)

## Getting started

### Configuration

```bash
git clone --recurse-submodules https://github.com/Radio-Sur/surcast.git
```

Copy `.env.example` to `.env` and adjust the values:

| Variable              | Description                                   | Default                     |
| --------------------- | --------------------------------------------- | --------------------------- |
| `DATABASE_URL`        | PostgreSQL connection string                  | `postgres://surcast:surcast@localhost:5433/surcast` |
| `JWT_SECRET`          | Secret used to sign JWTs                      | – (required)                |
| `JWT_ACCESS_EXPIRY`   | Access token lifetime (seconds)               | `900`                       |
| `JWT_REFRESH_EXPIRY`  | Refresh token lifetime (seconds)              | `604800`                    |
| `SERVER_HOST`         | Backend bind address                          | `0.0.0.0`                   |
| `SERVER_PORT`         | Backend port                                  | `3001`                      |
| `UPLOAD_DIR`          | Directory for uploaded audio                  | `./../uploads`              |
| `RUST_LOG`            | Logging directives                            | `surcast_backend=debug,tower_http=debug` |
| `LASTFM_API_KEY`      | Optional Last.fm key                          | –                            |

### Docker

```bash
docker compose up --build
```

Builds the backend and frontend into a single image and exposes:

- API + frontend at <http://localhost:80>
- Icecast stream at <http://localhost:8000>

### Nix

```bash
nix develop
dev
```

The dev shell provides Rust, bun, PostgreSQL, Icecast, sqlx-cli, cargo-watch and
the Playwright/Chromium system dependencies. The `dev`, `pg-start`, `pg-stop`
and `pg-status` scripts are on `PATH`. Everything is served at
<http://localhost:6767>.

### Manual

```bash
cp .env.example .env
bun install        # at the repo root (frontend tooling)
scripts/dev        # starts postgres, then backend (cargo watch) + frontend
```

## Ports

| Port | Service                                |
| ---- | -------------------------------------- |
| 6767 | Frontend dev server (Vite)             |
| 3001 | Backend REST API + WebSocket (`/api/ws`) |
| 8000 | Icecast stream                         |
| 5433 | PostgreSQL (dev)                       |

## Scripts

Found in `scripts/` (and on `PATH` inside the Nix dev shell):

| Script       | Description                                  |
| ------------ | -------------------------------------------- |
| `dev`        | Start PostgreSQL, backend (cargo watch) and frontend |
| `e2e`        | Bring everything up and run Playwright E2E    |
| `pg-start`   | Start the development PostgreSQL instance (port 5433) |
| `pg-stop`    | Stop it                                       |
| `pg-status`  | Show build status                               |

## License

GPL-3.0 — see [LICENSE](LICENSE).

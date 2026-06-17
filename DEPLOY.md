# Deploying to Fly.io

## Prerequisites

- [flyctl](https://fly.io/docs/flyctl/install/) installed and authenticated (`fly auth login`)
- A `fly.toml` already present in this repo (app name/region can be changed before launch)

## Steps

```bash
# 1. Register the app on Fly without deploying yet (uses the existing fly.toml)
fly launch --no-deploy

# 2. Create a 1GB persistent volume for the SQLite database.
#    Must be in the same region as primary_region in fly.toml.
fly volumes create aiops_data --size 1

# 3. Set required secrets (never commit these — they're injected at runtime)
fly secrets set NVIDIA_API_KEY=...

# Bearer token protecting /ws, /api/trigger, /api/reset, /api/alerts.
# Generate a random 32-char token rather than typing your own:
#   openssl rand -hex 16
fly secrets set AUTH_TOKEN=<random-32-char-string>

# Optional secrets, if you use them:
# fly secrets set ANTHROPIC_API_KEY=...
# fly secrets set PROMETHEUS_URL=https://your-prometheus:9090

# 4. Deploy
fly deploy

# 5. Verify
fly logs
```

## What to look for in `fly logs`

- `"msg":"store opened","path":"/data/alerts.db"` — confirms the app detected it's running on Fly (`FLY_APP_NAME` is set) and is writing the SQLite DB to the persistent volume, not the ephemeral container filesystem.
- `"msg":"aiops-bot starting","addr":":8080"` — confirms it bound to all interfaces, not just loopback.
- No `"AI analysis disabled"` warning — confirms `NVIDIA_API_KEY` was picked up from secrets.
- No `"auth disabled — set AUTH_TOKEN in production"` warning — confirms `AUTH_TOKEN` was picked up from secrets.
- Health checks passing (no repeated `checks` failures) — confirms `GET /health` is reachable on the internal port declared in `fly.toml`.

## Auth notes

- `/health`, `/`, and static assets are reachable without a token; `/ws`, `/api/trigger`, `/api/reset`, and `/api/alerts` require `Authorization: Bearer <AUTH_TOKEN>`.
- The dashboard at `/` gets the token injected into a `<meta name="AUTH_TOKEN">` tag server-side, and its JS reads that to authenticate `fetch`/WebSocket calls automatically — no login step.
- **Caveat:** because `/` itself is unauthenticated and the real token is embedded in that page's HTML, anyone who can load the dashboard can read the token from page source. This token is meant to keep out bots/scrapers hitting the API directly, not as a real access-control boundary for the dashboard itself. If you need the dashboard restricted too, put it behind a separate layer (Fly's private networking, a reverse proxy with its own auth, etc.).
- If `AUTH_TOKEN` is unset while `auth.enabled: true` (the default), the app logs a warning and effectively runs with auth disabled rather than locking everyone out.

## Notes

- `config.yaml` is gitignored and intentionally **not** baked into the Docker image — only `config.yaml.example` is. If you need non-default config values (custom thresholds, etc.) on Fly, set them via env var overrides (`NVIDIA_API_KEY`, `ANTHROPIC_API_KEY`, `PROMETHEUS_URL`) rather than shipping a config file, or extend the image build to copy a real `config.yaml` you manage outside of git.
- The volume must exist before the first deploy that uses it; if you ever resize or recreate the app, recreate the volume too (`fly volumes create aiops_data --size 1`) and reattach it in `fly.toml`'s `[mounts]` block.
- `auto_stop_machines`/`min_machines_running = 0` in `fly.toml` means the machine can scale to zero when idle. The first request after a cold start will be slower (machine boot + DB open).

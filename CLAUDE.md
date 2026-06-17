# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the app (requires config.yaml or env vars)
go run .

# Build
go build -o aiops-bot .

# Run tests with race detection
go test -race ./...

# Run tests with coverage
go test -cover ./...

# Format and vet
gofmt -w .
go vet ./...

# Docker
docker build -t aiops-bot .
docker run -p 8080:8080 -e AUTH_TOKEN=secret aiops-bot
```

## Configuration

Copy `config.yaml.example` to `config.yaml` and fill in values. The app starts without a config file (uses defaults). Env vars always override YAML:

| Env Var | Purpose |
|---|---|
| `PROMETHEUS_URL` | Enable Prometheus metric scraping |
| `NVIDIA_API_KEY` | API key when `ai.provider: nvidia` |
| `ANTHROPIC_API_KEY` | API key when `ai.provider: anthropic` |
| `AUTH_TOKEN` | Bearer token for protected endpoints |

Auth is enabled by default. Without `AUTH_TOKEN` set, auth is disabled with a startup warning. WebSocket auth uses `?token=` query param (browsers can't set custom headers on WS handshakes).

## Architecture

All code is in a single `package main` at the repo root. The startup sequence in `main.go` wires these components in order:

```
Config → Store → Hub → Simulator → ClaudeClient → AlertEngine → HTTP server
```

**Data flow:**
- `Simulator.Run()` ticks every 500ms, probing `/ping` for latency/error-rate and reading real CPU/memory via gopsutil (or Prometheus if `PROMETHEUS_URL` is set). Emits `Metrics` on a channel.
- A goroutine in `main.go` fans out those metrics to all WebSocket clients via `Hub.broadcastJSON`.
- `AlertEngine.Run()` evaluates `sim.Current()` every 2s against config thresholds, fires `Alert` structs, persists them to SQLite via `Store`, and asynchronously calls the AI provider. AI responses are also broadcast over WebSocket.

**Metric sources (priority order):**
1. Prometheus scrape (CPU, memory, latency, error rate) — when `PROMETHEUS_URL` is set
2. gopsutil host metrics (CPU, memory) + `/ping` self-probe (latency, error rate) — default

**Triggering spikes:** `POST /api/trigger` sets `TriggerMode` on the Simulator. CPU/Memory triggers override the measured value synthetically. Latency/ErrorRate triggers work by making `/ping` sleep or return 500s, which the probe naturally captures.

**WebSocket message types** (all JSON `{type, payload}`):
- `metrics` — every 500ms snapshot
- `alert` — threshold breach
- `ai_response` — async AI analysis result

**Auth bypass paths** (never wrapped or skipped in middleware): `GET /`, `GET /health`, `GET /static/*`. The index page has the auth token injected as a `<meta name="AUTH_TOKEN">` tag so the dashboard JS can read it.

**SQLite schema:** Single `alerts` table. `SaveAlert` inserts on alert fire; `UpdateAIResponse` fills `root_cause`, `remediation`, `confidence` when AI responds. On Fly.io (`FLY_APP_NAME` env set), the DB path defaults to `/data/alerts.db` (persistent volume).

## Key Files

| File | Role |
|---|---|
| `config.go` | `Config` struct, `LoadConfig()` — YAML + env overlay |
| `main.go` | Startup wiring and goroutine fan-out |
| `server.go` | All HTTP handlers, `authMiddleware`, WebSocket upgrade |
| `alerts.go` | `AlertEngine` — threshold evaluation, cooldown, AI dispatch |
| `claude.go` | `ClaudeClient` — NVIDIA NIM (OpenAI-compat) and Anthropic APIs |
| `simulator.go` | `Simulator` — real metrics + `/ping` probe + trigger overrides |
| `prometheus.go` | `PrometheusClient` — optional scrape |
| `hub.go` | WebSocket hub — register/unregister/broadcast |
| `store.go` | SQLite persistence for alerts and AI responses |
| `static/index.html` | Single-file dashboard (WebSocket, charts, trigger UI) |

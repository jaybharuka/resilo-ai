# AIOps Bot

A real-time ops dashboard with WebSocket live metrics, an alert engine, and AI-powered incident analysis via NVIDIA NIM (OpenAI-compatible chat completions API).

## Features

- **Live metrics** — CPU, memory, latency, error rate simulated every 500ms with sparklines
- **Alert engine** — threshold evaluation every 2s, warning + critical levels, configurable cooldown
- **AI analysis** — async NVIDIA NIM or Anthropic API call on every alert, streamed back as `ai_response` WebSocket message
- **ChatOps panel** — AI root cause + remediation displayed in real time
- **Trigger scenarios** — one-click CPU/memory/latency/error-rate spikes via dashboard buttons
- **REST API** — `POST /api/trigger`, `POST /api/reset`, `GET /api/alerts`
- **Authentication** — Bearer token authentication with rate limiting
- **Metrics endpoint** — Prometheus-compatible `/metrics` endpoint for monitoring
- **Database persistence** — SQLite storage for alerts and AI responses
- **Graceful shutdown** — Clean shutdown with proper resource cleanup

## Quick start

```bash
# Without AI (no key needed)
go run .

# With AI analysis
NVIDIA_API_KEY=nvapi-... go run .
```

Open http://localhost:8080

## Configuration

The application uses `config.yaml` for configuration. See `config.yaml.example` for all available options:

```yaml
server:
  port: 8080

ai:
  provider: nvidia  # or anthropic
  api_key: ""       # overridden by NVIDIA_API_KEY or ANTHROPIC_API_KEY
  model: "abacusai/dracarys-llama-3.1-70b-instruct"

prometheus:
  url: ""           # overridden by PROMETHEUS_URL

alerts:
  cpu_warning: 70
  cpu_critical: 85
  memory_warning: 65
  memory_critical: 80
  latency_warning_ms: 800
  latency_critical_ms: 1500
  error_warning: 5
  error_critical: 10
  cooldown_seconds: 60

auth:
  enabled: true
  token: ""         # overridden by AUTH_TOKEN

ratelimit:
  enabled: true
  requests_per_minute: 10
  burst: 5

store:
  path: "alerts.db"
```

## Docker

```bash
cp .env.example .env           # add your NVIDIA_API_KEY
docker compose up --build -d
```

## API

| Method | Path           | Body                                                          | Description            |
|--------|----------------|---------------------------------------------------------------|------------------------|
| POST   | /api/trigger   | `{"cpu":bool,"memory":bool,"latency":bool,"error_rate":bool}` | Spike metrics          |
| POST   | /api/reset     | —                                                             | Clear all spikes       |
| GET    | /ws            | WebSocket upgrade                                             | Real-time data stream  |

## WebSocket message types

| Type          | Direction  | Description                        |
|---------------|------------|------------------------------------|
| `metrics`     | server→client | Live metric snapshot every 500ms |
| `alert`       | server→client | Threshold breach notification     |
| `ai_response` | server→client | AI incident analysis              |

## Thresholds

| Metric     | Warning | Critical |
|------------|---------|----------|
| CPU        | 70%     | 85%      |
| Memory     | 65%     | 80%      |
| Latency    | 800ms   | 1500ms   |
| Error Rate | 5%      | 10%      |

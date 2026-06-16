# AIOps Bot

A real-time ops dashboard with WebSocket live metrics, an alert engine, and AI-powered incident analysis via NVIDIA NIM (OpenAI-compatible chat completions API).

## Features

- **Live metrics** — CPU, memory, latency, error rate simulated every 500ms with sparklines
- **Alert engine** — threshold evaluation every 2s, warning + critical levels, 30s cooldown
- **AI analysis** — async NVIDIA NIM call on every alert, streamed back as `ai_response` WebSocket message
- **ChatOps panel** — AI root cause + remediation displayed in real time
- **Trigger scenarios** — one-click CPU/memory/latency/error-rate spikes via dashboard buttons
- **REST API** — `POST /api/trigger`, `POST /api/reset`

## Quick start

```bash
# Without AI (no key needed)
go run .

# With AI analysis
NVIDIA_API_KEY=nvapi-... go run .
```

Open http://localhost:8080

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

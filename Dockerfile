# ── Build stage ──────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o aiops-bot .

# ── Runtime stage ─────────────────────────────────────────────────
FROM scratch

WORKDIR /app

COPY --from=builder /app/aiops-bot .
COPY static/ ./static/

EXPOSE 8080

ENTRYPOINT ["/app/aiops-bot"]

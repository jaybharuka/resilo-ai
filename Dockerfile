# ── Build stage ──────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o aiops-bot .

# ── Runtime stage ─────────────────────────────────────────────────
FROM scratch

WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/aiops-bot .
COPY static/ ./static/
COPY config.yaml.example ./

EXPOSE 8080

ENTRYPOINT ["/app/aiops-bot"]

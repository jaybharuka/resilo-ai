package main

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// bucket is a token-bucket state for one IP address.
type bucket struct {
	tokens   float64
	lastSeen time.Time
	mu       sync.Mutex
}

// RateLimiter enforces a token bucket per IP using sync.Map.
type RateLimiter struct {
	buckets sync.Map
	rate    float64 // tokens added per second
	burst   float64
}

func newRateLimiter(cfg *Config) *RateLimiter {
	rl := &RateLimiter{
		rate:  float64(cfg.RateLimit.RequestsPerMinute) / 60.0,
		burst: float64(cfg.RateLimit.Burst),
	}
	go rl.cleanup()
	return rl
}

// clientIP extracts the requester IP, preferring X-Forwarded-For (set by Fly.io).
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For can be a comma-separated list; the first entry is the client.
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// allow returns (true, 0) when the request is within the rate limit, or
// (false, retryAfter) when it is rejected.
func (rl *RateLimiter) allow(ip string) (bool, int) {
	v, _ := rl.buckets.LoadOrStore(ip, &bucket{tokens: rl.burst, lastSeen: time.Now()})
	b := v.(*bucket)

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.lastSeen = now

	// Refill tokens proportional to elapsed time.
	b.tokens = min(rl.burst, b.tokens+elapsed*rl.rate)

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}

	// Seconds until one token is available.
	retryAfter := int((1-b.tokens)/rl.rate) + 1
	return false, retryAfter
}

// cleanup removes buckets idle for more than 10 minutes, running every 5 minutes.
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		rl.buckets.Range(func(k, v any) bool {
			b := v.(*bucket)
			b.mu.Lock()
			idle := b.lastSeen.Before(cutoff)
			b.mu.Unlock()
			if idle {
				rl.buckets.Delete(k)
			}
			return true
		})
	}
}

// rateLimitMiddleware wraps a handler with IP-based rate limiting.
func rateLimitMiddleware(rl *RateLimiter, cfg *Config, route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !cfg.RateLimit.Enabled {
			next(w, r)
			return
		}
		ip := clientIP(r)
		ok, retryAfter := rl.allow(ip)
		if !ok {
			slog.Warn("rate limit hit", "ip", ip, "route", route)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"error":                "rate limit exceeded",
				"retry_after_seconds": retryAfter,
			})
			return
		}
		next(w, r)
	}
}

// min returns the smaller of two float64 values (stdlib min added in Go 1.21).
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// TriggerRequest is the body for POST /api/trigger.
type TriggerRequest struct {
	CPU       bool `json:"cpu"`
	Memory    bool `json:"memory"`
	Latency   bool `json:"latency"`
	ErrorRate bool `json:"error_rate"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status        string         `json:"status"`
	UptimeSeconds int64          `json:"uptime_seconds"`
	Metrics       HealthMetrics  `json:"metrics"`
}

// HealthMetrics holds the last-sampled values for the health check.
type HealthMetrics struct {
	CPU       float64 `json:"cpu"`
	Memory    float64 `json:"memory"`
	Latency   float64 `json:"latency"`
	ErrorRate float64 `json:"error_rate"`
}

// authMiddleware enforces Authorization: Bearer <token> on the wrapped handler.
// GET /health, GET /, and static files are never wrapped with this in practice,
// but the path checks below are kept as a defensive no-op for those cases too.
// /ws is a special case: browsers can't set custom headers on the WebSocket
// handshake, so it also accepts the token via a ?token= query param.
func authMiddleware(cfg *Config, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && (r.URL.Path == "/" || r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/static/")) {
			next(w, r)
			return
		}
		if !cfg.Auth.Enabled || cfg.Auth.Token == "" {
			next(w, r)
			return
		}

		token := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			token = strings.TrimPrefix(h, "Bearer ")
		} else if r.URL.Path == "/ws" {
			token = r.URL.Query().Get("token")
		}

		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(cfg.Auth.Token)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

// serveIndex serves static/index.html with an AUTH_TOKEN meta tag injected,
// so the dashboard's JS can read the token without a separate auth step.
func serveIndex(cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := os.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "index not found", http.StatusInternalServerError)
			return
		}
		meta := `<meta name="AUTH_TOKEN" content="` + cfg.Auth.Token + `">`
		html := strings.Replace(string(body), "</head>", meta+"\n</head>", 1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
	}
}

func newServeMux(hub *Hub, sim *Simulator, store *Store, startTime time.Time, cfg *Config) *http.ServeMux {
	rl := newRateLimiter(cfg)
	mux := http.NewServeMux()

	// GET /health — machine-readable status with uptime and current metrics
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		m := sim.Current()
		body := HealthResponse{
			Status:        "ok",
			UptimeSeconds: int64(time.Since(startTime).Seconds()),
			Metrics: HealthMetrics{
				CPU:       math.Round(m.CPU*100) / 100,
				Memory:    math.Round(m.Memory*100) / 100,
				Latency:   math.Round(m.Latency*100) / 100,
				ErrorRate: math.Round(m.ErrorRate*100) / 100,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(body)
	})

	// GET /ping — real latency/error probe target
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		trigger := sim.GetTrigger()
		if trigger.Latency {
			time.Sleep(1800 * time.Millisecond)
		}
		if trigger.ErrorRate && rand.Float64() < 0.40 {
			http.Error(w, "simulated error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Static files — "/" serves index.html with the auth token meta tag injected;
	// everything else falls through to the plain file server.
	fileServer := http.FileServer(http.Dir("static"))
	index := serveIndex(cfg)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			index(w, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	})

	// WebSocket endpoint
	mux.HandleFunc("/ws", rateLimitMiddleware(rl, cfg, "/ws", authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Warn("ws upgrade failed", "err", err, "remote", r.RemoteAddr)
			return
		}
		client := &Client{
			conn: conn,
			send: make(chan []byte, 64),
		}
		hub.register <- client
		go client.writePump()
		go client.readPump(hub)
	})))

	// GET /api/alerts — query persisted alerts with optional limit and severity filter
	mux.HandleFunc("/api/alerts", authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 {
				limit = n
			}
		}
		severity := strings.ToLower(r.URL.Query().Get("severity"))
		rows, err := store.QueryAlerts(limit, severity)
		if err != nil {
			slog.Error("QueryAlerts failed", "err", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if rows == nil {
			rows = []AlertRow{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rows)
	}))

	// POST /api/trigger — spike specific metrics
	mux.HandleFunc("/api/trigger", rateLimitMiddleware(rl, cfg, "/api/trigger", authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req TriggerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		sim.SetTrigger(TriggerMode{
			CPU:       req.CPU,
			Memory:    req.Memory,
			Latency:   req.Latency,
			ErrorRate: req.ErrorRate,
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})))

	// POST /api/reset — clear all triggers
	mux.HandleFunc("/api/reset", rateLimitMiddleware(rl, cfg, "/api/reset", authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sim.Reset()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
	})))

	return mux
}

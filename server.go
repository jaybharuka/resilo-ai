package main

import (
	"compress/gzip"
	crand "crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"regexp"
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
		requestID := generateRequestID()
		
		if !cfg.RateLimit.Enabled {
			next(w, r)
			return
		}
		ip := clientIP(r)
		ok, retryAfter := rl.allow(ip)
		if !ok {
			slog.Warn("rate limit hit", "request_id", requestID, "ip", ip, "route", route)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"error":                "rate limit exceeded",
				"retry_after_seconds": retryAfter,
				"request_id":          requestID,
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

// generateRequestID creates a random 8-character hex string for request tracking.
func generateRequestID() string {
	bytes := make([]byte, 4)
	crand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// corsMiddleware adds CORS headers for API endpoints.
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		next(w, r)
	}
}

// loggingMiddleware adds HTTP request logging for API endpoints.
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Create a response writer wrapper to capture status code
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: 200}
		
		next(lrw, r)
		
		duration := time.Since(start)
		slog.Info("HTTP request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", lrw.statusCode,
			"duration_ms", duration.Milliseconds(),
			"remote", clientIP(r),
		)
	}
}

// loggingResponseWriter wraps http.ResponseWriter to capture status code.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// compressionMiddleware adds gzip compression to HTTP responses.
func compressionMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Don't compress WebSocket connections
		if r.Header.Get("Upgrade") == "websocket" {
			next(w, r)
			return
		}
		
		// Check if client accepts gzip
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next(w, r)
			return
		}
		
		// Create gzip writer
		gz := gzip.NewWriter(w)
		defer gz.Close()
		
		// Wrap response writer
		cw := &compressionResponseWriter{
			Writer:         gz,
			ResponseWriter: w,
		}
		
		// Set compression header
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		
		next(cw, r)
	}
}

// compressionResponseWriter wraps gzip.Writer with http.ResponseWriter.
type compressionResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (cw *compressionResponseWriter) Write(b []byte) (int, error) {
	return cw.Writer.Write(b)
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
	Database      string         `json:"database"`
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

// sessionMiddleware checks the session_token cookie and redirects to /login if missing or expired.
func sessionMiddleware(store *Store, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session_token")
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		if _, err := store.GetUserBySession(cookie.Value); err != nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		next(w, r)
	}
}

// signupHandler creates a new user account and session.
func signupHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/signup", http.StatusSeeOther)
			return
		}
		email := strings.TrimSpace(r.FormValue("email"))
		password := r.FormValue("password")
		if email == "" || len(password) < 8 {
			http.Redirect(w, r, "/signup?error=invalid", http.StatusSeeOther)
			return
		}
		hash, err := HashPassword(password)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		user, err := store.CreateUser(email, hash)
		if err != nil {
			http.Redirect(w, r, "/signup?error=exists", http.StatusSeeOther)
			return
		}
		token, err := store.CreateSession(user.ID)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     "session_token",
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			Secure:   true,
			Expires:  time.Now().Add(7 * 24 * time.Hour),
			SameSite: http.SameSiteLaxMode,
		})
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
	}
}

// loginHandler verifies credentials and creates a session.
func loginHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		email := strings.TrimSpace(r.FormValue("email"))
		password := r.FormValue("password")
		user, err := store.GetUserByEmail(email)
		if err != nil || !CheckPassword(user.PasswordHash, password) {
			http.Redirect(w, r, "/login?error=invalid", http.StatusSeeOther)
			return
		}
		token, err := store.CreateSession(user.ID)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     "session_token",
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			Secure:   true,
			Expires:  time.Now().Add(7 * 24 * time.Hour),
			SameSite: http.SameSiteLaxMode,
		})
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
	}
}

// logoutHandler deletes the session and clears the cookie.
func logoutHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie("session_token"); err == nil {
			store.DeleteSession(cookie.Value)
		}
		http.SetCookie(w, &http.Cookie{
			Name:    "session_token",
			Value:   "",
			Path:    "/",
			MaxAge:  -1,
			Expires: time.Unix(0, 0),
		})
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	}
}

const resetBaseURL = "https://resilo-ai.fly.dev"

// forgotPasswordHandler accepts an email, creates a reset token, sends the email.
// Always returns 200 — never reveals whether an email exists.
func forgotPasswordHandler(store *Store, mailer *Mailer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		msg := map[string]string{"message": "If that email exists you will receive a reset link"}

		user, err := store.GetUserByEmail(strings.TrimSpace(strings.ToLower(req.Email)))
		if err != nil {
			// Don't reveal that the email isn't found.
			json.NewEncoder(w).Encode(msg)
			return
		}
		token, err := store.CreatePasswordReset(user.ID)
		if err != nil {
			slog.Error("CreatePasswordReset failed", "err", err)
			json.NewEncoder(w).Encode(msg)
			return
		}
		if mailer != nil {
			resetURL := resetBaseURL + "/reset-password?token=" + token
			if err := mailer.SendPasswordReset(user.Email, resetURL); err != nil {
				slog.Error("SendPasswordReset email failed", "err", err)
			}
		}
		json.NewEncoder(w).Encode(msg)
	}
}

// resetPasswordHandler validates a token and updates the user's password.
func resetPasswordHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Token       string `json:"token"`
			NewPassword string `json:"new_password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if len(req.NewPassword) < 8 {
			jsonErr(w, http.StatusBadRequest, "password must be at least 8 characters")
			return
		}
		pr, err := store.GetPasswordReset(req.Token)
		if err != nil {
			jsonErr(w, http.StatusBadRequest, err.Error())
			return
		}
		hash, err := HashPassword(req.NewPassword)
		if err != nil {
			slog.Error("resetPasswordHandler: hash failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "server error")
			return
		}
		if err := store.UpdatePassword(pr.UserID, hash); err != nil {
			slog.Error("UpdatePassword failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "server error")
			return
		}
		if err := store.MarkPasswordResetUsed(req.Token); err != nil {
			slog.Error("MarkPasswordResetUsed failed", "err", err)
		}
		slog.Info("password reset completed", "user_id", pr.UserID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "password reset"})
	}
}

// metricsHandler writes the Prometheus text exposition format for all aiops metrics.
// Exempted from auth so Grafana Cloud can scrape without a token.
func metricsHandler(sim *Simulator, ae *AlertEngine, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m := sim.Current()
		critical, warning := ae.Counts()
		active := ae.ActiveCount()
		clients := hub.ClientCount()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		fmt.Fprintf(w, "# HELP aiops_cpu_percent Current host CPU usage percent\n")
		fmt.Fprintf(w, "# TYPE aiops_cpu_percent gauge\n")
		fmt.Fprintf(w, "aiops_cpu_percent %g\n\n", m.CPU)

		fmt.Fprintf(w, "# HELP aiops_memory_percent Current host memory usage percent\n")
		fmt.Fprintf(w, "# TYPE aiops_memory_percent gauge\n")
		fmt.Fprintf(w, "aiops_memory_percent %g\n\n", m.Memory)

		fmt.Fprintf(w, "# HELP aiops_latency_ms Current p99 latency in milliseconds\n")
		fmt.Fprintf(w, "# TYPE aiops_latency_ms gauge\n")
		fmt.Fprintf(w, "aiops_latency_ms %g\n\n", m.Latency)

		fmt.Fprintf(w, "# HELP aiops_error_rate_percent Current HTTP error rate percent\n")
		fmt.Fprintf(w, "# TYPE aiops_error_rate_percent gauge\n")
		fmt.Fprintf(w, "aiops_error_rate_percent %g\n\n", m.ErrorRate)

		fmt.Fprintf(w, "# HELP aiops_alerts_total Total alerts fired since startup\n")
		fmt.Fprintf(w, "# TYPE aiops_alerts_total counter\n")
		fmt.Fprintf(w, "aiops_alerts_total{severity=\"critical\"} %d\n", critical)
		fmt.Fprintf(w, "aiops_alerts_total{severity=\"warning\"} %d\n\n", warning)

		fmt.Fprintf(w, "# HELP aiops_active_incidents Current number of open (unresolved) incidents\n")
		fmt.Fprintf(w, "# TYPE aiops_active_incidents gauge\n")
		fmt.Fprintf(w, "aiops_active_incidents %d\n\n", active)

		fmt.Fprintf(w, "# HELP aiops_ws_clients Current number of connected WebSocket clients\n")
		fmt.Fprintf(w, "# TYPE aiops_ws_clients gauge\n")
		fmt.Fprintf(w, "aiops_ws_clients %d\n", clients)
	}
}

func newServeMux(hub *Hub, sim *Simulator, ae *AlertEngine, store *Store, mailer *Mailer, claude *ClaudeClient, startTime time.Time, cfg *Config) *http.ServeMux {
	rl := newRateLimiter(cfg)
	mux := http.NewServeMux()

	// GET /metrics — Prometheus text format; no auth so Grafana Cloud can scrape freely
	mux.HandleFunc("/metrics", metricsHandler(sim, ae, hub))

	// GET /health — machine-readable status with uptime and current metrics
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		m := sim.Current()
		
		// Check database connectivity
		dbStatus := "ok"
		if err := store.db.Ping(); err != nil {
			dbStatus = "error"
			slog.Error("health check: database ping failed", "err", err)
		}
		
		body := HealthResponse{
			Status:        "ok",
			UptimeSeconds: int64(time.Since(startTime).Seconds()),
			Database:      dbStatus,
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

	// Auth routes — public, no session required.
	mux.HandleFunc("/auth/signup", signupHandler(store))
	mux.HandleFunc("/auth/login", loginHandler(store))
	mux.HandleFunc("/auth/logout", logoutHandler(store))
	mux.HandleFunc("/auth/forgot-password", forgotPasswordHandler(store, mailer))
	mux.HandleFunc("/auth/reset-password", resetPasswordHandler(store))

	// Login / signup / password reset pages.
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/login.html")
	})
	mux.HandleFunc("/signup", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/signup.html")
	})
	mux.HandleFunc("/forgot-password", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/forgot.html")
	})
	mux.HandleFunc("/reset-password", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/reset.html")
	})

	// Dashboard — protected by session middleware.
	mux.HandleFunc("/dashboard", sessionMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		body, err := os.ReadFile("static/dashboard.html")
		if err != nil {
			http.Error(w, "dashboard not found", http.StatusInternalServerError)
			return
		}
		slug := ""
		if cookie, cookieErr := r.Cookie("session_token"); cookieErr == nil {
			if u, sessionErr := store.GetUserBySession(cookie.Value); sessionErr == nil {
				slug = u.Slug
			}
		}
		metas := `<meta name="AUTH_TOKEN" content="` + cfg.Auth.Token + `">` +
			`<meta name="USER_SLUG" content="` + slug + `">`
		html := strings.Replace(string(body), "</head>", metas+"\n</head>", 1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
	}))

	// Static files — "/" redirects to /dashboard or /login based on session;
	// other paths fall through to the file server for CSS/JS/assets.
	fileServer := http.FileServer(http.Dir("static"))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			fileServer.ServeHTTP(w, r)
			return
		}
		if cookie, err := r.Cookie("session_token"); err == nil {
			if _, err := store.GetUserBySession(cookie.Value); err == nil {
				http.Redirect(w, r, "/dashboard", http.StatusFound)
				return
			}
		}
		http.ServeFile(w, r, "static/index.html")
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
	mux.HandleFunc("/api/alerts", compressionMiddleware(loggingMiddleware(corsMiddleware(authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
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
	})))))

	// POST /api/trigger — spike specific metrics
	mux.HandleFunc("/api/trigger", compressionMiddleware(loggingMiddleware(corsMiddleware(rateLimitMiddleware(rl, cfg, "/api/trigger", authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
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
	}))))))

	// POST /api/reset — clear all triggers
	mux.HandleFunc("/api/reset", compressionMiddleware(loggingMiddleware(corsMiddleware(rateLimitMiddleware(rl, cfg, "/api/reset", authMiddleware(cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sim.Reset()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
	}))))))

	// Profile routes — session-cookie auth.
	mux.HandleFunc("PUT /api/profile/slug", profileSlugHandler(store))

	// Ask AI page + API.
	mux.HandleFunc("GET /ask", sessionMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/ask.html")
	}))
	mux.HandleFunc("POST /api/ask", askHandler(store, claude))

	// Settings page.
	mux.HandleFunc("GET /settings", sessionMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/settings.html")
	}))

	// Incidents page.
	mux.HandleFunc("GET /incidents", sessionMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/incidents.html")
	}))
	mux.HandleFunc("GET /api/incidents", incidentsHandler(store))

	// System incidents — new incidents table (session-cookie auth).
	mux.HandleFunc("GET /api/system/incidents", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := currentUser(store, w, r); !ok {
			return
		}
		limit := 20
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err2 := strconv.Atoi(l); err2 == nil && n > 0 && n <= 100 {
				limit = n
			}
		}
		incs, err := store.GetRecentIncidents(limit)
		if err != nil {
			slog.Error("GetRecentIncidents failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "db error")
			return
		}
		if incs == nil {
			incs = []Incident{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(incs)
	})

	// Prometheus snapshot for any job label — used by the service selector.
	mux.HandleFunc("GET /api/prom/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := currentUser(store, w, r); !ok {
			return
		}
		job := r.URL.Query().Get("job")
		if job == "" {
			job = "prod-api"
		}
		snap, err := sim.FetchJobSnapshot(job)
		if err != nil {
			jsonErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	})

	// Account management routes — session-cookie auth.
	mux.HandleFunc("GET /api/account/me", accountMeHandler(store))
	mux.HandleFunc("PUT /api/account/password", accountPasswordHandler(store))
	mux.HandleFunc("PUT /api/account/email", accountEmailHandler(store))
	mux.HandleFunc("DELETE /api/account", accountDeleteHandler(store))

	// Public status page — no auth required.
	mux.HandleFunc("GET /status/{slug}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/status.html")
	})
	mux.HandleFunc("GET /api/status/{slug}", statusDataHandler(store))

	return mux
}

// currentUser extracts the session user from the request cookie.
// Returns false and writes 401 JSON if the session is missing or expired.
func currentUser(store *Store, w http.ResponseWriter, r *http.Request) (User, bool) {
	cookie, err := r.Cookie("session_token")
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
		return User{}, false
	}
	user, err := store.GetUserBySession(cookie.Value)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
		return User{}, false
	}
	return user, true
}

func incidentsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}
		page := 1
		limit := 20
		if p := r.URL.Query().Get("page"); p != "" {
			if n, err := strconv.Atoi(p); err == nil && n > 0 {
				page = n
			}
		}
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 100 {
				limit = n
			}
		}
		offset := (page - 1) * limit
		rows, total, err := store.GetOutagesByUser(user.ID, limit, offset)
		if err != nil {
			slog.Error("GetOutagesByUser failed", "err", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if rows == nil {
			rows = []OutageRow{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"incidents":   rows,
			"total":       total,
			"page":        page,
			"limit":       limit,
			"total_pages": (total + limit - 1) / limit,
		})
	}
}

func askHandler(store *Store, claude *ClaudeClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}
		var req struct {
			Question string `json:"question"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		req.Question = strings.TrimSpace(req.Question)
		if req.Question == "" {
			jsonErr(w, http.StatusBadRequest, "question is required")
			return
		}
		if len(req.Question) > 1000 {
			jsonErr(w, http.StatusBadRequest, "question too long (max 1000 chars)")
			return
		}

		infraCtx, err := store.GetInfraContext(user.ID)
		if err != nil {
			slog.Error("askHandler: GetInfraContext failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "failed to gather infrastructure data")
			return
		}

		if claude == nil {
			jsonErr(w, http.StatusServiceUnavailable, "AI provider not configured — set ANTHROPIC_API_KEY or NVIDIA_API_KEY")
			return
		}

		answer, err := claude.AskInfra(req.Question, infraCtx)
		if err != nil {
			slog.Error("askHandler: AskInfra failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "AI request failed: "+err.Error())
			return
		}

		// Build a brief data-used summary for the UI.
		monitorNames := make([]string, 0, len(infraCtx.Monitors))
		for _, m := range infraCtx.Monitors {
			monitorNames = append(monitorNames, m.Name)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"answer": answer,
			"data_used": map[string]interface{}{
				"monitors":       monitorNames,
				"outages_30d":    len(infraCtx.Outages),
				"time_range":     "last 30 days",
				"as_of":          infraCtx.AsOf,
			},
		})
	}
}

// MonitorStatus is the public view of a monitor for the status page.
type MonitorStatus struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	URL           string    `json:"url"`
	Status        string    `json:"status"` // "up" | "down" | "unknown"
	LastStatusCode *int     `json:"last_status_code"`
	LastLatencyMs  *int     `json:"last_latency_ms"`
	LastCheckedAt  *string  `json:"last_checked_at"`
	UptimePercent  float64  `json:"uptime_percent"`
	DailyUptime    []float64 `json:"daily_uptime"` // 30 values; -1 = no data
}

// StatusPageData is the JSON payload returned by GET /api/status/{slug}.
type StatusPageData struct {
	Slug          string          `json:"slug"`
	OverallStatus string          `json:"overall_status"` // "operational" | "partial" | "major"
	LastUpdated   string          `json:"last_updated"`
	Monitors      []MonitorStatus `json:"monitors"`
}

func statusDataHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		user, err := store.GetUserBySlug(slug)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		monitors, err := store.GetMonitorsByUser(user.ID)
		if err != nil {
			slog.Error("statusDataHandler: GetMonitorsByUser failed", "err", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}

		statuses := make([]MonitorStatus, 0, len(monitors))
		downCount := 0
		for _, m := range monitors {
			if !m.Enabled {
				continue
			}
			ms := MonitorStatus{
				ID:            m.ID,
				Name:          m.Name,
				URL:           m.URL,
				LastStatusCode: m.LastStatus,
				LastLatencyMs:  m.LastLatencyMs,
				LastCheckedAt:  m.LastCheckedAt,
			}

			if m.LastStatus == nil {
				ms.Status = "unknown"
			} else if *m.LastStatus >= 200 && *m.LastStatus < 300 {
				ms.Status = "up"
			} else {
				ms.Status = "down"
				downCount++
			}

			ms.UptimePercent, _ = store.GetUptimePercent(m.ID, 7)
			ms.DailyUptime, _ = store.GetDailyUptime(m.ID, 30)
			statuses = append(statuses, ms)
		}

		overall := "operational"
		if downCount > 0 && downCount < len(statuses) {
			overall = "partial"
		} else if len(statuses) > 0 && downCount == len(statuses) {
			overall = "major"
		}

		data := StatusPageData{
			Slug:          slug,
			OverallStatus: overall,
			LastUpdated:   time.Now().UTC().Format(time.RFC3339),
			Monitors:      statuses,
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(data)
	}
}

var validSlugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)

func profileSlugHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}

		var body struct {
			Slug string `json:"slug"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
			return
		}

		slug := strings.ToLower(strings.TrimSpace(body.Slug))
		if len(slug) < 3 || len(slug) > 30 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "slug must be 3–30 characters"})
			return
		}
		if !validSlugRe.MatchString(slug) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "letters, numbers, and hyphens only; cannot start or end with a hyphen"})
			return
		}

		available, err := store.IsSlugAvailable(slug, user.ID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if !available {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{"error": "that slug is already taken"})
			return
		}

		if err := store.UpdateUserSlug(user.ID, slug); err != nil {
			slog.Error("UpdateUserSlug failed", "err", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}

		slog.Info("slug updated", "user_id", user.ID, "slug", slug)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"slug": slug})
	}
}

func accountMeHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"email":        user.Email,
			"id":           user.ID,
			"max_monitors": user.MaxMonitors,
		})
	}
}

func jsonErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func accountPasswordHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}
		var req struct {
			CurrentPassword string `json:"current_password"`
			NewPassword     string `json:"new_password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if !CheckPassword(user.PasswordHash, req.CurrentPassword) {
			jsonErr(w, http.StatusBadRequest, "current password is incorrect")
			return
		}
		if len(req.NewPassword) < 8 {
			jsonErr(w, http.StatusBadRequest, "new password must be at least 8 characters")
			return
		}
		hash, err := HashPassword(req.NewPassword)
		if err != nil {
			slog.Error("accountPasswordHandler: hash failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "server error")
			return
		}
		if err := store.UpdatePassword(user.ID, hash); err != nil {
			slog.Error("UpdatePassword failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "server error")
			return
		}
		slog.Info("password changed", "user_id", user.ID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "password updated"})
	}
}

func accountEmailHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}
		var req struct {
			NewEmail string `json:"new_email"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if !CheckPassword(user.PasswordHash, req.Password) {
			jsonErr(w, http.StatusBadRequest, "password is incorrect")
			return
		}
		newEmail := strings.TrimSpace(strings.ToLower(req.NewEmail))
		if newEmail == "" || !strings.Contains(newEmail, "@") {
			jsonErr(w, http.StatusBadRequest, "invalid email address")
			return
		}
		if _, err := store.GetUserByEmail(newEmail); err == nil {
			jsonErr(w, http.StatusBadRequest, "that email is already in use")
			return
		}
		if err := store.UpdateEmail(user.ID, newEmail); err != nil {
			slog.Error("UpdateEmail failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "server error")
			return
		}
		slog.Info("email changed", "user_id", user.ID, "new_email", newEmail)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "email updated"})
	}
}

func accountDeleteHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := currentUser(store, w, r)
		if !ok {
			return
		}
		var req struct {
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if !CheckPassword(user.PasswordHash, req.Password) {
			jsonErr(w, http.StatusBadRequest, "password is incorrect")
			return
		}
		if err := store.DeleteAccount(user.ID); err != nil {
			slog.Error("DeleteAccount failed", "err", err)
			jsonErr(w, http.StatusInternalServerError, "server error")
			return
		}
		// Clear the session cookie.
		http.SetCookie(w, &http.Cookie{
			Name:     "session_token",
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: true,
		})
		slog.Info("account deleted", "user_id", user.ID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
	}
}

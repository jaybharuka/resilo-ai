package main

import (
	"encoding/json"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
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

func newServeMux(hub *Hub, sim *Simulator, store *Store, startTime time.Time) *http.ServeMux {
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

	// Static files
	mux.Handle("/", http.FileServer(http.Dir("static")))

	// WebSocket endpoint
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /api/alerts — query persisted alerts with optional limit and severity filter
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// POST /api/trigger — spike specific metrics
	mux.HandleFunc("/api/trigger", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// POST /api/reset — clear all triggers
	mux.HandleFunc("/api/reset", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sim.Reset()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
	})

	return mux
}

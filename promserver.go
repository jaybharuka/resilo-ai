package main

import (
	"encoding/json"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// simSeries is one labelled time series with simulated state.
type simSeries struct {
	name     string
	job      string
	instance string
	lo, hi   float64 // normal operating range
	spike    float64 // value during a trigger
	current  float64 // live value, updated by drift()
}

// PromServer is a Prometheus-compatible HTTP server that simulates realistic metrics.
type PromServer struct {
	mu     sync.RWMutex
	series []*simSeries
	spikes map[string]bool // "cpu", "memory", "latency", "error_rate"
}

func newPromServer() *PromServer {
	ps := &PromServer{
		series: []*simSeries{
			{name: "node_cpu_usage_percent", job: "prod-api", instance: "api-1:9100", lo: 35, hi: 45, spike: 91},
			{name: "node_memory_usage_percent", job: "prod-api", instance: "api-1:9100", lo: 55, hi: 65, spike: 92},
			{name: "http_request_duration_p99_ms", job: "prod-api", instance: "api-1:9100", lo: 90, hi: 140, spike: 960},
			{name: "http_error_rate_percent", job: "prod-api", instance: "api-1:9100", lo: 0.1, hi: 0.5, spike: 19.5},
			{name: "node_cpu_usage_percent", job: "prod-db", instance: "db-1:9100", lo: 25, hi: 35, spike: 82},
			{name: "node_memory_usage_percent", job: "prod-db", instance: "db-1:9100", lo: 70, hi: 80, spike: 94},
		},
		spikes: map[string]bool{},
	}
	for _, s := range ps.series {
		s.current = (s.lo + s.hi) / 2
	}
	return ps
}

// Run starts the drift goroutine and then blocks serving HTTP on addr.
func (ps *PromServer) Run(addr string) {
	go ps.runDrift()
	slog.Info("prometheus simulator running", "addr", addr)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", ps.handleInstant)
	mux.HandleFunc("/api/v1/query_range", ps.handleRange)
	mux.HandleFunc("/api/v1/admin/trigger", ps.handleTrigger)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("promserver error", "err", err)
	}
}

// runDrift updates metric values every 5 seconds with realistic noise and drift.
func (ps *PromServer) runDrift() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ps.mu.Lock()
		for _, s := range ps.series {
			if ps.spiked(s) {
				noise := (rand.Float64()*2 - 1) * s.spike * 0.04
				s.current = math.Max(0, s.spike+noise)
			} else {
				step := (s.hi - s.lo) * 0.10
				s.current += (rand.Float64()*2 - 1) * step
				s.current = math.Max(s.lo, math.Min(s.hi, s.current))
			}
		}
		ps.mu.Unlock()
	}
}

// spiked returns whether a series is currently in a triggered state. Caller must hold mu.
func (ps *PromServer) spiked(s *simSeries) bool {
	switch {
	case s.name == "node_cpu_usage_percent" && s.job == "prod-api":
		return ps.spikes["cpu"]
	case s.name == "node_memory_usage_percent" && s.job == "prod-api":
		return ps.spikes["memory"]
	case s.name == "http_request_duration_p99_ms":
		return ps.spikes["latency"]
	case s.name == "http_error_rate_percent":
		return ps.spikes["error_rate"]
	}
	return false
}

// handleTrigger accepts a TriggerMode JSON body and updates spike state.
func (ps *PromServer) handleTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var t TriggerMode
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	ps.mu.Lock()
	ps.spikes["cpu"] = t.CPU
	ps.spikes["memory"] = t.Memory
	ps.spikes["latency"] = t.Latency
	ps.spikes["error_rate"] = t.ErrorRate
	ps.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleInstant serves GET /api/v1/query (Prometheus instant query).
func (ps *PromServer) handleInstant(w http.ResponseWriter, r *http.Request) {
	expr := r.URL.Query().Get("query")
	name, job := parseExpr(expr)

	ps.mu.RLock()
	matches := ps.match(name, job)
	ps.mu.RUnlock()

	now := float64(time.Now().Unix())
	results := make([]json.RawMessage, 0, len(matches))
	for _, s := range matches {
		val := strconv.FormatFloat(s.current, 'f', 4, 64)
		raw, _ := json.Marshal(map[string]interface{}{
			"metric": map[string]string{
				"__name__": s.name,
				"job":      s.job,
				"instance": s.instance,
			},
			"value": []interface{}{now, val},
		})
		results = append(results, raw)
	}
	writePromResponse(w, "vector", results)
}

// handleRange serves GET /api/v1/query_range (Prometheus range query).
func (ps *PromServer) handleRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	expr := q.Get("query")
	name, job := parseExpr(expr)

	startF, _ := strconv.ParseFloat(q.Get("start"), 64)
	endF, _ := strconv.ParseFloat(q.Get("end"), 64)
	step := parseDurationSecs(q.Get("step"))
	if step <= 0 {
		step = 60
	}
	if endF == 0 {
		endF = float64(time.Now().Unix())
	}
	if startF == 0 {
		startF = endF - 3600
	}

	ps.mu.RLock()
	matches := ps.match(name, job)
	ps.mu.RUnlock()

	results := make([]json.RawMessage, 0, len(matches))
	for _, s := range matches {
		values := syntheticRange(s, startF, endF, step)
		raw, _ := json.Marshal(map[string]interface{}{
			"metric": map[string]string{
				"__name__": s.name,
				"job":      s.job,
				"instance": s.instance,
			},
			"values": values,
		})
		results = append(results, raw)
	}
	writePromResponse(w, "matrix", results)
}

// match returns all series whose name and (optionally) job match. Caller must hold mu.
func (ps *PromServer) match(name, job string) []*simSeries {
	var out []*simSeries
	for _, s := range ps.series {
		if s.name != name {
			continue
		}
		if job != "" && s.job != job {
			continue
		}
		out = append(out, s)
	}
	return out
}

// syntheticRange builds [ts, val] data points for a time range.
func syntheticRange(s *simSeries, startF, endF, stepSecs float64) [][2]interface{} {
	baseline := (s.lo + s.hi) / 2
	noise := (s.hi - s.lo) / 4
	var pts [][2]interface{}
	for ts := startF; ts <= endF; ts += stepSecs {
		v := baseline + (rand.Float64()*2-1)*noise
		v = math.Max(s.lo*0.8, math.Min(s.hi*1.2, v))
		val := strconv.FormatFloat(v, 'f', 4, 64)
		pts = append(pts, [2]interface{}{ts, val})
	}
	return pts
}

// parseExpr extracts metric name and optional job label from a simple PromQL expression.
// Supports: metric_name or metric_name{job="x", ...}
func parseExpr(expr string) (name, job string) {
	expr = strings.TrimSpace(expr)
	lbrace := strings.Index(expr, "{")
	if lbrace < 0 {
		return expr, ""
	}
	name = expr[:lbrace]
	labels := expr[lbrace:]
	// Find job="..."
	const key = `job="`
	idx := strings.Index(labels, key)
	if idx < 0 {
		return name, ""
	}
	rest := labels[idx+len(key):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return name, ""
	}
	return name, rest[:end]
}

// parseDurationSecs parses a Prometheus step string ("60s", "5m", "60") into seconds.
func parseDurationSecs(s string) float64 {
	if s == "" {
		return 0
	}
	// Try as plain number (seconds).
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	// Try Go duration string (e.g. "60s", "5m", "1h").
	if d, err := time.ParseDuration(s); err == nil {
		return d.Seconds()
	}
	return 0
}

type promEnvelope struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string            `json:"resultType"`
		Result     []json.RawMessage `json:"result"`
	} `json:"data"`
}

func writePromResponse(w http.ResponseWriter, resultType string, results []json.RawMessage) {
	if results == nil {
		results = []json.RawMessage{}
	}
	var env promEnvelope
	env.Status = "success"
	env.Data.ResultType = resultType
	env.Data.Result = results
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(env)
}

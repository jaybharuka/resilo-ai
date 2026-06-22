package main

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Alert severity levels.
const (
	SeverityCritical = "critical"
	SeverityWarning  = "warning"
)

// Alert represents a fired threshold breach.
type Alert struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Severity  string  `json:"severity"`
	Message   string  `json:"message"`
	Timestamp int64   `json:"timestamp"`
}

// WSMessage is the envelope sent over WebSocket.
type WSMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

var alertSeq int

func nextAlertID() string {
	alertSeq++
	return fmt.Sprintf("ALT-%04d", alertSeq)
}

// activeAlert tracks an alert that has fired for a given metric.
// resolvedAt is zero while the alert is still open.
// After resolution the entry is kept until the post-resolution cooldown
// expires so we don't immediately re-fire on a flapping metric.
type activeAlert struct {
	id         string
	metric     string
	firedAt    time.Time
	resolvedAt time.Time
}

// AlertEngine evaluates metrics and fires alerts.
type AlertEngine struct {
	hub       *Hub
	sim       *Simulator
	claudeAPI *ClaudeClient
	store     *Store
	cfg       *Config

	// activeAlerts is keyed by the short metric key ("cpu", "mem", "lat", "err").
	// An entry with a zero resolvedAt is an open incident.
	// An entry with a non-zero resolvedAt is in post-resolution cooldown.
	// mu protects activeAlerts for concurrent reads (e.g. /metrics handler).
	mu           sync.RWMutex
	activeAlerts map[string]activeAlert

	// Lifetime counters — safe for concurrent reads via atomic.
	criticalCount atomic.Int64
	warningCount  atomic.Int64
}

func newAlertEngine(hub *Hub, sim *Simulator, claude *ClaudeClient, store *Store, cfg *Config) *AlertEngine {
	return &AlertEngine{
		hub:          hub,
		sim:          sim,
		claudeAPI:    claude,
		store:        store,
		cfg:          cfg,
		activeAlerts: make(map[string]activeAlert),
	}
}

// ActiveCount returns the number of currently open (unresolved) incidents.
func (ae *AlertEngine) ActiveCount() int {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	n := 0
	for _, a := range ae.activeAlerts {
		if a.resolvedAt.IsZero() {
			n++
		}
	}
	return n
}

// Counts returns the lifetime critical and warning alert totals.
func (ae *AlertEngine) Counts() (critical, warning int64) {
	return ae.criticalCount.Load(), ae.warningCount.Load()
}

// GetAlertStats returns comprehensive alert statistics for monitoring.
func (ae *AlertEngine) GetAlertStats() map[string]interface{} {
	critical, warning := ae.Counts()
	return map[string]interface{}{
		"critical_total":    critical,
		"warning_total":     warning,
		"active_incidents":  ae.ActiveCount(),
		"total_alerts":      critical + warning,
	}
}

func (ae *AlertEngine) evaluate(m Metrics) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	a := ae.cfg.Alerts
	cooldown := time.Duration(a.CooldownSeconds) * time.Second

	checks := []struct {
		key      string
		metric   string
		value    float64
		critical float64
		warning  float64
	}{
		{"cpu", "CPU Usage", m.CPU, a.CPUCritical, a.CPUWarning},
		{"mem", "Memory Usage", m.Memory, a.MemoryCritical, a.MemoryWarning},
		{"lat", "Latency", m.Latency, a.LatencyCriticalMs, a.LatencyWarningMs},
		{"err", "Error Rate", m.ErrorRate, a.ErrorCritical, a.ErrorWarning},
	}

	for _, c := range checks {
		active, hasActive := ae.activeAlerts[c.key]

		// --- Resolution path: metric dropped below warning ---
		if c.value < c.warning {
			if hasActive && active.resolvedAt.IsZero() {
				// Open incident just cleared.
				now := time.Now()
				dur := int(now.Sub(active.firedAt).Seconds())
				slog.Info("alert resolved",
					"id", active.id,
					"metric", active.metric,
					"duration_seconds", dur,
				)
				if ae.store != nil {
					ae.store.ResolveAlert(active.id, now)
				}
				ae.hub.broadcastJSON(WSMessage{
					Type: "alert_resolved",
					Payload: map[string]interface{}{
						"alert_id":         active.id,
						"metric":           active.metric,
						"duration_seconds": dur,
						"resolved_at":      now.UnixMilli(),
					},
				})
				active.resolvedAt = now
				ae.activeAlerts[c.key] = active
			} else if hasActive && !active.resolvedAt.IsZero() && time.Since(active.resolvedAt) >= cooldown {
				// Post-resolution cooldown expired — clean up entry.
				delete(ae.activeAlerts, c.key)
			}
			continue
		}

		// --- Firing path: metric is at or above warning ---
		var severity string
		var threshold float64
		if c.value >= c.critical {
			severity = SeverityCritical
			threshold = c.critical
		} else {
			severity = SeverityWarning
			threshold = c.warning
		}

		if hasActive {
			if active.resolvedAt.IsZero() {
				// Incident already open — do not re-fire.
				continue
			}
			if time.Since(active.resolvedAt) < cooldown {
				// Still within post-resolution cooldown — do not re-fire.
				continue
			}
			// Cooldown expired while metric is still elevated — treat as new incident.
			delete(ae.activeAlerts, c.key)
		}

		unit := "%"
		if c.key == "lat" {
			unit = "ms"
		}

		now := time.Now()
		alert := Alert{
			ID:        nextAlertID(),
			Type:      "alert",
			Metric:    c.metric,
			Value:     c.value,
			Threshold: threshold,
			Severity:  severity,
			Message:   fmt.Sprintf("%s is %.2f%s (threshold: %.0f%s)", c.metric, c.value, unit, threshold, unit),
			Timestamp: now.UnixMilli(),
		}

		ae.activeAlerts[c.key] = activeAlert{
			id:      alert.ID,
			metric:  c.metric,
			firedAt: now,
		}

		if severity == SeverityCritical {
			ae.criticalCount.Add(1)
		} else {
			ae.warningCount.Add(1)
		}

		slog.Warn("alert fired",
			"id", alert.ID,
			"metric", alert.Metric,
			"severity", alert.Severity,
			"value", alert.Value,
			"threshold", alert.Threshold,
		)
		if ae.store != nil {
			ae.store.SaveAlert(alert)
		}
		ae.hub.broadcastJSON(WSMessage{Type: "alert", Payload: alert})

		go ae.analyzeWithClaude(alert, m)
	}
}

func (ae *AlertEngine) analyzeWithClaude(alert Alert, m Metrics) {
	if ae.claudeAPI == nil {
		return
	}
	resp, err := ae.claudeAPI.Analyze(alert, m)
	if err != nil {
		slog.Error("AI analysis failed", "alert_id", alert.ID, "err", err)
		ae.hub.broadcastJSON(WSMessage{
			Type: "ai_response",
			Payload: map[string]interface{}{
				"alert_id": alert.ID,
				"error":    err.Error(),
			},
		})
		return
	}
	slog.Info("AI analysis ready", "alert_id", alert.ID, "confidence", resp.Confidence)
	if ae.store != nil {
		ae.store.UpdateAIResponse(resp.AlertID, resp.RootCause, resp.Remediation, resp.Confidence)
	}
	ae.hub.broadcastJSON(WSMessage{
		Type:    "ai_response",
		Payload: resp,
	})
}

// Run evaluates metrics every 2 seconds.
func (ae *AlertEngine) Run() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ae.evaluate(ae.sim.Current())
	}
}

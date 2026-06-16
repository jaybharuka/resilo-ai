package main

import (
	"fmt"
	"log/slog"
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

// Thresholds defines the alert trigger points.
var Thresholds = struct {
	CPUCritical    float64
	CPUWarning     float64
	MemCritical    float64
	MemWarning     float64
	LatCritical    float64
	LatWarning     float64
	ErrCritical    float64
	ErrWarning     float64
}{
	CPUCritical: 85,
	CPUWarning:  70,
	MemCritical: 80,
	MemWarning:  65,
	LatCritical: 1500,
	LatWarning:  800,
	ErrCritical: 10,
	ErrWarning:  5,
}

var alertSeq int

func nextAlertID() string {
	alertSeq++
	return fmt.Sprintf("ALT-%04d", alertSeq)
}

// AlertEngine evaluates metrics and fires alerts.
type AlertEngine struct {
	hub       *Hub
	sim       *Simulator
	claudeAPI *ClaudeClient
	store     *Store

	// cooldown tracks last-fired time per metric to avoid storms
	cooldown map[string]time.Time
}

func newAlertEngine(hub *Hub, sim *Simulator, claude *ClaudeClient, store *Store) *AlertEngine {
	return &AlertEngine{
		hub:       hub,
		sim:       sim,
		claudeAPI: claude,
		store:     store,
		cooldown:  make(map[string]time.Time),
	}
}

func (ae *AlertEngine) cooldownOK(key string, dur time.Duration) bool {
	last, ok := ae.cooldown[key]
	if !ok || time.Since(last) > dur {
		ae.cooldown[key] = time.Now()
		return true
	}
	return false
}

func (ae *AlertEngine) evaluate(m Metrics) {
	checks := []struct {
		key       string
		metric    string
		value     float64
		critical  float64
		warning   float64
	}{
		{"cpu", "CPU Usage", m.CPU, Thresholds.CPUCritical, Thresholds.CPUWarning},
		{"mem", "Memory Usage", m.Memory, Thresholds.MemCritical, Thresholds.MemWarning},
		{"lat", "Latency", m.Latency, Thresholds.LatCritical, Thresholds.LatWarning},
		{"err", "Error Rate", m.ErrorRate, Thresholds.ErrCritical, Thresholds.ErrWarning},
	}

	for _, c := range checks {
		var severity string
		var threshold float64
		if c.value >= c.critical {
			severity = SeverityCritical
			threshold = c.critical
		} else if c.value >= c.warning {
			severity = SeverityWarning
			threshold = c.warning
		} else {
			continue
		}

		cooldownDur := 30 * time.Second
		if severity == SeverityWarning {
			cooldownDur = 60 * time.Second
		}
		if !ae.cooldownOK(c.key+severity, cooldownDur) {
			continue
		}

		unit := "%"
		if c.key == "lat" {
			unit = "ms"
		}

		alert := Alert{
			ID:        nextAlertID(),
			Type:      "alert",
			Metric:    c.metric,
			Value:     c.value,
			Threshold: threshold,
			Severity:  severity,
			Message:   fmt.Sprintf("%s is %.2f%s (threshold: %.0f%s)", c.metric, c.value, unit, threshold, unit),
			Timestamp: time.Now().UnixMilli(),
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

		// Async Claude analysis
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

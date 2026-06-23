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

// IncidentCategory classifies the type of production incident.
type IncidentCategory string

const (
	ResourceExhaustion   IncidentCategory = "ResourceExhaustion"
	DeploymentRegression IncidentCategory = "DeploymentRegression"
	DependencyFailure    IncidentCategory = "DependencyFailure"
	NetworkIssue         IncidentCategory = "NetworkIssue"
	Unknown              IncidentCategory = "Unknown"
)

// ClassifyIncident determines the incident category from the current metrics.
// Pure logic, no AI, designed to run on every alert evaluation cycle.
func ClassifyIncident(m Metrics) IncidentCategory {
	if m.CPU > 85 || m.Memory > 85 {
		return ResourceExhaustion
	}
	if m.Latency > 500 && m.CPU < 65 && m.Memory < 75 {
		return NetworkIssue
	}
	if m.ErrorRate > 5 && m.CPU < 70 && m.Memory < 75 {
		return DependencyFailure
	}
	if m.ErrorRate > 2 {
		return DeploymentRegression
	}
	return Unknown
}

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
	category   IncidentCategory
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
	// Validate alert thresholds
	if err := validateAlertConfig(cfg.Alerts); err != nil {
		slog.Error("invalid alert configuration", "err", err)
	}
	
	return &AlertEngine{
		hub:          hub,
		sim:          sim,
		claudeAPI:    claude,
		store:        store,
		cfg:          cfg,
		activeAlerts: make(map[string]activeAlert),
	}
}

// validateAlertConfig checks that alert thresholds are sensible.
func validateAlertConfig(a AlertsConfig) error {
	if a.CPUWarning <= 0 || a.CPUWarning > 100 {
		return fmt.Errorf("CPU warning threshold must be between 0-100, got %f", a.CPUWarning)
	}
	if a.CPUCritical <= 0 || a.CPUCritical > 100 {
		return fmt.Errorf("CPU critical threshold must be between 0-100, got %f", a.CPUCritical)
	}
	if a.CPUCritical <= a.CPUWarning {
		return fmt.Errorf("CPU critical threshold (%f) must be greater than warning (%f)", a.CPUCritical, a.CPUWarning)
	}
	
	if a.MemoryWarning <= 0 || a.MemoryWarning > 100 {
		return fmt.Errorf("Memory warning threshold must be between 0-100, got %f", a.MemoryWarning)
	}
	if a.MemoryCritical <= 0 || a.MemoryCritical > 100 {
		return fmt.Errorf("Memory critical threshold must be between 0-100, got %f", a.MemoryCritical)
	}
	if a.MemoryCritical <= a.MemoryWarning {
		return fmt.Errorf("Memory critical threshold (%f) must be greater than warning (%f)", a.MemoryCritical, a.MemoryWarning)
	}
	
	if a.LatencyWarningMs <= 0 {
		return fmt.Errorf("Latency warning threshold must be positive, got %f", a.LatencyWarningMs)
	}
	if a.LatencyCriticalMs <= 0 {
		return fmt.Errorf("Latency critical threshold must be positive, got %f", a.LatencyCriticalMs)
	}
	if a.LatencyCriticalMs <= a.LatencyWarningMs {
		return fmt.Errorf("Latency critical threshold (%f) must be greater than warning (%f)", a.LatencyCriticalMs, a.LatencyWarningMs)
	}
	
	if a.ErrorWarning <= 0 || a.ErrorWarning > 100 {
		return fmt.Errorf("Error warning threshold must be between 0-100, got %f", a.ErrorWarning)
	}
	if a.ErrorCritical <= 0 || a.ErrorCritical > 100 {
		return fmt.Errorf("Error critical threshold must be between 0-100, got %f", a.ErrorCritical)
	}
	if a.ErrorCritical <= a.ErrorWarning {
		return fmt.Errorf("Error critical threshold (%f) must be greater than warning (%f)", a.ErrorCritical, a.ErrorWarning)
	}
	
	if a.CooldownSeconds <= 0 {
		return fmt.Errorf("Cooldown seconds must be positive, got %d", a.CooldownSeconds)
	}
	
	return nil
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
	// Classify and fetch similar incidents outside the lock — these may do I/O.
	category := ClassifyIncident(m)

	var similar []PastIncident
	if ae.store != nil {
		similar, _ = ae.store.FindSimilarIncidents(category, 5)
	}

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
				now := time.Now()
				dur := int(now.Sub(active.firedAt).Seconds())
				slog.Info("alert resolved",
					"id", active.id,
					"metric", active.metric,
					"duration_seconds", dur,
				)
				if ae.store != nil {
					ae.store.ResolveAlert(active.id, now)
					ae.store.ResolveIncident(active.id, now)
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
				continue // incident already open — do not re-fire
			}
			if time.Since(active.resolvedAt) < cooldown {
				continue // within post-resolution cooldown
			}
			delete(ae.activeAlerts, c.key) // cooldown expired, new incident
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
			id:       alert.ID,
			metric:   c.metric,
			category: category,
			firedAt:  now,
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
			"category", category,
			"similar_past", len(similar),
		)

		if ae.store != nil {
			ae.store.SaveAlert(alert)
			ae.store.SaveIncident(Incident{
				ID:       alert.ID,
				Category: category,
				Severity: severity,
				CPU:      m.CPU,
				Memory:   m.Memory,
				Latency:  m.Latency,
				ErrorRate: m.ErrorRate,
				FiredAt:  now,
			})
		}

		ae.hub.broadcastJSON(WSMessage{
			Type: "alert",
			Payload: map[string]interface{}{
				"id":           alert.ID,
				"type":         alert.Type,
				"metric":       alert.Metric,
				"value":        alert.Value,
				"threshold":    alert.Threshold,
				"severity":     alert.Severity,
				"message":      alert.Message,
				"timestamp":    alert.Timestamp,
				"category":     string(category),
				"similar_count": len(similar),
			},
		})

		go ae.analyzeWithClaude(alert, m, category, similar)
	}
}

func (ae *AlertEngine) analyzeWithClaude(alert Alert, m Metrics, category IncidentCategory, similar []PastIncident) {
	if ae.claudeAPI == nil {
		ae.hub.broadcastJSON(WSMessage{
			Type: "ai_response",
			Payload: map[string]interface{}{
				"alert_id":         alert.ID,
				"category":         string(category),
				"similar_count":    len(similar),
				"model":            "disabled",
				"root_cause":       "AI analysis is disabled — no API key configured.",
				"immediate_action": "Check system metrics manually.",
				"verification":     "Monitor metrics until they return to baseline.",
				"prevention":       "Configure ANTHROPIC_API_KEY or NVIDIA_API_KEY.",
				"confidence":       "none",
				"timestamp":        time.Now().UnixMilli(),
			},
		})
		return
	}

	resp, err := ae.claudeAPI.AnalyzeIncident(alert, m, category, similar)
	if err != nil {
		slog.Error("AI analysis failed", "alert_id", alert.ID, "err", err)
		ae.hub.broadcastJSON(WSMessage{
			Type: "ai_response",
			Payload: map[string]interface{}{
				"alert_id":         alert.ID,
				"category":         string(category),
				"similar_count":    len(similar),
				"model":            ae.claudeAPI.getModel(),
				"root_cause":       fmt.Sprintf("AI analysis failed: %s", err.Error()),
				"immediate_action": "Check system metrics manually.",
				"verification":     "Monitor metrics until they return to baseline.",
				"prevention":       "Investigate the AI provider configuration.",
				"confidence":       "none",
				"timestamp":        time.Now().UnixMilli(),
			},
		})
		return
	}

	slog.Info("AI analysis ready",
		"alert_id", alert.ID,
		"category", category,
		"similar_count", len(similar),
		"confidence", resp.Confidence,
	)

	if ae.store != nil {
		ae.store.UpdateAIResponse(resp.AlertID, resp.RootCause, resp.Remediation, resp.Confidence)
		// Update incident row with AI fields.
		ae.store.SaveIncident(Incident{
			ID:              alert.ID,
			Category:        category,
			Severity:        alert.Severity,
			CPU:             m.CPU,
			Memory:          m.Memory,
			Latency:         m.Latency,
			ErrorRate:       m.ErrorRate,
			RootCause:       resp.RootCause,
			ImmediateAction: resp.ImmediateAction,
			Verification:    resp.Verification,
			Prevention:      resp.Prevention,
			FiredAt:         time.UnixMilli(alert.Timestamp),
		})
	}

	ae.hub.broadcastJSON(WSMessage{Type: "ai_response", Payload: resp})
}

// Run evaluates metrics every 2 seconds.
func (ae *AlertEngine) Run() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ae.evaluate(ae.sim.Current())
	}
}

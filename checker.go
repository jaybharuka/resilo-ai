package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Checker runs periodic HTTP checks against all enabled monitors and
// triggers AI analysis + email alerts when status flips UP↔DOWN.
type Checker struct {
	store      *Store
	claude     *ClaudeClient
	mailer     *Mailer
	lastCheck  sync.Map // monitorID -> time.Time
	prevStatus sync.Map // monitorID -> "up" | "down"
	downSince  sync.Map // monitorID -> time.Time (when it went down)
	client     *http.Client
}

func newChecker(store *Store, claude *ClaudeClient, mailer *Mailer) *Checker {
	return &Checker{
		store:  store,
		claude: claude,
		mailer: mailer,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Run ticks every 10 seconds and fires checks for monitors that are due.
func (c *Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick()
		}
	}
}

func (c *Checker) tick() {
	monitors, err := c.store.GetAllEnabledMonitors()
	if err != nil {
		slog.Error("checker: failed to load monitors", "err", err)
		return
	}
	for _, m := range monitors {
		m := m
		if c.isDue(m) {
			go c.check(m)
		}
	}
}

func (c *Checker) isDue(m Monitor) bool {
	val, ok := c.lastCheck.Load(m.ID)
	if !ok {
		return true
	}
	return time.Since(val.(time.Time)) >= time.Duration(m.IntervalSeconds)*time.Second
}

func (c *Checker) check(m Monitor) {
	c.lastCheck.Store(m.ID, time.Now())

	start := time.Now()
	resp, httpErr := c.client.Get(m.URL)
	latencyMs := int(time.Since(start).Milliseconds())

	// Build a MonitorResult for downstream use.
	var result MonitorResult
	result.MonitorID = m.ID
	result.LatencyMs = &latencyMs

	if httpErr != nil {
		errStr := httpErr.Error()
		zero := 0
		result.StatusCode = &zero
		result.Error = &errStr
		slog.Info("checker: down", "name", m.Name, "err", errStr)
	} else {
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		sc := resp.StatusCode
		result.StatusCode = &sc
		slog.Info("checker: checked", "name", m.Name, "status", sc, "latency_ms", latencyMs)
	}

	// Persist the raw check result.
	errStr := ""
	if result.Error != nil {
		errStr = *result.Error
	}
	sc := 0
	if result.StatusCode != nil {
		sc = *result.StatusCode
	}
	if saveErr := c.store.SaveResult(m.ID, sc, latencyMs, errStr); saveErr != nil {
		slog.Error("checker: save result failed", "monitor_id", m.ID, "err", saveErr)
	}

	// Determine current and previous status.
	newStatus := "down"
	if result.StatusCode != nil && *result.StatusCode >= 200 && *result.StatusCode < 300 {
		newStatus = "up"
	}

	prevVal, _ := c.prevStatus.Load(m.ID)
	prevStatus, _ := prevVal.(string)
	c.prevStatus.Store(m.ID, newStatus)

	switch {
	case prevStatus != "down" && newStatus == "down":
		// Transition UP→DOWN (or first-ever check that fails).
		c.downSince.Store(m.ID, time.Now())
		go c.analyzeOutage(m, result)

	case prevStatus == "down" && newStatus == "up":
		// Transition DOWN→UP.
		downDur := time.Duration(0)
		if dsVal, ok := c.downSince.Load(m.ID); ok {
			downDur = time.Since(dsVal.(time.Time))
			c.downSince.Delete(m.ID)
		}
		go c.handleRecovery(m, downDur)
	}
}

// analyzeOutage creates an outage record, runs AI analysis, then emails the owner.
// Runs in its own goroutine so the checker tick loop is not blocked.
func (c *Checker) analyzeOutage(m Monitor, result MonitorResult) {
	outage, err := c.store.CreateOutage(m.ID, result)
	if err != nil {
		slog.Error("checker: create outage failed", "monitor_id", m.ID, "err", err)
		return
	}

	rootCause := "Analysis unavailable — no AI provider configured."
	remediation := ""

	if c.claude != nil {
		rc, rem, aiErr := c.claude.AnalyzeOutage(m, result)
		if aiErr != nil {
			slog.Error("checker: AI outage analysis failed", "monitor_id", m.ID, "err", aiErr)
		} else {
			rootCause = rc
			remediation = rem
			if updateErr := c.store.UpdateOutageAnalysis(outage.ID, rootCause, remediation); updateErr != nil {
				slog.Error("checker: update outage analysis failed", "outage_id", outage.ID, "err", updateErr)
			}
		}
	}

	user, err := c.store.GetUserByMonitorID(m.ID)
	if err != nil {
		slog.Error("checker: get user by monitor failed", "monitor_id", m.ID, "err", err)
		return
	}

	if c.mailer != nil && user.Email != "" {
		latencyMs := 0
		if result.LatencyMs != nil {
			latencyMs = *result.LatencyMs
		}
		if mailErr := c.mailer.SendDownAlert(user.Email, m.Name, m.URL, rootCause, remediation, latencyMs); mailErr != nil {
			slog.Error("checker: send down alert failed", "to", user.Email, "err", mailErr)
		} else {
			slog.Info("checker: down alert sent", "to", user.Email, "monitor", m.Name)
		}
	}
}

// handleRecovery resolves the outage record and emails the owner.
// Runs in its own goroutine.
func (c *Checker) handleRecovery(m Monitor, downDur time.Duration) {
	if err := c.store.ResolveOutage(m.ID, time.Now()); err != nil {
		slog.Error("checker: resolve outage failed", "monitor_id", m.ID, "err", err)
	}

	user, err := c.store.GetUserByMonitorID(m.ID)
	if err != nil {
		slog.Error("checker: get user by monitor failed", "monitor_id", m.ID, "err", err)
		return
	}

	if c.mailer != nil && user.Email != "" {
		if mailErr := c.mailer.SendRecoveryAlert(user.Email, m.Name, m.URL, downDur); mailErr != nil {
			slog.Error("checker: send recovery alert failed", "to", user.Email, "err", mailErr)
		} else {
			slog.Info("checker: recovery alert sent", "to", user.Email, "monitor", m.Name, "down_dur", downDur)
		}
	}
}

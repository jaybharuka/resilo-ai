package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"
)

// Checker runs periodic HTTP checks against all enabled monitors and
// triggers AI analysis + email alerts when status flips UP↔DOWN.
type Checker struct {
	store         *Store
	claude        *ClaudeClient
	mailer        *Mailer
	lastCheck     sync.Map // monitorID -> time.Time
	prevStatus    sync.Map // monitorID -> "up" | "down"
	downSince     sync.Map // monitorID -> time.Time (when it went down)
	sslAlertSent  sync.Map // monitorID -> "warning" | "critical" (last SSL alert level sent)
	client        *http.Client
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

	// Capture per-phase timing via httptrace.
	var (
		dnsStart  time.Time
		dnsMs     = -1
		connStart time.Time
		connMs    = -1
		reqSent   time.Time
		ttfbMs    = -1
	)
	trace := &httptrace.ClientTrace{
		DNSStart:     func(_ httptrace.DNSStartInfo)      { dnsStart = time.Now() },
		DNSDone:      func(_ httptrace.DNSDoneInfo)       { dnsMs = int(time.Since(dnsStart).Milliseconds()) },
		ConnectStart: func(_, _ string)                   { connStart = time.Now() },
		ConnectDone:  func(_, _ string, _ error)          { connMs = int(time.Since(connStart).Milliseconds()) },
		WroteRequest: func(_ httptrace.WroteRequestInfo)  { reqSent = time.Now() },
		GotFirstResponseByte: func()                      { ttfbMs = int(time.Since(reqSent).Milliseconds()) },
	}

	req, reqErr := http.NewRequestWithContext(
		httptrace.WithClientTrace(context.Background(), trace),
		http.MethodGet, m.URL, nil,
	)

	// Build a MonitorResult for downstream use.
	var result MonitorResult
	result.MonitorID = m.ID

	start := time.Now()

	if reqErr != nil {
		// Malformed URL — treat as immediate down.
		latencyMs := 0
		result.LatencyMs = &latencyMs
		errStr := reqErr.Error()
		zero := 0
		result.StatusCode = &zero
		result.Error = &errStr
		slog.Error("checker: bad request", "name", m.Name, "err", errStr)
	} else {
		resp, httpErr := c.client.Do(req)
		latencyMs := int(time.Since(start).Milliseconds())
		result.LatencyMs = &latencyMs

		if httpErr != nil {
			errStr := httpErr.Error()
			zero := 0
			result.StatusCode = &zero
			result.Error = &errStr
			// Timing data unreliable on connection failure — keep -1 sentinels.
			slog.Info("checker: down", "name", m.Name, "err", errStr)
		} else {
			defer resp.Body.Close()
			sc := resp.StatusCode
			result.StatusCode = &sc

			// For keyword checks we need the body; otherwise discard it.
			// Always cap at 500 KB to avoid memory spikes.
			const maxBodyBytes = 500 * 1024
			if m.Keyword != "" && sc >= 200 && sc < 300 {
				bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
				if !strings.Contains(strings.ToLower(string(bodyBytes)), strings.ToLower(m.Keyword)) {
					zero := 0
					result.StatusCode = &zero
					errMsg := fmt.Sprintf("keyword not found: %s", m.Keyword)
					result.Error = &errMsg
					slog.Info("checker: keyword missing", "name", m.Name, "keyword", m.Keyword)
				}
			} else {
				io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
			}

			if result.Error == nil {
				slog.Info("checker: checked", "name", m.Name, "status", sc,
					"latency_ms", latencyMs, "dns_ms", dnsMs, "connect_ms", connMs, "ttfb_ms", ttfbMs)
			}

			// Extract TLS certificate expiry for HTTPS monitors.
			if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
				expiry := resp.TLS.PeerCertificates[0].NotAfter
				if err := c.store.UpdateSSLExpiry(m.ID, &expiry); err != nil {
					slog.Error("checker: update ssl_expiry failed", "monitor_id", m.ID, "err", err)
				}
				c.checkSSLExpiry(m, expiry)
			}
		}
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
	latencyMs := 0
	if result.LatencyMs != nil {
		latencyMs = *result.LatencyMs
	}
	if saveErr := c.store.SaveResult(m.ID, sc, latencyMs, dnsMs, connMs, ttfbMs, errStr); saveErr != nil {
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

// checkSSLExpiry fires a warning or critical alert email when a cert is close to expiry.
// Tracks the last alert level sent per monitor to avoid repeated emails.
func (c *Checker) checkSSLExpiry(m Monitor, expiry time.Time) {
	daysLeft := int(time.Until(expiry).Hours() / 24)

	var alertLevel string
	switch {
	case daysLeft <= 1:
		alertLevel = "critical"
	case daysLeft <= 7:
		alertLevel = "warning"
	default:
		// Cert is healthy — reset so future threshold crossings re-alert.
		c.sslAlertSent.Delete(m.ID)
		return
	}

	prevVal, _ := c.sslAlertSent.Load(m.ID)
	prevLevel, _ := prevVal.(string)

	// Send if we haven't sent this level yet, or if we're escalating to critical.
	if prevLevel == alertLevel || (alertLevel == "warning" && prevLevel == "critical") {
		return
	}
	c.sslAlertSent.Store(m.ID, alertLevel)
	go c.sendSSLAlert(m, expiry, alertLevel == "critical")
}

func (c *Checker) sendSSLAlert(m Monitor, expiry time.Time, critical bool) {
	user, err := c.store.GetUserByMonitorID(m.ID)
	if err != nil {
		slog.Error("checker: get user for ssl alert failed", "monitor_id", m.ID, "err", err)
		return
	}
	if c.mailer == nil || user.Email == "" {
		return
	}
	level := "warning"
	if critical {
		level = "critical"
	}
	if err := c.mailer.SendSSLExpiryAlert(user.Email, m.Name, m.URL, expiry, critical); err != nil {
		slog.Error("checker: send ssl alert failed", "to", user.Email, "err", err)
	} else {
		slog.Info("checker: ssl alert sent", "to", user.Email, "monitor", m.Name, "level", level)
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

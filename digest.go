package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// DigestRunner sends the weekly health digest email every Monday at 09:00 UTC.
type DigestRunner struct {
	store  *Store
	claude *ClaudeClient
	mailer *Mailer
	// lastFired tracks the last Monday date we already sent the digest,
	// so we send exactly once even if the ticker fires multiple times in the same minute.
	lastFired string // "2006-01-02" formatted date of last send
}

func newDigestRunner(store *Store, claude *ClaudeClient, mailer *Mailer) *DigestRunner {
	return &DigestRunner{store: store, claude: claude, mailer: mailer}
}

// Run ticks every minute and fires the digest when it's Monday 09:00 UTC.
func (d *DigestRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			utc := t.UTC()
			if utc.Weekday() == time.Monday && utc.Hour() == 9 && utc.Minute() == 0 {
				today := utc.Format("2006-01-02")
				if d.lastFired != today {
					d.lastFired = today
					go d.send()
				}
			}
		}
	}
}

// MonitorSection is one monitor's stats + AI narrative for the weekly digest email.
type MonitorSection struct {
	Stats     MonitorDigestStats
	Narrative string
}

// MonitorDigestStats holds the gathered metrics for one monitor.
type MonitorDigestStats struct {
	Monitor         Monitor
	Uptime7d        float64 // 0–100
	AvgLatency7d    float64 // ms, 0 if no data
	AvgLatencyPrev  float64 // ms for days 8–14, 0 if no data
	LatencyChangePct float64 // positive = slower
	Outages7d       int
	KeywordFails7d  int
	SSLDaysLeft     int     // -1 if no SSL data
}

func (d *DigestRunner) send() {
	slog.Info("digest: starting weekly send")

	users, err := d.store.GetAllUsersWithMonitors()
	if err != nil {
		slog.Error("digest: GetAllUsersWithMonitors", "err", err)
		return
	}
	if len(users) == 0 {
		slog.Info("digest: no users with monitors, skipping")
		return
	}

	for _, user := range users {
		d.sendForUser(user)
	}
	slog.Info("digest: weekly send complete", "users", len(users))
}

func (d *DigestRunner) sendForUser(user User) {
	monitors, err := d.store.GetMonitorsByUser(user.ID)
	if err != nil {
		slog.Error("digest: GetMonitorsByUser", "user_id", user.ID, "err", err)
		return
	}
	if len(monitors) == 0 {
		return
	}

	// Gather stats for each monitor.
	var stats []MonitorDigestStats
	for _, m := range monitors {
		if !m.Enabled {
			continue
		}
		s := MonitorDigestStats{Monitor: m, SSLDaysLeft: -1}

		s.Uptime7d, _ = d.store.GetUptimePercent(m.ID, 7)
		s.AvgLatency7d, _ = d.store.GetAvgLatencyRange(m.ID, 7, 0)
		s.AvgLatencyPrev, _ = d.store.GetAvgLatencyRange(m.ID, 14, 7)
		if s.AvgLatencyPrev > 0 && s.AvgLatency7d > 0 {
			s.LatencyChangePct = (s.AvgLatency7d - s.AvgLatencyPrev) / s.AvgLatencyPrev * 100
		}
		s.Outages7d, _ = d.store.GetOutageCountRange(m.ID, 7)
		s.KeywordFails7d, _ = d.store.GetKeywordFailureCount(m.ID, 7)

		if m.SSLExpiry != nil && *m.SSLExpiry != "" {
			if exp, err := time.Parse(time.RFC3339, *m.SSLExpiry); err == nil {
				s.SSLDaysLeft = int(time.Until(exp).Hours() / 24)
			}
		}

		stats = append(stats, s)
	}

	if len(stats) == 0 {
		return
	}

	sections := make([]MonitorSection, len(stats))
	for i, s := range stats {
		sections[i] = MonitorSection{Stats: s, Narrative: d.generateNarrative(s)}
	}

	if err := d.mailer.SendWeeklyDigest(user.Email, sections); err != nil {
		slog.Error("digest: send failed", "to", user.Email, "err", err)
	} else {
		slog.Info("digest: sent", "to", user.Email, "monitors", len(sections))
	}
}

// generateNarrative asks the AI for a one-paragraph interpretation of a monitor's weekly stats.
// Falls back to a rule-based summary if AI is unavailable.
func (d *DigestRunner) generateNarrative(s MonitorDigestStats) string {
	if d.claude != nil {
		text, err := d.aiNarrative(s)
		if err == nil && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
		slog.Warn("digest: AI narrative failed, using fallback", "monitor", s.Monitor.Name, "err", err)
	}
	return d.ruleNarrative(s)
}

func (d *DigestRunner) aiNarrative(s MonitorDigestStats) (string, error) {
	latencyTrend := "stable"
	if s.AvgLatencyPrev > 0 {
		switch {
		case s.LatencyChangePct > 20:
			latencyTrend = fmt.Sprintf("increased by %.0f%%", s.LatencyChangePct)
		case s.LatencyChangePct < -20:
			latencyTrend = fmt.Sprintf("decreased by %.0f%%", -s.LatencyChangePct)
		}
	}

	sslNote := ""
	if s.SSLDaysLeft >= 0 {
		sslNote = fmt.Sprintf("SSL certificate expires in %d days.", s.SSLDaysLeft)
	}

	kwNote := ""
	if s.Monitor.Keyword != "" && s.KeywordFails7d > 0 {
		kwNote = fmt.Sprintf("Keyword '%s' was missing in %d check(s).", s.Monitor.Keyword, s.KeywordFails7d)
	}

	prompt := fmt.Sprintf(`You are an SRE writing a friendly weekly health summary for a non-technical user.

Monitor: %s (%s)
7-day uptime: %.2f%%
Outages this week: %d
Average response time this week: %.0fms (trend: %s vs last week)
%s
%s

Write ONE concise paragraph (3–5 sentences) interpreting these numbers. Be specific about what the numbers mean, suggest a likely cause if something is off, and give one actionable tip. Do not use bullet points. Do not use markdown. Plain prose only.`,
		s.Monitor.Name, s.Monitor.URL,
		s.Uptime7d,
		s.Outages7d,
		s.AvgLatency7d, latencyTrend,
		sslNote, kwNote,
	)

	var text string
	var err error
	if d.claude.provider == "anthropic" {
		text, err = d.claude.callAnthropic(prompt)
	} else {
		text, err = d.claude.callNVIDIA(prompt)
	}
	return text, err
}

// ruleNarrative builds a plain-English summary without AI.
func (d *DigestRunner) ruleNarrative(s MonitorDigestStats) string {
	var parts []string

	if s.Uptime7d >= 99.9 {
		parts = append(parts, fmt.Sprintf("%s had excellent uptime this week at %.2f%%.", s.Monitor.Name, s.Uptime7d))
	} else if s.Uptime7d >= 99.0 {
		parts = append(parts, fmt.Sprintf("%s was mostly healthy with %.2f%% uptime.", s.Monitor.Name, s.Uptime7d))
	} else {
		parts = append(parts, fmt.Sprintf("%s had degraded availability at %.2f%% uptime — investigate recurring failures.", s.Monitor.Name, s.Uptime7d))
	}

	if s.Outages7d > 0 {
		parts = append(parts, fmt.Sprintf("There were %d outage(s) in the past 7 days.", s.Outages7d))
	}

	if s.AvgLatency7d > 0 {
		if s.AvgLatencyPrev > 0 {
			switch {
			case s.LatencyChangePct > 20:
				parts = append(parts, fmt.Sprintf("Average response time was %.0fms — up %.0f%% from last week, which may indicate increased load or slower backend queries.", s.AvgLatency7d, s.LatencyChangePct))
			case s.LatencyChangePct < -20:
				parts = append(parts, fmt.Sprintf("Average response time improved to %.0fms (down %.0f%% from last week).", s.AvgLatency7d, -s.LatencyChangePct))
			default:
				parts = append(parts, fmt.Sprintf("Average response time was %.0fms, consistent with last week.", s.AvgLatency7d))
			}
		} else {
			parts = append(parts, fmt.Sprintf("Average response time was %.0fms.", s.AvgLatency7d))
		}
	}

	if s.Monitor.Keyword != "" && s.KeywordFails7d > 0 {
		parts = append(parts, fmt.Sprintf("Keyword '%s' was missing in %d check(s) — the page content may have changed.", s.Monitor.Keyword, s.KeywordFails7d))
	}

	if s.SSLDaysLeft >= 0 && s.SSLDaysLeft <= 30 {
		parts = append(parts, fmt.Sprintf("SSL certificate expires in %d days — renew it soon.", s.SSLDaysLeft))
	}

	if len(parts) == 0 {
		return fmt.Sprintf("%s appears healthy with no notable issues this week.", s.Monitor.Name)
	}
	return strings.Join(parts, " ")
}

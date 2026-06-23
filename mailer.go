package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
	"time"
)

// Mailer sends transactional email via Gmail SMTP.
type Mailer struct {
	host     string
	port     int
	from     string
	password string
}

func newMailer(cfg *Config) *Mailer {
	if cfg.Mail.FromEmail == "" || cfg.Mail.AppPassword == "" {
		return nil
	}
	slog.Info("mailer configured", "from", cfg.Mail.FromEmail, "host", cfg.Mail.SMTPHost)
	return &Mailer{
		host:     cfg.Mail.SMTPHost,
		port:     cfg.Mail.SMTPPort,
		from:     cfg.Mail.FromEmail,
		password: cfg.Mail.AppPassword,
	}
}

func (m *Mailer) send(to, subject, htmlBody string) error {
	auth := smtp.PlainAuth("", m.from, m.password, m.host)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "From: Resilo AI <%s>\r\n", m.from)
	fmt.Fprintf(&buf, "To: %s\r\n", to)
	fmt.Fprintf(&buf, "Subject: %s\r\n", subject)
	fmt.Fprintf(&buf, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&buf, "Content-Type: text/html; charset=UTF-8\r\n")
	fmt.Fprintf(&buf, "\r\n")
	buf.WriteString(htmlBody)

	addr := fmt.Sprintf("%s:%d", m.host, m.port)
	return smtp.SendMail(addr, auth, m.from, []string{to}, buf.Bytes())
}

// SendDownAlert notifies a user that a monitor has gone DOWN with AI root cause.
func (m *Mailer) SendDownAlert(to, monitorName, url, rootCause, remediation string, latencyMs int) error {
	subject := fmt.Sprintf("🚨 Monitor Down: %s", monitorName)

	remHtml := ""
	for i, line := range splitRemediation(remediation) {
		remHtml += fmt.Sprintf(`<div style="padding:4px 0;"><span style="color:#58a6ff;font-weight:700;">%d.</span> %s</div>`, i+1, htmlEscape(line))
	}
	if remHtml == "" {
		remHtml = `<div style="color:#8b949e;">No remediation steps available.</div>`
	}

	body := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:20px;">
<div style="max-width:540px;margin:0 auto;">
  <div style="background:#f85149;padding:18px 24px;border-radius:8px 8px 0 0;">
    <h1 style="margin:0;font-size:18px;color:#fff;">🚨 Monitor Down</h1>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-top:none;border-radius:0 0 8px 8px;padding:24px;">
    <table style="width:100%%;border-collapse:collapse;margin-bottom:20px;">
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;width:90px;">Monitor</td><td style="font-weight:700;">%s</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">URL</td><td><a href="%s" style="color:#58a6ff;text-decoration:none;">%s</a></td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Latency</td><td>%dms</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Time</td><td>%s</td></tr>
    </table>
    <div style="font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:#8b949e;margin-bottom:8px;">Root Cause</div>
    <div style="background:#21262d;border-radius:6px;padding:12px 14px;font-size:13px;line-height:1.6;margin-bottom:16px;">%s</div>
    <div style="font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:#8b949e;margin-bottom:8px;">Remediation Steps</div>
    <div style="background:#21262d;border-radius:6px;padding:12px 14px;font-size:13px;line-height:1.8;">%s</div>
    <p style="margin-top:24px;font-size:11px;color:#8b949e;">Sent by <a href="https://resilo-ai.fly.dev" style="color:#58a6ff;">Resilo AI</a></p>
  </div>
</div>
</body></html>`,
		htmlEscape(monitorName), url, htmlEscape(url), latencyMs,
		time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		htmlEscape(rootCause), remHtml,
	)
	return m.send(to, subject, body)
}

// SendRecoveryAlert notifies a user that a monitor has come back UP.
func (m *Mailer) SendRecoveryAlert(to, monitorName, url string, downDuration time.Duration) error {
	subject := fmt.Sprintf("✅ Monitor Recovered: %s", monitorName)

	dur := formatDuration(downDuration)

	body := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:20px;">
<div style="max-width:540px;margin:0 auto;">
  <div style="background:#3fb950;padding:18px 24px;border-radius:8px 8px 0 0;">
    <h1 style="margin:0;font-size:18px;color:#0d1117;">✅ Monitor Recovered</h1>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-top:none;border-radius:0 0 8px 8px;padding:24px;">
    <table style="width:100%%;border-collapse:collapse;margin-bottom:20px;">
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;width:90px;">Monitor</td><td style="font-weight:700;">%s</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">URL</td><td><a href="%s" style="color:#58a6ff;text-decoration:none;">%s</a></td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Down For</td><td style="color:#f85149;font-weight:700;">%s</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Recovered At</td><td>%s</td></tr>
    </table>
    <div style="background:#21262d;border-radius:6px;padding:12px 14px;font-size:13px;color:#3fb950;">
      ✓ The monitor is responding normally again.
    </div>
    <p style="margin-top:24px;font-size:11px;color:#8b949e;">Sent by <a href="https://resilo-ai.fly.dev" style="color:#58a6ff;">Resilo AI</a></p>
  </div>
</div>
</body></html>`,
		htmlEscape(monitorName), url, htmlEscape(url), dur,
		time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
	)
	return m.send(to, subject, body)
}

// SendSSLExpiryAlert notifies a user that a monitor's TLS cert is near expiry.
func (m *Mailer) SendSSLExpiryAlert(to, monitorName, url string, expiresAt time.Time, critical bool) error {
	daysLeft := int(time.Until(expiresAt).Hours() / 24)
	emoji := "⚠️"
	level := "Warning"
	headerColor := "#d29922"
	if critical {
		emoji = "🚨"
		level = "Critical"
		headerColor = "#f85149"
	}
	subject := fmt.Sprintf("%s SSL Certificate Expiring: %s", emoji, monitorName)

	body := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:20px;">
<div style="max-width:540px;margin:0 auto;">
  <div style="background:%s;padding:18px 24px;border-radius:8px 8px 0 0;">
    <h1 style="margin:0;font-size:18px;color:#fff;">%s SSL Certificate %s</h1>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-top:none;border-radius:0 0 8px 8px;padding:24px;">
    <table style="width:100%%;border-collapse:collapse;margin-bottom:20px;">
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;width:90px;">Monitor</td><td style="font-weight:700;">%s</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">URL</td><td><a href="%s" style="color:#58a6ff;text-decoration:none;">%s</a></td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Expires</td><td style="color:%s;font-weight:700;">%s (%d day(s) left)</td></tr>
    </table>
    <div style="background:#21262d;border-radius:6px;padding:12px 14px;font-size:13px;line-height:1.6;">
      Renew the TLS certificate for <strong>%s</strong> before it expires to prevent connection errors.
    </div>
    <p style="margin-top:24px;font-size:11px;color:#8b949e;">Sent by <a href="https://resilo-ai.fly.dev" style="color:#58a6ff;">Resilo AI</a></p>
  </div>
</div>
</body></html>`,
		headerColor, emoji, level,
		htmlEscape(monitorName), url, htmlEscape(url),
		headerColor, expiresAt.UTC().Format("2006-01-02"), daysLeft,
		htmlEscape(url),
	)
	return m.send(to, subject, body)
}

// SendWeeklyDigest emails the Monday health summary for all of a user's monitors.
func (m *Mailer) SendWeeklyDigest(to string, sections []MonitorSection) error {
	now := time.Now().UTC()
	weekStr := now.AddDate(0, 0, -7).Format("Jan 2") + " – " + now.Format("Jan 2, 2006")
	subject := "Your Resilo weekly health digest — " + now.Format("Jan 2")

	monitorRows := ""
	for _, sec := range sections {
		s := sec.Stats
		m_ := s.Monitor

		uptimeColor := "#3fb950"
		if s.Uptime7d < 99.0 {
			uptimeColor = "#f85149"
		} else if s.Uptime7d < 99.9 {
			uptimeColor = "#d29922"
		}

		latencyTrend := ""
		if s.AvgLatencyPrev > 0 && s.AvgLatency7d > 0 {
			sign := "+"
			color := "#f85149"
			val := s.LatencyChangePct
			if val < 0 {
				sign = ""
				color = "#3fb950"
			}
			latencyTrend = fmt.Sprintf(` <span style="color:%s;font-size:11px;">(%s%.0f%%)</span>`, color, sign, val)
		}

		sslRow := ""
		if s.SSLDaysLeft >= 0 {
			sslColor := "#3fb950"
			if s.SSLDaysLeft <= 7 {
				sslColor = "#f85149"
			} else if s.SSLDaysLeft <= 30 {
				sslColor = "#d29922"
			}
			sslRow = fmt.Sprintf(`<tr><td style="color:#8b949e;padding:3px 0;font-size:12px;">SSL expires</td><td style="color:%s;font-weight:600;">%d days</td></tr>`, sslColor, s.SSLDaysLeft)
		}

		kwRow := ""
		if m_.Keyword != "" {
			kwColor := "#3fb950"
			kwText := "0 failures"
			if s.KeywordFails7d > 0 {
				kwColor = "#f85149"
				kwText = fmt.Sprintf("%d failure(s)", s.KeywordFails7d)
			}
			kwRow = fmt.Sprintf(`<tr><td style="color:#8b949e;padding:3px 0;font-size:12px;">Keyword checks</td><td style="color:%s;">%s</td></tr>`, kwColor, kwText)
		}

		latencyCell := "—"
		if s.AvgLatency7d > 0 {
			latencyCell = fmt.Sprintf("%.0fms%s", s.AvgLatency7d, latencyTrend)
		}

		monitorRows += fmt.Sprintf(`
  <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin-bottom:16px;">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
      <div>
        <div style="font-size:15px;font-weight:700;">%s</div>
        <div style="font-size:11px;color:#8b949e;margin-top:2px;"><a href="%s" style="color:#58a6ff;text-decoration:none;">%s</a></div>
      </div>
      <span style="color:%s;font-size:20px;font-weight:700;">%.2f%%</span>
    </div>
    <table style="width:100%%;border-collapse:collapse;margin-bottom:14px;">
      <tr><td style="color:#8b949e;padding:3px 0;font-size:12px;width:120px;">Uptime (7d)</td><td style="color:%s;font-weight:600;">%.2f%%</td></tr>
      <tr><td style="color:#8b949e;padding:3px 0;font-size:12px;">Outages</td><td style="color:%s;">%d</td></tr>
      <tr><td style="color:#8b949e;padding:3px 0;font-size:12px;">Avg latency</td><td>%s</td></tr>
      %s
      %s
    </table>
    <div style="background:#21262d;border-left:3px solid #58a6ff;border-radius:0 6px 6px 0;padding:12px 14px;font-size:13px;line-height:1.7;color:#e6edf3;">
      %s
    </div>
  </div>`,
			htmlEscape(m_.Name), m_.URL, htmlEscape(m_.URL),
			uptimeColor, s.Uptime7d,
			uptimeColor, s.Uptime7d,
			func() string {
				if s.Outages7d == 0 {
					return "#3fb950"
				}
				return "#f85149"
			}(), s.Outages7d,
			latencyCell,
			sslRow, kwRow,
			htmlEscape(sec.Narrative),
		)
	}

	body := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:20px;">
<div style="max-width:580px;margin:0 auto;">
  <div style="font-size:20px;font-weight:700;color:#58a6ff;margin-bottom:4px;">ResiloAI</div>
  <div style="font-size:12px;color:#8b949e;margin-bottom:6px;">Weekly health digest</div>
  <div style="font-size:11px;color:#8b949e;margin-bottom:24px;">%s</div>
  %s
  <p style="margin-top:24px;font-size:11px;color:#8b949e;text-align:center;">
    <a href="https://resilo-ai.fly.dev/dashboard" style="color:#58a6ff;">View live dashboard</a> ·
    <a href="https://resilo-ai.fly.dev/incidents" style="color:#58a6ff;">Incident history</a><br><br>
    Sent by <a href="https://resilo-ai.fly.dev" style="color:#58a6ff;">Resilo AI</a> every Monday at 09:00 UTC
  </p>
</div>
</body></html>`, weekStr, monitorRows)

	return m.send(to, subject, body)
}

// SendPasswordReset emails a one-time password reset link.
func (m *Mailer) SendPasswordReset(to, resetURL string) error {
	subject := "Reset your Resilo password"
	body := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:20px;">
<div style="max-width:480px;margin:0 auto;">
  <div style="font-size:20px;font-weight:700;color:#58a6ff;margin-bottom:6px;">ResiloAI</div>
  <div style="font-size:12px;color:#8b949e;margin-bottom:28px;">AI-powered uptime monitoring</div>

  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:28px;">
    <h2 style="margin:0 0 12px;font-size:17px;font-weight:700;">Reset your password</h2>
    <p style="font-size:13px;color:#8b949e;margin:0 0 24px;line-height:1.6;">
      We received a request to reset your password. Click the button below to choose a new one.
    </p>
    <a href="%s"
       style="display:inline-block;background:#58a6ff;color:#0d1117;font-weight:700;font-size:14px;
              padding:11px 28px;border-radius:7px;text-decoration:none;margin-bottom:24px;">
      Reset password →
    </a>
    <p style="font-size:12px;color:#8b949e;margin:0;line-height:1.6;">
      This link expires in <strong style="color:#e6edf3;">1 hour</strong>.
      If you didn't request a password reset, you can safely ignore this email.
    </p>
  </div>
  <p style="margin-top:20px;font-size:11px;color:#8b949e;text-align:center;">
    Sent by <a href="https://resilo-ai.fly.dev" style="color:#58a6ff;">Resilo AI</a>
  </p>
</div>
</body></html>`, resetURL)
	return m.send(to, subject, body)
}

// SendLatencyAlert notifies a user that a monitor's response time exceeded the threshold.
func (m *Mailer) SendLatencyAlert(to, monitorName, url string, latencyMs, thresholdMs int) error {
	subject := fmt.Sprintf("⚡ Slow Response Detected: %s", monitorName)

	body := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;margin:0;padding:20px;">
<div style="max-width:540px;margin:0 auto;">
  <div style="background:#d29922;padding:18px 24px;border-radius:8px 8px 0 0;">
    <h1 style="margin:0;font-size:18px;color:#0d1117;">⚡ Slow Response Detected</h1>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-top:none;border-radius:0 0 8px 8px;padding:24px;">
    <table style="width:100%%;border-collapse:collapse;margin-bottom:20px;">
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;width:110px;">Monitor</td><td style="font-weight:700;">%s</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">URL</td><td><a href="%s" style="color:#58a6ff;text-decoration:none;">%s</a></td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Response Time</td><td style="color:#d29922;font-weight:700;">%dms</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Threshold</td><td>%dms</td></tr>
      <tr><td style="color:#8b949e;padding:5px 0;font-size:12px;">Detected At</td><td>%s</td></tr>
    </table>
    <div style="background:#21262d;border-radius:6px;padding:12px 14px;font-size:13px;line-height:1.6;color:#8b949e;">
      💡 Check the TTFB breakdown on the dashboard to identify whether the slowdown is in DNS resolution, TCP connect, or server processing time.
    </div>
    <p style="margin-top:24px;font-size:11px;color:#8b949e;">Sent by <a href="https://resilo-ai.fly.dev" style="color:#58a6ff;">Resilo AI</a> — alerts repeat at most every 30 minutes.</p>
  </div>
</div>
</body></html>`,
		htmlEscape(monitorName), url, htmlEscape(url),
		latencyMs, thresholdMs,
		time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
	)
	return m.send(to, subject, body)
}

func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&#34;")
	return s
}

func splitRemediation(s string) []string {
	if s == "" {
		return nil
	}
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		// strip leading "1. " "2. " etc.
		if len(l) > 2 && l[1] == '.' {
			l = strings.TrimSpace(l[2:])
		}
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}

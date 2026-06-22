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
    <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
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
    <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
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

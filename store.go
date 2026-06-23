package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS alerts (
  id           TEXT PRIMARY KEY,
  metric       TEXT,
  severity     TEXT,
  value        REAL,
  threshold    REAL,
  message      TEXT,
  root_cause   TEXT,
  remediation  TEXT,
  confidence   TEXT,
  fired_at     DATETIME,
  resolved_at  DATETIME
);

CREATE TABLE IF NOT EXISTS users (
  id            TEXT PRIMARY KEY,
  email         TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
  token      TEXT PRIMARY KEY,
  user_id    TEXT NOT NULL,
  expires_at DATETIME NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS monitors (
  id               TEXT PRIMARY KEY,
  user_id          TEXT NOT NULL,
  name             TEXT NOT NULL,
  url              TEXT NOT NULL,
  keyword          TEXT,
  interval_seconds INTEGER NOT NULL DEFAULT 60,
  enabled          INTEGER NOT NULL DEFAULT 1,
  created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS monitor_results (
  id          TEXT PRIMARY KEY,
  monitor_id  TEXT NOT NULL,
  status_code INTEGER,
  latency_ms  INTEGER,
  error       TEXT,
  checked_at  DATETIME NOT NULL,
  FOREIGN KEY (monitor_id) REFERENCES monitors(id)
);

CREATE TABLE IF NOT EXISTS outages (
  id           TEXT PRIMARY KEY,
  monitor_id   TEXT NOT NULL,
  started_at   DATETIME NOT NULL,
  resolved_at  DATETIME,
  root_cause   TEXT,
  remediation  TEXT,
  status_code  INTEGER,
  error        TEXT,
  FOREIGN KEY (monitor_id) REFERENCES monitors(id)
);

CREATE TABLE IF NOT EXISTS password_resets (
  token      TEXT PRIMARY KEY,
  user_id    TEXT NOT NULL,
  expires_at DATETIME NOT NULL,
  used       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS incidents (
  id               TEXT PRIMARY KEY,
  category         TEXT NOT NULL,
  severity         TEXT NOT NULL,
  cpu              REAL,
  memory           REAL,
  latency          REAL,
  errors           REAL,
  root_cause       TEXT,
  immediate_action TEXT,
  verification     TEXT,
  prevention       TEXT,
  fired_at         DATETIME NOT NULL,
  resolved_at      DATETIME
);`

// Store persists alerts and AI responses to SQLite.
type Store struct {
	db        *sql.DB
	closeOnce sync.Once
}

// AlertRow is the shape returned by GET /api/alerts.
type AlertRow struct {
	ID          string   `json:"id"`
	Metric      string   `json:"metric"`
	Severity    string   `json:"severity"`
	Value       float64  `json:"value"`
	Threshold   float64  `json:"threshold"`
	Message     string   `json:"message"`
	RootCause   *string  `json:"root_cause"`
	Remediation *string  `json:"remediation"`
	Confidence  *string  `json:"confidence"`
	FiredAt     string   `json:"fired_at"`
	ResolvedAt  *string  `json:"resolved_at"`
}

// Open creates or opens the SQLite DB at path and runs the migration.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	
	// Configure connection pool for better performance
	db.SetMaxOpenConns(10)        // Maximum number of open connections
	db.SetMaxIdleConns(5)         // Maximum number of idle connections
	db.SetConnMaxLifetime(0)     // Connections are reused forever (0 = unlimited)
	db.SetConnMaxIdleTime(300)   // Maximum time a connection can be idle (5 minutes)
	
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	// Migration: add slug column to users if it doesn't exist.
	var hasSlug bool
	pragmaRows, err := db.Query(`PRAGMA table_info(users)`)
	if err == nil {
		for pragmaRows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if pragmaRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil && name == "slug" {
				hasSlug = true
			}
		}
		pragmaRows.Close()
	}
	if !hasSlug {
		if _, err := db.Exec(`ALTER TABLE users ADD COLUMN slug TEXT`); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate users.slug: %w", err)
		}
		// Backfill slugs for any existing users.
		backfillRows, backfillErr := db.Query(`SELECT id, email FROM users WHERE slug IS NULL OR slug = ''`)
		if backfillErr == nil {
			type row struct{ id, email string }
			var pending []row
			for backfillRows.Next() {
				var r row
				backfillRows.Scan(&r.id, &r.email)
				pending = append(pending, r)
			}
			backfillRows.Close()
			for _, r := range pending {
				slug := deriveSlug(r.email)
				// Make unique by appending first 4 chars of the user id.
				slug = slug + "-" + r.id[:4]
				db.Exec(`UPDATE users SET slug = ? WHERE id = ?`, slug, r.id)
			}
		}
		slog.Info("store: migrated users.slug column")
	}

	// Migration: add ssl_expiry column to monitors if it doesn't exist.
	var hasSSLExpiry bool
	monRows, err2 := db.Query(`PRAGMA table_info(monitors)`)
	if err2 == nil {
		for monRows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if monRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil && name == "ssl_expiry" {
				hasSSLExpiry = true
			}
		}
		monRows.Close()
	}
	if !hasSSLExpiry {
		if _, err := db.Exec(`ALTER TABLE monitors ADD COLUMN ssl_expiry DATETIME`); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate monitors.ssl_expiry: %w", err)
		}
		slog.Info("store: migrated monitors.ssl_expiry column")
	}

	// Migration: add keyword column to monitors if it doesn't exist.
	var hasKeyword bool
	kwRows, err3 := db.Query(`PRAGMA table_info(monitors)`)
	if err3 == nil {
		for kwRows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if kwRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil && name == "keyword" {
				hasKeyword = true
			}
		}
		kwRows.Close()
	}
	if !hasKeyword {
		if _, err := db.Exec(`ALTER TABLE monitors ADD COLUMN keyword TEXT`); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate monitors.keyword: %w", err)
		}
		slog.Info("store: migrated monitors.keyword column")
	}

	// Migration: add dns_ms / connect_ms / ttfb_ms columns to monitor_results.
	mrCols := map[string]bool{}
	mrRows, _ := db.Query(`PRAGMA table_info(monitor_results)`)
	if mrRows != nil {
		for mrRows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if mrRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil {
				mrCols[name] = true
			}
		}
		mrRows.Close()
	}
	for _, col := range []string{"dns_ms", "connect_ms", "ttfb_ms"} {
		if !mrCols[col] {
			if _, err := db.Exec(`ALTER TABLE monitor_results ADD COLUMN ` + col + ` INTEGER`); err != nil {
				db.Close()
				return nil, fmt.Errorf("migrate monitor_results.%s: %w", col, err)
			}
			slog.Info("store: migrated monitor_results column", "col", col)
		}
	}

	// Migration: add latency_threshold_ms and latency_alert_sent_at to monitors.
	monCols2 := map[string]bool{}
	mon2Rows, _ := db.Query(`PRAGMA table_info(monitors)`)
	if mon2Rows != nil {
		for mon2Rows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if mon2Rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil {
				monCols2[name] = true
			}
		}
		mon2Rows.Close()
	}
	for _, colDef := range []struct{ name, ddl string }{
		{"latency_threshold_ms", "ALTER TABLE monitors ADD COLUMN latency_threshold_ms INTEGER"},
		{"latency_alert_sent_at", "ALTER TABLE monitors ADD COLUMN latency_alert_sent_at DATETIME"},
	} {
		if !monCols2[colDef.name] {
			if _, err := db.Exec(colDef.ddl); err != nil {
				db.Close()
				return nil, fmt.Errorf("migrate monitors.%s: %w", colDef.name, err)
			}
			slog.Info("store: migrated monitors column", "col", colDef.name)
		}
	}

	// Migration: add max_monitors column to users if it doesn't exist.
	var hasMaxMonitors bool
	umRows, _ := db.Query(`PRAGMA table_info(users)`)
	if umRows != nil {
		for umRows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if umRows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil && name == "max_monitors" {
				hasMaxMonitors = true
			}
		}
		umRows.Close()
	}
	if !hasMaxMonitors {
		if _, err := db.Exec(`ALTER TABLE users ADD COLUMN max_monitors INTEGER NOT NULL DEFAULT 10`); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate users.max_monitors: %w", err)
		}
		slog.Info("store: migrated users.max_monitors column")
	}

	// Migration: add method and request_body columns to monitors.
	monCols3 := map[string]bool{}
	mon3Rows, _ := db.Query(`PRAGMA table_info(monitors)`)
	if mon3Rows != nil {
		for mon3Rows.Next() {
			var cid, notNull, pk int
			var name, colType string
			var dflt interface{}
			if mon3Rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil {
				monCols3[name] = true
			}
		}
		mon3Rows.Close()
	}
	for _, colDef := range []struct{ name, ddl string }{
		{"method", "ALTER TABLE monitors ADD COLUMN method TEXT DEFAULT 'GET'"},
		{"request_body", "ALTER TABLE monitors ADD COLUMN request_body TEXT"},
	} {
		if !monCols3[colDef.name] {
			if _, err := db.Exec(colDef.ddl); err != nil {
				db.Close()
				return nil, fmt.Errorf("migrate monitors.%s: %w", colDef.name, err)
			}
			slog.Info("store: migrated monitors column", "col", colDef.name)
		}
	}

	slog.Info("store opened", "path", path, "max_open_conns", 10, "max_idle_conns", 5)
	return &Store{db: db}, nil
}

// Close releases the DB connection. Safe to call more than once.
func (s *Store) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.db.Close() })
	return err
}

// SaveAlert inserts a new alert row.
func (s *Store) SaveAlert(a Alert) {
	if a.ID == "" {
		slog.Error("store.SaveAlert failed: alert ID cannot be empty")
		return
	}
	if a.Metric == "" {
		slog.Error("store.SaveAlert failed: metric cannot be empty", "id", a.ID)
		return
	}
	
	result, err := s.db.Exec(
		`INSERT OR IGNORE INTO alerts (id, metric, severity, value, threshold, message, fired_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.Metric, a.Severity, a.Value, a.Threshold, a.Message,
		time.UnixMilli(a.Timestamp).UTC().Format(time.RFC3339),
	)
	if err != nil {
		slog.Error("store.SaveAlert failed", "id", a.ID, "metric", a.Metric, "err", err)
		return
	}
	
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		slog.Warn("store.SaveAlert: duplicate alert ignored", "id", a.ID)
	}
}

// ResolveAlert sets resolved_at on an existing alert row.
func (s *Store) ResolveAlert(id string, resolvedAt time.Time) {
	_, err := s.db.Exec(
		`UPDATE alerts SET resolved_at=? WHERE id=?`,
		resolvedAt.UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		slog.Error("store.ResolveAlert failed", "id", id, "err", err)
	}
}

// Incident is a persisted record of a fired alert plus AI analysis.
type Incident struct {
	ID              string           `json:"id"`
	Category        IncidentCategory `json:"category"`
	Severity        string           `json:"severity"`
	CPU             float64          `json:"cpu"`
	Memory          float64          `json:"memory"`
	Latency         float64          `json:"latency"`
	ErrorRate       float64          `json:"errors"`
	RootCause       string           `json:"root_cause"`
	ImmediateAction string           `json:"immediate_action"`
	Verification    string           `json:"verification"`
	Prevention      string           `json:"prevention"`
	FiredAt         time.Time        `json:"fired_at"`
	ResolvedAt      *time.Time       `json:"resolved_at,omitempty"`
}

// PastIncident is a lightweight view returned by FindSimilarIncidents for AI context.
type PastIncident struct {
	Category        IncidentCategory `json:"category"`
	CPU             float64          `json:"cpu"`
	Memory          float64          `json:"memory"`
	Latency         float64          `json:"latency"`
	ErrorRate       float64          `json:"errors"`
	RootCause       string           `json:"root_cause"`
	ImmediateAction string           `json:"immediate_action"`
	FiredAt         string           `json:"fired_at"`
	DurationMins    *int             `json:"duration_mins,omitempty"`
}

// SaveIncident inserts or updates an incident row. Safe to call twice (initial + AI update).
func (s *Store) SaveIncident(inc Incident) error {
	_, err := s.db.Exec(`
		INSERT INTO incidents
			(id, category, severity, cpu, memory, latency, errors,
			 root_cause, immediate_action, verification, prevention, fired_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			root_cause       = excluded.root_cause,
			immediate_action = excluded.immediate_action,
			verification     = excluded.verification,
			prevention       = excluded.prevention`,
		inc.ID, string(inc.Category), inc.Severity,
		inc.CPU, inc.Memory, inc.Latency, inc.ErrorRate,
		inc.RootCause, inc.ImmediateAction, inc.Verification, inc.Prevention,
		inc.FiredAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		slog.Error("store.SaveIncident failed", "id", inc.ID, "err", err)
	}
	return err
}

// FindSimilarIncidents returns up to limit past incidents of the same category, newest first.
func (s *Store) FindSimilarIncidents(category IncidentCategory, limit int) ([]PastIncident, error) {
	rows, err := s.db.Query(`
		SELECT category, cpu, memory, latency, errors,
		       COALESCE(root_cause,''), COALESCE(immediate_action,''),
		       fired_at, resolved_at
		FROM incidents
		WHERE category = ?
		ORDER BY fired_at DESC
		LIMIT ?`, string(category), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PastIncident
	for rows.Next() {
		var p PastIncident
		var cat string
		var resolvedAt *string
		if err := rows.Scan(&cat, &p.CPU, &p.Memory, &p.Latency, &p.ErrorRate,
			&p.RootCause, &p.ImmediateAction, &p.FiredAt, &resolvedAt); err != nil {
			continue
		}
		p.Category = IncidentCategory(cat)
		if resolvedAt != nil {
			firedT, _ := time.Parse(time.RFC3339, p.FiredAt)
			resolvedT, _ := time.Parse(time.RFC3339, *resolvedAt)
			if !firedT.IsZero() && !resolvedT.IsZero() {
				mins := int(resolvedT.Sub(firedT).Minutes())
				p.DurationMins = &mins
			}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ResolveIncident sets resolved_at on an open incident row.
func (s *Store) ResolveIncident(id string, resolvedAt time.Time) error {
	_, err := s.db.Exec(
		`UPDATE incidents SET resolved_at=? WHERE id=? AND resolved_at IS NULL`,
		resolvedAt.UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		slog.Error("store.ResolveIncident failed", "id", id, "err", err)
	}
	return err
}

// GetRecentIncidents returns the most recent incidents (both open and resolved), newest first.
func (s *Store) GetRecentIncidents(limit int) ([]Incident, error) {
	rows, err := s.db.Query(`
		SELECT id, category, severity, cpu, memory, latency, errors,
		       COALESCE(root_cause,''), COALESCE(immediate_action,''),
		       COALESCE(verification,''), COALESCE(prevention,''),
		       fired_at, resolved_at
		FROM incidents
		ORDER BY fired_at DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Incident
	for rows.Next() {
		var inc Incident
		var cat, firedAt string
		var resolvedAt *string
		if err := rows.Scan(&inc.ID, &cat, &inc.Severity,
			&inc.CPU, &inc.Memory, &inc.Latency, &inc.ErrorRate,
			&inc.RootCause, &inc.ImmediateAction, &inc.Verification, &inc.Prevention,
			&firedAt, &resolvedAt); err != nil {
			continue
		}
		inc.Category = IncidentCategory(cat)
		inc.FiredAt, _ = time.Parse(time.RFC3339, firedAt)
		if resolvedAt != nil {
			t, _ := time.Parse(time.RFC3339, *resolvedAt)
			if !t.IsZero() {
				inc.ResolvedAt = &t
			}
		}
		out = append(out, inc)
	}
	return out, rows.Err()
}

// UpdateAIResponse fills root_cause, remediation, and confidence for an existing alert.
func (s *Store) UpdateAIResponse(id, rootCause, remediation, confidence string) {
	_, err := s.db.Exec(
		`UPDATE alerts SET root_cause=?, remediation=?, confidence=? WHERE id=?`,
		rootCause, remediation, confidence, id,
	)
	if err != nil {
		slog.Error("store.UpdateAIResponse failed", "id", id, "err", err)
	}
}

// User is a registered account.
type User struct {
	ID           string
	Email        string
	PasswordHash string
	Slug         string
	MaxMonitors  int
	CreatedAt    time.Time
}

// deriveSlug converts an email address to a URL-safe slug using the local part.
// Trailing digits are stripped so jaybharuka7@gmail.com → base "jaybharuka".
func deriveSlug(email string) string {
	prefix := email
	if i := strings.Index(email, "@"); i > 0 {
		prefix = email[:i]
	}
	var b strings.Builder
	for _, r := range strings.ToLower(prefix) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	// Strip trailing digits (e.g. "jaybharuka7" → "jaybharuka").
	s = strings.TrimRight(s, "0123456789")
	s = strings.TrimRight(s, "-")
	if s == "" {
		return "user"
	}
	return s
}

// CreateUser inserts a new user and returns the created record.
func (s *Store) CreateUser(email, passwordHash string) (User, error) {
	id := GenerateToken()[:16]
	slug := s.uniqueSlug(deriveSlug(email))
	const defaultMax = 10
	_, err := s.db.Exec(
		`INSERT INTO users (id, email, password_hash, slug, max_monitors) VALUES (?, ?, ?, ?, ?)`,
		id, email, passwordHash, slug, defaultMax,
	)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return User{ID: id, Email: email, PasswordHash: passwordHash, Slug: slug, MaxMonitors: defaultMax, CreatedAt: time.Now()}, nil
}

// SetUserMaxMonitors updates the monitor limit for a user (used by demo seeding / admin).
func (s *Store) SetUserMaxMonitors(userID string, max int) error {
	_, err := s.db.Exec(`UPDATE users SET max_monitors = ? WHERE id = ?`, max, userID)
	return err
}

// GetMonitorCount returns how many monitors a user currently has.
func (s *Store) GetMonitorCount(userID string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM monitors WHERE user_id = ? AND enabled = 1`, userID).Scan(&n)
	return n, err
}

// SeedDemoAccount creates or updates a demo user with elevated monitor limit.
// If the user already exists only max_monitors is updated.
func (s *Store) SeedDemoAccount(email, passwordHash string, maxMonitors int) error {
	existing, err := s.GetUserByEmail(email)
	if err == nil {
		// Already exists — just ensure the limit is right.
		return s.SetUserMaxMonitors(existing.ID, maxMonitors)
	}
	u, err := s.CreateUser(email, passwordHash)
	if err != nil {
		return err
	}
	return s.SetUserMaxMonitors(u.ID, maxMonitors)
}

// uniqueSlug returns base if it's free, otherwise base-2, base-3, etc.
func (s *Store) uniqueSlug(base string) string {
	candidate := base
	for i := 2; i <= 999; i++ {
		var count int
		s.db.QueryRow(`SELECT COUNT(*) FROM users WHERE slug = ?`, candidate).Scan(&count)
		if count == 0 {
			return candidate
		}
		candidate = fmt.Sprintf("%s-%d", base, i)
	}
	return base + "-" + GenerateToken()[:6]
}

// UpdateUserSlug sets a new slug for a user.
func (s *Store) UpdateUserSlug(userID, slug string) error {
	_, err := s.db.Exec(`UPDATE users SET slug = ? WHERE id = ?`, slug, userID)
	return err
}

// IsSlugAvailable reports whether a slug is not in use by any user other than excludeUserID.
func (s *Store) IsSlugAvailable(slug, excludeUserID string) (bool, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM users WHERE slug = ? AND id != ?`, slug, excludeUserID,
	).Scan(&count)
	return count == 0, err
}

// GetUserByEmail looks up a user by email address.
func (s *Store) GetUserByEmail(email string) (User, error) {
	var u User
	var createdAt string
	err := s.db.QueryRow(
		`SELECT id, email, password_hash, COALESCE(slug,''), COALESCE(max_monitors,10), created_at FROM users WHERE email = ?`, email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Slug, &u.MaxMonitors, &createdAt)
	if err != nil {
		return User{}, err
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return u, nil
}

// CreateSession generates a token, persists it, and returns the token.
func (s *Store) CreateSession(userID string) (string, error) {
	token := GenerateToken()
	expiresAt := time.Now().Add(7 * 24 * time.Hour)
	_, err := s.db.Exec(
		`INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)`,
		token, userID, expiresAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}
	return token, nil
}

// GetUserBySession returns the user associated with a non-expired session token.
func (s *Store) GetUserBySession(token string) (User, error) {
	var u User
	var createdAt string
	err := s.db.QueryRow(`
		SELECT u.id, u.email, u.password_hash, COALESCE(u.slug,''), COALESCE(u.max_monitors,10), u.created_at
		FROM users u
		JOIN sessions s ON s.user_id = u.id
		WHERE s.token = ? AND s.expires_at > ?`,
		token, time.Now().UTC().Format(time.RFC3339),
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Slug, &u.MaxMonitors, &createdAt)
	if err != nil {
		return User{}, err
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return u, nil
}

// DeleteSession removes a session token.
func (s *Store) DeleteSession(token string) error {
	_, err := s.db.Exec(`DELETE FROM sessions WHERE token = ?`, token)
	return err
}

// PasswordReset is a single-use token for resetting a forgotten password.
type PasswordReset struct {
	Token     string
	UserID    string
	ExpiresAt time.Time
	Used      bool
}

// CreatePasswordReset generates a 32-byte hex token valid for 1 hour.
func (s *Store) CreatePasswordReset(userID string) (string, error) {
	token := GenerateToken() // 32 hex bytes
	expiresAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO password_resets (token, user_id, expires_at) VALUES (?, ?, ?)`,
		token, userID, expiresAt,
	)
	return token, err
}

// GetPasswordReset returns the reset record, or an error if not found, expired, or already used.
func (s *Store) GetPasswordReset(token string) (PasswordReset, error) {
	var pr PasswordReset
	var expiresAt string
	var used int
	err := s.db.QueryRow(
		`SELECT token, user_id, expires_at, used FROM password_resets WHERE token = ?`, token,
	).Scan(&pr.Token, &pr.UserID, &expiresAt, &used)
	if err != nil {
		return PasswordReset{}, fmt.Errorf("reset token not found")
	}
	pr.ExpiresAt, _ = time.Parse(time.RFC3339, expiresAt)
	pr.Used = used == 1
	if pr.Used {
		return PasswordReset{}, fmt.Errorf("reset token already used")
	}
	if time.Now().After(pr.ExpiresAt) {
		return PasswordReset{}, fmt.Errorf("reset token expired")
	}
	return pr, nil
}

// MarkPasswordResetUsed prevents a token from being used a second time.
func (s *Store) MarkPasswordResetUsed(token string) error {
	_, err := s.db.Exec(`UPDATE password_resets SET used = 1 WHERE token = ?`, token)
	return err
}

// GetUserByMonitorID returns the owner of a monitor (used by checker for email routing).
func (s *Store) GetUserByMonitorID(monitorID string) (User, error) {
	var u User
	var createdAt string
	err := s.db.QueryRow(`
		SELECT u.id, u.email, u.password_hash, COALESCE(u.slug,''), COALESCE(u.max_monitors,10), u.created_at
		FROM users u
		JOIN monitors m ON m.user_id = u.id
		WHERE m.id = ?`, monitorID,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Slug, &u.MaxMonitors, &createdAt)
	if err != nil {
		return User{}, err
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return u, nil
}

// GetUserBySlug looks up a user by their public status page slug.
func (s *Store) GetUserBySlug(slug string) (User, error) {
	var u User
	var createdAt string
	err := s.db.QueryRow(
		`SELECT id, email, password_hash, COALESCE(slug,''), created_at FROM users WHERE slug = ?`, slug,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Slug, &createdAt)
	if err != nil {
		return User{}, err
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return u, nil
}

// GetAllUsersWithMonitors returns every user that has at least one enabled monitor.
func (s *Store) GetAllUsersWithMonitors() ([]User, error) {
	rows, err := s.db.Query(`
		SELECT DISTINCT u.id, u.email, u.password_hash, COALESCE(u.slug,''), COALESCE(u.max_monitors,10), u.created_at
		FROM users u
		JOIN monitors m ON m.user_id = u.id AND m.enabled = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []User
	for rows.Next() {
		var u User
		var createdAt string
		if err := rows.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Slug, &u.MaxMonitors, &createdAt); err != nil {
			return nil, err
		}
		u.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		out = append(out, u)
	}
	return out, rows.Err()
}

// GetAvgLatencyRange returns the average latency_ms for a monitor over a time window.
// daysAgo is the start offset from now; daysEnd is the end offset (0 = now).
func (s *Store) GetAvgLatencyRange(monitorID string, daysAgo, daysEnd int) (float64, error) {
	startStr := fmt.Sprintf("-%d days", daysAgo)
	var avg sql.NullFloat64
	var err error
	if daysEnd == 0 {
		err = s.db.QueryRow(`
			SELECT AVG(latency_ms) FROM monitor_results
			WHERE monitor_id = ? AND checked_at >= datetime('now', ?)
			  AND status_code >= 200 AND status_code < 300`,
			monitorID, startStr,
		).Scan(&avg)
	} else {
		endStr := fmt.Sprintf("-%d days", daysEnd)
		err = s.db.QueryRow(`
			SELECT AVG(latency_ms) FROM monitor_results
			WHERE monitor_id = ? AND checked_at >= datetime('now', ?) AND checked_at < datetime('now', ?)
			  AND status_code >= 200 AND status_code < 300`,
			monitorID, startStr, endStr,
		).Scan(&avg)
	}
	if err != nil {
		return 0, err
	}
	return avg.Float64, nil
}

// GetOutageCountRange counts outages that started within a time window.
func (s *Store) GetOutageCountRange(monitorID string, daysAgo int) (int, error) {
	daysStr := fmt.Sprintf("-%d days", daysAgo)
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM outages
		WHERE monitor_id = ? AND started_at >= datetime('now', ?)`,
		monitorID, daysStr,
	).Scan(&count)
	return count, err
}

// GetKeywordFailureCount counts checks where the error contains "keyword not found" within N days.
func (s *Store) GetKeywordFailureCount(monitorID string, daysAgo int) (int, error) {
	daysStr := fmt.Sprintf("-%d days", daysAgo)
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM monitor_results
		WHERE monitor_id = ? AND checked_at >= datetime('now', ?)
		  AND error LIKE '%keyword not found%'`,
		monitorID, daysStr,
	).Scan(&count)
	return count, err
}

// InfraContext is the full snapshot of a user's infrastructure data used by the Ask AI handler.
type InfraContext struct {
	AsOf     string              `json:"as_of"`
	Monitors []MonitorContext    `json:"monitors"`
	Outages  []OutageContext     `json:"outages_last_30d"`
}

// MonitorContext is per-monitor data within InfraContext.
type MonitorContext struct {
	ID              string             `json:"id"`
	Name            string             `json:"name"`
	URL             string             `json:"url"`
	Method          string             `json:"method"`
	Keyword         string             `json:"keyword,omitempty"`
	Enabled         bool               `json:"enabled"`
	Status          string             `json:"current_status"`
	LastCheckedAt   string             `json:"last_checked_at,omitempty"`
	Uptime7d        float64            `json:"uptime_pct_7d"`
	Uptime30d       float64            `json:"uptime_pct_30d"`
	AvgLatency7d    float64            `json:"avg_latency_ms_7d"`
	AvgLatency30d   float64            `json:"avg_latency_ms_30d"`
	HourlyLatency7d []HourlyLatency    `json:"hourly_latency_7d"`
	SSLDaysLeft     *int               `json:"ssl_days_left,omitempty"`
	KeywordFails7d  int                `json:"keyword_failures_7d,omitempty"`
}

// HourlyLatency is an hourly average data point for time-series questions.
type HourlyLatency struct {
	Hour      string  `json:"hour"`  // "2006-01-02T15"
	AvgMs     float64 `json:"avg_ms"`
}

// OutageContext is a recent outage for InfraContext.
type OutageContext struct {
	MonitorName     string  `json:"monitor_name"`
	StartedAt       string  `json:"started_at"`
	DurationMinutes *int    `json:"duration_minutes,omitempty"`
	RootCause       string  `json:"root_cause,omitempty"`
	Ongoing         bool    `json:"ongoing"`
}

// GetInfraContext gathers all monitoring data for a user into one struct for AI consumption.
func (s *Store) GetInfraContext(userID string) (InfraContext, error) {
	ctx := InfraContext{AsOf: time.Now().UTC().Format(time.RFC3339)}

	monitors, err := s.GetMonitorsByUser(userID)
	if err != nil {
		return ctx, fmt.Errorf("GetInfraContext monitors: %w", err)
	}

	for _, m := range monitors {
		mc := MonitorContext{
			ID:      m.ID,
			Name:    m.Name,
			URL:     m.URL,
			Method:  m.Method,
			Keyword: m.Keyword,
			Enabled: m.Enabled,
		}

		// Current status.
		if m.LastStatus != nil {
			if *m.LastStatus >= 200 && *m.LastStatus < 300 {
				mc.Status = "up"
			} else {
				mc.Status = "down"
			}
		} else {
			mc.Status = "unknown"
		}
		if m.LastCheckedAt != nil {
			mc.LastCheckedAt = *m.LastCheckedAt
		}

		// Uptime.
		mc.Uptime7d, _ = s.GetUptimePercent(m.ID, 7)
		mc.Uptime30d, _ = s.GetUptimePercent(m.ID, 30)

		// Average latency.
		mc.AvgLatency7d, _ = s.GetAvgLatencyRange(m.ID, 7, 0)
		mc.AvgLatency30d, _ = s.GetAvgLatencyRange(m.ID, 30, 0)

		// Hourly latency for last 7 days.
		mc.HourlyLatency7d, _ = s.getHourlyLatency(m.ID, 7)

		// SSL.
		if m.SSLExpiry != nil && *m.SSLExpiry != "" {
			if exp, err := time.Parse(time.RFC3339, *m.SSLExpiry); err == nil {
				days := int(time.Until(exp).Hours() / 24)
				mc.SSLDaysLeft = &days
			}
		}

		// Keyword failures.
		mc.KeywordFails7d, _ = s.GetKeywordFailureCount(m.ID, 7)

		ctx.Monitors = append(ctx.Monitors, mc)
	}

	// Recent outages (last 30 days across all monitors).
	rows, err := s.db.Query(`
		SELECT o.started_at, o.resolved_at, o.root_cause, m.name
		FROM outages o JOIN monitors m ON m.id = o.monitor_id
		WHERE m.user_id = ? AND o.started_at >= datetime('now', '-30 days')
		ORDER BY o.started_at DESC LIMIT 50`, userID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var oc OutageContext
			var resolvedAt *string
			var rootCause *string
			if err := rows.Scan(&oc.StartedAt, &resolvedAt, &rootCause, &oc.MonitorName); err != nil {
				continue
			}
			if rootCause != nil {
				oc.RootCause = *rootCause
			}
			if resolvedAt != nil {
				start, _ := time.Parse(time.RFC3339, oc.StartedAt)
				end, _ := time.Parse(time.RFC3339, *resolvedAt)
				if !start.IsZero() && !end.IsZero() {
					mins := int(end.Sub(start).Minutes())
					oc.DurationMinutes = &mins
				}
			} else {
				oc.Ongoing = true
			}
			ctx.Outages = append(ctx.Outages, oc)
		}
	}

	return ctx, nil
}

func (s *Store) getHourlyLatency(monitorID string, days int) ([]HourlyLatency, error) {
	daysStr := fmt.Sprintf("-%d days", days)
	rows, err := s.db.Query(`
		SELECT strftime('%Y-%m-%dT%H', checked_at) AS hour, AVG(latency_ms)
		FROM monitor_results
		WHERE monitor_id = ? AND checked_at >= datetime('now', ?)
		  AND status_code >= 200 AND status_code < 300
		GROUP BY hour ORDER BY hour`,
		monitorID, daysStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HourlyLatency
	for rows.Next() {
		var hl HourlyLatency
		if err := rows.Scan(&hl.Hour, &hl.AvgMs); err != nil {
			continue
		}
		out = append(out, hl)
	}
	return out, rows.Err()
}

// GetUptimePercent returns the percentage of successful checks for a monitor over the last N days.
func (s *Store) GetUptimePercent(monitorID string, days int) (float64, error) {
	daysStr := fmt.Sprintf("-%d days", days)
	var total, upCount int
	err := s.db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END), 0)
		FROM monitor_results
		WHERE monitor_id = ? AND checked_at >= datetime('now', ?)`,
		monitorID, daysStr,
	).Scan(&total, &upCount)
	if err != nil {
		return 100, err
	}
	if total == 0 {
		return 100, nil
	}
	return float64(upCount) * 100.0 / float64(total), nil
}

// GetDailyUptime returns one uptime-percentage value per day for the last N days,
// ordered oldest-first. -1 means no data for that day.
func (s *Store) GetDailyUptime(monitorID string, days int) ([]float64, error) {
	daysStr := fmt.Sprintf("-%d days", days)
	rows, err := s.db.Query(`
		SELECT date(checked_at) AS day,
		       COUNT(*) AS total,
		       COALESCE(SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END), 0) AS up_count
		FROM monitor_results
		WHERE monitor_id = ? AND checked_at >= datetime('now', ?)
		GROUP BY date(checked_at)
		ORDER BY day`, monitorID, daysStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dayMap := make(map[string]float64)
	for rows.Next() {
		var day string
		var total, upCount int
		if err := rows.Scan(&day, &total, &upCount); err != nil {
			return nil, err
		}
		if total > 0 {
			dayMap[day] = float64(upCount) * 100.0 / float64(total)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]float64, days)
	now := time.Now().UTC()
	for i := 0; i < days; i++ {
		day := now.AddDate(0, 0, -(days-1-i)).Format("2006-01-02")
		if uptime, ok := dayMap[day]; ok {
			result[i] = uptime
		} else {
			result[i] = -1
		}
	}
	return result, nil
}

// Monitor is a URL the user wants to check on a schedule.
type Monitor struct {
	ID                  string  `json:"id"`
	UserID              string  `json:"user_id"`
	Name                string  `json:"name"`
	URL                 string  `json:"url"`
	Keyword             string  `json:"keyword"`
	IntervalSeconds     int     `json:"interval_seconds"`
	LatencyThresholdMs  int     `json:"latency_threshold_ms"`
	Method              string  `json:"method"`
	RequestBody         string  `json:"request_body"`
	Enabled             bool    `json:"enabled"`
	CreatedAt           string  `json:"created_at"`
	LastStatus          *int    `json:"last_status"`
	LastLatencyMs       *int    `json:"last_latency_ms"`
	LastError           *string `json:"last_error"`
	LastCheckedAt       *string `json:"last_checked_at"`
	SSLExpiry           *string `json:"ssl_expiry"`
	LatencyAlertSentAt  *string `json:"latency_alert_sent_at"`
}

// MonitorResult is one check outcome for a monitor.
type MonitorResult struct {
	ID         string  `json:"id"`
	MonitorID  string  `json:"monitor_id"`
	StatusCode *int    `json:"status_code"`
	LatencyMs  *int    `json:"latency_ms"`
	DnsMs      *int    `json:"dns_ms"`
	ConnectMs  *int    `json:"connect_ms"`
	TtfbMs     *int    `json:"ttfb_ms"`
	Error      *string `json:"error"`
	CheckedAt  string  `json:"checked_at"`
}

// CreateMonitor inserts a new monitor row.
func (s *Store) CreateMonitor(userID, name, url, keyword, method, requestBody string, intervalSeconds, latencyThresholdMs int) (Monitor, error) {
	id := GenerateToken()[:16]
	now := time.Now().UTC().Format(time.RFC3339)
	var kwVal interface{}
	if keyword != "" {
		kwVal = keyword
	}
	var ltVal interface{}
	if latencyThresholdMs > 0 {
		ltVal = latencyThresholdMs
	}
	if method == "" {
		method = "GET"
	}
	var rbVal interface{}
	if requestBody != "" {
		rbVal = requestBody
	}
	_, err := s.db.Exec(
		`INSERT INTO monitors (id, user_id, name, url, keyword, interval_seconds, latency_threshold_ms, method, request_body, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, userID, name, url, kwVal, intervalSeconds, ltVal, method, rbVal, now,
	)
	if err != nil {
		return Monitor{}, fmt.Errorf("create monitor: %w", err)
	}
	return Monitor{ID: id, UserID: userID, Name: name, URL: url, Keyword: keyword, Method: method, RequestBody: requestBody, IntervalSeconds: intervalSeconds, LatencyThresholdMs: latencyThresholdMs, Enabled: true, CreatedAt: now}, nil
}

// GetMonitorsByUser returns all monitors for a user with the latest result joined.
func (s *Store) GetMonitorsByUser(userID string) ([]Monitor, error) {
	rows, err := s.db.Query(`
		SELECT m.id, m.user_id, m.name, m.url, COALESCE(m.keyword,''), m.interval_seconds,
		       COALESCE(m.latency_threshold_ms, 0),
		       COALESCE(m.method,'GET'), COALESCE(m.request_body,''),
		       m.enabled, m.created_at,
		       r.status_code, r.latency_ms, r.error, r.checked_at,
		       m.ssl_expiry, m.latency_alert_sent_at
		FROM monitors m
		LEFT JOIN monitor_results r ON r.id = (
		    SELECT id FROM monitor_results WHERE monitor_id = m.id ORDER BY checked_at DESC LIMIT 1
		)
		WHERE m.user_id = ?
		ORDER BY m.created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Monitor
	for rows.Next() {
		var m Monitor
		var enabled int
		if err := rows.Scan(
			&m.ID, &m.UserID, &m.Name, &m.URL, &m.Keyword, &m.IntervalSeconds,
			&m.LatencyThresholdMs, &m.Method, &m.RequestBody,
			&enabled, &m.CreatedAt,
			&m.LastStatus, &m.LastLatencyMs, &m.LastError, &m.LastCheckedAt,
			&m.SSLExpiry, &m.LatencyAlertSentAt,
		); err != nil {
			return nil, err
		}
		m.Enabled = enabled == 1
		result = append(result, m)
	}
	return result, rows.Err()
}

// GetAllEnabledMonitors returns all enabled monitors across all users (used by checker).
func (s *Store) GetAllEnabledMonitors() ([]Monitor, error) {
	rows, err := s.db.Query(`
		SELECT id, user_id, name, url, COALESCE(keyword,''), interval_seconds,
		       COALESCE(latency_threshold_ms, 0),
		       COALESCE(method,'GET'), COALESCE(request_body,''),
		       enabled, created_at, latency_alert_sent_at
		FROM monitors WHERE enabled = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Monitor
	for rows.Next() {
		var m Monitor
		var enabled int
		if err := rows.Scan(
			&m.ID, &m.UserID, &m.Name, &m.URL, &m.Keyword, &m.IntervalSeconds,
			&m.LatencyThresholdMs, &m.Method, &m.RequestBody,
			&enabled, &m.CreatedAt, &m.LatencyAlertSentAt,
		); err != nil {
			return nil, err
		}
		m.Enabled = enabled == 1
		result = append(result, m)
	}
	return result, rows.Err()
}

// DeleteMonitor removes a monitor owned by the given user.
func (s *Store) DeleteMonitor(id, userID string) error {
	_, err := s.db.Exec(`DELETE FROM monitors WHERE id = ? AND user_id = ?`, id, userID)
	return err
}

// UpdateMonitor updates editable fields on a monitor owned by the given user.
func (s *Store) UpdateMonitor(id, userID, name, url, keyword, method, requestBody string, intervalSeconds, latencyThresholdMs int) error {
	var kwVal interface{}
	if keyword != "" {
		kwVal = keyword
	}
	var ltVal interface{}
	if latencyThresholdMs > 0 {
		ltVal = latencyThresholdMs
	}
	if method == "" {
		method = "GET"
	}
	var rbVal interface{}
	if requestBody != "" {
		rbVal = requestBody
	}
	res, err := s.db.Exec(
		`UPDATE monitors SET name = ?, url = ?, keyword = ?, interval_seconds = ?, latency_threshold_ms = ?, method = ?, request_body = ? WHERE id = ? AND user_id = ?`,
		name, url, kwVal, intervalSeconds, ltVal, method, rbVal, id, userID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("monitor not found or not owned by user")
	}
	return nil
}

// UpdateLatencyAlertSent sets or clears the latency_alert_sent_at timestamp.
func (s *Store) UpdateLatencyAlertSent(monitorID string, sentAt *time.Time) error {
	var val interface{}
	if sentAt != nil {
		val = sentAt.UTC().Format(time.RFC3339)
	}
	_, err := s.db.Exec(`UPDATE monitors SET latency_alert_sent_at = ? WHERE id = ?`, val, monitorID)
	return err
}

// UpdatePassword sets a new bcrypt hash for the user.
func (s *Store) UpdatePassword(userID, newHash string) error {
	_, err := s.db.Exec(`UPDATE users SET password_hash = ? WHERE id = ?`, newHash, userID)
	return err
}

// UpdateEmail changes the user's email address.
func (s *Store) UpdateEmail(userID, newEmail string) error {
	_, err := s.db.Exec(`UPDATE users SET email = ? WHERE id = ?`, newEmail, userID)
	return err
}

// DeleteAccount removes all data belonging to userID in FK-safe order.
func (s *Store) DeleteAccount(userID string) error {
	stmts := []string{
		`DELETE FROM monitor_results WHERE monitor_id IN (SELECT id FROM monitors WHERE user_id = ?)`,
		`DELETE FROM outages         WHERE monitor_id IN (SELECT id FROM monitors WHERE user_id = ?)`,
		`DELETE FROM monitors        WHERE user_id = ?`,
		`DELETE FROM sessions        WHERE user_id = ?`,
		`DELETE FROM users           WHERE id = ?`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt, userID); err != nil {
			return err
		}
	}
	return nil
}

// UpdateSSLExpiry sets the ssl_expiry timestamp on a monitor.
func (s *Store) UpdateSSLExpiry(monitorID string, expiry *time.Time) error {
	var val interface{}
	if expiry != nil {
		val = expiry.UTC().Format(time.RFC3339)
	}
	_, err := s.db.Exec(`UPDATE monitors SET ssl_expiry = ? WHERE id = ?`, val, monitorID)
	return err
}

// SaveResult inserts a new check result for a monitor.
// Pass -1 for dnsMs/connectMs/ttfbMs when timing data is unavailable (e.g. connection error).
func (s *Store) SaveResult(monitorID string, statusCode, latencyMs, dnsMs, connectMs, ttfbMs int, errStr string) error {
	id := GenerateToken()[:16]
	now := time.Now().UTC().Format(time.RFC3339)
	nullable := func(v int) interface{} {
		if v < 0 {
			return nil
		}
		return v
	}
	var errVal interface{}
	if errStr != "" {
		errVal = errStr
	}
	_, err := s.db.Exec(
		`INSERT INTO monitor_results (id, monitor_id, status_code, latency_ms, dns_ms, connect_ms, ttfb_ms, error, checked_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, monitorID, statusCode, latencyMs, nullable(dnsMs), nullable(connectMs), nullable(ttfbMs), errVal, now,
	)
	return err
}

// GetRecentResults returns the most recent results for a monitor.
func (s *Store) GetRecentResults(monitorID string, limit int) ([]MonitorResult, error) {
	rows, err := s.db.Query(`
		SELECT id, monitor_id, status_code, latency_ms, dns_ms, connect_ms, ttfb_ms, error, checked_at
		FROM monitor_results WHERE monitor_id = ?
		ORDER BY checked_at DESC LIMIT ?`, monitorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []MonitorResult
	for rows.Next() {
		var r MonitorResult
		if err := rows.Scan(&r.ID, &r.MonitorID, &r.StatusCode, &r.LatencyMs, &r.DnsMs, &r.ConnectMs, &r.TtfbMs, &r.Error, &r.CheckedAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// QueryAlerts returns up to limit rows, optionally filtered by severity.
func (s *Store) QueryAlerts(limit int, severity string) ([]AlertRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if severity != "" {
		rows, err = s.db.Query(
			`SELECT id, metric, severity, value, threshold, message,
			        root_cause, remediation, confidence, fired_at, resolved_at
			 FROM alerts WHERE severity = ? ORDER BY fired_at DESC LIMIT ?`,
			severity, limit,
		)
	} else {
		rows, err = s.db.Query(
			`SELECT id, metric, severity, value, threshold, message,
			        root_cause, remediation, confidence, fired_at, resolved_at
			 FROM alerts ORDER BY fired_at DESC LIMIT ?`,
			limit,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AlertRow
	for rows.Next() {
		var r AlertRow
		if err := rows.Scan(
			&r.ID, &r.Metric, &r.Severity, &r.Value, &r.Threshold, &r.Message,
			&r.RootCause, &r.Remediation, &r.Confidence, &r.FiredAt, &r.ResolvedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

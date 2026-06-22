package main

import (
	"database/sql"
	"fmt"
	"log/slog"
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
  interval_seconds INTEGER DEFAULT 60,
  enabled          INTEGER DEFAULT 1,
  created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS monitor_results (
  id          TEXT PRIMARY KEY,
  monitor_id  TEXT NOT NULL,
  status_code INTEGER,
  latency_ms  INTEGER,
  error       TEXT,
  checked_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (monitor_id) REFERENCES monitors(id)
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
	CreatedAt    time.Time
}

// CreateUser inserts a new user and returns the created record.
func (s *Store) CreateUser(email, passwordHash string) (User, error) {
	id := GenerateToken()[:16]
	_, err := s.db.Exec(
		`INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)`,
		id, email, passwordHash,
	)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return User{ID: id, Email: email, PasswordHash: passwordHash, CreatedAt: time.Now()}, nil
}

// GetUserByEmail looks up a user by email address.
func (s *Store) GetUserByEmail(email string) (User, error) {
	var u User
	var createdAt string
	err := s.db.QueryRow(
		`SELECT id, email, password_hash, created_at FROM users WHERE email = ?`, email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &createdAt)
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
		SELECT u.id, u.email, u.password_hash, u.created_at
		FROM users u
		JOIN sessions s ON s.user_id = u.id
		WHERE s.token = ? AND s.expires_at > ?`,
		token, time.Now().UTC().Format(time.RFC3339),
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &createdAt)
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

// Monitor is a URL the user wants to check on a schedule.
type Monitor struct {
	ID              string  `json:"id"`
	UserID          string  `json:"user_id"`
	Name            string  `json:"name"`
	URL             string  `json:"url"`
	IntervalSeconds int     `json:"interval_seconds"`
	Enabled         bool    `json:"enabled"`
	CreatedAt       string  `json:"created_at"`
	LastStatus      *int    `json:"last_status"`
	LastLatencyMs   *int    `json:"last_latency_ms"`
	LastError       *string `json:"last_error"`
	LastCheckedAt   *string `json:"last_checked_at"`
}

// MonitorResult is one check outcome for a monitor.
type MonitorResult struct {
	ID         string  `json:"id"`
	MonitorID  string  `json:"monitor_id"`
	StatusCode *int    `json:"status_code"`
	LatencyMs  *int    `json:"latency_ms"`
	Error      *string `json:"error"`
	CheckedAt  string  `json:"checked_at"`
}

// CreateMonitor inserts a new monitor row.
func (s *Store) CreateMonitor(userID, name, url string, intervalSeconds int) (Monitor, error) {
	id := GenerateToken()[:16]
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO monitors (id, user_id, name, url, interval_seconds, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, userID, name, url, intervalSeconds, now,
	)
	if err != nil {
		return Monitor{}, fmt.Errorf("create monitor: %w", err)
	}
	return Monitor{ID: id, UserID: userID, Name: name, URL: url, IntervalSeconds: intervalSeconds, Enabled: true, CreatedAt: now}, nil
}

// GetMonitorsByUser returns all monitors for a user with the latest result joined.
func (s *Store) GetMonitorsByUser(userID string) ([]Monitor, error) {
	rows, err := s.db.Query(`
		SELECT m.id, m.user_id, m.name, m.url, m.interval_seconds, m.enabled, m.created_at,
		       r.status_code, r.latency_ms, r.error, r.checked_at
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
			&m.ID, &m.UserID, &m.Name, &m.URL, &m.IntervalSeconds, &enabled, &m.CreatedAt,
			&m.LastStatus, &m.LastLatencyMs, &m.LastError, &m.LastCheckedAt,
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
		SELECT id, user_id, name, url, interval_seconds, enabled, created_at
		FROM monitors WHERE enabled = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Monitor
	for rows.Next() {
		var m Monitor
		var enabled int
		if err := rows.Scan(&m.ID, &m.UserID, &m.Name, &m.URL, &m.IntervalSeconds, &enabled, &m.CreatedAt); err != nil {
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

// SaveResult inserts a new check result for a monitor.
func (s *Store) SaveResult(monitorID string, statusCode, latencyMs int, errStr string) error {
	id := GenerateToken()[:16]
	now := time.Now().UTC().Format(time.RFC3339)
	var errVal interface{}
	if errStr != "" {
		errVal = errStr
	}
	_, err := s.db.Exec(
		`INSERT INTO monitor_results (id, monitor_id, status_code, latency_ms, error, checked_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, monitorID, statusCode, latencyMs, errVal, now,
	)
	return err
}

// GetRecentResults returns the most recent results for a monitor.
func (s *Store) GetRecentResults(monitorID string, limit int) ([]MonitorResult, error) {
	rows, err := s.db.Query(`
		SELECT id, monitor_id, status_code, latency_ms, error, checked_at
		FROM monitor_results WHERE monitor_id = ?
		ORDER BY checked_at DESC LIMIT ?`, monitorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []MonitorResult
	for rows.Next() {
		var r MonitorResult
		if err := rows.Scan(&r.ID, &r.MonitorID, &r.StatusCode, &r.LatencyMs, &r.Error, &r.CheckedAt); err != nil {
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

package main

import (
	"database/sql"
	"log/slog"
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
);`

// Store persists alerts and AI responses to SQLite.
type Store struct {
	db *sql.DB
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
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	slog.Info("store opened", "path", path)
	return &Store{db: db}, nil
}

// Close releases the DB connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// SaveAlert inserts a new alert row.
func (s *Store) SaveAlert(a Alert) {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO alerts (id, metric, severity, value, threshold, message, fired_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.Metric, a.Severity, a.Value, a.Threshold, a.Message,
		time.UnixMilli(a.Timestamp).UTC().Format(time.RFC3339),
	)
	if err != nil {
		slog.Error("store.SaveAlert failed", "id", a.ID, "err", err)
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

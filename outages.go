package main

import (
	"fmt"
	"time"
)

// Outage is a period during which a monitor was down.
type Outage struct {
	ID          string  `json:"id"`
	MonitorID   string  `json:"monitor_id"`
	StartedAt   string  `json:"started_at"`
	ResolvedAt  *string `json:"resolved_at"`
	RootCause   *string `json:"root_cause"`
	Remediation *string `json:"remediation"`
	StatusCode  *int    `json:"status_code"`
	Error       *string `json:"error"`
}

// CreateOutage inserts a new outage record when a monitor transitions to DOWN.
func (s *Store) CreateOutage(monitorID string, result MonitorResult) (Outage, error) {
	id := GenerateToken()[:16]
	now := time.Now().UTC().Format(time.RFC3339)

	var statusCode interface{}
	if result.StatusCode != nil && *result.StatusCode != 0 {
		statusCode = *result.StatusCode
	}
	var errVal interface{}
	if result.Error != nil && *result.Error != "" {
		errVal = *result.Error
	}

	_, err := s.db.Exec(
		`INSERT INTO outages (id, monitor_id, started_at, status_code, error) VALUES (?, ?, ?, ?, ?)`,
		id, monitorID, now, statusCode, errVal,
	)
	if err != nil {
		return Outage{}, fmt.Errorf("create outage: %w", err)
	}
	return Outage{
		ID:         id,
		MonitorID:  monitorID,
		StartedAt:  now,
		StatusCode: result.StatusCode,
		Error:      result.Error,
	}, nil
}

// ResolveOutage sets resolved_at on the most recent unresolved outage for a monitor.
func (s *Store) ResolveOutage(monitorID string, resolvedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE outages SET resolved_at = ?
		WHERE id = (
			SELECT id FROM outages
			WHERE monitor_id = ? AND resolved_at IS NULL
			ORDER BY started_at DESC LIMIT 1
		)`, resolvedAt.UTC().Format(time.RFC3339), monitorID)
	return err
}

// UpdateOutageAnalysis fills in the AI root cause and remediation for an outage.
func (s *Store) UpdateOutageAnalysis(id, rootCause, remediation string) error {
	_, err := s.db.Exec(
		`UPDATE outages SET root_cause = ?, remediation = ? WHERE id = ?`,
		rootCause, remediation, id,
	)
	return err
}

// OutageRow is the enriched shape returned by the cross-monitor incidents API.
type OutageRow struct {
	ID              string  `json:"id"`
	MonitorID       string  `json:"monitor_id"`
	MonitorName     string  `json:"monitor_name"`
	MonitorURL      string  `json:"monitor_url"`
	StartedAt       string  `json:"started_at"`
	ResolvedAt      *string `json:"resolved_at"`
	DurationSeconds *int    `json:"duration_seconds"`
	RootCause       *string `json:"root_cause"`
	Remediation     *string `json:"remediation"`
	StatusCode      *int    `json:"status_code"`
	Error           *string `json:"error"`
	IsOngoing       bool    `json:"is_ongoing"`
}

// GetOutagesByUser returns paginated outages across all monitors owned by userID.
func (s *Store) GetOutagesByUser(userID string, limit, offset int) ([]OutageRow, int, error) {
	var total int
	if err := s.db.QueryRow(`
		SELECT COUNT(*) FROM outages o
		JOIN monitors m ON m.id = o.monitor_id
		WHERE m.user_id = ?`, userID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.Query(`
		SELECT o.id, o.monitor_id, m.name, m.url,
		       o.started_at, o.resolved_at,
		       CASE WHEN o.resolved_at IS NOT NULL
		            THEN CAST((julianday(o.resolved_at) - julianday(o.started_at)) * 86400 AS INTEGER)
		            ELSE NULL END AS duration_seconds,
		       o.root_cause, o.remediation, o.status_code, o.error
		FROM outages o
		JOIN monitors m ON m.id = o.monitor_id
		WHERE m.user_id = ?
		ORDER BY o.started_at DESC
		LIMIT ? OFFSET ?`, userID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []OutageRow
	for rows.Next() {
		var r OutageRow
		if err := rows.Scan(
			&r.ID, &r.MonitorID, &r.MonitorName, &r.MonitorURL,
			&r.StartedAt, &r.ResolvedAt, &r.DurationSeconds,
			&r.RootCause, &r.Remediation, &r.StatusCode, &r.Error,
		); err != nil {
			return nil, 0, err
		}
		r.IsOngoing = r.ResolvedAt == nil
		result = append(result, r)
	}
	return result, total, rows.Err()
}

// GetOutagesByMonitor returns the most recent outages for a monitor.
func (s *Store) GetOutagesByMonitor(monitorID string, limit int) ([]Outage, error) {
	rows, err := s.db.Query(`
		SELECT id, monitor_id, started_at, resolved_at, root_cause, remediation, status_code, error
		FROM outages WHERE monitor_id = ?
		ORDER BY started_at DESC LIMIT ?`, monitorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Outage
	for rows.Next() {
		var o Outage
		if err := rows.Scan(
			&o.ID, &o.MonitorID, &o.StartedAt, &o.ResolvedAt,
			&o.RootCause, &o.Remediation, &o.StatusCode, &o.Error,
		); err != nil {
			return nil, err
		}
		result = append(result, o)
	}
	return result, rows.Err()
}

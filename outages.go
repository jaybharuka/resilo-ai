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

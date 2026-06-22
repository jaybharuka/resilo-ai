package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Checker runs periodic HTTP checks against all enabled monitors.
type Checker struct {
	store     *Store
	lastCheck sync.Map // monitorID -> time.Time
	client    *http.Client
}

func newChecker(store *Store) *Checker {
	return &Checker{
		store:  store,
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
	last := val.(time.Time)
	return time.Since(last) >= time.Duration(m.IntervalSeconds)*time.Second
}

func (c *Checker) check(m Monitor) {
	c.lastCheck.Store(m.ID, time.Now())

	start := time.Now()
	resp, err := c.client.Get(m.URL)
	latencyMs := int(time.Since(start).Milliseconds())

	if err != nil {
		slog.Info("checker: monitor down", "name", m.Name, "url", m.URL, "err", err)
		if saveErr := c.store.SaveResult(m.ID, 0, latencyMs, err.Error()); saveErr != nil {
			slog.Error("checker: save result failed", "monitor_id", m.ID, "err", saveErr)
		}
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	slog.Info("checker: monitor checked", "name", m.Name, "status", resp.StatusCode, "latency_ms", latencyMs)
	if saveErr := c.store.SaveResult(m.ID, resp.StatusCode, latencyMs, ""); saveErr != nil {
		slog.Error("checker: save result failed", "monitor_id", m.ID, "err", saveErr)
	}
}

package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// Metrics is the snapshot broadcast to WebSocket clients and evaluated by AlertEngine.
type Metrics struct {
	CPU       float64 `json:"cpu"`
	Memory    float64 `json:"memory"`
	Latency   float64 `json:"latency"`
	ErrorRate float64 `json:"error_rate"`
	Timestamp int64   `json:"timestamp"`
}

// TriggerMode lets callers spike individual metrics.
type TriggerMode struct {
	CPU       bool `json:"cpu"`
	Memory    bool `json:"memory"`
	Latency   bool `json:"latency"`
	ErrorRate bool `json:"error_rate"`
}

// Simulator collects real host metrics and emits Metrics snapshots.
type Simulator struct {
	mu      sync.RWMutex
	current Metrics
	trigger TriggerMode
	pingURL string
	hc      *http.Client
}

func newSimulator(cfg *Config) *Simulator {
	return &Simulator{
		pingURL: fmt.Sprintf("http://localhost:%d/ping", cfg.Server.Port),
		hc:      &http.Client{Timeout: 6 * time.Second},
	}
}

func (s *Simulator) SetTrigger(t TriggerMode) {
	s.mu.Lock()
	s.trigger = t
	s.mu.Unlock()
}

func (s *Simulator) GetTrigger() TriggerMode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trigger
}

func (s *Simulator) Reset() {
	s.mu.Lock()
	s.trigger = TriggerMode{}
	s.mu.Unlock()
}

// probePing fires several HTTP requests to /ping and returns (avgLatencyMs, errorRatePct).
// The /ping handler already honours trigger.Latency and trigger.ErrorRate.
func (s *Simulator) probePing() (float64, float64) {
	const probes = 5
	var totalMs float64
	var errCount int

	for i := 0; i < probes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.pingURL, nil)
		if err != nil {
			cancel()
			errCount++
			totalMs += 5000
			continue
		}
		start := time.Now()
		resp, err := s.hc.Do(req)
		cancel()
		ms := float64(time.Since(start).Milliseconds())
		if err != nil {
			errCount++
			totalMs += 5000
		} else {
			resp.Body.Close()
			totalMs += ms
			if resp.StatusCode >= 500 {
				errCount++
			}
		}
	}

	return totalMs / float64(probes), float64(errCount) * 100.0 / float64(probes)
}

// refresh queries host metrics and updates the cached Metrics.
func (s *Simulator) refresh() {
	// CPU — 200 ms sample window.
	var cpuVal float64
	pcts, err := cpu.Percent(200*time.Millisecond, false)
	if err == nil && len(pcts) > 0 {
		cpuVal = pcts[0]
	}

	// Memory.
	var memVal float64
	if vm, err := mem.VirtualMemory(); err == nil && vm != nil {
		memVal = vm.UsedPercent
	}

	// Latency + error rate via /ping probe (trigger flags are read by /ping itself).
	latVal, errVal := s.probePing()

	// Override CPU/Memory when trigger is set.
	s.mu.RLock()
	t := s.trigger
	s.mu.RUnlock()
	if t.CPU {
		cpuVal = 92.0 + rand.Float64()*4
	}
	if t.Memory {
		memVal = 88.0 + rand.Float64()*4
	}

	m := Metrics{
		CPU:       math.Round(cpuVal*100) / 100,
		Memory:    math.Round(memVal*100) / 100,
		Latency:   math.Round(latVal*100) / 100,
		ErrorRate: math.Round(errVal*100) / 100,
		Timestamp: time.Now().UnixMilli(),
	}
	s.mu.Lock()
	s.current = m
	s.mu.Unlock()
}

func (s *Simulator) Current() Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Run fetches metrics every 30s and emits the cached value every 5s.
// The addr parameter is unused but kept for call-site compatibility.
func (s *Simulator) Run(_ string) <-chan Metrics {
	ch := make(chan Metrics, 8)
	go func() {
		// Give the HTTP server a moment to bind before probing /ping.
		time.Sleep(500 * time.Millisecond)
		s.refresh()
		ch <- s.Current()

		slog.Info("simulator: first metrics snapshot emitted")

		refreshTick := time.NewTicker(30 * time.Second)
		emitTick := time.NewTicker(5 * time.Second)
		defer refreshTick.Stop()
		defer emitTick.Stop()

		for {
			select {
			case <-refreshTick.C:
				s.refresh()
			case <-emitTick.C:
				ch <- s.Current()
			}
		}
	}()
	return ch
}

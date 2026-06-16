package main

import (
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

// Metrics holds the current simulated metric values.
type Metrics struct {
	CPU       float64 `json:"cpu"`
	Memory    float64 `json:"memory"`
	Latency   float64 `json:"latency"`
	ErrorRate float64 `json:"error_rate"`
	Timestamp int64   `json:"timestamp"`
}

// TriggerMode lets callers spike individual metrics.
type TriggerMode struct {
	CPU       bool
	Memory    bool
	Latency   bool
	ErrorRate bool
}

const probeWindowSize = 10

// Simulator collects real CPU/memory from the host and probes /ping for latency/error-rate.
type Simulator struct {
	mu      sync.RWMutex
	current Metrics
	trigger TriggerMode

	// rolling probe window
	probeLatency float64              // last measured RTT ms
	probeErrors  [probeWindowSize]bool // ring buffer: true = error
	probeIdx     int
	probeFilled  bool

	httpClient *http.Client
	promClient *PrometheusClient // non-nil when PROMETHEUS_URL is set
}

func newSimulator(cfg *Config) *Simulator {
	s := &Simulator{
		probeLatency: 0,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	if cfg.Prometheus.URL != "" {
		s.promClient = NewPrometheusClient(cfg.Prometheus.URL)
		slog.Info("prometheus mode active", "url", cfg.Prometheus.URL)
	} else {
		slog.Info("host metrics mode active")
	}
	return s
}

func (s *Simulator) SetTrigger(t TriggerMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trigger = t
}

func (s *Simulator) GetTrigger() TriggerMode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trigger
}

func (s *Simulator) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trigger = TriggerMode{}
	s.probeLatency = 0
	s.probeErrors = [probeWindowSize]bool{}
	s.probeIdx = 0
	s.probeFilled = false
}

func clamp(v, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, v))
}

func noise(scale float64) float64 {
	return (rand.Float64()*2 - 1) * scale
}

// realCPU returns the host CPU usage percent averaged over a 200ms sample.
// Falls back to 0 on error rather than crashing.
func realCPU() float64 {
	pcts, err := cpu.Percent(200*time.Millisecond, false)
	if err != nil || len(pcts) == 0 {
		slog.Error("cpu.Percent failed", "err", err)
		return 0
	}
	return pcts[0]
}

// realMemory returns the host virtual memory used percent.
func realMemory() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		slog.Error("mem.VirtualMemory failed", "err", err)
		return 0
	}
	return v.UsedPercent
}

// probePing fires a GET /ping, records RTT and whether it was an error.
// Runs in its own goroutine every 500ms.
func (s *Simulator) probePing(addr string) {
	start := time.Now()
	resp, err := s.httpClient.Get(fmt.Sprintf("http://%s/ping", addr))
	rtt := float64(time.Since(start).Milliseconds())

	isErr := err != nil
	if err == nil {
		resp.Body.Close()
		isErr = resp.StatusCode != http.StatusOK
	}

	s.mu.Lock()
	s.probeLatency = rtt
	s.probeErrors[s.probeIdx] = isErr
	s.probeIdx = (s.probeIdx + 1) % probeWindowSize
	if s.probeIdx == 0 {
		s.probeFilled = true
	}
	s.mu.Unlock()
}

// errorRate computes error % from the rolling window. Must be called with s.mu held.
func (s *Simulator) errorRate() float64 {
	size := probeWindowSize
	if !s.probeFilled {
		size = s.probeIdx
	}
	if size == 0 {
		return 0
	}
	var errs int
	for i := 0; i < size; i++ {
		if s.probeErrors[i] {
			errs++
		}
	}
	return float64(errs) / float64(size) * 100
}

func (s *Simulator) tick() Metrics {
	// --- CPU & Memory: Prometheus preferred, gopsutil fallback ---
	var cpuVal, memVal float64
	var promLatency, promErr float64 = -1, -1

	if s.promClient != nil {
		snap := s.promClient.Fetch()
		promLatency = snap.Latency
		promErr = snap.ErrorRate
		if snap.CPU >= 0 {
			cpuVal = snap.CPU
		} else {
			cpuVal = realCPU()
		}
		if snap.Memory >= 0 {
			memVal = snap.Memory
		} else {
			memVal = realMemory()
		}
	} else {
		// cpu.Percent blocks 200ms — call before acquiring lock
		cpuVal = realCPU()
		memVal = realMemory()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// --- Latency & ErrorRate: Prometheus preferred, /ping probe fallback ---
	var lat, errRate float64
	if promLatency >= 0 {
		lat = promLatency
	} else {
		lat = s.probeLatency
	}
	if promErr >= 0 {
		errRate = promErr
	} else {
		errRate = s.errorRate()
	}

	// CPU/Memory have no real spike mechanism, so triggers still synthesize a value.
	// Latency/ErrorRate are left alone here: /ping already behaves differently when
	// these triggers are active, so probeLatency/errorRate() already reflect the spike.
	if s.trigger.CPU {
		cpuVal = clamp(88+noise(4), 85, 100)
	}
	if s.trigger.Memory {
		memVal = clamp(85+noise(3), 80, 100)
	}

	s.current = Metrics{
		CPU:       math.Round(clamp(cpuVal, 0, 100)*100) / 100,
		Memory:    math.Round(clamp(memVal, 0, 100)*100) / 100,
		Latency:   math.Round(clamp(lat, 0, 5000)*100) / 100,
		ErrorRate: math.Round(clamp(errRate, 0, 100)*100) / 100,
		Timestamp: time.Now().UnixMilli(),
	}
	return s.current
}

func (s *Simulator) Current() Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Run emits metrics every 500ms and probes /ping on the given address.
func (s *Simulator) Run(addr string) <-chan Metrics {
	ch := make(chan Metrics, 4)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			go s.probePing(addr)
			ch <- s.tick()
		}
	}()
	return ch
}

package main

import (
	"math"
	"math/rand"
	"sync"
	"time"
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

// Simulator generates fake metric data with random noise.
type Simulator struct {
	mu      sync.RWMutex
	current Metrics
	trigger TriggerMode

	// base values that drift slowly
	cpuBase    float64
	memBase    float64
	latBase    float64
	errBase    float64
}

func newSimulator() *Simulator {
	return &Simulator{
		cpuBase: 40,
		memBase: 55,
		latBase: 300,
		errBase: 2,
	}
}

func (s *Simulator) SetTrigger(t TriggerMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trigger = t
}

func (s *Simulator) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trigger = TriggerMode{}
	s.cpuBase = 40
	s.memBase = 55
	s.latBase = 300
	s.errBase = 2
}

func clamp(v, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, v))
}

func noise(scale float64) float64 {
	return (rand.Float64()*2 - 1) * scale
}

func (s *Simulator) tick() Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Slow drift
	s.cpuBase = clamp(s.cpuBase+noise(1.5), 5, 95)
	s.memBase = clamp(s.memBase+noise(0.8), 20, 98)
	s.latBase = clamp(s.latBase+noise(30), 50, 2000)
	s.errBase = clamp(s.errBase+noise(0.5), 0, 25)

	cpu := s.cpuBase + noise(3)
	mem := s.memBase + noise(2)
	lat := s.latBase + noise(50)
	err := s.errBase + noise(1)

	// Apply trigger spikes
	if s.trigger.CPU {
		cpu = clamp(88+noise(4), 85, 100)
	}
	if s.trigger.Memory {
		mem = clamp(85+noise(3), 80, 100)
	}
	if s.trigger.Latency {
		lat = clamp(1600+noise(200), 1500, 3000)
	}
	if s.trigger.ErrorRate {
		err = clamp(12+noise(3), 10, 30)
	}

	s.current = Metrics{
		CPU:       math.Round(clamp(cpu, 0, 100)*100) / 100,
		Memory:    math.Round(clamp(mem, 0, 100)*100) / 100,
		Latency:   math.Round(clamp(lat, 0, 5000)*100) / 100,
		ErrorRate: math.Round(clamp(err, 0, 100)*100) / 100,
		Timestamp: time.Now().UnixMilli(),
	}
	return s.current
}

func (s *Simulator) Current() Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Run emits metrics every 500ms via the returned channel.
func (s *Simulator) Run() <-chan Metrics {
	ch := make(chan Metrics, 4)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			ch <- s.tick()
		}
	}()
	return ch
}

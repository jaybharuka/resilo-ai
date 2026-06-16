package main

import (
	"log"
	"math"
	"math/rand"
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

// Simulator collects real CPU/memory from the host and simulates latency/error-rate.
type Simulator struct {
	mu      sync.RWMutex
	current Metrics
	trigger TriggerMode

	// simulated base values for latency and error rate (drift over time)
	latBase float64
	errBase float64
}

func newSimulator() *Simulator {
	return &Simulator{
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
	s.latBase = 300
	s.errBase = 2
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
		log.Printf("[sim] cpu.Percent error: %v", err)
		return 0
	}
	return pcts[0]
}

// realMemory returns the host virtual memory used percent.
func realMemory() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("[sim] mem.VirtualMemory error: %v", err)
		return 0
	}
	return v.UsedPercent
}

func (s *Simulator) tick() Metrics {
	// Collect real host metrics outside the lock (cpu.Percent blocks 200ms)
	cpuVal := realCPU()
	memVal := realMemory()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Simulated latency and error rate drift
	s.latBase = clamp(s.latBase+noise(30), 50, 2000)
	s.errBase = clamp(s.errBase+noise(0.5), 0, 25)

	lat := s.latBase + noise(50)
	err := s.errBase + noise(1)

	// Apply trigger spikes
	if s.trigger.CPU {
		cpuVal = clamp(88+noise(4), 85, 100)
	}
	if s.trigger.Memory {
		memVal = clamp(85+noise(3), 80, 100)
	}
	if s.trigger.Latency {
		lat = clamp(1600+noise(200), 1500, 3000)
	}
	if s.trigger.ErrorRate {
		err = clamp(12+noise(3), 10, 30)
	}

	s.current = Metrics{
		CPU:       math.Round(clamp(cpuVal, 0, 100)*100) / 100,
		Memory:    math.Round(clamp(memVal, 0, 100)*100) / 100,
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

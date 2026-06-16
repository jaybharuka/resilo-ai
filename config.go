package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the full application configuration, loaded from config.yaml
// and overlaid with environment variable overrides.
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	AI         AIConfig         `yaml:"ai"`
	Prometheus PrometheusConfig `yaml:"prometheus"`
	Alerts     AlertsConfig     `yaml:"alerts"`
	Store      StoreConfig      `yaml:"store"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type AIConfig struct {
	Provider string `yaml:"provider"` // nvidia | anthropic
	APIKey   string `yaml:"api_key"`  // overridden by NVIDIA_API_KEY or ANTHROPIC_API_KEY
	Model    string `yaml:"model"`
}

type PrometheusConfig struct {
	URL string `yaml:"url"` // overridden by PROMETHEUS_URL
}

type AlertsConfig struct {
	CPUWarning        float64 `yaml:"cpu_warning"`
	CPUCritical       float64 `yaml:"cpu_critical"`
	MemoryWarning     float64 `yaml:"memory_warning"`
	MemoryCritical    float64 `yaml:"memory_critical"`
	LatencyWarningMs  float64 `yaml:"latency_warning_ms"`
	LatencyCriticalMs float64 `yaml:"latency_critical_ms"`
	ErrorWarning      float64 `yaml:"error_warning"`
	ErrorCritical     float64 `yaml:"error_critical"`
	CooldownSeconds   int     `yaml:"cooldown_seconds"`
}

type StoreConfig struct {
	Path string `yaml:"path"`
}

func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{Port: 8080},
		AI: AIConfig{
			Provider: "nvidia",
			Model:    "abacusai/dracarys-llama-3.1-70b-instruct",
		},
		Alerts: AlertsConfig{
			CPUWarning:        70,
			CPUCritical:       85,
			MemoryWarning:     65,
			MemoryCritical:    80,
			LatencyWarningMs:  800,
			LatencyCriticalMs: 1500,
			ErrorWarning:      5,
			ErrorCritical:     10,
			CooldownSeconds:   30,
		},
		Store: StoreConfig{Path: "alerts.db"},
	}
}

// LoadConfig reads path (if it exists) over a set of defaults, then applies
// environment variable overrides for secrets and deployment-specific values.
func LoadConfig(path string) (*Config, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	if v := os.Getenv("PROMETHEUS_URL"); v != "" {
		cfg.Prometheus.URL = v
	}

	envKey := "NVIDIA_API_KEY"
	if strings.EqualFold(cfg.AI.Provider, "anthropic") {
		envKey = "ANTHROPIC_API_KEY"
	}
	if v := os.Getenv(envKey); v != "" {
		cfg.AI.APIKey = v
	}

	return cfg, nil
}

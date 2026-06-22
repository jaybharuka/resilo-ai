package main

import (
	"fmt"
	"log/slog"
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
	Auth       AuthConfig       `yaml:"auth"`
	RateLimit  RateLimitConfig  `yaml:"ratelimit"`
	Mail       MailConfig       `yaml:"mail"`
}

type RateLimitConfig struct {
	Enabled            bool `yaml:"enabled"`
	RequestsPerMinute  int  `yaml:"requests_per_minute"`
	Burst              int  `yaml:"burst"`
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

type AuthConfig struct {
	Enabled bool   `yaml:"enabled"`
	Token   string `yaml:"token"` // overridden by AUTH_TOKEN
}

type MailConfig struct {
	SMTPHost    string `yaml:"smtp_host"`
	SMTPPort    int    `yaml:"smtp_port"`
	FromEmail   string `yaml:"from_email"`  // overridden by GMAIL_ADDRESS
	AppPassword string `yaml:"app_password"` // overridden by GMAIL_APP_PASSWORD
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
			CooldownSeconds:   60,
		},
		Store: StoreConfig{Path: defaultStorePath()},
		Auth:  AuthConfig{Enabled: true},
		Mail: MailConfig{
			SMTPHost: "smtp.gmail.com",
			SMTPPort: 587,
		},
		RateLimit: RateLimitConfig{
			Enabled:           true,
			RequestsPerMinute: 10,
			Burst:             5,
		},
	}
}

// defaultStorePath points at the Fly.io persistent volume mount when running
// on a Fly machine (FLY_APP_NAME is always set there), otherwise the local file.
func defaultStorePath() string {
	if os.Getenv("FLY_APP_NAME") != "" {
		return "/data/alerts.db"
	}
	return "alerts.db"
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

	if v := os.Getenv("AUTH_TOKEN"); v != "" {
		cfg.Auth.Token = v
	}

	if v := os.Getenv("GMAIL_ADDRESS"); v != "" {
		cfg.Mail.FromEmail = v
	}
	if v := os.Getenv("GMAIL_APP_PASSWORD"); v != "" {
		cfg.Mail.AppPassword = v
	}

	// Validate critical configuration
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	
	return cfg, nil
}

// validateConfig performs comprehensive configuration validation.
func validateConfig(cfg *Config) error {
	if cfg.Server.Port <= 0 || cfg.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", cfg.Server.Port)
	}
	if cfg.Store.Path == "" {
		return fmt.Errorf("store path cannot be empty")
	}
	if cfg.RateLimit.Enabled {
		if cfg.RateLimit.RequestsPerMinute <= 0 {
			return fmt.Errorf("rate limit requests per minute must be positive")
		}
		if cfg.RateLimit.Burst <= 0 {
			return fmt.Errorf("rate limit burst must be positive")
		}
	}
	return nil
}

// ReloadConfig reloads configuration from the same path and applies changes.
// Note: Some changes (like server port) require restart to take effect.
func ReloadConfig(cfg *Config, path string) error {
	newCfg, err := LoadConfig(path)
	if err != nil {
		return err
	}
	
	// Update mutable fields
	cfg.Prometheus = newCfg.Prometheus
	cfg.AI = newCfg.AI
	cfg.Auth = newCfg.Auth
	cfg.RateLimit = newCfg.RateLimit
	cfg.Alerts = newCfg.Alerts
	
	slog.Info("configuration reloaded", "path", path)
	return nil
}

// ValidateEnvironment checks critical environment variables and logs warnings.
func ValidateEnvironment() {
	// Check for common configuration issues
	if os.Getenv("PROMETHEUS_URL") != "" {
		if !strings.HasPrefix(os.Getenv("PROMETHEUS_URL"), "http://") && 
		   !strings.HasPrefix(os.Getenv("PROMETHEUS_URL"), "https://") {
			slog.Warn("PROMETHEUS_URL should include protocol (http:// or https://)")
		}
	}
	
	provider := os.Getenv("AI_PROVIDER")
	if provider != "" {
		provider = strings.ToLower(provider)
		if provider != "nvidia" && provider != "anthropic" {
			slog.Warn("AI_PROVIDER should be 'nvidia' or 'anthropic'", "provider", provider)
		}
	}
	
	if os.Getenv("AUTH_TOKEN") != "" {
		token := os.Getenv("AUTH_TOKEN")
		if len(token) < 16 {
			slog.Warn("AUTH_TOKEN should be at least 16 characters for security")
		}
	}
	
	// Log configuration summary
	slog.Info("environment validation complete",
		"prometheus_url_set", os.Getenv("PROMETHEUS_URL") != "",
		"ai_provider", provider,
		"auth_token_set", os.Getenv("AUTH_TOKEN") != "",
	)
}

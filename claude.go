package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const nvidiaAPIURL = "https://integrate.api.nvidia.com/v1/chat/completions"
const anthropicAPIURL = "https://api.anthropic.com/v1/messages"

// ClaudeClient wraps either the NVIDIA NIM (OpenAI-compatible) chat completions
// API or the Anthropic Messages API, selected by AIConfig.Provider.
type ClaudeClient struct {
	provider   string
	apiKey     string
	model      string
	httpClient *http.Client
}

// AIResponse is broadcast to WebSocket clients.
type AIResponse struct {
	AlertID     string `json:"alert_id"`
	Model       string `json:"model"`
	RootCause   string `json:"root_cause"`
	Remediation string `json:"remediation"`
	Confidence  string `json:"confidence"`
	Timestamp   int64  `json:"timestamp"`
}

func newClaudeClient(cfg *Config) *ClaudeClient {
	if cfg.AI.APIKey == "" {
		return nil
	}
	return &ClaudeClient{
		provider:   strings.ToLower(cfg.AI.Provider),
		apiKey:     cfg.AI.APIKey,
		model:      cfg.AI.Model,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

type chatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	TopP        float64       `json:"top_p"`
	MaxTokens   int           `json:"max_tokens"`
	Stream      bool          `json:"stream"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Text string `json:"text"`
	} `json:"content"`
}

type analysisJSON struct {
	RootCause   string `json:"root_cause"`
	Remediation string `json:"remediation"`
	Confidence  string `json:"confidence"`
}

func (c *ClaudeClient) Analyze(alert Alert, m Metrics) (*AIResponse, error) {
	prompt := fmt.Sprintf(`You are an expert SRE/DevOps AI assistant analyzing a production incident.

INCIDENT ALERT:
- Alert ID: %s
- Metric: %s
- Current Value: %.2f
- Threshold: %.0f
- Severity: %s
- Message: %s

CURRENT SYSTEM METRICS:
- CPU Usage: %.2f%%
- Memory Usage: %.2f%%
- Latency: %.2fms
- Error Rate: %.2f%%

Provide a concise JSON response with exactly these fields:
{
  "root_cause": "2-3 sentence explanation of the most likely root cause",
  "remediation": "3-5 numbered action steps to resolve the issue",
  "confidence": "high|medium|low"
}

IMPORTANT: Respond with ONLY the raw JSON object. Do NOT wrap it in markdown code fences or backticks. No text before or after the JSON.`,
		alert.ID, alert.Metric, alert.Value, alert.Threshold, alert.Severity, alert.Message,
		m.CPU, m.Memory, m.Latency, m.ErrorRate,
	)

	var text string
	var err error
	if c.provider == "anthropic" {
		text, err = c.callAnthropic(prompt)
	} else {
		text, err = c.callNVIDIA(prompt)
	}
	if err != nil {
		return nil, err
	}

	// Strip markdown code fences if the model wrapped the JSON
	clean := strings.TrimSpace(text)
	if idx := strings.Index(clean, "{"); idx > 0 {
		clean = clean[idx:]
	}
	if idx := strings.LastIndex(clean, "}"); idx >= 0 && idx < len(clean)-1 {
		clean = clean[:idx+1]
	}

	var analysis analysisJSON
	if err := json.Unmarshal([]byte(clean), &analysis); err != nil {
		// Fallback: display raw text as root cause
		analysis.RootCause = text
		analysis.Remediation = "See root cause for details."
		analysis.Confidence = "low"
	}

	return &AIResponse{
		AlertID:     alert.ID,
		Model:       c.model,
		RootCause:   analysis.RootCause,
		Remediation: analysis.Remediation,
		Confidence:  analysis.Confidence,
		Timestamp:   time.Now().UnixMilli(),
	}, nil
}

func (c *ClaudeClient) callNVIDIA(prompt string) (string, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		slog.Info("AI API call completed",
			"provider", "nvidia",
			"model", c.model,
			"duration_ms", duration.Milliseconds(),
		)
	}()

	reqBody := chatCompletionRequest{
		Model: c.model,
		Messages: []chatMessage{
			{Role: "user", Content: prompt},
		},
		Temperature: 0.5,
		TopP:        1,
		MaxTokens:   1024,
		Stream:      false,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", nvidiaAPIURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("nvidia API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp chatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if len(apiResp.Choices) == 0 {
		return "", fmt.Errorf("empty response from model")
	}
	return apiResp.Choices[0].Message.Content, nil
}

func (c *ClaudeClient) callAnthropic(prompt string) (string, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		slog.Info("AI API call completed",
			"provider", "anthropic",
			"model", c.model,
			"duration_ms", duration.Milliseconds(),
		)
	}()

	reqBody := anthropicRequest{
		Model:     c.model,
		MaxTokens: 1024,
		Messages: []anthropicMessage{
			{Role: "user", Content: prompt},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", anthropicAPIURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("anthropic API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp anthropicResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if len(apiResp.Content) == 0 {
		return "", fmt.Errorf("empty response from model")
	}
	return apiResp.Content[0].Text, nil
}

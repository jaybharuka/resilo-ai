package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const nvidiaAPIURL = "https://integrate.api.nvidia.com/v1/chat/completions"
const nvidiaModel = "abacusai/dracarys-llama-3.1-70b-instruct"

// ClaudeClient wraps the NVIDIA NIM (OpenAI-compatible) chat completions API.
type ClaudeClient struct {
	apiKey     string
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

func newClaudeClient() *ClaudeClient {
	key := os.Getenv("NVIDIA_API_KEY")
	if key == "" {
		return nil
	}
	return &ClaudeClient{
		apiKey:     key,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

type chatCompletionRequest struct {
	Model       string          `json:"model"`
	Messages    []chatMessage   `json:"messages"`
	Temperature float64         `json:"temperature"`
	TopP        float64         `json:"top_p"`
	MaxTokens   int             `json:"max_tokens"`
	Stream      bool            `json:"stream"`
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

	reqBody := chatCompletionRequest{
		Model: nvidiaModel,
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
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", nvidiaAPIURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("nvidia API error %d: %s", resp.StatusCode, string(body))
	}

	var apiResp chatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(apiResp.Choices) == 0 {
		return nil, fmt.Errorf("empty response from model")
	}

	text := apiResp.Choices[0].Message.Content

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
		Model:       nvidiaModel,
		RootCause:   analysis.RootCause,
		Remediation: analysis.Remediation,
		Confidence:  analysis.Confidence,
		Timestamp:   time.Now().UnixMilli(),
	}, nil
}

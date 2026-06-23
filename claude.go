package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

// AIResponse is broadcast to WebSocket clients and used for incident persistence.
type AIResponse struct {
	AlertID         string           `json:"alert_id"`
	Category        IncidentCategory `json:"category"`
	SimilarCount    int              `json:"similar_count"`
	Model           string           `json:"model"`
	RootCause       string           `json:"root_cause"`
	ImmediateAction string           `json:"immediate_action"`
	Verification    string           `json:"verification"`
	Prevention      string           `json:"prevention"`
	Remediation     string           `json:"remediation"` // backward compat alias for ImmediateAction
	Confidence      string           `json:"confidence"`
	Timestamp       int64            `json:"timestamp"`
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

// getModel returns the configured model name.
func (c *ClaudeClient) getModel() string {
	return c.model
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

type incidentAnalysisJSON struct {
	RootCause       string `json:"root_cause"`
	ImmediateAction string `json:"immediate_action"`
	Verification    string `json:"verification"`
	Prevention      string `json:"prevention"`
	Confidence      string `json:"confidence"`
}

// AnalyzeIncident calls the AI with full category context and past-incident memory.
func (c *ClaudeClient) AnalyzeIncident(alert Alert, m Metrics, category IncidentCategory, similar []PastIncident) (*AIResponse, error) {
	// Build the similar-incidents section.
	var pastSection string
	if len(similar) == 0 {
		pastSection = "No similar past incidents on record yet."
	} else {
		var sb strings.Builder
		for i, p := range similar {
			dur := ""
			if p.DurationMins != nil {
				dur = fmt.Sprintf(", resolved in %dm", *p.DurationMins)
			}
			fmt.Fprintf(&sb, "  %d. [%s] CPU:%.0f%% Mem:%.0f%% Lat:%.0fms Err:%.2f%%%s\n",
				i+1, p.FiredAt, p.CPU, p.Memory, p.Latency, p.ErrorRate, dur)
			if p.RootCause != "" {
				fmt.Fprintf(&sb, "     Root cause: %s\n", p.RootCause)
			}
			if p.ImmediateAction != "" {
				fmt.Fprintf(&sb, "     Fixed with: %s\n", p.ImmediateAction)
			}
		}
		pastSection = sb.String()
	}

	prompt := fmt.Sprintf(`You are an expert SRE on-call assistant. Classify and diagnose this production incident.

Category detected: %s
Current metrics: CPU %.1f%%, Memory %.1f%%, Latency %.0fms, Error rate %.2f%%
Service: prod-api (Go HTTP service, PostgreSQL backend, deployed on Fly.io)
Alert: %s (severity: %s)

Similar past incidents:
%s
Provide a JSON response with exactly these fields:
{
  "root_cause": "one sentence, specific to this category and these metrics",
  "immediate_action": "exact command or step to take right now",
  "verification": "how to confirm the fix worked",
  "prevention": "one long-term fix to prevent recurrence",
  "confidence": "high|medium|low"
}

IMPORTANT: Respond with ONLY the raw JSON. No markdown fences, no text before or after.`,
		category,
		m.CPU, m.Memory, m.Latency, m.ErrorRate,
		alert.Message, alert.Severity,
		pastSection,
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

	clean := strings.TrimSpace(text)
	if idx := strings.Index(clean, "{"); idx > 0 {
		clean = clean[idx:]
	}
	if idx := strings.LastIndex(clean, "}"); idx >= 0 && idx < len(clean)-1 {
		clean = clean[:idx+1]
	}

	var analysis incidentAnalysisJSON
	if jsonErr := json.Unmarshal([]byte(clean), &analysis); jsonErr != nil {
		analysis.RootCause = text
		analysis.ImmediateAction = "Investigate system metrics manually."
		analysis.Confidence = "low"
	}

	return &AIResponse{
		AlertID:         alert.ID,
		Category:        category,
		SimilarCount:    len(similar),
		Model:           c.model,
		RootCause:       analysis.RootCause,
		ImmediateAction: analysis.ImmediateAction,
		Verification:    analysis.Verification,
		Prevention:      analysis.Prevention,
		Remediation:     analysis.ImmediateAction, // backward compat alias
		Confidence:      analysis.Confidence,
		Timestamp:       time.Now().UnixMilli(),
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

// AskInfra answers a natural-language question using the user's infrastructure context.
func (c *ClaudeClient) AskInfra(question string, ctx InfraContext) (string, error) {
	ctxBytes, err := json.Marshal(ctx)
	if err != nil {
		return "", fmt.Errorf("marshal infra context: %w", err)
	}

	systemPrompt := `You are an infrastructure analyst for a developer. You have access to their monitoring data below. Answer their question in plain English. Be specific — reference actual numbers, dates, and monitor names. If you see concerning patterns mention them even if not asked. If the question cannot be answered from the data, say so honestly. Format your response with clear paragraphs. Use **bold** for important values or names. Keep the answer focused and practical.`

	userMsg := fmt.Sprintf("Infrastructure data (JSON):\n%s\n\nQuestion: %s", string(ctxBytes), question)

	var text string
	if c.provider == "anthropic" {
		// For Anthropic we embed the system prompt as a prefixed user message since
		// the simple callAnthropic helper doesn't take a system param.
		combined := systemPrompt + "\n\n" + userMsg
		text, err = c.callAnthropic(combined)
	} else {
		combined := systemPrompt + "\n\n" + userMsg
		text, err = c.callNVIDIA(combined)
	}
	return text, err
}

// AnalyzeOutage asks the AI for a root cause and remediation steps when a monitor goes DOWN.
func (c *ClaudeClient) AnalyzeOutage(monitor Monitor, result MonitorResult) (rootCause, remediation string, err error) {
	statusStr := "connection error"
	if result.StatusCode != nil && *result.StatusCode != 0 {
		statusStr = fmt.Sprintf("%d", *result.StatusCode)
	}
	errStr := ""
	if result.Error != nil {
		errStr = *result.Error
	}
	latencyMs := 0
	if result.LatencyMs != nil {
		latencyMs = *result.LatencyMs
	}

	prompt := fmt.Sprintf(`You are an SRE AI diagnosing a website outage.

Monitor name: %s
URL: %s
HTTP status: %s
Error message: %s
Response latency: %dms

Respond with ONLY raw JSON (no markdown, no backticks):
{
  "root_cause": "one clear sentence explaining why the site is down",
  "remediation": "1. First step\n2. Second step\n3. Third step"
}`, monitor.Name, monitor.URL, statusStr, errStr, latencyMs)

	var text string
	if c.provider == "anthropic" {
		text, err = c.callAnthropic(prompt)
	} else {
		text, err = c.callNVIDIA(prompt)
	}
	if err != nil {
		return "", "", err
	}

	clean := strings.TrimSpace(text)
	if idx := strings.Index(clean, "{"); idx > 0 {
		clean = clean[idx:]
	}
	if idx := strings.LastIndex(clean, "}"); idx >= 0 && idx < len(clean)-1 {
		clean = clean[:idx+1]
	}

	var parsed struct {
		RootCause   string `json:"root_cause"`
		Remediation string `json:"remediation"`
	}
	if parseErr := json.Unmarshal([]byte(clean), &parsed); parseErr != nil {
		return text, "", nil
	}
	return parsed.RootCause, parsed.Remediation, nil
}

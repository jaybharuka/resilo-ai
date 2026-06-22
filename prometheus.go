package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// promQueries maps each metric name to its PromQL expression.
// These queries expect a standard Prometheus node_exporter and http metrics setup.
var promQueries = map[string]string{
	// CPU: Non-idle percentage across all cores (1m average)
	"cpu": `100-(avg(rate(node_cpu_seconds_total{mode="idle"}[1m]))*100)`,
	// Memory: Percentage of used memory (1 - available/total)
	"memory": `(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100`,
	// Latency: 99th percentile HTTP request duration in milliseconds
	"latency": `histogram_quantile(0.99,rate(http_request_duration_seconds_bucket[1m]))*1000`,
	// Errors: 5xx response rate as percentage of total requests
	"errors": `rate(http_requests_total{status=~"5.."}[1m])/rate(http_requests_total[1m])*100`,
}

// promResponse is the envelope returned by /api/v1/query.
type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value [2]json.RawMessage `json:"value"` // [timestamp, "value"]
		} `json:"result"`
	} `json:"data"`
}

// MetricSnapshot holds one scraped value per metric; -1 means no data.
type MetricSnapshot struct {
	CPU       float64
	Memory    float64
	Latency   float64
	ErrorRate float64
}

// PrometheusClient queries a Prometheus-compatible API.
type PrometheusClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewPrometheusClient creates a client for the given base URL.
func NewPrometheusClient(baseURL string) *PrometheusClient {
	return &PrometheusClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

// queryOne runs a single instant PromQL query and returns the scalar result.
// Returns -1 if the result is empty or cannot be parsed.
func (p *PrometheusClient) queryOne(query string) float64 {
	endpoint := fmt.Sprintf("%s/api/v1/query?query=%s", p.baseURL, url.QueryEscape(query))
	resp, err := p.httpClient.Get(endpoint)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1
	}

	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return -1
	}
	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return -1
	}

	// value[1] is the string-encoded float
	raw := pr.Data.Result[0].Value[1]
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return -1
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return -1
	}
	return v
}

// Fetch queries all four metrics concurrently and returns a MetricSnapshot.
// Any metric that returns -1 was unavailable; the caller should fall back.
func (p *PrometheusClient) Fetch() MetricSnapshot {
	type result struct {
		key string
		val float64
	}
	ch := make(chan result, 4)

	for k, q := range promQueries {
		k, q := k, q
		go func() {
			ch <- result{key: k, val: p.queryOne(q)}
		}()
	}

	snap := MetricSnapshot{CPU: -1, Memory: -1, Latency: -1, ErrorRate: -1}
	for range promQueries {
		r := <-ch
		switch r.key {
		case "cpu":
			snap.CPU = r.val
		case "memory":
			snap.Memory = r.val
		case "latency":
			snap.Latency = r.val
		case "errors":
			snap.ErrorRate = r.val
		}
	}
	return snap
}

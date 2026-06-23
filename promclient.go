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

// MetricSnapshot holds one scraped value per metric; -1 means no data available.
type MetricSnapshot struct {
	CPU       float64
	Memory    float64
	Latency   float64
	ErrorRate float64
}

// DataPoint is a single timestamp + value from a range query.
type DataPoint struct {
	Timestamp int64   `json:"ts"`
	Value     float64 `json:"value"`
}

// PromClient queries a Prometheus-compatible /api/v1 endpoint.
type PromClient struct {
	baseURL    string
	httpClient *http.Client
}

func newPromClient(baseURL string) *PromClient {
	return &PromClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// FetchMetrics runs four instant PromQL queries in parallel and returns a MetricSnapshot.
func (c *PromClient) FetchMetrics() (MetricSnapshot, error) {
	type kv struct {
		key string
		val float64
	}
	queries := map[string]string{
		"cpu":        `node_cpu_usage_percent{job="prod-api"}`,
		"memory":     `node_memory_usage_percent{job="prod-api"}`,
		"latency":    `http_request_duration_p99_ms{job="prod-api"}`,
		"error_rate": `http_error_rate_percent{job="prod-api"}`,
	}
	ch := make(chan kv, len(queries))
	for k, q := range queries {
		k, q := k, q
		go func() {
			v, _ := c.queryInstant(q)
			ch <- kv{k, v}
		}()
	}

	snap := MetricSnapshot{CPU: -1, Memory: -1, Latency: -1, ErrorRate: -1}
	for range queries {
		r := <-ch
		switch r.key {
		case "cpu":
			snap.CPU = r.val
		case "memory":
			snap.Memory = r.val
		case "latency":
			snap.Latency = r.val
		case "error_rate":
			snap.ErrorRate = r.val
		}
	}
	return snap, nil
}

// FetchRange fetches time-series data for the given metric+job over the last duration.
func (c *PromClient) FetchRange(metric, job string, duration time.Duration) ([]DataPoint, error) {
	end := time.Now()
	start := end.Add(-duration)
	query := fmt.Sprintf(`%s{job="%s"}`, metric, job)
	return c.queryRange(query, start, end, "60s")
}

// queryInstant runs a single instant PromQL query and returns the first scalar result or -1.
func (c *PromClient) queryInstant(query string) (float64, error) {
	endpoint := fmt.Sprintf("%s/api/v1/query?query=%s", c.baseURL, url.QueryEscape(query))
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}

	var pr struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &pr); err != nil {
		return -1, err
	}
	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return -1, nil
	}
	var s string
	if err := json.Unmarshal(pr.Data.Result[0].Value[1], &s); err != nil {
		return -1, err
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return -1, err
	}
	return v, nil
}

// queryRange fetches a time-series matrix from /api/v1/query_range.
func (c *PromClient) queryRange(query string, start, end time.Time, step string) ([]DataPoint, error) {
	endpoint := fmt.Sprintf(
		"%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
		c.baseURL,
		url.QueryEscape(query),
		start.Unix(),
		end.Unix(),
		step,
	)
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pr struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][2]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, err
	}
	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return nil, nil
	}

	var pts []DataPoint
	for _, pair := range pr.Data.Result[0].Values {
		var tsF float64
		if err := json.Unmarshal(pair[0], &tsF); err != nil {
			continue
		}
		var s string
		if err := json.Unmarshal(pair[1], &s); err != nil {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		pts = append(pts, DataPoint{Timestamp: int64(tsF), Value: v})
	}
	return pts, nil
}

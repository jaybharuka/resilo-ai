package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	startTime := time.Now()

	hub := newHub()
	go hub.run()

	const listenAddr = "localhost:8080"

	sim := newSimulator()
	metricsCh := sim.Run(listenAddr)

	claude := newClaudeClient()
	if claude == nil {
		slog.Warn("AI analysis disabled", "reason", "NVIDIA_API_KEY not set")
	}

	alertEngine := newAlertEngine(hub, sim, claude)
	go alertEngine.Run()

	// Fan-out simulator metrics to WebSocket clients
	go func() {
		for m := range metricsCh {
			hub.broadcastJSON(WSMessage{Type: "metrics", Payload: m})
		}
	}()

	mux := newServeMux(hub, sim, startTime)

	slog.Info("aiops-bot starting", "addr", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

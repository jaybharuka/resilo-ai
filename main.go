package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	startTime := time.Now()

	store, err := Open(cfg.Store.Path)
	if err != nil {
		slog.Error("failed to open store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	hub := newHub()
	go hub.run()

	listenAddr := fmt.Sprintf("localhost:%d", cfg.Server.Port)

	sim := newSimulator(cfg)
	metricsCh := sim.Run(listenAddr)

	claude := newClaudeClient(cfg)
	if claude == nil {
		slog.Warn("AI analysis disabled", "reason", "no AI API key configured")
	}

	alertEngine := newAlertEngine(hub, sim, claude, store, cfg)
	go alertEngine.Run()

	// Fan-out simulator metrics to WebSocket clients
	go func() {
		for m := range metricsCh {
			hub.broadcastJSON(WSMessage{Type: "metrics", Payload: m})
		}
	}()

	mux := newServeMux(hub, sim, store, startTime)

	slog.Info("aiops-bot starting", "addr", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

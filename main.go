package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	hub := newHub()
	go hub.run()

	// Bind on all interfaces so Fly's proxy (and Docker port mapping) can reach
	// the app; the simulator's self-probe still targets it via loopback.
	bindAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	probeAddr := fmt.Sprintf("localhost:%d", cfg.Server.Port)

	sim := newSimulator(cfg)
	metricsCh := sim.Run(probeAddr)

	claude := newClaudeClient(cfg)
	if claude == nil {
		slog.Warn("AI analysis disabled", "reason", "no AI API key configured")
	}

	if cfg.Auth.Enabled && cfg.Auth.Token == "" {
		slog.Warn("auth disabled — set AUTH_TOKEN in production")
	}

	alertEngine := newAlertEngine(hub, sim, claude, store, cfg)
	go alertEngine.Run()

	// Fan-out simulator metrics to WebSocket clients.
	go func() {
		for m := range metricsCh {
			hub.broadcastJSON(WSMessage{Type: "metrics", Payload: m})
		}
	}()

	mux := newServeMux(hub, sim, alertEngine, store, startTime, cfg)

	// WriteTimeout covers the 1800ms /ping sleep plus NVIDIA API calls (~several seconds).
	srv := &http.Server{
		Addr:         bindAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Block until SIGTERM or SIGINT is received.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	go func() {
		slog.Info("aiops-bot starting", "addr", bindAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-sigCtx.Done()
	stop() // release signal resources before doing blocking work

	slog.Info("shutdown signal received, initiating graceful shutdown")

	// Create shutdown context with extended timeout for large responses
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown HTTP server with detailed logging
	slog.Info("shutting down HTTP server...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "err", err)
	} else {
		slog.Info("HTTP server stopped gracefully")
	}

	// Close database connection with detailed logging
	slog.Info("closing database connection...")
	if err := store.Close(); err != nil {
		slog.Error("database close error", "err", err)
	} else {
		slog.Info("database connection closed")
	}

	slog.Info("graceful shutdown complete")
}

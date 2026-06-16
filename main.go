package main

import (
	"log"
	"net/http"
)

func main() {
	hub := newHub()
	go hub.run()

	const listenAddr = "localhost:8080"

	sim := newSimulator()
	metricsCh := sim.Run(listenAddr)

	claude := newClaudeClient()
	if claude == nil {
		log.Println("[main] NVIDIA_API_KEY not set — AI analysis disabled")
	}

	alertEngine := newAlertEngine(hub, sim, claude)
	go alertEngine.Run()

	// Fan-out simulator metrics to WebSocket clients
	go func() {
		for m := range metricsCh {
			hub.broadcastJSON(WSMessage{Type: "metrics", Payload: m})
		}
	}()

	mux := newServeMux(hub, sim)

	log.Printf("[main] aiops-bot listening on %s", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatalf("[main] server error: %v", err)
	}
}

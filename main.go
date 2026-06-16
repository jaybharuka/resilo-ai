package main

import (
	"log"
	"net/http"
)

func main() {
	hub := newHub()
	go hub.run()

	sim := newSimulator()
	metricsCh := sim.Run()

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

	log.Println("[main] aiops-bot listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("[main] server error: %v", err)
	}
}

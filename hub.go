package main

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/gorilla/websocket"
)

// Client represents a single WebSocket connection.
type Client struct {
	conn *websocket.Conn
	send chan []byte
	mu   sync.Mutex
}

// Hub maintains the set of active clients and broadcasts messages.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			slog.Info("ws client connected", "total", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			slog.Info("ws client disconnected", "total", len(h.clients))

		case msg := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- msg:
				default:
					// slow client — drop and close
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// ClientCount returns the number of currently connected WebSocket clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) broadcastJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		slog.Error("broadcast marshal failed", "err", err)
		return
	}
	h.broadcast <- data
}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
		slog.Info("ws connection closed", "remote", c.conn.RemoteAddr())
	}()
	for msg := range c.send {
		c.mu.Lock()
		err := c.conn.WriteMessage(websocket.TextMessage, msg)
		c.mu.Unlock()
		if err != nil {
			slog.Warn("ws write error", "err", err, "remote", c.conn.RemoteAddr())
			return
		}
	}
}

func (c *Client) readPump(hub *Hub) {
	defer func() {
		hub.unregister <- c
		c.conn.Close()
		slog.Info("ws client unregistered", "remote", c.conn.RemoteAddr())
	}()
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

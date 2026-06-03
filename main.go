package main

import (
	"httpq/config"
	"log/slog"
	"net/http"
	"time"
)

func main() {
	httpQ := NewHTTPQ(config.Config{
		RequestTimeout: 20 * time.Second,
	})
	if err := http.ListenAndServeTLS(":24744", "cert.pem", "key.pem", httpQ.Handler()); err != nil {
		slog.Error("failed to start server", "error", err)
	}
}

func NewHTTPQ(cfg config.Config) *HTTPQ {
	return &HTTPQ{
		topics: map[string]chan []byte{},
		config: cfg,
	}
}

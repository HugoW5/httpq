package main

import (
	"context"
	"httpq/config"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/go-chi/chi"
)

type HTTPQ struct {
	Stats
	mu     sync.Mutex
	topics map[string]chan []byte
	config config.Config
}

func (h *HTTPQ) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("/stats", h.HandleStats().ServeHTTP)

	r.Get("/{topic}", h.Consume().ServeHTTP)
	r.Post("/{topic}", h.Publish().ServeHTTP)

	return r
}

func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := chi.URLParam(r, "topic")

		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal Server Error"))
			atomic.AddInt64(&h.PubFails, 1)
			return
		}

		ch := h.getOrCreateTopic(topic)
		ctx, cancel := context.WithTimeout(r.Context(), h.config.RequestTimeout)
		defer cancel()

		select {
		case ch <- data:
			atomic.AddInt64(&h.TxBytes, int64(len(data)))
		case <-ctx.Done():
			atomic.AddInt64(&h.PubFails, 1)
			w.WriteHeader(http.StatusRequestTimeout)
			_, _ = w.Write([]byte("request timed out"))
		}
	})
}

func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := chi.URLParam(r, "topic")

		ch := h.getOrCreateTopic(topic)
		ctx, cancel := context.WithTimeout(r.Context(), h.config.RequestTimeout)
		defer cancel()

		select {
		case data := <-ch:
			_, err := w.Write(data)
			if err != nil {
				atomic.AddInt64(&h.SubFails, 1)
				return
			}
			atomic.AddInt64(&h.RxBytes, int64(len(data)))
		case <-ctx.Done():
			atomic.AddInt64(&h.SubFails, 1)
			w.WriteHeader(http.StatusRequestTimeout)
			_, _ = w.Write([]byte("request timed out"))
		}
	})
}

func (h *HTTPQ) getOrCreateTopic(topic string) chan []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := h.topics[topic]
	if ch == nil {
		ch = make(chan []byte)
		h.topics[topic] = ch
	}

	return ch
}

package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type Stats struct {
	RxBytes  int64 `json:"rxBytes"`  // number of bytes (message body) consumed
	TxBytes  int64 `json:"txBytes"`  // number of bytes (message body) published
	PubFails int64 `json:"pubFails"` // number of publish failures (e.g. timeout)
	SubFails int64 `json:"subFails"` // number of subscribe failures (e.g. timeout)
}

// Snapshot returns a consistent copy of the counters using atomic loads.
func (h *HTTPQ) statsSnapshot() Stats {
	return Stats{
		RxBytes:  atomic.LoadInt64(&h.RxBytes),
		TxBytes:  atomic.LoadInt64(&h.TxBytes),
		PubFails: atomic.LoadInt64(&h.PubFails),
		SubFails: atomic.LoadInt64(&h.SubFails),
	}
}

func (h *HTTPQ) HandleStats() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := json.Marshal(h.statsSnapshot())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal server error"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})
}

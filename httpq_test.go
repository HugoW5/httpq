package main_test

import (
	main "httpq"
	"httpq/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPublish(t *testing.T) {

}

func TestConsume(t *testing.T) {

}

func TestPublishAndConsume(t *testing.T) {
	h := main.NewHTTPQ(config.Config{
		RequestTimeout: 1 * time.Second,
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	go func() {
		http.Post(srv.URL+"/mytopic", "text/plain", strings.NewReader("hello"))
	}()
	resp, _ := http.Get(srv.URL + "/mytopic")
	body, _ := io.ReadAll(resp.Body)

	if string(body) != "hello" {
		t.Errorf("expected hello, got %s", body)
	}
}

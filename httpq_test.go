package main_test

import (
	"encoding/json"
	"errors"
	"fmt"
	main "httpq"
	"httpq/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	h, srv := createTestServer(t)
	defer srv.Close()
	url := srv.URL + "/testTopic"

	done := post(t, url, "hello")
	res := get(t, url)

	require.NoError(t, <-done)
	require.Equal(t, "hello", res)
	require.Equal(t, int64(5), h.TxBytes) // hello = 5 bytes
	require.Equal(t, int64(0), h.PubFails)
}

func TestConsume(t *testing.T) {
	h, srv := createTestServer(t)
	defer srv.Close()
	url := srv.URL + "/testTopic2"

	done := post(t, url, "hello")

	res := get(t, url)

	require.NoError(t, <-done)
	require.Equal(t, "hello", res)
	require.Equal(t, int64(5), h.RxBytes)
	require.Equal(t, int64(0), h.SubFails)
}

func TestPublishRequestTimeout(t *testing.T) {
	h, srv := createTestServer(t)
	defer srv.Close()
	url := srv.URL + "/testTopic"

	done := post(t, url, "hello")
	err := <-done

	require.Equal(t, "request timeout", err.Error())
	require.Equal(t, int64(1), h.PubFails)
	require.Equal(t, int64(0), h.TxBytes)
}

func TestSubscribeRequestTimeout(t *testing.T) {
	h, srv := createTestServer(t)
	defer srv.Close()
	url := srv.URL + "/testTopic"

	get(t, url)
	time.Sleep(150 * time.Millisecond)

	require.Equal(t, int64(1), h.SubFails)
	require.Equal(t, int64(0), h.RxBytes)

}

func TestPublishAndConsume(t *testing.T) {
	h, srv := createTestServer(t)
	defer srv.Close()
	url := srv.URL + "/testTopic"

	done := post(t, url, "hello")

	res := get(t, url)

	require.NoError(t, <-done)
	require.Equal(t, "hello", res)
	require.Equal(t, int64(5), h.TxBytes)
	require.Equal(t, int64(0), h.PubFails)
	require.Equal(t, int64(5), h.RxBytes)
	require.Equal(t, int64(0), h.SubFails)
}

func TestStatsEndpoint(t *testing.T) {
	_, srv := createTestServer(t)
	defer srv.Close()

	statsURL := srv.URL + "/stats"
	topicURL := srv.URL + "/testTopic"

	res1 := get(t, statsURL)

	done := post(t, topicURL, "hello")
	require.Equal(t, "hello", get(t, topicURL))
	require.NoError(t, <-done)

	res2 := get(t, statsURL)

	var stats1 main.HTTPQ
	var stats2 main.HTTPQ

	require.NoError(t, json.Unmarshal([]byte(res1), &stats1))
	require.NoError(t, json.Unmarshal([]byte(res2), &stats2))

	require.Equal(t, int64(0), stats1.RxBytes)
	require.Equal(t, int64(0), stats1.TxBytes)

	require.Equal(t, int64(5), stats2.RxBytes)
	require.Equal(t, int64(5), stats2.TxBytes)
}

func TestStressPublishConsumeManyTopics(t *testing.T) {
	h := main.NewHTTPQ(config.Config{
		RequestTimeout: 5 * time.Second,
	})

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	const n = 50

	errs := make(chan error, n*2)

	for i := 0; i < n; i++ {
		topicURL := fmt.Sprintf("%s/topic-%d", srv.URL, i)
		body := fmt.Sprintf("hello-%d", i)

		go func() {
			resp, err := http.Post(topicURL, "text/plain", strings.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("POST got status %d", resp.StatusCode)
				return
			}

			errs <- nil
		}()

		go func() {
			resp, err := http.Get(topicURL)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				errs <- err
				return
			}

			if string(data) != body {
				errs <- fmt.Errorf("expected %q, got %q", body, string(data))
				return
			}

			errs <- nil
		}()
	}

	for i := 0; i < n*2; i++ {
		require.NoError(t, <-errs)
	}

	require.Equal(t, int64(0), atomic.LoadInt64(&h.PubFails))
	require.Equal(t, int64(0), atomic.LoadInt64(&h.SubFails))
}

func TestStressSameTopic(t *testing.T) {
	h := main.NewHTTPQ(config.Config{
		RequestTimeout: 10 * time.Second,
	})

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var wg sync.WaitGroup
	const n = 50
	wg.Add(n * 2)
	url := srv.URL + "/same-topic"

	errs := make(chan error, n*2)

	for i := 0; i < n; i++ {
		body := fmt.Sprintf("msg-%d", i)

		go func() {
			defer wg.Done()
			resp, err := http.Post(url, "text/plain", strings.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("POST got status %d", resp.StatusCode)
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			resp, err := http.Get(url)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				errs <- err
				return
			}

			if len(data) == 0 {
				errs <- fmt.Errorf("got empty message")
				return
			}

			errs <- nil
		}()
	}

	wg.Wait()
	close(errs)

	for i := 0; i < n*2; i++ {
		require.NoError(t, <-errs)
	}

	require.Equal(t, int64(0), atomic.LoadInt64(&h.PubFails))
	require.Equal(t, int64(0), atomic.LoadInt64(&h.SubFails))
}

func createTestServer(t *testing.T) (*main.HTTPQ, *httptest.Server) {
	t.Helper()

	h := main.NewHTTPQ(config.Config{RequestTimeout: 100 * time.Millisecond})
	srv := httptest.NewServer(h.Handler())

	return h, srv
}

func post(t *testing.T, url, body string) <-chan error {
	t.Helper()

	done := make(chan error, 1)

	go func() {
		res, err := http.Post(url, "text/plain", strings.NewReader(body))
		if err != nil {
			done <- err
			return
		}
		defer res.Body.Close()

		if res.StatusCode == http.StatusRequestTimeout {
			done <- errors.New("request timeout")
			return
		}
		if res.StatusCode != http.StatusOK {
			done <- errors.New("unexpected status code")
			return
		}

		done <- nil
	}()

	return done
}

func get(t *testing.T, url string) string {
	t.Helper()
	res, err := http.Get(url)
	require.NoError(t, err)
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return string(data)
}

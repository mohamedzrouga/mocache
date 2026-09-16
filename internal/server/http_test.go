package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/med/mocache/internal/cache"
)

func TestReadyzTracksGate(t *testing.T) {
	c := cache.New(1)
	defer c.Close()
	g := NewGate()
	ts := httptest.NewServer(NewMux(c, g))
	defer ts.Close()

	// Not ready until the process explicitly flips the gate (after listen).
	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz before SetReady: %d", resp.StatusCode)
	}

	g.SetReady(true)
	resp, err = http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("readyz ready: %d", resp.StatusCode)
	}

	// Drain: fail readiness but keep liveness so kube will not SIGKILL us.
	g.SetReady(false)
	resp, err = http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz draining: %d", resp.StatusCode)
	}
	resp, err = http.Get(ts.URL + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("livez draining: %d", resp.StatusCode)
	}
}

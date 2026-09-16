package mocache

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/med/mocache/internal/cache"
	"github.com/med/mocache/internal/server"
)

func startHTTP(t *testing.T, c *cache.Cache) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(server.NewMux(c))
	t.Cleanup(ts.Close)
	return ts
}

func startRPC(t *testing.T, c *cache.Cache) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go server.ServeRPC(ln, c)
	return ln.Addr().String()
}

func TestHTTPClientRoundTrip(t *testing.T) {
	c := cache.New(100)
	ts := startHTTP(t, c)
	cli := New([]string{ts.URL}, WithTimeout(2*time.Second))

	if err := cli.Set("user:123", []byte("some_value"), 300*time.Second); err != nil {
		t.Fatal(err)
	}
	val, ok, err := cli.Get("user:123")
	if err != nil || !ok || string(val) != "some_value" {
		t.Fatalf("get: val=%q ok=%v err=%v", val, ok, err)
	}
	if err := cli.Delete("user:123"); err != nil {
		t.Fatal(err)
	}
	val, ok, err = cli.Get("user:123")
	if err != nil || ok || val != nil {
		t.Fatalf("miss: val=%q ok=%v err=%v", val, ok, err)
	}
}

func TestGRPCClientRoundTrip(t *testing.T) {
	c := cache.New(100)
	addr := startRPC(t, c)
	_, port, _ := net.SplitHostPort(addr)
	httpish := "http://127.0.0.1:8090" // host used for hashing + rpc addr rewrite
	cli := New([]string{httpish}, WithProtocol(ProtocolGRPC), WithRPCPort(atoiPort(t, port)), WithTimeout(2*time.Second))
	t.Cleanup(func() { _ = cli.Close() })
	if err := cli.Set("k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	val, ok, err := cli.Get("k")
	if err != nil || !ok || string(val) != "v" {
		t.Fatalf("get: val=%q ok=%v err=%v", val, ok, err)
	}
	if err := cli.Delete("k"); err != nil {
		t.Fatal(err)
	}
	_, ok, err = cli.Get("k")
	if err != nil || ok {
		t.Fatalf("expected miss err=%v ok=%v", err, ok)
	}
}

func TestHealthzAndMetrics(t *testing.T) {
	c := cache.New(10)
	c.Set("a", []byte("b"), 0)
	c.Get("a")
	c.Get("missing")
	ts := startHTTP(t, c)

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok\n" {
		t.Fatalf("healthz %d %q", resp.StatusCode, body)
	}

	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	s := string(body)
	for _, want := range []string{"hits 1", "misses 1", "evictions 0", "item_count 1"} {
		if !strings.Contains(s, want) {
			t.Fatalf("metrics missing %q in %q", want, s)
		}
	}
}

func TestHTTPMissIsNotError(t *testing.T) {
	ts := startHTTP(t, cache.New(10))
	cli := New([]string{ts.URL})
	_, ok, err := cli.Get("nope")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestSetBadJSON(t *testing.T) {
	ts := startHTTP(t, cache.New(10))
	resp, err := http.Post(ts.URL+"/set", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func atoiPort(t *testing.T, port string) int {
	t.Helper()
	var p int
	if _, err := fmt.Sscanf(port, "%d", &p); err != nil {
		t.Fatal(err)
	}
	return p
}

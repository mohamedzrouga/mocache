package mocache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type httpTransport struct {
	client *http.Client
}

func (t *httpTransport) Get(node, key string) ([]byte, bool, error) {
	u, err := join(node, "/get", key)
	if err != nil {
		return nil, false, wrapErr("Get", node, key, err)
	}
	resp, err := t.do("Get", node, key, func() (*http.Response, error) {
		return t.client.Get(u)
	})
	if err != nil {
		return nil, false, err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, wrapErr("Get", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, false, wrapErr("Get", node, key, err)
	}
	return b, true, nil
}

func (t *httpTransport) Set(node, key string, value []byte, ttl time.Duration) error {
	body, err := json.Marshal(struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		TTL   int    `json:"ttl_seconds"`
	}{Key: key, Value: string(value), TTL: int(ttl / time.Second)})
	if err != nil {
		return wrapErr("Set", node, key, err)
	}
	u, err := join(node, "/set", "")
	if err != nil {
		return wrapErr("Set", node, key, err)
	}
	resp, err := t.do("Set", node, key, func() (*http.Response, error) {
		return t.client.Post(u, "application/json", bytes.NewReader(body))
	})
	if err != nil {
		return err
	}
	drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return wrapErr("Set", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	return nil
}

func (t *httpTransport) Delete(node, key string) error {
	u, err := join(node, "/delete", key)
	if err != nil {
		return wrapErr("Delete", node, key, err)
	}
	resp, err := t.do("Delete", node, key, func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodDelete, u, nil)
		if err != nil {
			return nil, err
		}
		return t.client.Do(req)
	})
	if err != nil {
		return err
	}
	drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return wrapErr("Delete", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	return nil
}

func (t *httpTransport) Close() error {
	t.client.CloseIdleConnections()
	return nil
}

// do retries once on a connection error so a rolling restart (RST after
// keep-alive) does not surface as a hard failure. Timeouts are not retried.
func (t *httpTransport) do(op, node, key string, fn func() (*http.Response, error)) (*http.Response, error) {
	resp, err := fn()
	if err != nil && retryable(err) {
		resp, err = fn()
	}
	if err != nil {
		return nil, wrapErr(op, node, key, err)
	}
	return resp, nil
}

func join(node, path, key string) (string, error) {
	u, err := url.Parse(node)
	if err != nil {
		return "", err
	}
	u.Path = path
	if key == "" {
		u.RawQuery = ""
		return u.String(), nil
	}
	q := u.Query()
	q.Set("key", key)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4<<20))
	_ = body.Close()
}

package mocache

import (
	"bytes"
	"context"
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

func (t *httpTransport) Get(ctx context.Context, node, key string) ([]byte, bool, error) {
	u, err := join(node, "/get", key)
	if err != nil {
		return nil, false, wrapErr("Get", node, key, err)
	}
	resp, err := t.do(ctx, "Get", node, key, http.MethodGet, u, nil, "")
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

func (t *httpTransport) Set(ctx context.Context, node, key string, value []byte, ttl time.Duration) error {
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
	resp, err := t.do(ctx, "Set", node, key, http.MethodPost, u, body, "application/json")
	if err != nil {
		return err
	}
	drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return wrapErr("Set", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	return nil
}

func (t *httpTransport) Delete(ctx context.Context, node, key string) error {
	u, err := join(node, "/delete", key)
	if err != nil {
		return wrapErr("Delete", node, key, err)
	}
	resp, err := t.do(ctx, "Delete", node, key, http.MethodDelete, u, nil, "")
	if err != nil {
		return err
	}
	drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return wrapErr("Delete", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	return nil
}

func (t *httpTransport) Invalidate(ctx context.Context, node, kind, pattern string) (int, error) {
	payload := map[string]string{}
	if kind == "prefix" {
		payload["prefix"] = pattern
	} else {
		payload["regex"] = pattern
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, wrapErr("Invalidate", node, pattern, err)
	}
	u, err := join(node, "/invalidate", "")
	if err != nil {
		return 0, wrapErr("Invalidate", node, pattern, err)
	}
	resp, err := t.do(ctx, "Invalidate", node, pattern, http.MethodPost, u, body, "application/json")
	if err != nil {
		return 0, err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, wrapErr("Invalidate", node, pattern, fmt.Errorf("http %d", resp.StatusCode))
	}
	var out struct {
		Deleted int `json:"deleted"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&out); err != nil {
		return 0, wrapErr("Invalidate", node, pattern, err)
	}
	return out.Deleted, nil
}

func (t *httpTransport) Close() error {
	t.client.CloseIdleConnections()
	return nil
}

func (t *httpTransport) do(ctx context.Context, op, node, key, method, rawURL string, body []byte, contentType string) (*http.Response, error) {
	fn := func() (*http.Response, error) {
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, rdr)
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		return t.client.Do(req)
	}
	resp, err := fn()
	if err != nil && retryable(err) && ctx.Err() == nil {
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

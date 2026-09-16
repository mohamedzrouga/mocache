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
	u, err := url.Parse(node)
	if err != nil {
		return nil, false, wrapErr("Get", node, key, err)
	}
	u.Path = "/get"
	q := u.Query()
	q.Set("key", key)
	u.RawQuery = q.Encode()
	resp, err := t.client.Get(u.String())
	if err != nil {
		return nil, false, wrapErr("Get", node, key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		io.Copy(io.Discard, resp.Body)
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
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
	u, err := url.Parse(node)
	if err != nil {
		return wrapErr("Set", node, key, err)
	}
	u.Path = "/set"
	u.RawQuery = ""
	resp, err := t.client.Post(u.String(), "application/json", bytes.NewReader(body))
	if err != nil {
		return wrapErr("Set", node, key, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return wrapErr("Set", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	return nil
}

func (t *httpTransport) Delete(node, key string) error {
	u, err := url.Parse(node)
	if err != nil {
		return wrapErr("Delete", node, key, err)
	}
	u.Path = "/delete"
	q := u.Query()
	q.Set("key", key)
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return wrapErr("Delete", node, key, err)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return wrapErr("Delete", node, key, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return wrapErr("Delete", node, key, fmt.Errorf("http %d", resp.StatusCode))
	}
	return nil
}

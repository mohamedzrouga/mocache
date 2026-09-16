// Package mocache is the Go client SDK for MoCache.
package mocache

import (
	"errors"
	"net/http"
	"time"
)

type transport interface {
	Get(node, key string) ([]byte, bool, error)
	Set(node, key string, value []byte, ttl time.Duration) error
	Delete(node, key string) error
}

type Option func(*Client)

func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

func WithVirtualNodes(n int) Option {
	return func(c *Client) { c.vnodes = n }
}

func WithProtocol(p Protocol) Option {
	return func(c *Client) { c.protocol = p }
}

func WithRPCPort(port int) Option {
	return func(c *Client) { c.rpcPort = port }
}

type Client struct {
	ring     *hashRing
	timeout  time.Duration
	vnodes   int
	protocol Protocol
	rpcPort  int
	tr       transport
}

func New(nodes []string, opts ...Option) *Client {
	c := &Client{timeout: time.Second, vnodes: 100, rpcPort: 8091}
	for _, opt := range opts {
		opt(c)
	}
	if c.timeout <= 0 {
		c.timeout = time.Second
	}
	if c.vnodes < 1 {
		c.vnodes = 100
	}
	c.ring = newHashRing(nodes, c.vnodes)
	if c.protocol == ProtocolGRPC {
		c.tr = newRPCTransport(nodes, c.rpcPort, c.timeout)
	} else {
		c.tr = &httpTransport{client: &http.Client{
			Timeout: c.timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			},
		}}
	}
	return c
}

func (c *Client) Set(key string, value []byte, ttl time.Duration) error {
	return c.tr.Set(c.ring.node(key), key, value, ttl)
}

func (c *Client) Get(key string) ([]byte, bool, error) {
	return c.tr.Get(c.ring.node(key), key)
}

func (c *Client) Delete(key string) error {
	return c.tr.Delete(c.ring.node(key), key)
}

func (c *Client) Close() error {
	if t, ok := c.tr.(*rpcTransport); ok {
		t.close()
	}
	return nil
}

type timeoutErr interface{ Timeout() bool }

func wrapErr(op, node, key string, err error) error {
	if err == nil {
		return nil
	}
	oe := &OpError{Op: op, Node: node, Key: key, Err: err}
	var te timeoutErr
	if errors.As(err, &te) && te.Timeout() {
		oe.Timeout = true
	}
	return oe
}

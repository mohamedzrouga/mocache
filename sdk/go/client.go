// Package mocache is the Go client SDK for MoCache.
//
// Import path: github.com/med/mocache/sdk/go
//
// Clients hash each key onto a static node list (no discovery) and issue
// exactly one RPC. A cache miss is (nil, false, nil). Network/timeout failures
// return *OpError. Close the client to drop idle HTTP/RPC connections — the
// process does not persist cache state, so a node restart looks like a miss
// (or a brief *OpError until the TCP session is re-established).
package mocache

import (
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

type transport interface {
	Get(node, key string) ([]byte, bool, error)
	Set(node, key string, value []byte, ttl time.Duration) error
	Delete(node, key string) error
	Close() error
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
	closed   atomic.Bool
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
				DisableKeepAlives:   false,
			},
		}}
	}
	return c
}

func (c *Client) Set(key string, value []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return wrapErr("Set", "", key, errClosed)
	}
	return c.tr.Set(c.ring.node(key), key, value, ttl)
}

func (c *Client) Get(key string) ([]byte, bool, error) {
	if c.closed.Load() {
		return nil, false, wrapErr("Get", "", key, errClosed)
	}
	return c.tr.Get(c.ring.node(key), key)
}

func (c *Client) Delete(key string) error {
	if c.closed.Load() {
		return wrapErr("Delete", "", key, errClosed)
	}
	return c.tr.Delete(c.ring.node(key), key)
}

func (c *Client) Close() error {
	c.closed.Store(true)
	if c.tr != nil {
		return c.tr.Close()
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

// Package mocache is the Go client SDK for MoCache.
//
// Import path: github.com/mohamedzrouga/mocache/sdk/go
//
// Sync methods (Get/Set/Delete) block until the node answers or the client
// timeout fires. Context methods (*Context) are the cancellable variants.
// Async methods (*Async) return a buffered channel and run the call in a
// goroutine — they never block the caller on the network.
//
// InvalidatePrefix / InvalidateRegex are broadcast to every configured node
// because matching keys can live on any shard of the hash ring.
package mocache

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

type transport interface {
	Get(ctx context.Context, node, key string) ([]byte, bool, error)
	Set(ctx context.Context, node, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, node, key string) error
	Invalidate(ctx context.Context, node, kind, pattern string) (int, error)
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
	nodes    []string
	ring     *hashRing
	timeout  time.Duration
	vnodes   int
	protocol Protocol
	rpcPort  int
	tr       transport
	closed   atomic.Bool
}

func New(nodes []string, opts ...Option) *Client {
	c := &Client{nodes: append([]string(nil), nodes...), timeout: time.Second, vnodes: 100, rpcPort: 8091}
	for _, opt := range opts {
		opt(c)
	}
	if c.timeout <= 0 {
		c.timeout = time.Second
	}
	if c.vnodes < 1 {
		c.vnodes = 100
	}
	c.ring = newHashRing(c.nodes, c.vnodes)
	if c.protocol == ProtocolGRPC {
		c.tr = newRPCTransport(c.nodes, c.rpcPort, c.timeout)
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

func (c *Client) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

func (c *Client) Set(key string, value []byte, ttl time.Duration) error {
	return c.SetContext(context.Background(), key, value, ttl)
}

func (c *Client) SetContext(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return wrapErr("Set", "", key, errClosed)
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	return c.tr.Set(ctx, c.ring.node(key), key, value, ttl)
}

func (c *Client) Get(key string) ([]byte, bool, error) {
	return c.GetContext(context.Background(), key)
}

func (c *Client) GetContext(ctx context.Context, key string) ([]byte, bool, error) {
	if c.closed.Load() {
		return nil, false, wrapErr("Get", "", key, errClosed)
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	return c.tr.Get(ctx, c.ring.node(key), key)
}

func (c *Client) Delete(key string) error {
	return c.DeleteContext(context.Background(), key)
}

func (c *Client) DeleteContext(ctx context.Context, key string) error {
	if c.closed.Load() {
		return wrapErr("Delete", "", key, errClosed)
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	return c.tr.Delete(ctx, c.ring.node(key), key)
}

// InvalidatePrefix deletes keys starting with prefix on every node.
func (c *Client) InvalidatePrefix(prefix string) (int, error) {
	return c.InvalidatePrefixContext(context.Background(), prefix)
}

func (c *Client) InvalidatePrefixContext(ctx context.Context, prefix string) (int, error) {
	return c.invalidate(ctx, "prefix", prefix)
}

// InvalidateRegex deletes keys matching expr (RE2 on the server) on every node.
func (c *Client) InvalidateRegex(expr string) (int, error) {
	return c.InvalidateRegexContext(context.Background(), expr)
}

func (c *Client) InvalidateRegexContext(ctx context.Context, expr string) (int, error) {
	return c.invalidate(ctx, "regex", expr)
}

func (c *Client) invalidate(ctx context.Context, kind, pattern string) (int, error) {
	if c.closed.Load() {
		return 0, wrapErr("Invalidate", "", pattern, errClosed)
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	total := 0
	var first error
	// Broadcast: a prefix/regex can match keys on any shard.
	for _, node := range c.nodes {
		n, err := c.tr.Invalidate(ctx, node, kind, pattern)
		total += n
		if err != nil && first == nil {
			first = err
		}
	}
	return total, first
}

// GetResult is delivered on GetAsync.
type GetResult struct {
	Value []byte
	Found bool
	Err   error
}

func (c *Client) GetAsync(ctx context.Context, key string) <-chan GetResult {
	ch := make(chan GetResult, 1)
	go func() {
		v, ok, err := c.GetContext(ctx, key)
		ch <- GetResult{Value: v, Found: ok, Err: err}
		close(ch)
	}()
	return ch
}

func (c *Client) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- c.SetContext(ctx, key, value, ttl)
		close(ch)
	}()
	return ch
}

func (c *Client) DeleteAsync(ctx context.Context, key string) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- c.DeleteContext(ctx, key)
		close(ch)
	}()
	return ch
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

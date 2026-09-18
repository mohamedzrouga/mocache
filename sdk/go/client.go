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

// WithSlots routes by Redis Cluster slots instead of the MD5 hash ring, so a
// key lands on the same node whether it is written through this SDK or through
// a Redis client on the RESP port. The map is node URL to the slot list that
// node's -cluster-peer entry declares:
//
//	mocache.WithSlots(map[string]string{
//		"http://cache-0:8090": "0-5460",
//		"http://cache-1:8090": "5461-10922",
//		"http://cache-2:8090": "10923-16383",
//	})
//
// The ranges must cover all 16384 slots exactly once. A bad map is reported by
// every operation rather than at New, which has no error to return; see
// slots.go for what this scheme does and does not track.
func WithSlots(slots map[string]string) Option {
	return func(c *Client) {
		parsed := make(map[string][]SlotRange, len(slots))
		for node, spec := range slots {
			ranges, err := ParseSlotRanges(spec)
			if err != nil {
				c.routeErr = err
				return
			}
			parsed[node] = ranges
		}
		c.slots = parsed
	}
}

// router maps a key to the node that should hold it. The ring and the slot map
// are the two implementations; both are immutable, so lookups need no lock.
type router interface {
	node(key string) string
}

type Client struct {
	nodes    []string
	route    router
	slots    map[string][]SlotRange
	routeErr error
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
	switch {
	case c.routeErr != nil:
		// Keep the parse error; every operation returns it.
	case len(c.slots) > 0:
		r, err := newSlotRouter(c.slots)
		if err != nil {
			c.routeErr = err
		} else {
			c.route = r
		}
	default:
		c.route = newHashRing(c.nodes, c.vnodes)
	}
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

// routerErr reports a slot map that could not be used. Every key-routed
// operation returns it, because New has no error to return and routing to the
// wrong node silently is worse than failing loudly.
func (c *Client) routerErr(op, key string) error {
	if c.routeErr != nil {
		return wrapErr(op, "", key, c.routeErr)
	}
	return nil
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
	if err := c.routerErr("Set", key); err != nil {
		return err
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	return c.tr.Set(ctx, c.route.node(key), key, value, ttl)
}

func (c *Client) Get(key string) ([]byte, bool, error) {
	return c.GetContext(context.Background(), key)
}

func (c *Client) GetContext(ctx context.Context, key string) ([]byte, bool, error) {
	if c.closed.Load() {
		return nil, false, wrapErr("Get", "", key, errClosed)
	}
	if err := c.routerErr("Get", key); err != nil {
		return nil, false, err
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	return c.tr.Get(ctx, c.route.node(key), key)
}

func (c *Client) Delete(key string) error {
	return c.DeleteContext(context.Background(), key)
}

func (c *Client) DeleteContext(ctx context.Context, key string) error {
	if c.closed.Load() {
		return wrapErr("Delete", "", key, errClosed)
	}
	if err := c.routerErr("Delete", key); err != nil {
		return err
	}
	ctx, cancel := c.opCtx(ctx)
	defer cancel()
	return c.tr.Delete(ctx, c.route.node(key), key)
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

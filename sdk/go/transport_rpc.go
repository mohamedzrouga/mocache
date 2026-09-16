package mocache

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohamedzrouga/mocache/internal/protocol"
)

type rpcTransport struct {
	timeout time.Duration
	id      atomic.Uint32
	mu      sync.Mutex
	conns   map[string]*rpcConn
	closed  bool
}

type rpcConn struct {
	mu   sync.Mutex
	addr string
	conn net.Conn
}

func newRPCTransport(nodes []string, rpcPort int, timeout time.Duration) *rpcTransport {
	t := &rpcTransport{timeout: timeout, conns: make(map[string]*rpcConn, len(nodes))}
	for _, node := range nodes {
		t.conns[node] = &rpcConn{addr: rpcAddr(node, rpcPort)}
	}
	return t
}

func rpcAddr(node string, port int) string {
	if u, err := url.Parse(node); err == nil && u.Host != "" {
		return net.JoinHostPort(u.Hostname(), strconv.Itoa(port))
	}
	host, _, err := net.SplitHostPort(node)
	if err == nil {
		return net.JoinHostPort(host, strconv.Itoa(port))
	}
	return net.JoinHostPort(node, strconv.Itoa(port))
}

func (t *rpcTransport) do(ctx context.Context, node, key, opName string, req protocol.Request) (protocol.Response, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return protocol.Response{}, wrapErr(opName, node, key, errClosed)
	}
	rc := t.conns[node]
	t.mu.Unlock()
	if rc == nil {
		return protocol.Response{}, wrapErr(opName, node, key, net.ErrClosed)
	}
	req.ID = t.id.Add(1)
	timeout := t.timeout
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
		if timeout <= 0 {
			return protocol.Response{}, wrapErr(opName, node, key, context.DeadlineExceeded)
		}
	}
	resp, err := rc.roundTrip(req, timeout)
	if err != nil {
		return protocol.Response{}, wrapErr(opName, node, key, err)
	}
	if resp.Status == protocol.StatusError {
		return resp, wrapErr(opName, node, key, errString(resp.Err))
	}
	return resp, nil
}

type stringError string

func (e stringError) Error() string { return string(e) }

func errString(s string) error {
	if s == "" {
		s = "rpc error"
	}
	return stringError(s)
}

func (t *rpcTransport) Get(ctx context.Context, node, key string) ([]byte, bool, error) {
	resp, err := t.do(ctx, node, key, "Get", protocol.Request{Op: protocol.OpGet, Key: key})
	if err != nil {
		return nil, false, err
	}
	if resp.Status == protocol.StatusMiss {
		return nil, false, nil
	}
	return resp.Value, true, nil
}

func (t *rpcTransport) Set(ctx context.Context, node, key string, value []byte, ttl time.Duration) error {
	var sec uint32
	if ttl > 0 {
		sec = uint32(ttl / time.Second)
	}
	_, err := t.do(ctx, node, key, "Set", protocol.Request{Op: protocol.OpSet, Key: key, Value: value, TTL: sec})
	return err
}

func (t *rpcTransport) Delete(ctx context.Context, node, key string) error {
	_, err := t.do(ctx, node, key, "Delete", protocol.Request{Op: protocol.OpDelete, Key: key})
	return err
}

func (t *rpcTransport) Invalidate(ctx context.Context, node, kind, pattern string) (int, error) {
	resp, err := t.do(ctx, node, pattern, "Invalidate", protocol.Request{
		Op: protocol.OpInvalidate, Key: pattern, Value: []byte(kind),
	})
	if err != nil {
		return 0, err
	}
	n, _ := strconv.Atoi(string(resp.Value))
	return n, nil
}

func (c *rpcConn) roundTrip(req protocol.Request, timeout time.Duration) (protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp, err := c.roundTripLocked(req, timeout)
	if err != nil && retryable(err) {
		c.reset()
		resp, err = c.roundTripLocked(req, timeout)
	}
	return resp, err
}

func (c *rpcConn) roundTripLocked(req protocol.Request, timeout time.Duration) (protocol.Response, error) {
	if c.conn == nil {
		conn, err := net.DialTimeout("tcp", c.addr, timeout)
		if err != nil {
			return protocol.Response{}, err
		}
		c.conn = conn
	}
	_ = c.conn.SetDeadline(time.Now().Add(timeout))
	if err := protocol.WriteRequest(c.conn, req); err != nil {
		c.reset()
		return protocol.Response{}, err
	}
	resp, err := protocol.ReadResponse(c.conn)
	if err != nil {
		c.reset()
		return protocol.Response{}, err
	}
	return resp, nil
}

func (c *rpcConn) reset() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (t *rpcTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	conns := t.conns
	t.conns = nil
	t.mu.Unlock()
	for _, c := range conns {
		c.mu.Lock()
		c.reset()
		c.mu.Unlock()
	}
	return nil
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	var te timeoutErr
	if errors.As(err, &te) && te.Timeout() {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return !ne.Timeout()
	}
	return true
}

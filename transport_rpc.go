package mocache

import (
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/med/mocache/internal/protocol"
)

type rpcTransport struct {
	timeout time.Duration
	id      atomic.Uint32
	mu      sync.Mutex
	conns   map[string]*rpcConn
}

type rpcConn struct {
	mu   sync.Mutex
	addr string
	conn net.Conn
}

func newRPCTransport(nodes []string, rpcPort int, timeout time.Duration) *rpcTransport {
	t := &rpcTransport{timeout: timeout, conns: make(map[string]*rpcConn, len(nodes))}
	for _, node := range nodes {
		addr := rpcAddr(node, rpcPort)
		t.conns[node] = &rpcConn{addr: addr}
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

func (t *rpcTransport) do(node, key, opName string, req protocol.Request) (protocol.Response, error) {
	t.mu.Lock()
	rc := t.conns[node]
	t.mu.Unlock()
	if rc == nil {
		return protocol.Response{}, wrapErr(opName, node, key, net.ErrClosed)
	}
	req.ID = t.id.Add(1)
	resp, err := rc.roundTrip(req, t.timeout)
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

func (t *rpcTransport) Get(node, key string) ([]byte, bool, error) {
	resp, err := t.do(node, key, "Get", protocol.Request{Op: protocol.OpGet, Key: key})
	if err != nil {
		return nil, false, err
	}
	if resp.Status == protocol.StatusMiss {
		return nil, false, nil
	}
	return resp.Value, true, nil
}

func (t *rpcTransport) Set(node, key string, value []byte, ttl time.Duration) error {
	var sec uint32
	if ttl > 0 {
		sec = uint32(ttl / time.Second)
	}
	_, err := t.do(node, key, "Set", protocol.Request{Op: protocol.OpSet, Key: key, Value: value, TTL: sec})
	return err
}

func (t *rpcTransport) Delete(node, key string) error {
	_, err := t.do(node, key, "Delete", protocol.Request{Op: protocol.OpDelete, Key: key})
	return err
}

func (c *rpcConn) roundTrip(req protocol.Request, timeout time.Duration) (protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		c.conn.Close()
		c.conn = nil
	}
}

func (t *rpcTransport) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.mu.Lock()
		c.reset()
		c.mu.Unlock()
	}
}

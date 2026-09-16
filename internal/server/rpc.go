package server

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/med/mocache/internal/cache"
	"github.com/med/mocache/internal/protocol"
)

const defaultMaxConns = 8192

// RPC is a unary-RPC server that tracks connections so drain can close them
// instead of waiting for idle deadlines (which would delay pod termination).
type RPC struct {
	cache    *cache.Cache
	maxConns int

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
	closed bool
}

func NewRPC(c *cache.Cache) *RPC {
	return &RPC{cache: c, maxConns: defaultMaxConns, conns: make(map[net.Conn]struct{})}
}

// Serve accepts until the listener is closed. Each connection is a goroutine
// that exits on I/O error, idle timeout, or Close().
func (s *RPC) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		if !s.track(conn) {
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer s.untrack(c)
			defer c.Close()
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("rpc panic: %v", rec)
				}
			}()
			serveRPCConn(c, s.cache)
		}(conn)
	}
}

// Close unblocks in-flight reads by closing tracked sockets, then waits for
// handler goroutines. The listener must be closed by the caller first (or
// concurrently) so Accept returns.
func (s *RPC) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *RPC) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.conns) >= s.maxConns {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *RPC) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// ServeRPC is a convenience for tests: serve until the listener closes.
func ServeRPC(l net.Listener, c *cache.Cache) error {
	return NewRPC(c).Serve(l)
}

func serveRPCConn(conn net.Conn, c *cache.Cache) {
	for {
		// Idle connections from a dead client must not live forever.
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
		req, err := protocol.ReadRequest(conn)
		if err != nil {
			if err != io.EOF {
				_ = protocol.WriteResponse(conn, protocol.Response{Status: protocol.StatusError, Err: err.Error()})
			}
			return
		}
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		resp := handleRPC(c, req)
		if err := protocol.WriteResponse(conn, resp); err != nil {
			return
		}
	}
}

func handleRPC(c *cache.Cache, req protocol.Request) protocol.Response {
	resp := protocol.Response{ID: req.ID, Status: protocol.StatusOK}
	switch req.Op {
	case protocol.OpGet:
		val, ok := c.Get(req.Key)
		if !ok {
			resp.Status = protocol.StatusMiss
			return resp
		}
		resp.Value = val
	case protocol.OpSet:
		var ttl time.Duration
		if req.TTL > 0 {
			ttl = time.Duration(req.TTL) * time.Second
		}
		c.Set(req.Key, req.Value, ttl)
	case protocol.OpDelete:
		c.Delete(req.Key)
	case protocol.OpHealth:
		resp.Value = []byte("ok")
	default:
		resp.Status = protocol.StatusError
		resp.Err = "unknown op"
	}
	return resp
}

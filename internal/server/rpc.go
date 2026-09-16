package server

import (
	"io"
	"net"
	"time"

	"github.com/med/mocache/internal/cache"
	"github.com/med/mocache/internal/protocol"
)

func ServeRPC(l net.Listener, c *cache.Cache) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go serveRPCConn(conn, c)
	}
}

func serveRPCConn(conn net.Conn, c *cache.Cache) {
	defer conn.Close()
	for {
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

package bus

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// ErrClosed is returned once a link or server has been shut down.
var ErrClosed = errors.New("bus: closed")

// Link is a persistent outbound connection to one peer, used for
// request/response gossip. It reconnects lazily: a peer that is down simply
// makes calls fail, and the next call tries again. Nothing here retries in a
// loop, because failure detection is the caller's job — a link that cannot be
// established *is* the signal the failure detector is looking for.
type Link struct {
	addr    string
	timeout time.Duration

	mu     sync.Mutex // serializes round trips; the protocol is not multiplexed
	conn   net.Conn
	r      *bufio.Reader
	closed bool
}

func NewLink(addr string, timeout time.Duration) *Link {
	return &Link{addr: addr, timeout: timeout}
}

func (l *Link) Addr() string { return l.addr }

// Connected reports whether a socket is currently established. Used for the
// link-state column in CLUSTER NODES.
func (l *Link) Connected() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conn != nil
}

// Do sends a frame and waits for the reply. On any I/O error the connection is
// dropped and retried once, so a peer restart costs one failed call rather than
// a permanently dead link.
func (l *Link) Do(f Frame) (Frame, error) { return l.DoTimeout(f, l.timeout) }

// DoTimeout bounds one exchange by a caller-supplied deadline instead of the
// link's. MIGRATE carries its own timeout and can ship a batch of keys that
// legitimately outlasts the gossip timeout this link was built with.
func (l *Link) DoTimeout(f Frame, timeout time.Duration) (Frame, error) {
	if timeout <= 0 {
		timeout = l.timeout
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return Frame{}, ErrClosed
	}
	reply, err := l.doLocked(f, timeout)
	if err == nil {
		return reply, nil
	}
	if errors.Is(err, ErrClosed) {
		return Frame{}, err
	}
	l.resetLocked()
	return l.doLocked(f, timeout)
}

func (l *Link) doLocked(f Frame, timeout time.Duration) (Frame, error) {
	if l.conn == nil {
		conn, err := net.DialTimeout("tcp", l.addr, timeout)
		if err != nil {
			return Frame{}, err
		}
		l.conn = conn
		l.r = bufio.NewReaderSize(conn, 32<<10)
	}
	deadline := time.Now().Add(timeout)
	_ = l.conn.SetDeadline(deadline)
	if err := Write(l.conn, f); err != nil {
		return Frame{}, err
	}
	reply, err := Read(l.r)
	if err != nil {
		return Frame{}, err
	}
	return reply, nil
}

func (l *Link) resetLocked() {
	if l.conn != nil {
		_ = l.conn.Close()
		l.conn = nil
		l.r = nil
	}
}

func (l *Link) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.resetLocked()
}

// Handler answers inbound bus frames.
//
// Gossip is request/response: Serve returns the reply frame. A replication
// subscription is different — it hijacks the connection and streams for as long
// as the replica keeps up — so Stream is called instead and owns the socket.
type Handler interface {
	Serve(f Frame) Frame
	Stream(f Frame, conn net.Conn)
}

// Server accepts bus connections. Every peer dials us for gossip, and replicas
// dial us for replication; the frame type on arrival says which.
type Server struct {
	h        Handler
	maxConns int

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
	closed bool
}

func NewServer(h Handler) *Server {
	return &Server{h: h, maxConns: 1024, conns: make(map[net.Conn]struct{})}
}

func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		if !s.track(conn) {
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("bus panic", "panic", rec)
				}
			}()
			s.handle(conn)
		}()
	}
}

func (s *Server) handle(conn net.Conn) {
	r := bufio.NewReaderSize(conn, 32<<10)
	for {
		// No read deadline: gossip links are long-lived and idle between pings.
		// A dead peer is detected by *our* pings failing, not by its silence.
		f, err := Read(r)
		if err != nil {
			_ = conn.Close()
			return
		}
		if f.Type == TypeReplSub {
			// The replication stream takes over this socket; Stream closes it.
			s.h.Stream(f, conn)
			return
		}
		reply := s.h.Serve(f)
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := Write(conn, reply); err != nil {
			_ = conn.Close()
			return
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.conns) >= s.maxConns {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

// Close drops inbound connections so handler goroutines exit. The listener is
// closed by the caller.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

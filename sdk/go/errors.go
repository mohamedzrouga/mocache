package mocache

import (
	"fmt"
)

// Protocol selects the wire used to talk to cache nodes.
type Protocol int

const (
	ProtocolHTTP Protocol = iota
	ProtocolGRPC          // unary RPC over TCP (native framing, not google.golang.org/grpc)
)

func (p Protocol) String() string {
	switch p {
	case ProtocolGRPC:
		return "grpc"
	default:
		return "http"
	}
}

// OpError is returned on timeout or connection failure. A cache miss is not an OpError.
type OpError struct {
	Op      string
	Node    string
	Key     string
	Timeout bool
	Err     error
}

func (e *OpError) Error() string {
	kind := "unavailable"
	if e.Timeout {
		kind = "timeout"
	}
	return fmt.Sprintf("mocache %s %s %q via %s: %v", kind, e.Op, e.Key, e.Node, e.Err)
}

func (e *OpError) Unwrap() error { return e.Err }

// IsTimeout reports whether the failure was a deadline.
func (e *OpError) IsTimeout() bool { return e.Timeout }

type closedError struct{}

func (closedError) Error() string { return "client closed" }

var errClosed = closedError{}

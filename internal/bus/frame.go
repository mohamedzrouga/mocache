// Package bus is MoCache's node-to-node transport: the cluster bus.
//
// Redis nodes gossip over a binary bus protocol. MoCache does not reimplement
// it, because nothing is gained: no Redis server will ever join a MoCache
// cluster, and only the *client*-facing commands have to match Redis. So the
// bus uses the same shape as internal/protocol — a length-prefixed frame — with
// a JSON header for control messages and a raw binary body for cache values.
// JSON keeps gossip and votes readable in a packet capture; the raw body keeps
// replication from paying base64 on every value.
//
// Two connection kinds share one listener:
//
//	gossip      request/response, one persistent link per peer, initiated by
//	            each side independently (so every pair has two links, like Redis)
//	replication one-way stream, dialed by the replica, pushed by the primary
package bus

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Type identifies a frame's payload.
type Type byte

const (
	TypeHello       Type = 1  // identify a gossip link
	TypePing        Type = 2  // gossip: sender state + what it knows of others
	TypePong        Type = 3  // gossip reply, same shape
	TypeFail        Type = 4  // broadcast: this node is confirmed failed
	TypeVoteRequest Type = 5  // replica asking primaries for a promotion vote
	TypeVoteGrant   Type = 6  // vote answer (granted or refused, with a reason)
	TypeUpdate      Type = 7  // new slot ownership at a higher config epoch
	TypeReplSub     Type = 8  // replica subscribing to a primary's stream
	TypeReplStart   Type = 9  // stream header: full resync or continue at offset
	TypeReplEntry   Type = 10 // one snapshot entry
	TypeReplEnd     Type = 11 // snapshot complete, live ops follow
	TypeReplOp      Type = 12 // one live mutation
	TypeAck         Type = 13 // generic acknowledgement
	TypeError       Type = 14 // request refused, Header carries {"error": "..."}
)

// MaxFrame bounds one frame so a peer cannot make us allocate without limit.
// It must exceed the cache's per-value cap (1 MiB by default) with room for the
// header.
const MaxFrame = 32 << 20

var (
	ErrTooLarge = errors.New("bus: frame too large")
	ErrShort    = errors.New("bus: truncated frame")
)

// Frame is one message: a typed JSON header plus an optional binary body.
type Frame struct {
	Type   Type
	Header []byte
	Body   []byte
}

// NewFrame marshals header as JSON.
func NewFrame(t Type, header any, body []byte) (Frame, error) {
	var hb []byte
	if header != nil {
		var err error
		if hb, err = json.Marshal(header); err != nil {
			return Frame{}, err
		}
	}
	return Frame{Type: t, Header: hb, Body: body}, nil
}

// Decode unmarshals the frame header into v.
func (f Frame) Decode(v any) error {
	if len(f.Header) == 0 {
		return nil
	}
	return json.Unmarshal(f.Header, v)
}

// Errorf builds an error frame.
func Errorf(format string, a ...any) Frame {
	f, _ := NewFrame(TypeError, map[string]string{"error": fmt.Sprintf(format, a...)}, nil)
	return f
}

// Err returns the error carried by a TypeError frame, or nil.
func (f Frame) Err() error {
	if f.Type != TypeError {
		return nil
	}
	var e struct {
		Error string `json:"error"`
	}
	_ = f.Decode(&e)
	if e.Error == "" {
		e.Error = "unspecified bus error"
	}
	return errors.New(e.Error)
}

// Wire layout: uint32 total | byte type | uint32 headerLen | header | body.
func Write(w io.Writer, f Frame) error {
	total := 1 + 4 + len(f.Header) + len(f.Body)
	if total > MaxFrame {
		return ErrTooLarge
	}
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[0:4], uint32(total))
	buf[4] = byte(f.Type)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(f.Header)))
	copy(buf[9:], f.Header)
	copy(buf[9+len(f.Header):], f.Body)
	_, err := w.Write(buf)
	return err
}

func Read(r *bufio.Reader) (Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	total := binary.BigEndian.Uint32(hdr[:])
	if total < 5 {
		return Frame{}, ErrShort
	}
	if total > MaxFrame {
		return Frame{}, ErrTooLarge
	}
	buf := make([]byte, total)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, err
	}
	hl := binary.BigEndian.Uint32(buf[1:5])
	if uint32(len(buf)) < 5+hl {
		return Frame{}, ErrShort
	}
	return Frame{
		Type:   Type(buf[0]),
		Header: buf[5 : 5+hl],
		Body:   buf[5+hl:],
	}, nil
}

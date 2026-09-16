// Package protocol implements MoCache's unary RPC framing (the fast "gRPC" path).
//
// Wire format: uint32be length (of the payload only) + payload. Payload starts
// with magic "MOC1" and version 1. This is not google.golang.org/grpc — that
// library would violate the zero-third-party-dependency constraint.
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	Magic    = "MOC1"
	Version  = byte(1)
	MaxFrame = 4 << 20 // reject oversized frames so a client cannot OOM us

	OpGet        = byte(1)
	OpSet        = byte(2)
	OpDelete     = byte(3)
	OpHealth     = byte(4)
	OpInvalidate = byte(5) // key=pattern; value="prefix"|"regex"

	StatusOK    = byte(0)
	StatusMiss  = byte(1)
	StatusError = byte(2)
)

var (
	ErrTooLarge = errors.New("mocache: frame too large")
	ErrMagic    = errors.New("mocache: bad magic")
	ErrVersion  = errors.New("mocache: unsupported version")
	ErrShort    = errors.New("mocache: truncated frame")
)

type Request struct {
	Op    byte
	ID    uint32
	Key   string
	Value []byte
	TTL   uint32 // seconds; 0 = no expiry
}

type Response struct {
	Status byte
	ID     uint32
	Value  []byte
	Err    string
}

func WriteRequest(w io.Writer, req Request) error {
	key := []byte(req.Key)
	if len(key) > 0xffff {
		return ErrTooLarge
	}
	n := 20 + len(key) + len(req.Value) // magic+ver+op+id+klen + key + ttl+vlen + value
	buf := make([]byte, 4+n)
	binary.BigEndian.PutUint32(buf[0:4], uint32(n))
	copy(buf[4:8], Magic)
	buf[8] = Version
	buf[9] = req.Op
	binary.BigEndian.PutUint32(buf[10:14], req.ID)
	binary.BigEndian.PutUint16(buf[14:16], uint16(len(key)))
	off := 16
	copy(buf[off:off+len(key)], key)
	off += len(key)
	binary.BigEndian.PutUint32(buf[off:off+4], req.TTL)
	off += 4
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(req.Value)))
	off += 4
	copy(buf[off:], req.Value)
	_, err := w.Write(buf)
	return err
}

func ReadRequest(r io.Reader) (Request, error) {
	payload, err := readFrame(r)
	if err != nil {
		return Request{}, err
	}
	if len(payload) < 20 {
		return Request{}, ErrShort
	}
	if string(payload[0:4]) != Magic {
		return Request{}, ErrMagic
	}
	if payload[4] != Version {
		return Request{}, ErrVersion
	}
	req := Request{Op: payload[5], ID: binary.BigEndian.Uint32(payload[6:10])}
	klen := int(binary.BigEndian.Uint16(payload[10:12]))
	if 20+klen > len(payload) {
		return Request{}, ErrShort
	}
	req.Key = string(payload[12 : 12+klen])
	off := 12 + klen
	req.TTL = binary.BigEndian.Uint32(payload[off : off+4])
	off += 4
	vlen := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	if off+vlen != len(payload) {
		return Request{}, ErrShort
	}
	if vlen > 0 {
		req.Value = append([]byte(nil), payload[off:off+vlen]...)
	}
	return req, nil
}

func WriteResponse(w io.Writer, resp Response) error {
	msg := []byte(resp.Err)
	if len(msg) > 0xffff {
		return ErrTooLarge
	}
	n := 16 + len(resp.Value) + len(msg)
	buf := make([]byte, 4+n)
	binary.BigEndian.PutUint32(buf[0:4], uint32(n))
	copy(buf[4:8], Magic)
	buf[8] = Version
	buf[9] = resp.Status
	binary.BigEndian.PutUint32(buf[10:14], resp.ID)
	binary.BigEndian.PutUint32(buf[14:18], uint32(len(resp.Value)))
	off := 18
	copy(buf[off:off+len(resp.Value)], resp.Value)
	off += len(resp.Value)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(msg)))
	off += 2
	copy(buf[off:], msg)
	_, err := w.Write(buf)
	return err
}

func ReadResponse(r io.Reader) (Response, error) {
	payload, err := readFrame(r)
	if err != nil {
		return Response{}, err
	}
	if len(payload) < 16 {
		return Response{}, ErrShort
	}
	if string(payload[0:4]) != Magic {
		return Response{}, ErrMagic
	}
	if payload[4] != Version {
		return Response{}, ErrVersion
	}
	resp := Response{Status: payload[5], ID: binary.BigEndian.Uint32(payload[6:10])}
	vlen := int(binary.BigEndian.Uint32(payload[10:14]))
	if 16+vlen > len(payload) {
		return Response{}, ErrShort
	}
	if vlen > 0 {
		resp.Value = append([]byte(nil), payload[14:14+vlen]...)
	}
	off := 14 + vlen
	if off+2 > len(payload) {
		return Response{}, ErrShort
	}
	elen := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	if off+elen != len(payload) {
		return Response{}, ErrShort
	}
	if elen > 0 {
		resp.Err = string(payload[off : off+elen])
	}
	return resp, nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > MaxFrame {
		return nil, ErrTooLarge
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

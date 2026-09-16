package protocol

import (
	"bytes"
	"testing"
)

func TestRequestRoundTrip(t *testing.T) {
	in := Request{Op: OpSet, ID: 42, Key: "user:123", Value: []byte("hello"), TTL: 300}
	var buf bytes.Buffer
	if err := WriteRequest(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadRequest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != in.Op || out.ID != in.ID || out.Key != in.Key || out.TTL != in.TTL || !bytes.Equal(out.Value, in.Value) {
		t.Fatalf("%+v vs %+v", out, in)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	in := Response{Status: StatusOK, ID: 7, Value: []byte("val"), Err: ""}
	var buf bytes.Buffer
	if err := WriteResponse(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadResponse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != in.Status || out.ID != in.ID || !bytes.Equal(out.Value, in.Value) || out.Err != in.Err {
		t.Fatalf("%+v vs %+v", out, in)
	}
}

func TestEmptyGet(t *testing.T) {
	in := Request{Op: OpGet, ID: 1, Key: "k"}
	var buf bytes.Buffer
	if err := WriteRequest(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadRequest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Key != "k" || len(out.Value) != 0 {
		t.Fatalf("%+v", out)
	}
}

func TestRejectsHugeFrame(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x40, 0x00, 0x01}) // 4MB+1
	if _, err := ReadRequest(&buf); err != ErrTooLarge {
		t.Fatalf("err=%v", err)
	}
}

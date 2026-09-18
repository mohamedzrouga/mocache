package resp

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestReadCommandArray(t *testing.T) {
	r := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nhello\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 3 || string(args[0]) != "SET" || string(args[2]) != "hello" {
		t.Fatalf("got %q", args)
	}
}

// Binary-safe: values may contain CRLF and NUL bytes, so lengths — not
// delimiters — must drive the parse.
func TestReadCommandBinarySafe(t *testing.T) {
	val := "a\r\nb\x00c"
	r := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\n" + val + "\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if string(args[2]) != val {
		t.Fatalf("value = %q, want %q", args[2], val)
	}
}

func TestReadCommandInline(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\nECHO  hi \r\n"))
	first, err := r.ReadCommand()
	if err != nil || len(first) != 1 || string(first[0]) != "PING" {
		t.Fatalf("inline PING: %q %v", first, err)
	}
	second, err := r.ReadCommand()
	if err != nil || len(second) != 2 || string(second[1]) != "hi" {
		t.Fatalf("inline ECHO: %q %v", second, err)
	}
}

func TestReadCommandRejectsOversized(t *testing.T) {
	// A huge declared bulk length must be refused before allocating it.
	r := NewReader(strings.NewReader("*1\r\n$99999999\r\n"))
	if _, err := r.ReadCommand(); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	r = NewReader(strings.NewReader("*99999999\r\n"))
	if _, err := r.ReadCommand(); !errors.Is(err, ErrTooManyArgs) {
		t.Fatalf("err = %v, want ErrTooManyArgs", err)
	}
}

func TestReadCommandProtocolError(t *testing.T) {
	r := NewReader(strings.NewReader("*1\r\n+notbulk\r\n"))
	if _, err := r.ReadCommand(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v, want ErrProtocol", err)
	}
}

func write(t *testing.T, proto int, fn func(w *Writer)) string {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetProtocol(proto)
	fn(w)
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return buf.String()
}

func TestWriterRESP2(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(w *Writer)
		want string
	}{
		{"simple", func(w *Writer) { w.OK() }, "+OK\r\n"},
		{"error", func(w *Writer) { w.Error("ERR nope") }, "-ERR nope\r\n"},
		{"int", func(w *Writer) { w.Int(-3) }, ":-3\r\n"},
		{"bulk", func(w *Writer) { w.BulkString("hi") }, "$2\r\nhi\r\n"},
		{"empty bulk", func(w *Writer) { w.BulkString("") }, "$0\r\n\r\n"},
		{"nil", func(w *Writer) { w.Nil() }, "$-1\r\n"},
		{"nil array", func(w *Writer) { w.NilArray() }, "*-1\r\n"},
		{"bool", func(w *Writer) { w.Bool(true) }, ":1\r\n"},
		{"map flattens", func(w *Writer) { w.Map(1); w.BulkString("k"); w.BulkString("v") }, "*2\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"verbatim degrades", func(w *Writer) { w.Verbatim("txt", "x") }, "$1\r\nx\r\n"},
	} {
		if got := write(t, RESP2, tc.fn); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestWriterRESP3(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(w *Writer)
		want string
	}{
		{"nil", func(w *Writer) { w.Nil() }, "_\r\n"},
		{"nil array", func(w *Writer) { w.NilArray() }, "_\r\n"},
		{"bool true", func(w *Writer) { w.Bool(true) }, "#t\r\n"},
		{"bool false", func(w *Writer) { w.Bool(false) }, "#f\r\n"},
		{"map", func(w *Writer) { w.Map(1); w.BulkString("k"); w.BulkString("v") }, "%1\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"set", func(w *Writer) { w.Set(0) }, "~0\r\n"},
		{"verbatim", func(w *Writer) { w.Verbatim("txt", "xy") }, "=6\r\ntxt:xy\r\n"},
	} {
		if got := write(t, RESP3, tc.fn); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

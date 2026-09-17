// Package resp implements the Redis serialization protocol, RESP2 and RESP3.
//
// It exists so MoCache can be driven by stock Redis clients (redis-py, go-redis,
// redis-cli) without those clients knowing anything about MoCache. Written
// against the protocol spec rather than linked from a library, for the same
// reason internal/obs writes Prometheus text by hand: the cache node ships with
// no third-party dependencies.
//
// Reading: clients send commands as arrays of bulk strings; inline commands
// ("PING\r\n" typed into netcat) are also accepted, as real Redis does.
// Writing: a Writer is created in RESP2 and switches to RESP3 when the client
// sends HELLO 3, which changes how nulls, maps, booleans and doubles are framed.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Protocol version negotiated with HELLO.
const (
	RESP2 = 2
	RESP3 = 3
)

// Limits. A client must not be able to make the server allocate without bound:
// these caps are the RESP equivalent of the 1 MiB HTTP body cap and the 4 MiB
// RPC frame cap elsewhere in the node.
const (
	MaxArgs      = 1 << 20 // elements in one command array
	MaxBulk      = 16 << 20
	MaxInlineLen = 64 << 10
)

var (
	ErrProtocol    = errors.New("resp: protocol error")
	ErrTooLarge    = errors.New("resp: argument too large")
	ErrTooManyArgs = errors.New("resp: too many arguments")
)

// Reader parses commands from a connection.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 16<<10)}
}

// ReadCommand returns the next command as its argument vector. A nil slice with
// a nil error means "empty command" (a bare newline), which callers skip.
func (r *Reader) ReadCommand() ([][]byte, error) {
	prefix, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		// Inline command: everything up to CRLF, split on whitespace.
		if err := r.r.UnreadByte(); err != nil {
			return nil, err
		}
		line, err := r.readLine(MaxInlineLen)
		if err != nil {
			return nil, err
		}
		return splitInline(line), nil
	}

	n, err := r.readCount()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil // *0 or *-1: nothing to execute
	}
	if n > MaxArgs {
		return nil, ErrTooManyArgs
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		b, err := r.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != '$' {
			return nil, fmt.Errorf("%w: expected $, got %q", ErrProtocol, b)
		}
		size, err := r.readCount()
		if err != nil {
			return nil, err
		}
		if size < 0 {
			args = append(args, nil)
			continue
		}
		if size > MaxBulk {
			return nil, ErrTooLarge
		}
		buf := make([]byte, size+2) // payload + CRLF
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		args = append(args, buf[:size])
	}
	return args, nil
}

func (r *Reader) readCount() (int, error) {
	line, err := r.readLine(32)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, fmt.Errorf("%w: bad length %q", ErrProtocol, line)
	}
	return n, nil
}

// readLine returns one CRLF-terminated line without the terminator.
func (r *Reader) readLine(max int) ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, ErrTooLarge
	}
	if err != nil {
		return nil, err
	}
	if len(line) > max {
		return nil, ErrTooLarge
	}
	end := len(line) - 1
	if end > 0 && line[end-1] == '\r' {
		end--
	}
	out := make([]byte, end)
	copy(out, line[:end])
	return out, nil
}

func splitInline(line []byte) [][]byte {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		start := i
		for i < len(line) && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		if i > start {
			args = append(args, line[start:i])
		}
	}
	return args
}

// Writer serializes replies. Not safe for concurrent use: one per connection,
// which is also how the protocol works (replies are ordered per connection).
type Writer struct {
	w     *bufio.Writer
	proto int
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 16<<10), proto: RESP2}
}

// SetProtocol switches framing after a successful HELLO.
func (w *Writer) SetProtocol(v int) { w.proto = v }
func (w *Writer) Protocol() int     { return w.proto }
func (w *Writer) Flush() error      { return w.w.Flush() }

func (w *Writer) Simple(s string) { w.w.WriteByte('+'); w.w.WriteString(s); w.crlf() }
func (w *Writer) OK()             { w.Simple("OK") }

// Error writes an error reply. The first word is the code clients switch on
// (ERR, WRONGTYPE, MOVED, ASK, CROSSSLOT), so callers pass it already prefixed.
func (w *Writer) Error(msg string) { w.w.WriteByte('-'); w.w.WriteString(msg); w.crlf() }

func (w *Writer) Errorf(format string, a ...any) { w.Error(fmt.Sprintf(format, a...)) }

func (w *Writer) Int(n int64) {
	w.w.WriteByte(':')
	w.w.Write(strconv.AppendInt(nil, n, 10))
	w.crlf()
}

func (w *Writer) Bulk(b []byte) {
	w.w.WriteByte('$')
	w.w.Write(strconv.AppendInt(nil, int64(len(b)), 10))
	w.crlf()
	w.w.Write(b)
	w.crlf()
}

func (w *Writer) BulkString(s string) { w.Bulk([]byte(s)) }

// Nil is a null reply: RESP3 has one null type, RESP2 spells it as a null bulk.
func (w *Writer) Nil() {
	if w.proto >= RESP3 {
		w.w.WriteString("_\r\n")
		return
	}
	w.w.WriteString("$-1\r\n")
}

// NilArray is the null array (RESP2 "*-1"); RESP3 folds it into the null type.
func (w *Writer) NilArray() {
	if w.proto >= RESP3 {
		w.w.WriteString("_\r\n")
		return
	}
	w.w.WriteString("*-1\r\n")
}

// Array writes a header for n elements; the caller writes the elements.
func (w *Writer) Array(n int) {
	w.w.WriteByte('*')
	w.w.Write(strconv.AppendInt(nil, int64(n), 10))
	w.crlf()
}

// Map writes a header for n key/value PAIRS. RESP2 has no map type, so it is
// flattened into an array of 2n elements — which is what RESP2 clients expect
// from commands like CONFIG GET and HELLO.
func (w *Writer) Map(n int) {
	if w.proto >= RESP3 {
		w.w.WriteByte('%')
		w.w.Write(strconv.AppendInt(nil, int64(n), 10))
		w.crlf()
		return
	}
	w.Array(n * 2)
}

// Set writes a set header; RESP2 degrades to an array.
func (w *Writer) Set(n int) {
	if w.proto >= RESP3 {
		w.w.WriteByte('~')
		w.w.Write(strconv.AppendInt(nil, int64(n), 10))
		w.crlf()
		return
	}
	w.Array(n)
}

func (w *Writer) Bool(b bool) {
	if w.proto >= RESP3 {
		if b {
			w.w.WriteString("#t\r\n")
		} else {
			w.w.WriteString("#f\r\n")
		}
		return
	}
	if b {
		w.Int(1)
	} else {
		w.Int(0)
	}
}

func (w *Writer) Double(f float64) {
	if w.proto >= RESP3 {
		w.w.WriteByte(',')
		w.w.Write(strconv.AppendFloat(nil, f, 'g', 17, 64))
		w.crlf()
		return
	}
	w.Bulk(strconv.AppendFloat(nil, f, 'g', 17, 64))
}

// Verbatim is RESP3's typed text (used by INFO); RESP2 sees a plain bulk string.
func (w *Writer) Verbatim(format, s string) {
	if w.proto >= RESP3 {
		w.w.WriteByte('=')
		w.w.Write(strconv.AppendInt(nil, int64(len(s)+4), 10))
		w.crlf()
		w.w.WriteString(format)
		w.w.WriteByte(':')
		w.w.WriteString(s)
		w.crlf()
		return
	}
	w.BulkString(s)
}

// Strings writes a flat array of bulk strings.
func (w *Writer) Strings(items []string) {
	w.Array(len(items))
	for _, s := range items {
		w.BulkString(s)
	}
}

func (w *Writer) crlf() { w.w.WriteString("\r\n") }

package redcon

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
)

type Conn interface {
	RemoteAddr() string
	Close() error
	WriteError(msg string)
	WriteString(str string)
	WriteBulk(bulk string)
	WriteInt(num int)
	WriteArray(count int)
	WriteNull()
}

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
)

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

func ListenAndServe(
	addr string, handler func(conn Conn, cmds [][]string),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	var mu sync.Mutex
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		wr := newWriter(conn)
		wrc := &connWriter{wr, conn.RemoteAddr().String()}
		if accept != nil && !accept(wrc) {
			conn.Close()
			continue
		}
		go func() {
			var err error
			defer func() {
				conn.Close()
				if closed != nil {
					mu.Lock()
					defer mu.Unlock()
					if err == io.EOF {
						err = nil
					}
					closed(wrc, err)
				}
			}()
			rd := newReader(conn)
			err = func() error {
				for {
					cmds, err := rd.ReadCommands()
					if err != nil {
						if err, ok := err.(*errProtocol); ok {
							// All protocol errors should attempt a response to
							// the client. Ignore errors.
							wr.WriteError("ERR " + err.Error())
							wr.Flush()
						}
						return err
					}
					if len(cmds) > 0 {
						handler(wrc, cmds)
					}
					if wr.err != nil {
						if wr.err == errClosed {
							return nil
						}
						return wr.err
					}
					if err := wr.Flush(); err != nil {
						return err
					}
				}
			}()
		}()
	}
}

type connWriter struct {
	wr   *respWriter
	addr string
}

func (wrc *connWriter) Close() error {
	return wrc.wr.Close()
}
func (wrc *connWriter) WriteString(str string) {
	wrc.wr.WriteString(str)
}
func (wrc *connWriter) WriteBulk(bulk string) {
	wrc.wr.WriteBulk(bulk)
}
func (wrc *connWriter) WriteInt(num int) {
	wrc.wr.WriteInt(num)
}
func (wrc *connWriter) WriteError(msg string) {
	wrc.wr.WriteError(msg)
}
func (wrc *connWriter) WriteArray(count int) {
	wrc.wr.WriteMultiBulkStart(count)
}
func (wrc *connWriter) WriteNull() {
	wrc.wr.WriteNull()
}
func (wrc *connWriter) RemoteAddr() string {
	return wrc.addr
}

// Reader represents a RESP command reader.
type respReader struct {
	r io.Reader // base reader
	b []byte    // unprocessed bytes
	a []byte    // static read buffer
}

// NewReader returns a RESP command reader.
func newReader(r io.Reader) *respReader {
	return &respReader{
		r: r,
		a: make([]byte, 8192),
	}
}

// ReadCommands reads one or more commands from the reader.
func (r *respReader) ReadCommands() ([][]string, error) {
	if len(r.b) > 0 {
		// we have some potential commands.
		var cmds [][]string
	next:
		switch r.b[0] {
		default:
			// just a plain text command
			for i := 0; i < len(r.b); i++ {
				if r.b[i] == '\n' {
					var line []byte
					if i > 0 && r.b[i-1] == '\r' {
						line = r.b[:i-1]
					} else {
						line = r.b[:i]
					}
					var args []string
					var quote bool
					var escape bool
				outer:
					for {
						nline := make([]byte, 0, len(line))
						for i := 0; i < len(line); i++ {
							c := line[i]
							if !quote {
								if c == ' ' {
									if len(nline) > 0 {
										args = append(args, string(nline))
									}
									line = line[i+1:]
									continue outer
								}
								if c == '"' {
									if i != 0 {
										return nil, errUnbalancedQuotes
									}
									quote = true
									line = line[i+1:]
									continue outer
								}
							} else {
								if escape {
									escape = false
									switch c {
									case 'n':
										c = '\n'
									case 'r':
										c = '\r'
									case 't':
										c = '\t'
									}
								} else if c == '"' {
									quote = false
									args = append(args, string(nline))
									line = line[i+1:]
									if len(line) > 0 && line[0] != ' ' {
										return nil, errUnbalancedQuotes
									}
									continue outer
								} else if c == '\\' {
									escape = true
									continue
								}
							}
							nline = append(nline, c)
						}
						if quote {
							return nil, errUnbalancedQuotes
						}
						if len(line) > 0 {
							args = append(args, string(line))
						}
						break
					}
					if len(args) > 0 {
						cmds = append(cmds, args)
					}
					r.b = r.b[i+1:]
					if len(r.b) > 0 {
						goto next
					} else {
						goto done
					}
				}
			}
		case '*':
			// resp formatted command
			var si int
		outer2:
			for i := 0; i < len(r.b); i++ {
				var args []string
				if r.b[i] == '\n' {
					if r.b[i-1] != '\r' {
						return nil, errInvalidMultiBulkLength
					}
					ni, err := strconv.ParseInt(string(r.b[si+1:i-1]), 10, 64)
					if err != nil || ni <= 0 {
						return nil, errInvalidMultiBulkLength
					}
					args = make([]string, 0, int(ni))
					for j := 0; j < int(ni); j++ {
						// read bulk length
						i++
						if i < len(r.b) {
							if r.b[i] != '$' {
								return nil, &errProtocol{"expected '$', got '" +
									string(r.b[i]) + "'"}
							}
							si = i
							for ; i < len(r.b); i++ {
								if r.b[i] == '\n' {
									if r.b[i-1] != '\r' {
										return nil, errInvalidBulkLength
									}
									s := string(r.b[si+1 : i-1])
									ni2, err := strconv.ParseInt(s, 10, 64)
									if err != nil || ni2 < 0 {
										return nil, errInvalidBulkLength
									}
									if i+int(ni2)+2 >= len(r.b) {
										// not ready
										break outer2
									}
									if r.b[i+int(ni2)+2] != '\n' ||
										r.b[i+int(ni2)+1] != '\r' {
										return nil, errInvalidBulkLength
									}
									arg := string(r.b[i+1 : i+1+int(ni2)])
									i += int(ni2) + 2
									args = append(args, arg)
									break
								}
							}
						}
					}
					if len(args) == cap(args) {
						cmds = append(cmds, args)
						r.b = r.b[i+1:]
						if len(r.b) > 0 {
							goto next
						} else {
							goto done
						}
					}
				}
			}
		}
	done:
		if len(r.b) == 0 {
			r.b = nil
		}
		if len(cmds) > 0 {
			return cmds, nil
		}
	}
	n, err := r.r.Read(r.a[:])
	if err != nil {
		if err == io.EOF {
			if len(r.b) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
		}
		return nil, err
	}
	r.b = append(r.b, r.a[:n]...)
	return r.ReadCommands()
}

var errClosed = errors.New("closed")

type respWriter struct {
	w   io.Writer
	b   *bytes.Buffer
	err error
}

func newWriter(w io.Writer) *respWriter {
	return &respWriter{w: w, b: &bytes.Buffer{}}
}

func (w *respWriter) WriteNull() error {
	if w.err != nil {
		return w.err
	}
	w.b.WriteString("$-1\r\n")
	return nil
}
func (w *respWriter) WriteMultiBulkStart(count int) error {
	if w.err != nil {
		return w.err
	}
	w.b.WriteByte('*')
	w.b.WriteString(strconv.FormatInt(int64(count), 10))
	w.b.WriteString("\r\n")
	return nil
}

func (w *respWriter) WriteBulk(bulk string) error {
	if w.err != nil {
		return w.err
	}
	w.b.WriteByte('$')
	w.b.WriteString(strconv.FormatInt(int64(len(bulk)), 10))
	w.b.WriteString("\r\n")
	w.b.WriteString(bulk)
	w.b.WriteString("\r\n")
	return nil
}

func (w *respWriter) Flush() error {
	if w.err != nil {
		return w.err
	}
	if w.b.Len() == 0 {
		return nil
	}
	if _, err := w.b.WriteTo(w.w); err != nil {
		w.err = err
		return err
	}
	w.b.Reset()
	return nil
}

func (w *respWriter) WriteMultiBulk(bulks []string) error {
	if err := w.WriteMultiBulkStart(len(bulks)); err != nil {
		return err
	}
	for _, bulk := range bulks {
		if err := w.WriteBulk(bulk); err != nil {
			return err
		}
	}
	return nil
}

func (w *respWriter) WriteError(msg string) error {
	if w.err != nil {
		return w.err
	}
	w.b.WriteByte('-')
	w.b.WriteString(msg)
	w.b.WriteString("\r\n")
	return nil
}

func (w *respWriter) WriteString(msg string) error {
	if w.err != nil {
		return w.err
	}
	w.b.WriteByte('+')
	w.b.WriteString(msg)
	w.b.WriteString("\r\n")
	return nil
}

func (w *respWriter) WriteInt(num int) error {
	if w.err != nil {
		return w.err
	}
	w.b.WriteByte(':')
	w.b.WriteString(strconv.FormatInt(int64(num), 10))
	w.b.WriteString("\r\n")
	return nil
}

func (w *respWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if err := w.Flush(); err != nil {
		w.err = err
		return err
	}
	w.err = errClosed
	return nil
}

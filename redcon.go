// Package redcon provides a custom redis server implementation.
package redcon

import (
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
)

// Conn represents a client connection
type Conn interface {
	RemoteAddr() string
	Close() error
	WriteError(msg string)
	WriteString(str string)
	WriteBulk(bulk string)
	WriteInt(num int)
	WriteArray(count int)
	WriteNull()
	SetReadBuffer(bytes int)
}

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
)

const defaultBufLen = 1024 * 64

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

// ListenAndServe creates a new server and binds to addr.
func ListenAndServe(
	addr string, handler func(conn Conn, cmds [][]string),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	tcpln := ln.(*net.TCPListener)
	if handler == nil {
		handler = func(conn Conn, cmds [][]string) {}
	}
	var mu sync.Mutex
	for {
		tcpc, err := tcpln.AcceptTCP()
		if err != nil {
			return err
		}
		c := &conn{
			tcpc,
			newWriter(tcpc),
			newReader(tcpc),
			tcpc.RemoteAddr().String(),
		}
		if accept != nil && !accept(c) {
			c.Close()
			continue
		}
		go handle(c, &mu, handler, closed)
	}
}
func handle(c *conn, mu *sync.Mutex,
	handler func(conn Conn, cmds [][]string),
	closed func(conn Conn, err error)) {
	var err error
	defer func() {
		c.conn.Close()
		if closed != nil {
			mu.Lock()
			defer mu.Unlock()
			if err == io.EOF {
				err = nil
			}
			closed(c, err)
		}
	}()
	err = func() error {
		for {
			cmds, err := c.rd.ReadCommands()
			if err != nil {
				if err, ok := err.(*errProtocol); ok {
					// All protocol errors should attempt a response to
					// the client. Ignore errors.
					c.wr.WriteError("ERR " + err.Error())
					c.wr.Flush()
				}
				return err
			}
			if len(cmds) > 0 {
				handler(c, cmds)
			}
			if c.wr.err != nil {
				if c.wr.err == errClosed {
					return nil
				}
				return c.wr.err
			}
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
	}()
}

type conn struct {
	conn *net.TCPConn
	wr   *writer
	rd   *reader
	addr string
}

func (c *conn) Close() error {
	return c.wr.Close()
}
func (c *conn) WriteString(str string) {
	c.wr.WriteString(str)
}
func (c *conn) WriteBulk(bulk string) {
	c.wr.WriteBulk(bulk)
}
func (c *conn) WriteInt(num int) {
	c.wr.WriteInt(num)
}
func (c *conn) WriteError(msg string) {
	c.wr.WriteError(msg)
}
func (c *conn) WriteArray(count int) {
	c.wr.WriteMultiBulkStart(count)
}
func (c *conn) WriteNull() {
	c.wr.WriteNull()
}
func (c *conn) RemoteAddr() string {
	return c.addr
}
func (c *conn) SetReadBuffer(bytes int) {
	c.rd.buflen = bytes
}

// Reader represents a RESP command reader.
type reader struct {
	r      io.Reader // base reader
	b      []byte    // unprocessed bytes
	a      []byte    // static read buffer
	buflen int       // buffer len
}

// NewReader returns a RESP command reader.
func newReader(r io.Reader) *reader {
	return &reader{
		r:      r,
		buflen: defaultBufLen,
	}
}

// ReadCommands reads one or more commands from the reader.
func (r *reader) ReadCommands() ([][]string, error) {
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
	if len(r.a) == 0 {
		r.a = make([]byte, r.buflen)
	}
	n, err := r.r.Read(r.a)
	if err != nil {
		if err == io.EOF {
			if len(r.b) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
		}
		return nil, err
	}
	if len(r.b) == 0 {
		r.b = r.a[:n]
	} else {
		r.b = append(r.b, r.a[:n]...)
	}
	r.a = r.a[n:]

	return r.ReadCommands()
}

var errClosed = errors.New("closed")

type writer struct {
	w   *net.TCPConn
	b   []byte
	err error
}

func newWriter(w *net.TCPConn) *writer {
	return &writer{w: w, b: make([]byte, 0, 512)}
}

func (w *writer) WriteNull() error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, '$', '-', '1', '\r', '\n')
	return nil
}
func (w *writer) WriteMultiBulkStart(count int) error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, '*')
	w.b = append(w.b, []byte(strconv.FormatInt(int64(count), 10))...)
	w.b = append(w.b, '\r', '\n')
	return nil
}

func (w *writer) WriteBulk(bulk string) error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, '$')
	w.b = append(w.b, []byte(strconv.FormatInt(int64(len(bulk)), 10))...)
	w.b = append(w.b, '\r', '\n')
	w.b = append(w.b, []byte(bulk)...)
	w.b = append(w.b, '\r', '\n')
	return nil
}

func (w *writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.b) == 0 {
		return nil
	}
	if _, err := w.w.Write(w.b); err != nil {
		w.err = err
		return err
	}
	w.b = w.b[:0]
	return nil
}

func (w *writer) WriteMultiBulk(bulks []string) error {
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

func (w *writer) WriteError(msg string) error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, '-')
	w.b = append(w.b, []byte(msg)...)
	w.b = append(w.b, '\r', '\n')
	return nil
}

func (w *writer) WriteString(msg string) error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, '+')
	w.b = append(w.b, []byte(msg)...)
	w.b = append(w.b, '\r', '\n')
	return nil
}

func (w *writer) WriteInt(num int) error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, ':')
	w.b = append(w.b, []byte(strconv.FormatInt(int64(num), 10))...)
	w.b = append(w.b, '\r', '\n')
	return nil
}

func (w *writer) Close() error {
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

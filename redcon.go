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
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() string
	// Close closes the connection.
	Close() error
	// WriteError writes an error to the client.
	WriteError(msg string)
	// WriteString writes a string to the client.
	WriteString(str string)
	// WriteBulk writes a bulk string to the client.
	WriteBulk(bulk string)
	// WriteInt writes an integer to the client.
	WriteInt(num int)
	// WriteArray writes an array header. You must then write addtional
	// sub-responses to the client to complete the response.
	// For example to write two strings:
	//
	//   c.WriteArray(2)
	//   c.WriteBulk("item 1")
	//   c.WriteBulk("item 2")
	WriteArray(count int)
	// WriteNull writes a null to the client
	WriteNull()
	// SetReadBuffer updates the buffer read size for the connection
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

// Server represents a Redcon server.
type Server struct {
	mu      sync.Mutex
	addr    string
	handler func(conn Conn, cmds [][]string)
	accept  func(conn Conn) bool
	closed  func(conn Conn, err error)
	ln      *net.TCPListener
	done    bool
	conns   map[*conn]bool
}

// NewServer returns a new server
func NewServer(
	addr string, handler func(conn Conn, cmds [][]string),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) *Server {
	return &Server{
		addr:    addr,
		handler: handler,
		accept:  accept,
		closed:  closed,
		conns:   make(map[*conn]bool),
	}
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *Server) Close() error {
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *Server) ListenAndServe() error {
	var addr = s.addr
	var handler = s.handler
	var accept = s.accept
	var closed = s.closed
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln.(*net.TCPListener)
	defer func() {
		ln.Close()
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for c := range s.conns {
				c.Close()
			}
			s.conns = nil
		}()
	}()
	if handler == nil {
		handler = func(conn Conn, cmds [][]string) {}
	}
	for {
		tcpc, err := s.ln.AcceptTCP()
		if err != nil {
			s.mu.Lock()
			done := s.done
			s.mu.Unlock()
			if done {
				return nil
			}
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
		s.mu.Lock()
		s.conns[c] = true
		s.mu.Unlock()
		go handle(s, c, handler, closed)
	}
}

// ListenAndServe creates a new server and binds to addr.
func ListenAndServe(
	addr string, handler func(conn Conn, cmds [][]string),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) error {
	return NewServer(addr, handler, accept, closed).ListenAndServe()
}

func handle(
	s *Server, c *conn,
	handler func(conn Conn, cmds [][]string),
	closed func(conn Conn, err error)) {
	var err error
	defer func() {
		c.conn.Close()
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.conns, c)
			if closed != nil {
				if err == io.EOF {
					err = nil
				}
				closed(c, err)
			}
		}()
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
	err := c.wr.Close() // flush and close the writer
	c.conn.Close()      // close the connection. ignore this error
	return err          // return the writer error only
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
	c.wr.WriteArrayStart(count)
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
func (w *writer) WriteArrayStart(count int) error {
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

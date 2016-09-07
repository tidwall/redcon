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
	// WriteBulkBytes writes bulk bytes to the client.
	WriteBulkBytes(bulk []byte)
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
	// Context returns a user-defined context
	Context() interface{}
	// SetContext sets a user-defined context
	SetContext(v interface{})
}

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
)

const (
	defaultBufLen   = 4 * 1024
	defaultPoolSize = 64
)

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

// Server represents a Redcon server.
type Server struct {
	mu       sync.Mutex
	addr     string
	shandler func(conn Conn, cmds [][]string)
	bhandler func(conn Conn, cmds [][][]byte)
	accept   func(conn Conn) bool
	closed   func(conn Conn, err error)
	ln       *net.TCPListener
	done     bool
	conns    map[*conn]bool
	rdpool   [][]byte
	wrpool   [][]byte
	initbuf  []byte
}

// NewServer returns a new server
func NewServer(
	addr string, handler func(conn Conn, cmds [][]string),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) *Server {
	return &Server{
		addr:     addr,
		shandler: handler,
		accept:   accept,
		closed:   closed,
		conns:    make(map[*conn]bool),
		initbuf:  make([]byte, defaultPoolSize*defaultBufLen),
	}
}

// NewServerBytes returns a new server
// It uses []byte instead of string for the handler commands.
func NewServerBytes(
	addr string, handler func(conn Conn, cmds [][][]byte),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) *Server {
	s := NewServer(addr, nil, accept, closed)
	s.bhandler = handler
	return s
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *Server) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

// ListenServeAndSignal serves incoming connections and passes nil or error
// when listening. signal can be nil.
func (s *Server) ListenServeAndSignal(signal chan error) error {
	var addr = s.addr
	var shandler = s.shandler
	var bhandler = s.bhandler
	var accept = s.accept
	var closed = s.closed
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		if signal != nil {
			signal <- err
		}
		return err
	}
	if signal != nil {
		signal <- nil
	}
	tln := ln.(*net.TCPListener)
	s.mu.Lock()
	s.ln = tln
	s.mu.Unlock()
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
	for {
		tcpc, err := tln.AcceptTCP()
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
			nil,
		}
		s.mu.Lock()
		if len(s.rdpool) > 0 {
			c.rd.buf = s.rdpool[len(s.rdpool)-1]
			s.rdpool = s.rdpool[:len(s.rdpool)-1]
		}
		if len(s.wrpool) > 0 {
			c.wr.b = s.wrpool[len(s.wrpool)-1]
			s.wrpool = s.wrpool[:len(s.wrpool)-1]
			c.wr.b = c.wr.b[:0]
		}
		s.conns[c] = true
		s.mu.Unlock()
		if accept != nil && !accept(c) {
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
			c.Close()
			continue
		}
		if shandler == nil && bhandler == nil {
			shandler = func(conn Conn, cmds [][]string) {}
		} else if shandler != nil {
			bhandler = nil
		} else if bhandler != nil {
			shandler = nil
		}
		go handle(s, c, shandler, bhandler, closed)
	}
}

// ListenAndServe creates a new server and binds to addr.
func ListenAndServe(
	addr string, handler func(conn Conn, cmds [][]string),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) error {
	return NewServer(addr, handler, accept, closed).ListenAndServe()
}

// ListenAndServeBytes creates a new server and binds to addr.
// It uses []byte instead of string for the handler commands.
func ListenAndServeBytes(
	addr string, handler func(conn Conn, cmds [][][]byte),
	accept func(conn Conn) bool, closed func(conn Conn, err error),
) error {
	return NewServerBytes(addr, handler, accept, closed).ListenAndServe()
}

func handle(
	s *Server, c *conn,
	shandler func(conn Conn, cmds [][]string),
	bhandler func(conn Conn, cmds [][][]byte),
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
			if len(s.rdpool) < defaultPoolSize && len(c.rd.buf) < defaultBufLen {
				s.rdpool = append(s.rdpool, c.rd.buf)
			}
			if len(s.wrpool) < defaultPoolSize && len(c.wr.b) < defaultBufLen {
				s.wrpool = append(s.wrpool, c.wr.b)
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
				if shandler != nil {
					// convert bytes to strings
					scmds := make([][]string, len(cmds))
					for i := 0; i < len(cmds); i++ {
						scmds[i] = make([]string, len(cmds[i]))
						for j := 0; j < len(scmds[i]); j++ {
							scmds[i][j] = string(scmds[i][j])
						}
					}
					shandler(c, scmds)
				} else if bhandler != nil {
					// copy the byte commands once, before exposing to the
					// client.
					for i := 0; i < len(cmds); i++ {
						for j := 0; j < len(cmds[i]); j++ {
							nb := make([]byte, len(cmds[i][j]))
							copy(nb, cmds[i][j])
							cmds[i][j] = nb
						}
					}
					bhandler(c, cmds)
				}
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
	ctx  interface{}
}

func (c *conn) Close() error {
	err := c.wr.Close() // flush and close the writer
	c.conn.Close()      // close the connection. ignore this error
	return err          // return the writer error only
}
func (c *conn) Context() interface{} {
	return c.ctx
}
func (c *conn) SetContext(v interface{}) {
	c.ctx = v
}
func (c *conn) WriteString(str string) {
	c.wr.WriteString(str)
}
func (c *conn) WriteBulk(bulk string) {
	c.wr.WriteBulk(bulk)
}
func (c *conn) WriteBulkBytes(bulk []byte) {
	c.wr.WriteBulkBytes(bulk)
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
	buf    []byte
	start  int
	end    int
	buflen int
}

// NewReader returns a RESP command reader.
func newReader(r io.Reader) *reader {
	return &reader{
		r:      r,
		buflen: defaultBufLen,
	}
}

// ReadCommands reads one or more commands from the reader.
func (r *reader) ReadCommands() ([][][]byte, error) {
	if r.end-r.start > 0 {
		b := r.buf[r.start:r.end]
		// we have some potential commands.
		var cmds [][][]byte
	next:
		switch b[0] {
		default:
			// just a plain text command
			for i := 0; i < len(b); i++ {
				if b[i] == '\n' {
					var line []byte
					if i > 0 && b[i-1] == '\r' {
						line = b[:i-1]
					} else {
						line = b[:i]
					}
					var args [][]byte
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
										args = append(args, nline)
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
									args = append(args, nline)
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
							args = append(args, line)
						}
						break
					}
					if len(args) > 0 {
						cmds = append(cmds, args)
					}
					b = b[i+1:]
					if len(b) > 0 {
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
			for i := 0; i < len(b); i++ {
				var args [][]byte
				if b[i] == '\n' {
					if b[i-1] != '\r' {
						return nil, errInvalidMultiBulkLength
					}
					ni, err := parseInt(b[si+1 : i-1])
					if err != nil || ni <= 0 {
						return nil, errInvalidMultiBulkLength
					}
					args = make([][]byte, 0, ni)
					for j := 0; j < ni; j++ {
						// read bulk length
						i++
						if i < len(b) {
							if b[i] != '$' {
								return nil, &errProtocol{"expected '$', got '" +
									string(b[i]) + "'"}
							}
							si = i
							for ; i < len(b); i++ {
								if b[i] == '\n' {
									if b[i-1] != '\r' {
										return nil, errInvalidBulkLength
									}
									ni2, err := parseInt(b[si+1 : i-1])
									if err != nil || ni2 < 0 {
										return nil, errInvalidBulkLength
									}
									if i+ni2+2 >= len(b) {
										// not ready
										break outer2
									}
									if b[i+ni2+2] != '\n' ||
										b[i+ni2+1] != '\r' {
										return nil, errInvalidBulkLength
									}
									i++
									arg := b[i : i+ni2]
									if len(args) == 0 {
										for j := 0; j < len(arg); j++ {
											if arg[j] >= 'A' && arg[j] <= 'Z' {
												arg[j] += 32
											}
										}
									}
									i += ni2 + 1
									args = append(args, arg)
									break
								}
							}
						}
					}
					if len(args) == cap(args) {
						cmds = append(cmds, args)
						b = b[i+1:]
						if len(b) > 0 {
							goto next
						} else {
							goto done
						}
					}
				}
			}
		}
	done:
		if len(b) == 0 {
			r.start = 0
			r.end = 0
		} else {
			r.start = r.end - len(b)
		}
		if len(cmds) > 0 {
			return cmds, nil
		}
	}
	if r.end == len(r.buf) {
		if len(r.buf) == 0 {
			r.buf = make([]byte, r.buflen)
		} else {
			nbuf := make([]byte, len(r.buf)*2)
			copy(nbuf, r.buf)
			r.buf = nbuf
		}
	}
	n, err := r.r.Read(r.buf[r.end:])
	if err != nil {
		if err == io.EOF {
			if r.end > 0 {
				return nil, io.ErrUnexpectedEOF
			}
		}
		return nil, err
	}
	r.end += n
	return r.ReadCommands()
}
func parseInt(b []byte) (int, error) {
	switch len(b) {
	case 1:
		if b[0] >= '0' && b[0] <= '9' {
			return int(b[0] - '0'), nil
		}
	case 2:
		if b[0] >= '0' && b[0] <= '9' && b[1] >= '0' && b[1] <= '9' {
			return int(b[0]-'0')*10 + int(b[1]-'0'), nil
		}
	}
	var n int
	for i := 0; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return 0, errors.New("invalid number")
		}
		n = n*10 + int(b[i]-'0')
	}
	return n, nil
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

func (w *writer) WriteBulkBytes(bulk []byte) error {
	if w.err != nil {
		return w.err
	}
	w.b = append(w.b, '$')
	w.b = append(w.b, []byte(strconv.FormatInt(int64(len(bulk)), 10))...)
	w.b = append(w.b, '\r', '\n')
	w.b = append(w.b, bulk...)
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
	if msg == "OK" {
		w.b = append(w.b, '+', 'O', 'K', '\r', '\n')
	} else {
		w.b = append(w.b, '+')
		w.b = append(w.b, []byte(msg)...)
		w.b = append(w.b, '\r', '\n')
	}
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

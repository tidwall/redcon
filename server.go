package redcon

import (
	"sync"
	"net"
	"errors"
)

// Server defines a server for clients for managing client connections.
type Server struct {
	mu        sync.Mutex
	net       string
	laddr     string
	handler   func(conn Conn, cmd Command)
	accept    func(conn Conn) bool
	closed    func(conn Conn, err error)
	conns     map[*conn]bool
	ln        net.Listener
	done      bool
	processor *Processor
}

// NewServer returns a new Redcon server configured on "tcp" network net.
func NewServer(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) *Server {
	return NewServerNetwork(
		"tcp", addr, handler, accept, closed,
		nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap)
}

// NewServerNetwork returns a new Redcon server. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func NewServerNetwork(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) *Server {
	if handler == nil {
		panic("handler is nil")
	}
	s := &Server{
		net:           net,
		laddr:         laddr,
		handler:       handler,
		accept:        accept,
		closed:        closed,
		conns:         make(map[*conn]bool),
	}
	s.processor = NewProcessor(
		s, nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap)
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

// Addr returns server's listen address
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

// ListenServeAndSignal serves incoming connections and passes nil or error
// when listening. signal can be nil.
func (s *Server) ListenServeAndSignal(signal chan error) error {
	ln, err := net.Listen(s.net, s.laddr)
	if err != nil {
		if signal != nil {
			signal <- err
		}
		return err
	}
	s.ln = ln
	if signal != nil {
		signal <- nil
	}
	return serve(s)
}

// ListenAndServe creates a new server and binds to addr configured on "tcp" network net.
func ListenAndServe(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) error {
	return ListenAndServeNetwork(
		"tcp", addr, handler, accept, closed,
		nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap)
}

// ListenAndServeNetwork creates a new server and binds to addr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func ListenAndServeNetwork(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) error {
	return NewServerNetwork(
		net, laddr, handler, accept, closed,
		nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap,
	).ListenAndServe()
}

package redcon

import (
	"crypto/tls"
	"errors"
)

// TLSServer defines a server for clients for managing client connections.
type TLSServer struct {
	*Server
	config *tls.Config
}

// NewServerTLS returns a new Redcon TLS server configured on "tcp" network net.
func NewServerTLS(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) *TLSServer {
	return NewServerNetworkTLS(
		"tcp", addr, handler, accept, closed, config,
		nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap)
}

// NewServerNetworkTLS returns a new TLS Redcon server. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func NewServerNetworkTLS(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) *TLSServer {
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

	tls := &TLSServer{
		config: config,
		Server: s,
	}
	return tls
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *TLSServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *TLSServer) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

// ListenServeAndSignal serves incoming connections and passes nil or error
// when listening. signal can be nil.
func (s *TLSServer) ListenServeAndSignal(signal chan error) error {
	ln, err := tls.Listen(s.net, s.laddr, s.config)
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
	return serve(s.Server)
}

// ListenAndServeTLS creates a new TLS server and binds to addr configured on "tcp" network net.
func ListenAndServeTLS(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) error {
	return ListenAndServeNetworkTLS(
		"tcp", addr, handler, accept, closed, config,
		nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap)
}

// ListenAndServeNetworkTLS creates a new TLS server and binds to addr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func ListenAndServeNetworkTLS(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
	nsParser namespaceParser,
	serveGoPoolSize int,
	highLevelGoPoolSize int,
	lowLevelGoPoolSize int,
	highLevelCmdsMap map[int][]string,
) error {
	return NewServerNetworkTLS(
		net, laddr, handler, accept, closed, config,
		nsParser, serveGoPoolSize, highLevelGoPoolSize,
		lowLevelGoPoolSize, highLevelCmdsMap,
	).ListenAndServe()
}

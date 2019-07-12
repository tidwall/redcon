// Package redcon implements a Redis compatible server framework
package redcon

import (
	"errors"
)

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
	errDetached               = errors.New("detached")
	errIncompleteCommand      = errors.New("incomplete command")
	errTooMuchData            = errors.New("too much data")
)

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

// Command represent a command
type Command struct {
	// Raw is a encoded RESP message.
	Raw []byte
	// Args is a series of arguments that make up the command.
	Args [][]byte
}

// BaseWriter returns the underlying connection writer, if any
func BaseWriter(c Conn) *Writer {
	if c, ok := c.(*conn); ok {
		return c.wr
	}
	return nil
}

func serve(s *Server) error {
	defer func() {
		s.ln.Close()
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for c := range s.conns {
				c.Close()
			}
			s.conns = nil
			s.processor.Close()
		}()
	}()
	for {
		lnconn, err := s.ln.Accept()
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
			conn: lnconn,
			addr: lnconn.RemoteAddr().String(),
			wr:   NewWriter(lnconn),
			rd:   NewReader(lnconn),
		}
		s.mu.Lock()
		s.conns[c] = true
		s.mu.Unlock()
		if s.accept != nil && !s.accept(c) {
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
			c.Close()
			continue
		}
		s.processor.serveGoPool.Serve(c)
	}
}

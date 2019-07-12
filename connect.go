package redcon

import (
	"net"
	"errors"
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
	// WriteBulk writes bulk bytes to the client.
	WriteBulk(bulk []byte)
	// WriteBulkString writes a bulk string to the client.
	WriteBulkString(bulk string)
	// WriteInt writes an integer to the client.
	WriteInt(num int)
	// WriteInt64 writes a 64-but signed integer to the client.
	WriteInt64(num int64)
	// WriteArray writes an array header. You must then write additional
	// sub-responses to the client to complete the response.
	// For example to write two strings:
	//
	//   c.WriteArray(2)
	//   c.WriteBulk("item 1")
	//   c.WriteBulk("item 2")
	WriteArray(count int)
	// WriteNull writes a null to the client
	WriteNull()
	// WriteRaw writes raw data to the client.
	WriteRaw(data []byte)
	// Context returns a user-defined context
	Context() interface{}
	// SetContext sets a user-defined context
	SetContext(v interface{})
	// SetReadBuffer updates the buffer read size for the connection
	SetReadBuffer(bytes int)
	// Detach return a connection that is detached from the server.
	// Useful for operations like PubSub.
	//
	//   dconn := conn.Detach()
	//   go func(){
	//       defer dconn.Close()
	//       cmd, err := dconn.ReadCommand()
	//       if err != nil{
	//           fmt.Printf("read failed: %v\n", err)
	//	         return
	//       }
	//       fmt.Printf("received command: %v", cmd)
	//       hconn.WriteString("OK")
	//       if err := dconn.Flush(); err != nil{
	//           fmt.Printf("write failed: %v\n", err)
	//	         return
	//       }
	//   }()
	Detach() DetachedConn
	// ReadPipeline returns all commands in current pipeline, if any
	// The commands are removed from the pipeline.
	ReadPipeline() []Command
	// PeekPipeline returns all commands in current pipeline, if any.
	// The commands remain in the pipeline.
	PeekPipeline() []Command
	// NetConn returns the base net.Conn connection
	NetConn() net.Conn
}

// conn represents a client connection
type conn struct {
	conn     net.Conn
	wr       *Writer
	rd       *Reader
	addr     string
	ctx      interface{}
	detached bool
	closed   bool
	cmds     []Command
}

func (c *conn) Close() error {
	c.wr.Flush()
	c.closed = true
	return c.conn.Close()
}

func (c *conn) Context() interface{}        { return c.ctx }
func (c *conn) SetContext(v interface{})    { c.ctx = v }
func (c *conn) SetReadBuffer(n int)         {}
func (c *conn) WriteString(str string)      { c.wr.WriteString(str) }
func (c *conn) WriteBulk(bulk []byte)       { c.wr.WriteBulk(bulk) }
func (c *conn) WriteBulkString(bulk string) { c.wr.WriteBulkString(bulk) }
func (c *conn) WriteInt(num int)            { c.wr.WriteInt(num) }
func (c *conn) WriteInt64(num int64)        { c.wr.WriteInt64(num) }
func (c *conn) WriteError(msg string)       { c.wr.WriteError(msg) }
func (c *conn) WriteArray(count int)        { c.wr.WriteArray(count) }
func (c *conn) WriteNull()                  { c.wr.WriteNull() }
func (c *conn) WriteRaw(data []byte)        { c.wr.WriteRaw(data) }
func (c *conn) RemoteAddr() string          { return c.addr }
func (c *conn) ReadPipeline() []Command {
	cmds := c.cmds
	c.cmds = nil
	return cmds
}
func (c *conn) PeekPipeline() []Command {
	return c.cmds
}
func (c *conn) NetConn() net.Conn {
	return c.conn
}

// DetachedConn represents a connection that is detached from the server
type DetachedConn interface {
	// Conn is the original connection
	Conn
	// ReadCommand reads the next client command.
	ReadCommand() (Command, error)
	// Flush flushes any writes to the network.
	Flush() error
}

// Detach removes the current connection from the server loop and returns
// a detached connection. This is useful for operations such as PubSub.
// The detached connection must be closed by calling Close() when done.
// All writes such as WriteString() will not be written to the client
// until Flush() is called.
func (c *conn) Detach() DetachedConn {
	c.detached = true
	cmds := c.cmds
	c.cmds = nil
	return &detachedConn{conn: c, cmds: cmds}
}

type detachedConn struct {
	*conn
	cmds []Command
}

// Flush writes and Write* calls to the client.
func (dc *detachedConn) Flush() error {
	return dc.conn.wr.Flush()
}

// ReadCommand read the next command from the client.
func (dc *detachedConn) ReadCommand() (Command, error) {
	if dc.closed {
		return Command{}, errors.New("closed")
	}
	if len(dc.cmds) > 0 {
		cmd := dc.cmds[0]
		if len(dc.cmds) == 1 {
			dc.cmds = nil
		} else {
			dc.cmds = dc.cmds[1:]
		}
		return cmd, nil
	}
	cmd, err := dc.rd.ReadCommand()
	if err != nil {
		return Command{}, err
	}
	return cmd, nil
}

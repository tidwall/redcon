package redcon

import (
	"io"
	"sync"

	"github.com/panjf2000/ants"
)

type namespaceParser func(key []byte) int

type Processor struct {
	nsParser namespaceParser

	serveGoPoolSize     int
	highLevelGoPoolSize int
	lowLevelGoPoolSize  int
	serveGoPool         *ants.PoolWithFunc
	highLevelPool       *ants.PoolWithFunc
	lowLevelPool        *ants.PoolWithFunc

	server *Server

	highLevelCmdsMap map[int][]string
}

func NewProcessor(server *Server, nsParser namespaceParser,
	serveGoPoolSize int, highLevelGoPoolSize,
	lowLevelGoPoolSize int, levelMap map[int][]string) *Processor {

	pro := new(Processor)
	pro.nsParser = nsParser
	pro.server = server
	pro.serveGoPoolSize = serveGoPoolSize
	pro.highLevelGoPoolSize = highLevelGoPoolSize
	pro.lowLevelGoPoolSize = lowLevelGoPoolSize
	pro.highLevelCmdsMap = levelMap

	pro.serveGoPool, _ = ants.NewPoolWithFunc(pro.serveGoPoolSize, func(arg interface{}) {
		c, _ := arg.(*conn)
		pro.handle(c)
	})

	pro.highLevelPool, _ = ants.NewPoolWithFunc(
		pro.highLevelGoPoolSize, pro.processLeveledTask)
	pro.lowLevelPool, _ = ants.NewPoolWithFunc(
		pro.lowLevelGoPoolSize, pro.processLeveledTask)
	return pro
}

func (pro *Processor) processLeveledTask(arg interface{}) {
	argList := arg.([]interface{})
	wg, _ := argList[0].(*sync.WaitGroup)
	c, _ := argList[1].(*conn)
	cmd, _ := argList[2].(Command)
	pro.server.handler(c, cmd)
	wg.Done()
}

func (pro *Processor) Close() {
	pro.serveGoPool.Release()
	pro.highLevelPool.Release()
	pro.lowLevelPool.Release()
}

// handle manages the server connection.
func (pro *Processor) handle(c *conn) {
	var err error
	defer func() {
		if err != errDetached {
			// do not close the connection when a detach is detected.
			c.conn.Close()
		}
		func() {
			// remove the conn from the server
			pro.server.mu.Lock()
			defer pro.server.mu.Unlock()
			delete(pro.server.conns, c)
			if pro.server.closed != nil {
				if err == io.EOF {
					err = nil
				}
				pro.server.closed(c, err)
			}
		}()
	}()

	err = func() error {
		// read commands and feed back to the client
		for {
			// read pipeline commands
			cmds, err := c.rd.readCommands(nil)
			if err != nil {
				if err, ok := err.(*errProtocol); ok {
					// All protocol errors should attempt a response to
					// the client. Ignore write errors.
					c.wr.WriteError("ERR " + err.Error())
					c.wr.Flush()
				}
				return err
			}
			var wg sync.WaitGroup
			c.cmds = cmds
			for len(c.cmds) > 0 {
				cmd := c.cmds[0]
				if len(c.cmds) == 1 {
					c.cmds = nil
				} else {
					c.cmds = c.cmds[1:]
				}

				wg.Add(1)
				if len(cmd.Args) <= 1 {
					pro.highLevelPool.Serve([]interface{}{&wg, c, cmd})
					continue
				}
				nsVal := pro.nsParser(cmd.Args[1])
				cnameList, ok := pro.highLevelCmdsMap[nsVal]
				if !ok {
					pro.highLevelPool.Serve([]interface{}{c, cmd})
				} else {
					for _, cname := range cnameList {
						if cname == string(cmd.Args[0]) {
							pro.highLevelPool.Serve([]interface{}{&wg, c, cmd})
						} else {
							pro.lowLevelPool.Serve([]interface{}{&wg, c, cmd})
						}
						break
					}
				}
			}
			wg.Wait()
			if c.detached {
				// client has been detached
				return errDetached
			}
			if c.closed {
				return nil
			}
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
	}()
}

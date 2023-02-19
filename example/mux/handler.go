package main

import (
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

type Handler struct {
	itemsMux sync.RWMutex
	items    map[string][]byte
}

func NewHandler() *Handler {
	return &Handler{
		items: make(map[string][]byte),
	}
}

func (h *Handler) detach(conn redcon.Conn, cmd redcon.Command) {
	detachedConn := conn.Detach()
	log.Printf("connection has been detached")
	go func(c redcon.DetachedConn) {
		defer c.Close()

		c.WriteString("OK")
		c.Flush()
	}(detachedConn)
}

func (h *Handler) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (h *Handler) quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func (h *Handler) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	h.itemsMux.Lock()
	h.items[string(cmd.Args[1])] = cmd.Args[2]
	h.itemsMux.Unlock()

	conn.WriteString("OK")
}

func (h *Handler) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	h.itemsMux.RLock()
	val, ok := h.items[string(cmd.Args[1])]
	h.itemsMux.RUnlock()

	if !ok {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (h *Handler) setnx(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	h.itemsMux.RLock()
	_, ok := h.items[string(cmd.Args[1])]
	h.itemsMux.RUnlock()

	if ok {
		conn.WriteInt(0)
		return
	}

	h.itemsMux.Lock()
	h.items[string(cmd.Args[1])] = cmd.Args[2]
	h.itemsMux.Unlock()

	conn.WriteInt(1)
}

func (h *Handler) delete(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	h.itemsMux.Lock()
	_, ok := h.items[string(cmd.Args[1])]
	delete(h.items, string(cmd.Args[1]))
	h.itemsMux.Unlock()

	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

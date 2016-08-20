package main

import (
	"log"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

var addr = ":6380"

func main() {
	var mu sync.RWMutex
	var items = make(map[string]string)
	go log.Printf("started server at %s", addr)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, commands [][]string) {
			for _, args := range commands {
				switch strings.ToLower(args[0]) {
				default:
					conn.WriteError("ERR unknown command '" + args[0] + "'")
				case "ping":
					conn.WriteString("PONG")
				case "quit":
					conn.WriteString("OK")
					conn.Close()
				case "set":
					if len(args) != 3 {
						conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
						continue
					}
					mu.Lock()
					items[args[1]] = args[2]
					mu.Unlock()
					conn.WriteString("OK")
				case "get":
					if len(args) != 2 {
						conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
						continue
					}
					mu.RLock()
					val, ok := items[args[1]]
					mu.RUnlock()
					if !ok {
						conn.WriteNull()
					} else {
						conn.WriteBulk(val)
					}
				case "del":
					if len(args) != 2 {
						conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
						continue
					}
					mu.Lock()
					_, ok := items[args[1]]
					delete(items, args[1])
					mu.Unlock()
					if !ok {
						conn.WriteInt(0)
					} else {
						conn.WriteInt(1)
					}
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

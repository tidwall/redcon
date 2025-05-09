package main

import (
	"log"

	"github.com/tidwall/redcon"
)

var addr = ":6380"

func main() {
	log.Printf("started server at %s", addr)

	handler := NewHandler()

	mux := redcon.NewServeMux()
	mux.HandleFunc("detach", handler.detach)
	mux.HandleFunc("ping", handler.ping)
	mux.HandleFunc("quit", handler.quit)
	mux.HandleFunc("set", handler.set)
	mux.HandleFunc("get", handler.get)
	mux.HandleFunc("setnx", handler.setnx)
	mux.HandleFunc("del", handler.delete)

	err := redcon.ListenAndServe(addr,
		mux.ServeRESP,
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

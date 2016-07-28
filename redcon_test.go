package redcon

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"testing"
	"time"
)

// TestRandomCommands fills a bunch of random commands and test various
// ways that the reader may receive data.
func TestRandomCommands(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// build random commands.
	gcmds := make([][]string, 10000)
	for i := 0; i < len(gcmds); i++ {
		args := make([]string, (rand.Int()%50)+1) // 1-50 args
		for j := 0; j < len(args); j++ {
			n := rand.Int() % 10
			if j == 0 {
				n++
			}
			arg := make([]byte, n)
			for k := 0; k < len(arg); k++ {
				arg[k] = byte(rand.Int() % 0xFF)
			}
			args[j] = string(arg)
		}
		gcmds[i] = args
	}
	// create a list of a buffers
	var bufs []string

	// pipe valid RESP commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid plain commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		var msg string
		for j := 0; j < len(args); j++ {
			quotes := false
			var narg []byte
			arg := args[j]
			if len(arg) == 0 {
				quotes = true
			}
			for k := 0; k < len(arg); k++ {
				switch arg[k] {
				default:
					narg = append(narg, arg[k])
				case ' ':
					quotes = true
					narg = append(narg, arg[k])
				case '\\', '"', '*':
					quotes = true
					narg = append(narg, '\\', arg[k])
				case '\r':
					quotes = true
					narg = append(narg, '\\', 'r')
				case '\n':
					quotes = true
					narg = append(narg, '\\', 'n')
				}
			}
			msg += " "
			if quotes {
				msg += "\""
			}
			msg += string(narg)
			if quotes {
				msg += "\""
			}
		}
		if msg != "" {
			msg = msg[1:]
		}
		msg += "\r\n"
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in broken chunks
	lmsg := ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in large broken chunks
	lmsg = ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		if len(lmsg) < 1500 {
			lmsg += msg
			continue
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// Pipe the buffers in a background routine
	rd, wr := io.Pipe()
	go func() {
		defer wr.Close()
		for _, msg := range bufs {
			io.WriteString(wr, msg)
		}
	}()
	defer rd.Close()
	cnt := 0
	idx := 0
	start := time.Now()
	r := newReader(rd)
	for {
		cmds, err := r.ReadCommands()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		for _, cmd := range cmds {
			if len(cmd) == 3 && cmd[0] == "RESET" && cmd[1] == "THE" && cmd[2] == "INDEX" {
				if idx != len(gcmds) {
					t.Fatalf("did not process all commands")
				}
				idx = 0
				break
			}
			if len(cmd) != len(gcmds[idx]) {
				t.Fatalf("len not equal for index %d -- %d != %d", idx, len(cmd), len(gcmds[idx]))
			}
			for i := 0; i < len(cmd); i++ {
				if cmd[i] != gcmds[idx][i] {
					t.Fatalf("not equal for index %d/%d", idx, i)
				}
			}
			idx++
			cnt++
		}
	}
	if false {
		dur := time.Now().Sub(start)
		fmt.Printf("%d commands in %s - %.0f ops/sec\n", cnt, dur, float64(cnt)/(float64(dur)/float64(time.Second)))
	}
}

/*
func TestServer(t *testing.T) {
	err := ListenAndServe(":11111",
		func(conn Conn, cmds [][]string) {
			for _, cmd := range cmds {
				switch strings.ToLower(cmd[0]) {
				default:
					conn.WriteError("ERR unknown command '" + cmd[0] + "'")
				case "ping":
					conn.WriteString("PONG")
				case "quit":
					conn.WriteString("OK")
					conn.Close()
				}
			}
		},
		func(conn Conn) bool {
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn Conn, err error) {
			log.Printf("closed: %s [%v]", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
*/

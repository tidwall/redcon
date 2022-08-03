package redcon

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func isEmptyRESP(resp RESP) bool {
	return resp.Type == 0 && resp.Count == 0 &&
		resp.Data == nil && resp.Raw == nil
}

func expectBad(t *testing.T, payload string) {
	t.Helper()
	n, resp := ReadNextRESP([]byte(payload))
	if n > 0 || !isEmptyRESP(resp) {
		t.Fatalf("expected empty resp")
	}
}

func respVOut(a RESP) string {
	var data string
	var raw string
	if a.Data == nil {
		data = "nil"
	} else {
		data = strconv.Quote(string(a.Data))
	}
	if a.Raw == nil {
		raw = "nil"
	} else {
		raw = strconv.Quote(string(a.Raw))
	}
	return fmt.Sprintf("{Type: %d, Count: %d, Data: %s, Raw: %s}",
		a.Type, a.Count, data, raw,
	)
}

func respEquals(a, b RESP) bool {
	if a.Count != b.Count {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if (a.Data == nil && b.Data != nil) || (a.Data != nil && b.Data == nil) {
		return false
	}
	if string(a.Data) != string(b.Data) {
		return false
	}
	if (a.Raw == nil && b.Raw != nil) || (a.Raw != nil && b.Raw == nil) {
		return false
	}
	if string(a.Raw) != string(b.Raw) {
		return false
	}
	return true
}

func expectGood(t *testing.T, payload string, exp RESP) {
	t.Helper()
	n, resp := ReadNextRESP([]byte(payload))
	if n != len(payload) || isEmptyRESP(resp) {
		t.Fatalf("expected good resp")
	}
	if string(resp.Raw) != payload {
		t.Fatalf("expected '%s', got '%s'", payload, resp.Raw)
	}
	exp.Raw = []byte(payload)
	switch exp.Type {
	case Integer, String, Error:
		exp.Data = []byte(payload[1 : len(payload)-2])
	}
	if !respEquals(resp, exp) {
		t.Fatalf("expected %v, got %v", respVOut(exp), respVOut(resp))
	}
}

func TestRESP(t *testing.T) {
	expectBad(t, "")
	expectBad(t, "^hello\r\n")
	expectBad(t, "+hello\r")
	expectBad(t, "+hello\n")
	expectBad(t, ":\r\n")
	expectBad(t, ":-\r\n")
	expectBad(t, ":-abc\r\n")
	expectBad(t, ":abc\r\n")
	expectGood(t, ":-123\r\n", RESP{Type: Integer})
	expectGood(t, ":123\r\n", RESP{Type: Integer})
	expectBad(t, "+\r")
	expectBad(t, "+\n")
	expectGood(t, "+\r\n", RESP{Type: String})
	expectGood(t, "+hello world\r\n", RESP{Type: String})
	expectBad(t, "-\r")
	expectBad(t, "-\n")
	expectGood(t, "-\r\n", RESP{Type: Error})
	expectGood(t, "-hello world\r\n", RESP{Type: Error})
	expectBad(t, "$")
	expectBad(t, "$\r")
	expectBad(t, "$\r\n")
	expectGood(t, "$-1\r\n", RESP{Type: Bulk})
	expectGood(t, "$0\r\n\r\n", RESP{Type: Bulk, Data: []byte("")})
	expectBad(t, "$5\r\nhello\r")
	expectBad(t, "$5\r\nhello\n\n")
	expectGood(t, "$5\r\nhello\r\n", RESP{Type: Bulk, Data: []byte("hello")})
	expectBad(t, "*a\r\n")
	expectBad(t, "*3\r\n")
	expectBad(t, "*3\r\n:hello\r")
	expectGood(t, "*3\r\n:1\r\n:2\r\n:3\r\n",
		RESP{Type: Array, Count: 3, Data: []byte(":1\r\n:2\r\n:3\r\n")})

	var xx int
	_, r := ReadNextRESP([]byte("*4\r\n:1\r\n:2\r\n:3\r\n:4\r\n"))
	r.ForEach(func(resp RESP) bool {
		xx++
		x, _ := strconv.Atoi(string(resp.Data))
		if x != xx {
			t.Fatalf("expected %v, got %v", x, xx)
		}
		if xx == 3 {
			return false
		}
		return true
	})
	if xx != 3 {
		t.Fatalf("expected %v, got %v", 3, xx)
	}
}

func TestNextCommand(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()
	for time.Since(start) < time.Second {
		// keep copy of pipeline args for final compare
		var plargs [][][]byte

		// create a pipeline of random number of commands with random data.
		N := rand.Int() % 10000
		var data []byte
		for i := 0; i < N; i++ {
			nargs := rand.Int() % 10
			data = AppendArray(data, nargs)
			var args [][]byte
			for j := 0; j < nargs; j++ {
				arg := make([]byte, rand.Int()%100)
				if _, err := rand.Read(arg); err != nil {
					t.Fatal(err)
				}
				data = AppendBulk(data, arg)
				args = append(args, arg)
			}
			plargs = append(plargs, args)
		}

		// break data into random number of chunks
		chunkn := rand.Int() % 100
		if chunkn == 0 {
			chunkn = 1
		}
		if len(data) < chunkn {
			continue
		}
		var chunks [][]byte
		var chunksz int
		for i := 0; i < len(data); i += chunksz {
			chunksz = rand.Int() % (len(data) / chunkn)
			var chunk []byte
			if i+chunksz < len(data) {
				chunk = data[i : i+chunksz]
			} else {
				chunk = data[i:]
			}
			chunks = append(chunks, chunk)
		}

		// process chunks
		var rbuf []byte
		var fargs [][][]byte
		for _, chunk := range chunks {
			var data []byte
			if len(rbuf) > 0 {
				data = append(rbuf, chunk...)
			} else {
				data = chunk
			}
			for {
				complete, args, _, leftover, err := ReadNextCommand(data, nil)
				data = leftover
				if err != nil {
					t.Fatal(err)
				}
				if !complete {
					break
				}
				fargs = append(fargs, args)
			}
			rbuf = append(rbuf[:0], data...)
		}
		// compare final args to original
		if len(plargs) != len(fargs) {
			t.Fatalf("not equal size: %v != %v", len(plargs), len(fargs))
		}
		for i := 0; i < len(plargs); i++ {
			if len(plargs[i]) != len(fargs[i]) {
				t.Fatalf("not equal size for item %v: %v != %v", i, len(plargs[i]), len(fargs[i]))
			}
			for j := 0; j < len(plargs[i]); j++ {
				if !bytes.Equal(plargs[i][j], plargs[i][j]) {
					t.Fatalf("not equal for item %v:%v: %v != %v", i, j, len(plargs[i][j]), len(fargs[i][j]))
				}
			}
		}
	}
}

func TestAppendBulkFloat(t *testing.T) {
	var b []byte
	b = AppendString(b, "HELLO")
	b = AppendBulkFloat(b, 9.123192839)
	b = AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n9.123192839\r\n+HELLO\r\n"
	if string(b) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b)
	}
}

func TestAppendBulkInt(t *testing.T) {
	var b []byte
	b = AppendString(b, "HELLO")
	b = AppendBulkInt(b, -9182739137)
	b = AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n-9182739137\r\n+HELLO\r\n"
	if string(b) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b)
	}
}

func TestAppendBulkUint(t *testing.T) {
	var b []byte
	b = AppendString(b, "HELLO")
	b = AppendBulkInt(b, 91827391370)
	b = AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n91827391370\r\n+HELLO\r\n"
	if string(b) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b)
	}
}

func TestArrayMap(t *testing.T) {
	var dst []byte
	dst = AppendArray(dst, 4)
	dst = AppendBulkString(dst, "key1")
	dst = AppendBulkString(dst, "val1")
	dst = AppendBulkString(dst, "key2")
	dst = AppendBulkString(dst, "val2")
	n, resp := ReadNextRESP(dst)
	if n != len(dst) {
		t.Fatalf("expected '%d', got '%d'", len(dst), n)
	}
	m := resp.Map()
	if len(m) != 2 {
		t.Fatalf("expected '%d', got '%d'", 2, len(m))
	}
	if m["key1"].String() != "val1" {
		t.Fatalf("expected '%s', got '%s'", "val1", m["key1"].String())
	}
	if m["key2"].String() != "val2" {
		t.Fatalf("expected '%s', got '%s'", "val2", m["key2"].String())
	}
}

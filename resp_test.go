package redcon

import (
	"fmt"
	"strconv"
	"testing"
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

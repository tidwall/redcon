package redcon

import (
	"strconv"
	"strings"
)

// Kind is the kind of command
type Kind int

const (
	// Redis is returned for Redis protocol commands
	Redis Kind = iota
	// Tile38 is returnd for Tile38 native protocol commands
	Tile38
	// Telnet is returnd for plain telnet commands
	Telnet
)

var errInvalidMessage = &errProtocol{"invalid message"}
var errIncompletePacket = &errProtocol{"incomplete packet"}

// ReadNextCommand reads the next command from the provided packet. It's
// possible that the packet contains multiple commands, or zero commands
// when the packet is incomplete.
// 'args' is an optional reusable buffer and it can be nil.
// 'argsout' are the output arguments for the command. 'kind' is the type of
// command that was read.
// 'stop' indicates that there are no more possible commands in the packet.
// 'err' is returned when a protocol error was encountered.
// 'leftover' is any remaining unused bytes which belong to the next command.
func ReadNextCommand(packet []byte, args [][]byte) (
	leftover []byte, argsout [][]byte, kind Kind, stop bool, err error,
) {
	args = args[:0]
	if len(packet) > 0 {
		if packet[0] != '*' {
			if packet[0] == '$' {
				return readTile38Command(packet, args)
			}
			return readTelnetCommand(packet, args)
		}
		// standard redis command
		for s, i := 1, 1; i < len(packet); i++ {
			if packet[i] == '\n' {
				if packet[i-1] != '\r' {
					println("here", i)
					return packet, args[:0], Redis, true, errInvalidMultiBulkLength
				}
				count, ok := parseInt(packet[s : i-1])
				if !ok || count < 0 {
					println("there", i, count, ok, "["+string(packet[s:i-1])+"]")
					return packet, args[:0], Redis, true, errInvalidMultiBulkLength
				}
				i++
				if count == 0 {
					return packet[i:], args[:0], Redis, i == len(packet), nil
				}
			nextArg:
				for j := 0; j < count; j++ {
					if i == len(packet) {
						break
					}
					if packet[i] != '$' {
						return packet, args[:0], Redis, true,
							&errProtocol{"expected '$', got '" +
								string(packet[i]) + "'"}
					}
					for s := i + 1; i < len(packet); i++ {
						if packet[i] == '\n' {
							if packet[i-1] != '\r' {
								return packet, args[:0], Redis, true, errInvalidBulkLength
							}
							n, ok := parseInt(packet[s : i-1])
							if !ok || count <= 0 {
								return packet, args[:0], Redis, true, errInvalidBulkLength
							}
							i++
							if len(packet)-i >= n+2 {
								if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
									return packet, args[:0], Redis, true, errInvalidBulkLength
								}
								args = append(args, packet[i:i+n])
								i += n + 2
								if j == count-1 {
									// done reading
									return packet[i:], args, Redis, i == len(packet), nil
								}
								continue nextArg
							}
							break
						}
					}
					break
				}
				break
			}
		}
	}
	return packet, args[:0], Redis, true, errIncompletePacket
}

func readTile38Command(b []byte, argsbuf [][]byte) (
	leftover []byte, args [][]byte, kind Kind, stop bool, err error,
) {
	for i := 1; i < len(b); i++ {
		if b[i] == ' ' {
			n, ok := parseInt(b[1:i])
			if !ok || n < 0 {
				return b, args[:0], Tile38, true, errInvalidMessage
			}
			i++
			if len(b) >= i+n+2 {
				if b[i+n] != '\r' || b[i+n+1] != '\n' {
					return b, args[:0], Tile38, true, errInvalidMessage
				}
				line := b[i : i+n]
			reading:
				for len(line) != 0 {
					if line[0] == '{' {
						// The native protocol cannot understand json boundaries so it assumes that
						// a json element must be at the end of the line.
						args = append(args, line)
						break
					}
					if line[0] == '"' && line[len(line)-1] == '"' {
						if len(args) > 0 &&
							strings.ToLower(string(args[0])) == "set" &&
							strings.ToLower(string(args[len(args)-1])) == "string" {
							// Setting a string value that is contained inside double quotes.
							// This is only because of the boundary issues of the native protocol.
							args = append(args, line[1:len(line)-1])
							break
						}
					}
					i := 0
					for ; i < len(line); i++ {
						if line[i] == ' ' {
							value := line[:i]
							if len(value) > 0 {
								args = append(args, value)
							}
							line = line[i+1:]
							continue reading
						}
					}
					args = append(args, line)
					break
				}
				return b[i+n+2:], args, Tile38, i == len(b), nil
			}
			break
		}
	}
	return b, args[:0], Tile38, true, errIncompletePacket
}
func readTelnetCommand(b []byte, argsbuf [][]byte) (
	leftover []byte, args [][]byte, kind Kind, stop bool, err error,
) {
	// just a plain text command
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			var line []byte
			if i > 0 && b[i-1] == '\r' {
				line = b[:i-1]
			} else {
				line = b[:i]
			}
			var quote bool
			var quotech byte
			var escape bool
		outer:
			for {
				nline := make([]byte, 0, len(line))
				for i := 0; i < len(line); i++ {
					c := line[i]
					if !quote {
						if c == ' ' {
							if len(nline) > 0 {
								args = append(args, nline)
							}
							line = line[i+1:]
							continue outer
						}
						if c == '"' || c == '\'' {
							if i != 0 {
								return b, args[:0], Telnet, true, errUnbalancedQuotes
							}
							quotech = c
							quote = true
							line = line[i+1:]
							continue outer
						}
					} else {
						if escape {
							escape = false
							switch c {
							case 'n':
								c = '\n'
							case 'r':
								c = '\r'
							case 't':
								c = '\t'
							}
						} else if c == quotech {
							quote = false
							quotech = 0
							args = append(args, nline)
							line = line[i+1:]
							if len(line) > 0 && line[0] != ' ' {
								return b, args[:0], Telnet, true, errUnbalancedQuotes
							}
							continue outer
						} else if c == '\\' {
							escape = true
							continue
						}
					}
					nline = append(nline, c)
				}
				if quote {
					return b, args[:0], Telnet, true, errUnbalancedQuotes
				}
				if len(line) > 0 {
					args = append(args, line)
				}
				break
			}
			return b[i+1:], args, Telnet, i == len(b), nil
		}
	}
	return b, args[:0], Telnet, true, errIncompletePacket
}

// AppendUint appends a Redis protocol uint64 to the input bytes.
func AppendUint(b []byte, n uint64) []byte {
	b = append(b, ':')
	b = strconv.AppendUint(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendInt appends a Redis protocol int64 to the input bytes.
func AppendInt(b []byte, n int64) []byte {
	b = append(b, ':')
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendArray appends a Redis protocol array to the input bytes.
func AppendArray(b []byte, n int) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(n), 10)
	return append(b, '\r', '\n')
}

// AppendBulk appends a Redis protocol bulk byte slice to the input bytes.
func AppendBulk(b []byte, bulk []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(bulk)), 10)
	b = append(b, '\r', '\n')
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendBulkString appends a Redis protocol bulk string to the input bytes.
func AppendBulkString(b []byte, bulk string) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(bulk)), 10)
	b = append(b, '\r', '\n')
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendString appends a Redis protocol string to the input bytes.
func AppendString(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendError appends a Redis protocol error to the input bytes.
func AppendError(b []byte, s string) []byte {
	b = append(b, '-')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendOK appends a Redis protocol OK to the input bytes.
func AppendOK(b []byte) []byte {
	return append(b, '+', 'O', 'K', '\r', '\n')
}
func stripNewlines(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			s = strings.Replace(s, "\r", " ", -1)
			s = strings.Replace(s, "\n", " ", -1)
			break
		}
	}
	return s
}

// AppendTile38 appends a Tile38 message to the input bytes.
func AppendTile38(b []byte, data []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(data)), 10)
	b = append(b, ' ')
	b = append(b, data...)
	return append(b, '\r', '\n')
}

// AppendNull appends a Redis protocol null to the input bytes.
func AppendNull(b []byte) []byte {
	return append(b, '$', '-', '1', '\r', '\n')
}

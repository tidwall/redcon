package redcon

import (
	"io"
)

// Writer allows for writing RESP messages.
type Writer struct {
	w io.Writer
	b []byte
}

// NewWriter creates a new RESP writer.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		w: wr,
	}
}

// WriteNull writes a null to the client
func (w *Writer) WriteNull() {
	w.b = AppendNull(w.b)
}

// WriteArray writes an array header. You must then write additional
// sub-responses to the client to complete the response.
// For example to write two strings:
//
//   c.WriteArray(2)
//   c.WriteBulk("item 1")
//   c.WriteBulk("item 2")
func (w *Writer) WriteArray(count int) {
	w.b = AppendArray(w.b, count)
}

// WriteBulk writes bulk bytes to the client.
func (w *Writer) WriteBulk(bulk []byte) {
	w.b = AppendBulk(w.b, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (w *Writer) WriteBulkString(bulk string) {
	w.b = AppendBulkString(w.b, bulk)
}

// Buffer returns the unflushed buffer. This is a copy so changes
// to the resulting []byte will not affect the writer.
func (w *Writer) Buffer() []byte {
	return append([]byte(nil), w.b...)
}

// SetBuffer replaces the unflushed buffer with new bytes.
func (w *Writer) SetBuffer(raw []byte) {
	w.b = w.b[:0]
	w.b = append(w.b, raw...)
}

// Flush writes all unflushed Write* calls to the underlying writer.
func (w *Writer) Flush() error {
	if _, err := w.w.Write(w.b); err != nil {
		return err
	}
	w.b = w.b[:0]
	return nil
}

// WriteError writes an error to the client.
func (w *Writer) WriteError(msg string) {
	w.b = AppendError(w.b, msg)
}

// WriteString writes a string to the client.
func (w *Writer) WriteString(msg string) {
	w.b = AppendString(w.b, msg)
}

// WriteInt writes an integer to the client.
func (w *Writer) WriteInt(num int) {
	w.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (w *Writer) WriteInt64(num int64) {
	w.b = AppendInt(w.b, num)
}

// WriteRaw writes raw data to the client.
func (w *Writer) WriteRaw(data []byte) {
	w.b = append(w.b, data...)
}

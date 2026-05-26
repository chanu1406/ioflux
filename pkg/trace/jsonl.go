package trace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MaxLineBytes is the largest single JSONL line the Reader will accept. A
// trace header may carry a long Targets table; ops themselves are small.
const MaxLineBytes = 16 * 1024 * 1024

// ErrHeaderAlreadyWritten is returned by Writer.WriteHeader if the header
// has already been written.
var ErrHeaderAlreadyWritten = errors.New("trace: header already written")

// ErrHeaderNotWritten is returned by Writer.WriteOp if WriteHeader has not
// been called yet. The header must be the first line of every .ioflux file.
var ErrHeaderNotWritten = errors.New("trace: header not written")

// Reader streams an .ioflux file. The header is parsed eagerly by NewReader;
// ops are produced one at a time by Next until io.EOF.
//
// Reader performs only structural (parse-level) validation. Schema/invariant
// checks live in Validate.
type Reader struct {
	sc        *bufio.Scanner
	hdr       Header
	headerRaw []byte
	line      int // 1-based; line of the last record returned (or attempted)
}

// NewReader reads and parses the header from r and returns a Reader ready
// for op iteration. On error the header line number (typically 1) is
// included in the message.
func NewReader(r io.Reader) (*Reader, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), MaxLineBytes)

	rd := &Reader{sc: sc}
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("trace: read header: %w", err)
		}
		return nil, fmt.Errorf("trace: empty input, expected header on line 1")
	}
	rd.line = 1
	rd.headerRaw = append(rd.headerRaw, sc.Bytes()...)
	if err := json.Unmarshal(rd.headerRaw, &rd.hdr); err != nil {
		return nil, fmt.Errorf("trace: parse header on line 1: %w", err)
	}
	return rd, nil
}

// Header returns the parsed header.
func (r *Reader) Header() Header { return r.hdr }

// HeaderRaw returns a copy of the raw header line as it appeared in the trace.
func (r *Reader) HeaderRaw() []byte {
	return append([]byte(nil), r.headerRaw...)
}

// Line returns the 1-based line number of the most recently returned record.
// After NewReader, Line() == 1 (the header). After each successful Next(),
// Line() is the line that op was read from.
func (r *Reader) Line() int { return r.line }

// Next returns the next op in the trace. It returns io.EOF when no more ops
// remain. Other errors are wrapped with the offending line number.
func (r *Reader) Next() (Op, error) {
	for {
		if !r.sc.Scan() {
			if err := r.sc.Err(); err != nil {
				return Op{}, fmt.Errorf("trace: read op after line %d: %w", r.line, err)
			}
			return Op{}, io.EOF
		}
		r.line++
		// Skip blank lines (purely whitespace). Accepting them is a cheap
		// robustness measure for tools that append a trailing newline.
		if len(bytes.TrimSpace(r.sc.Bytes())) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(r.sc.Bytes(), &op); err != nil {
			return Op{}, fmt.Errorf("trace: parse op on line %d: %w", r.line, err)
		}
		return op, nil
	}
}

// Writer emits an .ioflux file as line-delimited JSON. The header must be
// written first via WriteHeader; subsequent WriteOp calls append ops. Writer
// does not own the underlying io.Writer and does not flush it.
type Writer struct {
	enc         *json.Encoder
	wroteHeader bool
}

// NewWriter returns a Writer that encodes to w. The caller is responsible
// for any buffering and for closing w.
func NewWriter(w io.Writer) *Writer {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &Writer{enc: enc}
}

// WriteHeader writes h as the first JSONL line. It returns
// ErrHeaderAlreadyWritten on a second call.
func (w *Writer) WriteHeader(h Header) error {
	if w.wroteHeader {
		return ErrHeaderAlreadyWritten
	}
	if err := w.enc.Encode(h); err != nil {
		return fmt.Errorf("trace: write header: %w", err)
	}
	w.wroteHeader = true
	return nil
}

// WriteOp appends op as a JSONL line. It returns ErrHeaderNotWritten if
// WriteHeader has not been called.
func (w *Writer) WriteOp(op Op) error {
	if !w.wroteHeader {
		return ErrHeaderNotWritten
	}
	if err := w.enc.Encode(op); err != nil {
		return fmt.Errorf("trace: write op: %w", err)
	}
	return nil
}

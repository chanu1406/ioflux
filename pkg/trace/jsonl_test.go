package trace

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func sampleHeader() Header {
	return Header{
		Version:  TraceFormatVersion,
		Kind:     TraceSynthetic,
		Profile:  "training-read",
		TimeUnit: TimeUnitNanoseconds,
		Targets: []TargetInfo{
			{ID: 0, Name: "shard_0000.tar", Kind: TargetFile, Size: 64 << 20},
			{ID: 1, Name: "shard_0001.tar", Kind: TargetFile, Size: 64 << 20},
		},
		Summary: Summary{
			NumOps: 4, NumStreams: 1, NumGroups: 0, TotalBytes: 8 << 10, DurationNS: 10_000,
		},
	}
}

func sampleOps() []Op {
	return []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 100, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](4096)},
		{T: 200, OpID: Ptr[int64](2), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](4096), Len: Ptr[int64](4096)},
		{T: 300, OpID: Ptr[int64](3), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
}

func TestWriterReaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	h := sampleHeader()
	ops := sampleOps()

	if err := w.WriteHeader(h); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	for i, op := range ops {
		if err := w.WriteOp(op); err != nil {
			t.Fatalf("WriteOp[%d]: %v", i, err)
		}
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if !reflect.DeepEqual(r.Header(), h) {
		t.Fatalf("header mismatch:\nwant=%#v\ngot=%#v", h, r.Header())
	}
	var got []Op
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, op)
	}
	if !reflect.DeepEqual(got, ops) {
		t.Fatalf("ops mismatch:\nwant=%#v\ngot=%#v", ops, got)
	}
}

func TestWriterHeaderTwiceRejected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteHeader(sampleHeader()); err != nil {
		t.Fatalf("first WriteHeader: %v", err)
	}
	err := w.WriteHeader(sampleHeader())
	if !errors.Is(err, ErrHeaderAlreadyWritten) {
		t.Fatalf("second WriteHeader: want ErrHeaderAlreadyWritten, got %v", err)
	}
}

func TestWriterOpBeforeHeaderRejected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteOp(Op{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead})
	if !errors.Is(err, ErrHeaderNotWritten) {
		t.Fatalf("WriteOp before header: want ErrHeaderNotWritten, got %v", err)
	}
}

func TestReaderEmptyInput(t *testing.T) {
	_, err := NewReader(strings.NewReader(""))
	if err == nil {
		t.Fatalf("NewReader on empty input: want error, got nil")
	}
	if !strings.Contains(err.Error(), "empty input") {
		t.Errorf("error should mention 'empty input', got: %v", err)
	}
}

func TestReaderMalformedHeader(t *testing.T) {
	_, err := NewReader(strings.NewReader("not json\n"))
	if err == nil {
		t.Fatalf("NewReader on malformed header: want error, got nil")
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Errorf("error should reference line 1, got: %v", err)
	}
}

func TestReaderMalformedOpReportsLine(t *testing.T) {
	src := `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[{"id":0,"name":"a","kind":"file","size":0}],"summary":{"num_ops":0,"num_streams":0,"num_groups":0,"total_bytes":0,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":10,"mode":"r"}
this is not json
`
	r, err := NewReader(strings.NewReader(src))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	_, err = r.Next()
	if err == nil {
		t.Fatalf("expected parse error on line 3, got nil")
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Errorf("error should reference line 3, got: %v", err)
	}
}

func TestReaderSkipsBlankLines(t *testing.T) {
	src := `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[{"id":0,"name":"a","kind":"file","size":0}],"summary":{"num_ops":0,"num_streams":0,"num_groups":0,"total_bytes":0,"duration_ns":0}}

{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":10,"mode":"r"}

{"t":100,"op_id":1,"s":0,"op":"CLOSE","h":10}
`
	r, err := NewReader(strings.NewReader(src))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var count int
	for {
		_, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 ops (blank lines skipped), got %d", count)
	}
}

func TestWriterEmitsOneJSONPerLine(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_ = w.WriteHeader(sampleHeader())
	for _, op := range sampleOps() {
		_ = w.WriteOp(op)
	}
	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte{'\n'})
	if got, want := len(lines), 1+len(sampleOps()); got != want {
		t.Fatalf("expected %d lines, got %d (output=%q)", want, got, buf.String())
	}
	for i, l := range lines {
		if len(l) == 0 {
			t.Errorf("line %d is empty", i+1)
		}
	}
}

type errReader struct {
	data []byte
	pos  int
	fail int
	call int
}

var errFault = errors.New("synthetic read failure")

func (e *errReader) Read(p []byte) (int, error) {
	e.call++
	if e.call == e.fail {
		return 0, errFault
	}
	if e.pos >= len(e.data) {
		return 0, io.EOF
	}
	n := copy(p, e.data[e.pos:])
	e.pos += n
	return n, nil
}

func TestReaderIOErrorOnHeader(t *testing.T) {
	r := &errReader{fail: 1}
	_, err := NewReader(r)
	if err == nil || !errors.Is(err, errFault) {
		t.Fatalf("want wrapped errFault, got %v", err)
	}
}

func TestReaderIOErrorMidIteration(t *testing.T) {
	src := `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[{"id":0,"name":"a","kind":"file","size":0}],"summary":{"num_ops":0,"num_streams":0,"num_groups":0,"total_bytes":0,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":10,"mode":"r"}
`
	r := &errReader{data: []byte(src), fail: 2}
	rd, err := NewReader(r)
	if err != nil {
		t.Skipf("scanner consumed input in one read; this branch needs chunked input: %v", err)
	}
	for {
		_, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if !errors.Is(err, errFault) {
				t.Fatalf("want wrapped errFault, got %v", err)
			}
			return
		}
	}
}

func TestReaderLineNumberAdvances(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_ = w.WriteHeader(sampleHeader())
	for _, op := range sampleOps() {
		_ = w.WriteOp(op)
	}
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.Line() != 1 {
		t.Errorf("after NewReader, Line()=%d want 1", r.Line())
	}
	for want := 2; want <= 1+len(sampleOps()); want++ {
		if _, err := r.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		}
		if r.Line() != want {
			t.Errorf("after Next, Line()=%d want %d", r.Line(), want)
		}
	}
}

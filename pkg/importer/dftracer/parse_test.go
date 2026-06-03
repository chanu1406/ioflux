package dftracer_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/importer/dftracer"
	"github.com/chanuollala/ioflux/pkg/trace"
)

func assertValid(t *testing.T, b []byte) {
	t.Helper()
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	rep, err := trace.Validate(r)
	if err != nil {
		t.Fatalf("Validate I/O error: %v", err)
	}
	if !rep.OK() {
		for _, e := range rep.Errors {
			t.Logf("validate error: %s", e)
		}
		t.Fatalf("imported trace failed validation (%d error(s))", len(rep.Errors))
	}
}

func readTrace(t *testing.T, b []byte) (trace.Header, []trace.Op) {
	t.Helper()
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()
	var ops []trace.Op
	for {
		op, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ops = append(ops, op)
	}
	return hdr, ops
}

func countKind(ops []trace.Op, k trace.OpKind) int {
	n := 0
	for _, op := range ops {
		if op.Op == k {
			n++
		}
	}
	return n
}

func hasFlag(flags []string, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}
	return false
}

func targetNames(hdr trace.Header) []string {
	var ns []string
	for _, tg := range hdr.Targets {
		ns = append(ns, tg.Name)
	}
	return ns
}

func TestImport_Basic(t *testing.T) {
	in, err := os.ReadFile("testdata/basic.pfw")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var buf bytes.Buffer
	rep, err := dftracer.Import(bytes.NewReader(in), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())

	// Stream 0 (pid 100 tid 100):
	//   OPEN(a.bin) READ(off=0,4096) READ(off=4096,2048) READ(off=100000,1024) CLOSE(a.bin)
	//   OPEN(b.bin) WRITE(off=0,1024) FSYNC CLOSE(b.bin)  → 9 ops
	// Stream 1 (pid 200 tid 200):
	//   OPEN(c.bin) READ(off=0,8192) CLOSE(c.bin)          → 3 ops
	if rep.NumOps != 12 || rep.NumStreams != 2 || rep.NumTargets != 3 {
		t.Errorf("report = %d ops / %d streams / %d targets; want 12/2/3",
			rep.NumOps, rep.NumStreams, rep.NumTargets)
	}
	wantSkips := map[string]int{
		"eof_read":          1,
		"failed_open":       1,
		"non_posix_event":   1,
		"unsupported_event": 1,
		"unresolved_fd":     1,
	}
	for reason, want := range wantSkips {
		if got := rep.SkippedReasons[reason]; got != want {
			t.Errorf("SkippedReasons[%q] = %d, want %d", reason, got, want)
		}
	}
	if rep.SkippedOps != 5 {
		t.Errorf("SkippedOps = %d, want 5", rep.SkippedOps)
	}

	hdr, ops := readTrace(t, buf.Bytes())

	if hdr.Kind != trace.TraceImported {
		t.Errorf("kind = %q, want imported", hdr.Kind)
	}
	if hdr.CaptureMethod != "import:dftracer" {
		t.Errorf("capture_method = %q, want import:dftracer", hdr.CaptureMethod)
	}
	if hdr.CaptureLimitations == "" {
		t.Error("imported trace missing capture_limitations")
	}

	names := map[string]bool{}
	for _, tg := range hdr.Targets {
		names[tg.Name] = true
	}
	for _, want := range []string{"/data/a.bin", "/data/b.bin", "/data/c.bin"} {
		if !names[want] {
			t.Errorf("expected target %q; got %v", want, targetNames(hdr))
		}
	}
	if names["/data/missing.bin"] {
		t.Error("/data/missing.bin must not be a target (failed open)")
	}
	if names["/data/x.bin"] {
		t.Error("/data/x.bin must not be a target (unresolved fd skip)")
	}

	// Read offset tracking: cursor-based for plain reads, explicit for pread64.
	var sawRead0, sawRead4096, sawPread, sawWriteMode bool
	for _, op := range ops {
		if op.Op == trace.OpOpen && op.Tgt != nil {
			tgt := *op.Tgt
			if tgt >= 0 && tgt < len(hdr.Targets) && hdr.Targets[tgt].Name == "/data/b.bin" {
				if op.Mode != trace.ModeWrite {
					t.Errorf("b.bin OPEN mode = %q, want write (flags=1/O_WRONLY)", op.Mode)
				}
				sawWriteMode = true
			}
		}
		if op.Op == trace.OpRead {
			if op.Off != nil && *op.Off == 0 && op.Len != nil && *op.Len == 4096 {
				sawRead0 = true
			}
			if op.Off != nil && *op.Off == 4096 && op.Len != nil && *op.Len == 2048 {
				sawRead4096 = true
			}
			if op.Off != nil && *op.Off == 100000 && op.Len != nil && *op.Len == 1024 {
				sawPread = true
			}
		}
	}
	if !sawRead0 {
		t.Error("missing READ at off=0 len=4096 (first sequential read)")
	}
	if !sawRead4096 {
		t.Error("missing READ at off=4096 len=2048 (cursor-tracked second read)")
	}
	if !sawPread {
		t.Error("missing READ at off=100000 len=1024 (pread64 positional)")
	}
	if !sawWriteMode {
		t.Error("b.bin (flags=1 / O_WRONLY) must open in write mode")
	}
	if countKind(ops, trace.OpFsync) != 1 {
		t.Errorf("FSYNC count = %d, want 1", countKind(ops, trace.OpFsync))
	}
}

func TestImport_Empty(t *testing.T) {
	var buf bytes.Buffer
	rep, err := dftracer.Import(bytes.NewReader(nil), &buf)
	if err != nil {
		t.Fatalf("Import empty: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumOps != 0 || rep.NumStreams != 0 {
		t.Errorf("empty import: report = %+v, want 0 ops / 0 streams", rep)
	}
}

func TestImport_EmptyArray(t *testing.T) {
	var buf bytes.Buffer
	rep, err := dftracer.Import(strings.NewReader("[\n]\n"), &buf)
	if err != nil {
		t.Fatalf("Import empty array: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumOps != 0 {
		t.Errorf("empty array: NumOps = %d, want 0", rep.NumOps)
	}
}

func TestImport_GarbageInputErrors(t *testing.T) {
	garbage := "this is not dftracer output\njust some random text\nmore lines\n"
	var buf bytes.Buffer
	if _, err := dftracer.Import(strings.NewReader(garbage), &buf); err == nil {
		t.Fatal("Import: want error for non-JSON input, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("garbage input wrote %d bytes; want 0", buf.Len())
	}
}

const noFileIOTrace = `[
{"name":"mmap","cat":"POSIX","ph":"X","ts":1000.0,"dur":1.0,"pid":100,"tid":100,"args":{"return_val":0}},
{"name":"brk","cat":"POSIX","ph":"X","ts":1010.0,"dur":1.0,"pid":100,"tid":100,"args":{"return_val":0}}
]`

func TestImport_NoFileIOIsValidEmpty(t *testing.T) {
	// POSIX events with unsupported names (mmap, brk) must not be treated as
	// garbage — a no-file-I/O process legitimately yields an empty trace.
	var buf bytes.Buffer
	rep, err := dftracer.Import(strings.NewReader(noFileIOTrace), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumOps != 0 {
		t.Errorf("no-file-I/O input: NumOps = %d, want 0", rep.NumOps)
	}
	if rep.SkippedReasons["unsupported_event"] != 2 {
		t.Errorf("unsupported_event = %d, want 2", rep.SkippedReasons["unsupported_event"])
	}
}

const stringFlagsTrace = `[
{"name":"open","cat":"POSIX","ph":"X","ts":1000.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","flags":"O_WRONLY|O_CREAT|O_TRUNC","return_val":3}},
{"name":"write","cat":"POSIX","ph":"X","ts":1010.0,"dur":5.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","fd":3,"count":512,"return_val":512}},
{"name":"close","cat":"POSIX","ph":"X","ts":1020.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","fd":3,"return_val":0}}
]`

func TestImport_StringOpenFlags(t *testing.T) {
	var buf bytes.Buffer
	rep, err := dftracer.Import(strings.NewReader(stringFlagsTrace), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumOps != 3 {
		t.Errorf("NumOps = %d, want 3", rep.NumOps)
	}
	_, ops := readTrace(t, buf.Bytes())
	for _, op := range ops {
		if op.Op == trace.OpOpen {
			if op.Mode != trace.ModeWrite {
				t.Errorf("OPEN mode = %q, want write (O_WRONLY)", op.Mode)
			}
			if !hasFlag(op.Flags, "create") {
				t.Error("OPEN flags missing 'create' (O_CREAT)")
			}
			if !hasFlag(op.Flags, "trunc") {
				t.Error("OPEN flags missing 'trunc' (O_TRUNC)")
			}
		}
	}
}

const lseekTrace = `[
{"name":"open","cat":"POSIX","ph":"X","ts":1000.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","flags":0,"return_val":3}},
{"name":"read","cat":"POSIX","ph":"X","ts":1010.0,"dur":5.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","fd":3,"count":100,"return_val":100}},
{"name":"lseek","cat":"POSIX","ph":"X","ts":1020.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","fd":3,"return_val":500}},
{"name":"read","cat":"POSIX","ph":"X","ts":1030.0,"dur":5.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","fd":3,"count":200,"return_val":200}},
{"name":"close","cat":"POSIX","ph":"X","ts":1040.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/f.bin","fd":3,"return_val":0}}
]`

func TestImport_Lseek(t *testing.T) {
	// first read: cursor 0→100; lseek return_val=500 → cursor 500;
	// second read: cursor-based off=500.
	var buf bytes.Buffer
	_, err := dftracer.Import(strings.NewReader(lseekTrace), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())
	_, ops := readTrace(t, buf.Bytes())
	var saw0, saw500 bool
	for _, op := range ops {
		if op.Op == trace.OpRead {
			if op.Off != nil && *op.Off == 0 && op.Len != nil && *op.Len == 100 {
				saw0 = true
			}
			if op.Off != nil && *op.Off == 500 && op.Len != nil && *op.Len == 200 {
				saw500 = true
			}
		}
	}
	if !saw0 {
		t.Error("missing READ at off=0 len=100 (initial read)")
	}
	if !saw500 {
		t.Error("missing READ at off=500 len=200 (read after lseek to 500)")
	}
}

const appendTrace = `[
{"name":"open","cat":"POSIX","ph":"X","ts":1000.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/log","flags":1025,"return_val":3}},
{"name":"write","cat":"POSIX","ph":"X","ts":1010.0,"dur":5.0,"pid":100,"tid":100,"args":{"fname":"/data/log","fd":3,"count":32,"return_val":32}},
{"name":"close","cat":"POSIX","ph":"X","ts":1020.0,"dur":1.0,"pid":100,"tid":100,"args":{"fname":"/data/log","fd":3,"return_val":0}}
]`

func TestImport_AppendWrite(t *testing.T) {
	// flags=1025 = O_WRONLY(1) | O_APPEND(1024): write must be skipped.
	var buf bytes.Buffer
	rep, err := dftracer.Import(strings.NewReader(appendTrace), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumOps != 2 {
		t.Errorf("NumOps = %d, want 2 (OPEN+CLOSE only; append WRITE skipped)", rep.NumOps)
	}
	if rep.SkippedReasons["append_write_unmodeled"] != 1 {
		t.Errorf("append_write_unmodeled = %d, want 1", rep.SkippedReasons["append_write_unmodeled"])
	}
	_, ops := readTrace(t, buf.Bytes())
	if countKind(ops, trace.OpWrite) != 0 {
		t.Error("WRITE must be skipped for append-mode opens")
	}
	for _, op := range ops {
		if op.Op == trace.OpOpen && hasFlag(op.Flags, "append") {
			return
		}
	}
	t.Error("OPEN must carry the 'append' flag even though its writes are skipped")
}

const multiStreamTrace = `[
{"name":"open","cat":"POSIX","ph":"X","ts":1000.0,"dur":1.0,"pid":10,"tid":10,"args":{"fname":"/data/p1.bin","flags":0,"return_val":3}},
{"name":"read","cat":"POSIX","ph":"X","ts":1010.0,"dur":5.0,"pid":10,"tid":10,"args":{"fname":"/data/p1.bin","fd":3,"count":256,"return_val":256}},
{"name":"close","cat":"POSIX","ph":"X","ts":1020.0,"dur":1.0,"pid":10,"tid":10,"args":{"fname":"/data/p1.bin","fd":3,"return_val":0}},
{"name":"open","cat":"POSIX","ph":"X","ts":2000.0,"dur":1.0,"pid":20,"tid":20,"args":{"fname":"/data/p2.bin","flags":0,"return_val":3}},
{"name":"read","cat":"POSIX","ph":"X","ts":2010.0,"dur":5.0,"pid":20,"tid":20,"args":{"fname":"/data/p2.bin","fd":3,"count":128,"return_val":128}},
{"name":"close","cat":"POSIX","ph":"X","ts":2020.0,"dur":1.0,"pid":20,"tid":20,"args":{"fname":"/data/p2.bin","fd":3,"return_val":0}}
]`

func TestImport_RealDFTracerFixture(t *testing.T) {
	// Uses testdata/real_dftracer.pfw: a fixture modelled on the format emitted
	// by the LLNL DFTracer library (llnl/dftracer dfanalyzer_old/test.pfw).
	// Verifies: "ret" field name, string-encoded count/ret (older serializer quirk),
	// "hostname" in args (ignored), and cursor advancement across sequential reads.
	in, err := os.ReadFile("testdata/real_dftracer.pfw")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var buf bytes.Buffer
	rep, err := dftracer.Import(bytes.NewReader(in), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())

	// fixture: open+write+close (write stream) + open+read+read+read(eof)+close = 8 ops
	// read(ret=0) is EOF → skipped; __lxstat → unsupported_event → skipped
	if rep.NumOps != 8 {
		t.Errorf("NumOps = %d, want 8", rep.NumOps)
	}
	if rep.SkippedReasons["eof_read"] != 1 {
		t.Errorf("eof_read = %d, want 1", rep.SkippedReasons["eof_read"])
	}
	if rep.SkippedReasons["unsupported_event"] != 1 {
		t.Errorf("unsupported_event = %d, want 1 (__lxstat)", rep.SkippedReasons["unsupported_event"])
	}

	_, ops := readTrace(t, buf.Bytes())
	// Three sequential reads at off=0, off=131072, off=262144; cursor must advance.
	offsets := map[int64]bool{}
	for _, op := range ops {
		if op.Op == trace.OpRead && op.Off != nil {
			offsets[*op.Off] = true
		}
	}
	for _, want := range []int64{0, 131072, 262144} {
		if !offsets[want] {
			t.Errorf("missing READ at off=%d (cursor tracking)", want)
		}
	}
}

func TestImport_RealDFTracerGzip(t *testing.T) {
	// Same fixture via .pfw.gz, decompressed before passing to Import (gzip
	// decompression is the CLI's job; here we exercise it explicitly at the
	// package level to confirm the content round-trips correctly).
	raw, err := os.ReadFile("testdata/real_dftracer.pfw.gz")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gz.Close()
	var buf bytes.Buffer
	if _, err := dftracer.Import(gz, &buf); err != nil {
		t.Fatalf("Import .pfw.gz: %v", err)
	}
	assertValid(t, buf.Bytes())
}

func TestImport_MultiStream(t *testing.T) {
	// Two processes each open fd=3; distinct streams must have non-colliding handles.
	var buf bytes.Buffer
	rep, err := dftracer.Import(strings.NewReader(multiStreamTrace), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumStreams != 2 {
		t.Errorf("NumStreams = %d, want 2", rep.NumStreams)
	}
	if rep.NumOps != 6 {
		t.Errorf("NumOps = %d, want 6", rep.NumOps)
	}
	_, ops := readTrace(t, buf.Bytes())
	handles := map[int64]int64{}
	for _, op := range ops {
		if op.Op == trace.OpOpen && op.H != nil {
			if prev, dup := handles[*op.H]; dup {
				t.Errorf("duplicate handle %d: first stream=%d, second stream=%d",
					*op.H, prev, op.S)
			}
			handles[*op.H] = op.S
		}
	}
}

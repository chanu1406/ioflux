package strace_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/importer"
	"github.com/chanuollala/ioflux/pkg/importer/strace"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// importString imports strace text and returns the report, header, and ops,
// failing the test if the result is not a valid trace.
func importString(t *testing.T, s string) (importer.Report, trace.Header, []trace.Op) {
	t.Helper()
	var buf bytes.Buffer
	rep, err := strace.Import(strings.NewReader(s), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())
	hdr, ops := readTrace(t, buf.Bytes())
	return rep, hdr, ops
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

func TestImport_PreservesDurations(t *testing.T) {
	_, _, ops := importString(t, `3201  10:00:00.000000 openat(AT_FDCWD, "/data/a.bin", O_RDONLY) = 3 <0.000010>
3201  10:00:00.000100 read(3, "x"..., 4) = 4 <0.000123>
3201  10:00:00.000200 close(3) = 0 <0.000020>
`)
	if len(ops) != 3 {
		t.Fatalf("ops=%d, want 3", len(ops))
	}
	if ops[1].Dur == nil || *ops[1].Dur != 123_000 {
		t.Fatalf("READ dur=%v, want 123000ns", ops[1].Dur)
	}
}

func TestImport_Basic(t *testing.T) {
	in, err := os.ReadFile("testdata/basic.strace")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var buf bytes.Buffer
	rep, err := strace.Import(bytes.NewReader(in), &buf)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	assertValid(t, buf.Bytes())

	if rep.NumOps != 11 || rep.NumStreams != 2 || rep.NumTargets != 3 {
		t.Errorf("report = %d ops / %d streams / %d targets; want 11/2/3", rep.NumOps, rep.NumStreams, rep.NumTargets)
	}
	wantSkips := map[string]int{
		"eof_read":               1,
		"unresolved_fd":          1,
		"failed_open":            1,
		"append_write_unmodeled": 1,
		"unresolved_dirfd":       1,
	}
	for reason, want := range wantSkips {
		if got := rep.SkippedReasons[reason]; got != want {
			t.Errorf("SkippedReasons[%q] = %d, want %d", reason, got, want)
		}
	}
	if rep.SkippedOps != 5 {
		t.Errorf("SkippedOps = %d, want 5", rep.SkippedOps)
	}
	if rep.TimestampClamped != 0 {
		t.Errorf("TimestampClamped = %d, want 0", rep.TimestampClamped)
	}

	hdr, ops := readTrace(t, buf.Bytes())
	if hdr.Kind != trace.TraceImported || hdr.CaptureMethod != "import:strace" {
		t.Errorf("header kind/method = %q/%q", hdr.Kind, hdr.CaptureMethod)
	}
	if hdr.CaptureLimitations == "" {
		t.Error("imported trace missing capture_limitations")
	}

	// Target table: resolved openat dirfd path present; directory itself absent.
	names := map[string]bool{}
	for _, tg := range hdr.Targets {
		names[tg.Name] = true
	}
	if !names["/data/dir/rel.bin"] {
		t.Error("expected resolved openat path /data/dir/rel.bin in targets")
	}
	if !names["/data/log"] {
		t.Error("expected /data/log in targets")
	}
	if names["/data/dir"] {
		t.Error("directory /data/dir must not be a target (no OPEN op emitted)")
	}

	// Spot-check specific ops.
	var sawPread, sawSecondRead, sawAppendOpen bool
	for _, op := range ops {
		if op.Op == trace.OpRead && op.Off != nil && *op.Off == 100000 && op.Len != nil && *op.Len == 1024 {
			sawPread = true
		}
		if op.Op == trace.OpRead && op.Off != nil && *op.Off == 4096 && op.Len != nil && *op.Len == 2048 {
			sawSecondRead = true // off tracked via cursor after the first 4096-byte read
		}
		if op.Op == trace.OpOpen && op.Mode == trace.ModeWrite && hasFlag(op.Flags, "append") {
			sawAppendOpen = true
		}
	}
	if !sawPread {
		t.Error("missing pread64 op at off=100000 len=1024")
	}
	if !sawSecondRead {
		t.Error("missing cursor-tracked read at off=4096 len=2048")
	}
	if !sawAppendOpen {
		t.Error("missing append-mode OPEN (flag preserved even though its writes are skipped)")
	}

	// No READ/WRITE should reference an offset from the skipped EOF/append ops.
	for _, op := range ops {
		if op.Op == trace.OpWrite {
			t.Errorf("unexpected WRITE op emitted; append writes should be skipped: %+v", op)
		}
	}
}

func hasFlag(flags []string, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}
	return false
}

const failedMetaTrace = `3201  10:00:00.000000 access("/missing", F_OK) = -1 ENOENT (No such file or directory) <0.000010>
3201  10:00:00.000100 stat("/gone", 0x7ffff) = -1 ENOENT (No such file or directory) <0.000010>
3201  10:00:00.000200 openat(AT_FDCWD, "/data/a.bin", O_RDONLY) = 3 <0.000010>
3201  10:00:00.000300 fstat(3, {st_mode=S_IFREG|0644, st_size=10}) = 0 <0.000010>
3201  10:00:00.000400 fsync(3) = -1 EIO (Input/output error) <0.000010>
3201  10:00:00.000500 close(3) = -1 EINTR (Interrupted system call) <0.000010>
`

func TestImport_FailedMetadataSyscallsSkipped(t *testing.T) {
	rep, hdr, ops := importString(t, failedMetaTrace)

	// Only the successful openat and fstat become ops.
	if got := countKind(ops, trace.OpOpen); got != 1 {
		t.Errorf("OPEN ops = %d, want 1", got)
	}
	if got := countKind(ops, trace.OpStat); got != 1 {
		t.Errorf("STAT ops = %d, want 1 (fstat success)", got)
	}
	if got := countKind(ops, trace.OpFsync); got != 0 {
		t.Errorf("FSYNC ops = %d, want 0 (fsync failed)", got)
	}
	if got := countKind(ops, trace.OpClose); got != 0 {
		t.Errorf("CLOSE ops = %d, want 0 (close failed)", got)
	}
	if rep.SkippedReasons["failed_syscall"] != 4 {
		t.Errorf("failed_syscall = %d, want 4 (access, stat, fsync, close)", rep.SkippedReasons["failed_syscall"])
	}
	// Failed access/stat must not pollute the target table.
	for _, tg := range hdr.Targets {
		if tg.Name == "/missing" || tg.Name == "/gone" {
			t.Errorf("failed-stat path %q must not be a target", tg.Name)
		}
	}
	if len(hdr.Targets) != 1 {
		t.Errorf("targets = %d, want 1 (/data/a.bin only)", len(hdr.Targets))
	}
}

const decorationTrace = `3202  10:00:00.000000 openat(5</data/dir>, "rel.bin", O_RDONLY) = 6 <0.000010>
3202  10:00:00.000100 read(6, "x"..., 100) = 100 <0.000010>
3202  10:00:00.000200 close(6) = 0 <0.000010>
3202  10:00:00.000300 fstat(7</data/other.bin>, {st_mode=S_IFREG, st_size=5}) = 0 <0.000010>
`

func TestImport_FdPathDecorationFallback(t *testing.T) {
	rep, hdr, ops := importString(t, decorationTrace)

	if rep.SkippedReasons["unresolved_dirfd"] != 0 || rep.SkippedReasons["unresolved_fd"] != 0 {
		t.Errorf("decoration should resolve fds without prior open; skips=%v", rep.SkippedReasons)
	}
	names := map[string]bool{}
	for _, tg := range hdr.Targets {
		names[tg.Name] = true
	}
	if !names["/data/dir/rel.bin"] {
		t.Error("openat dirfd decoration not used: missing /data/dir/rel.bin")
	}
	if !names["/data/other.bin"] {
		t.Error("fstat fd decoration not used: missing /data/other.bin")
	}
	if countKind(ops, trace.OpRead) != 1 {
		t.Errorf("READ ops = %d, want 1", countKind(ops, trace.OpRead))
	}
}

const openat2Trace = `3203  10:00:00.000000 openat2(AT_FDCWD, "/data/log2", {flags=O_WRONLY|O_APPEND, mode=0, resolve=0}, 24) = 3 <0.000010>
3203  10:00:00.000100 write(3, "x"..., 4) = 4 <0.000010>
3203  10:00:00.000200 close(3) = 0 <0.000010>
3203  10:00:00.000300 openat2(AT_FDCWD, "/data/d", {flags=O_RDONLY|O_DIRECTORY, mode=0, resolve=0}, 24) = 4 <0.000010>
3203  10:00:00.000400 openat2(4, "f.bin", {flags=O_RDONLY, mode=0, resolve=0}, 24) = 5 <0.000010>
3203  10:00:00.000500 read(5, "y"..., 8) = 8 <0.000010>
3203  10:00:00.000600 close(5) = 0 <0.000010>
3203  10:00:00.000700 close(4) = 0 <0.000010>
`

func TestImport_Openat2StructFlags(t *testing.T) {
	rep, hdr, ops := importString(t, openat2Trace)

	// O_APPEND inside the open_how struct must be detected so the write is skipped.
	if rep.SkippedReasons["append_write_unmodeled"] != 1 {
		t.Errorf("append_write_unmodeled = %d, want 1 (openat2 append flag must be parsed)", rep.SkippedReasons["append_write_unmodeled"])
	}
	if countKind(ops, trace.OpWrite) != 0 {
		t.Errorf("WRITE ops = %d, want 0", countKind(ops, trace.OpWrite))
	}
	var sawAppendOpen bool
	for _, op := range ops {
		if op.Op == trace.OpOpen && hasFlag(op.Flags, "append") {
			sawAppendOpen = true
		}
	}
	if !sawAppendOpen {
		t.Error("openat2 append OPEN missing append flag")
	}

	names := map[string]bool{}
	for _, tg := range hdr.Targets {
		names[tg.Name] = true
	}
	// O_DIRECTORY via openat2 -> directory recorded for resolution, not a target.
	if names["/data/d"] {
		t.Error("openat2 O_DIRECTORY target must not be emitted")
	}
	if !names["/data/d/f.bin"] {
		t.Error("openat2 relative path against dirfd not resolved: missing /data/d/f.bin")
	}
	if countKind(ops, trace.OpRead) != 1 {
		t.Errorf("READ ops = %d, want 1", countKind(ops, trace.OpRead))
	}
}

func openTimeFor(hdr trace.Header, ops []trace.Op, name string) (int64, bool) {
	id := -1
	for _, tg := range hdr.Targets {
		if tg.Name == name {
			id = tg.ID
		}
	}
	if id < 0 {
		return 0, false
	}
	for _, op := range ops {
		if op.Op == trace.OpOpen && op.Tgt != nil && *op.Tgt == id {
			return op.T, true
		}
	}
	return 0, false
}

// Later-completing process emitted first; the earlier process appears second.
const outOfOrderTrace = `2  10:00:05.000000 openat(AT_FDCWD, "/data/late.bin", O_RDONLY) = 3 <0.000010>
2  10:00:05.000100 read(3, "x"..., 100) = 100 <0.000010>
2  10:00:05.000200 close(3) = 0 <0.000010>
1  10:00:01.000000 openat(AT_FDCWD, "/data/early.bin", O_RDONLY) = 3 <0.000010>
1  10:00:01.000100 read(3, "y"..., 50) = 50 <0.000010>
1  10:00:01.000200 close(3) = 0 <0.000010>
`

func TestImport_RebasesToGlobalMinTimestamp(t *testing.T) {
	_, hdr, ops := importString(t, outOfOrderTrace)

	early, ok1 := openTimeFor(hdr, ops, "/data/early.bin")
	late, ok2 := openTimeFor(hdr, ops, "/data/late.bin")
	if !ok1 || !ok2 {
		t.Fatal("missing OPEN ops for one of the targets")
	}
	// The earliest event in wall-clock terms must rebase to t=0 even though it
	// appears second in the file; the later event keeps its +4s offset.
	if early != 0 {
		t.Errorf("earliest event t = %d, want 0", early)
	}
	if late != 4_000_000_000 {
		t.Errorf("later event t = %d, want 4000000000 (4s after earliest, not clamped)", late)
	}
}

const atEmptyPathTrace = `3204  10:00:00.000000 openat(AT_FDCWD, "/data/f.bin", O_RDONLY) = 3 <0.000010>
3204  10:00:00.000100 newfstatat(3, "", {st_mode=S_IFREG|0644, st_size=10}, AT_EMPTY_PATH) = 0 <0.000010>
3204  10:00:00.000200 close(3) = 0 <0.000010>
`

func TestImport_NewfstatatAtEmptyPath(t *testing.T) {
	_, hdr, ops := importString(t, atEmptyPathTrace)

	// newfstatat(fd, "", ..., AT_EMPTY_PATH) stats the fd's own file, so it must
	// resolve to /data/f.bin, not to a bogus "/data/f.bin/" path under it.
	if len(hdr.Targets) != 1 {
		t.Fatalf("targets = %d, want 1; got %v", len(hdr.Targets), targetNames(hdr))
	}
	if hdr.Targets[0].Name != "/data/f.bin" {
		t.Errorf("target = %q, want /data/f.bin", hdr.Targets[0].Name)
	}
	if countKind(ops, trace.OpStat) != 1 {
		t.Errorf("STAT ops = %d, want 1", countKind(ops, trace.OpStat))
	}
}

func targetNames(hdr trace.Header) []string {
	var n []string
	for _, tg := range hdr.Targets {
		n = append(n, tg.Name)
	}
	return n
}

func TestImport_GarbageInputErrors(t *testing.T) {
	garbage := "this is not strace output\njust some random text\nmore lines\n"
	var buf bytes.Buffer
	if _, err := strace.Import(strings.NewReader(garbage), &buf); err == nil {
		t.Fatal("Import: want error for non-strace input, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("garbage input wrote %d bytes; want 0", buf.Len())
	}
}

func TestImport_NoFileIOIsValidEmpty(t *testing.T) {
	// Recognizable strace lines that perform no file I/O must NOT be treated as
	// garbage: a no-I/O process legitimately yields an empty trace.
	noIO := `3201  10:00:00.000000 mmap(NULL, 4096, PROT_READ, MAP_PRIVATE, 3, 0) = 0x7f0000000000 <0.000010>
3201  10:00:00.000100 brk(NULL) = 0x555500000000 <0.000010>
`
	rep, _, ops := importString(t, noIO)
	if len(ops) != 0 || rep.NumOps != 0 {
		t.Errorf("expected empty trace for no-file-I/O input; got %d ops", len(ops))
	}
}

func TestImport_Empty(t *testing.T) {
	var buf bytes.Buffer
	rep, err := strace.Import(bytes.NewReader(nil), &buf)
	if err != nil {
		t.Fatalf("Import empty: %v", err)
	}
	assertValid(t, buf.Bytes())
	if rep.NumOps != 0 || rep.NumStreams != 0 {
		t.Errorf("empty import report = %+v, want 0 ops / 0 streams", rep)
	}
}

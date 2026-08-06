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

// TestImport_CaptureLimitationsDeclareTracingOverhead verifies that the
// declared capture_limitations warn that ptrace-based tracing distorts the
// captured timeline, so a timeline/scaled replay of a strace-imported trace
// isn't mistaken for an undistorted pacing measurement.
func TestImport_CaptureLimitationsDeclareTracingOverhead(t *testing.T) {
	_, hdr, _ := importString(t, `3201  10:00:00.000000 openat(AT_FDCWD, "/data/a.bin", O_RDONLY) = 3
3201  10:00:00.000100 close(3) = 0
`)
	for _, want := range []string{"overhead", "distort", "timeline"} {
		if !strings.Contains(hdr.CaptureLimitations, want) {
			t.Errorf("capture_limitations=%q, want it to mention %q", hdr.CaptureLimitations, want)
		}
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
		// The fixture's second read asks for 4096 and gets 2048, so it carries both
		// counts; its offset follows the *transferred* bytes of the first read.
		if op.Op == trace.OpRead && op.Off != nil && *op.Off == 4096 &&
			op.Len != nil && *op.Len == 4096 && op.Ret != nil && *op.Ret == 2048 {
			sawSecondRead = true
		}
		if op.Op == trace.OpOpen && op.Mode == trace.ModeWrite && hasFlag(op.Flags, "append") {
			sawAppendOpen = true
		}
	}
	if !sawPread {
		t.Error("missing pread64 op at off=100000 len=1024")
	}
	if !sawSecondRead {
		t.Error("missing cursor-tracked short read at off=4096 len=4096 ret=2048")
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

// TestImport_ShortReadPreservesRequestedLength pins the fix for the one
// dimension the qual-01 reconciliation measured as failing: a short read used to
// lose the byte count the application asked for, so replay issued a request the
// size of the source's *result* and reported a green, short-read-free run for a
// request the application never made.
//
// The qualification fixture (qualification/FIXTURE.md) reads shards whose size
// is not a multiple of its block size, which produces exactly this case; the
// count was cross-checked against an independent oracle.
func TestImport_ShortReadPreservesRequestedLength(t *testing.T) {
	const in = `12:00:00.000000 openat(AT_FDCWD, "/data/shard.bin", O_RDONLY) = 3 <0.000010>
12:00:00.000100 read(3, ""..., 262144) = 262144 <0.000050>
12:00:00.000200 read(3, ""..., 262144) = 111392 <0.000040>
12:00:00.000300 read(3, "", 262144) = 0 <0.000005>
12:00:00.000400 close(3) = 0 <0.000005>
`
	rep, hdr, ops := importString(t, in)

	// Nothing is lost any more: strace reports the requested count and the IR can
	// now hold it.
	if len(rep.Lossy) != 0 {
		t.Errorf("Lossy = %v, want empty: the requested length is representable", rep.Lossy)
	}
	// The EOF read remains a skip, not a loss: it produced no op at all.
	if rep.SkippedReasons["eof_read"] != 1 {
		t.Errorf("eof_read skips = %d, want 1", rep.SkippedReasons["eof_read"])
	}

	var reads []trace.Op
	for _, op := range ops {
		if op.Op == trace.OpRead {
			reads = append(reads, op)
		}
	}
	if len(reads) != 2 {
		t.Fatalf("got %d READ ops, want 2", len(reads))
	}
	// Full-length read: one count suffices, so ret is omitted.
	if *reads[0].Len != 262144 || reads[0].Ret != nil {
		t.Errorf("full READ = (len %d, ret %v), want (262144, nil)", *reads[0].Len, reads[0].Ret)
	}
	// Short read: both counts survive. len is what replay will request.
	if *reads[1].Len != 262144 {
		t.Errorf("short READ len = %d, want 262144 (the requested count)", *reads[1].Len)
	}
	if reads[1].Ret == nil || *reads[1].Ret != 111392 {
		t.Fatalf("short READ ret = %v, want 111392 (the returned count)", reads[1].Ret)
	}

	// A build predating ret would read len as the transferred count and replay a
	// different request, so the trace must declare a version that build rejects.
	if hdr.Version != trace.VersionPartialTransfer {
		t.Errorf("header version = %d, want %d", hdr.Version, trace.VersionPartialTransfer)
	}
	// The count reaches a consumer holding only the header.
	if hdr.Summary.NumPartialReads != 1 {
		t.Errorf("summary.num_partial_reads = %d, want 1", hdr.Summary.NumPartialReads)
	}
	// total_bytes counts what moved, so it agrees with what a replay reports.
	if want := int64(262144 + 111392); hdr.Summary.TotalBytes != want {
		t.Errorf("summary.total_bytes = %d, want %d (transferred, not requested)",
			hdr.Summary.TotalBytes, want)
	}
	for _, want := range []string{"records both the byte count the source requested",
		"EOF) is dropped", "replayed positionally"} {
		if !strings.Contains(hdr.CaptureLimitations, want) {
			t.Errorf("capture limitations missing %q: %q", want, hdr.CaptureLimitations)
		}
	}
}

// TestImport_FullTransfersAreNotLossy guards against ret firing on ordinary
// full-length reads, which would push every trace to v2 for no reason and make
// the partial-read count meaningless.
func TestImport_FullTransfersAreNotLossy(t *testing.T) {
	const in = `12:00:00.000000 openat(AT_FDCWD, "/data/f", O_RDONLY) = 3 <0.000010>
12:00:00.000100 read(3, ""..., 4096) = 4096 <0.000050>
12:00:00.000200 pread64(3, ""..., 4096, 8192) = 4096 <0.000050>
12:00:00.000300 close(3) = 0 <0.000005>
`
	rep, hdr, ops := importString(t, in)
	if len(rep.Lossy) != 0 {
		t.Errorf("Lossy = %v, want empty for full-length transfers", rep.Lossy)
	}
	if strings.Contains(hdr.Notes, "lossy:") {
		t.Errorf("notes should not mention loss when none occurred: %q", hdr.Notes)
	}
	for _, op := range ops {
		if op.Ret != nil {
			t.Errorf("%s carries ret %d, want nil for a full transfer", op.Op, *op.Ret)
		}
	}
	if hdr.Version != trace.TraceFormatVersion {
		t.Errorf("header version = %d, want %d: a trace using no v2 field must stay readable "+
			"by builds that predate one", hdr.Version, trace.TraceFormatVersion)
	}
	if hdr.Summary.NumPartialReads != 0 {
		t.Errorf("summary.num_partial_reads = %d, want 0", hdr.Summary.NumPartialReads)
	}
}

// TestImport_ShortPositionalReadPreservesRequestedLength covers pread64, whose
// requested count shares read's argument position but which carries an explicit
// offset that must not be confused with it.
func TestImport_ShortPositionalReadPreservesRequestedLength(t *testing.T) {
	const in = `12:00:00.000000 openat(AT_FDCWD, "/data/f", O_RDONLY) = 3 <0.000010>
12:00:00.000100 pread64(3, ""..., 65536, 131072) = 4096 <0.000050>
12:00:00.000200 close(3) = 0 <0.000005>
`
	rep, _, ops := importString(t, in)
	if len(rep.Lossy) != 0 {
		t.Errorf("Lossy = %v, want empty", rep.Lossy)
	}
	var found bool
	for _, op := range ops {
		if op.Op != trace.OpRead {
			continue
		}
		found = true
		if *op.Off != 131072 {
			t.Errorf("pread64 offset = %d, want 131072", *op.Off)
		}
		if *op.Len != 65536 {
			t.Errorf("pread64 len = %d, want 65536 (requested)", *op.Len)
		}
		if op.Ret == nil || *op.Ret != 4096 {
			t.Errorf("pread64 ret = %v, want 4096 (returned)", op.Ret)
		}
	}
	if !found {
		t.Fatal("no READ op emitted")
	}
}

// TestImport_ShortWriteKeepsTransferredLength pins the deliberate asymmetry: a
// partial read is a property of the target's length and is reproducible, while a
// partial write is backend state (a full disk) that a healthy replay backend
// cannot be asked to reproduce. The write records what landed and says so.
func TestImport_ShortWriteKeepsTransferredLength(t *testing.T) {
	const in = `12:00:00.000000 openat(AT_FDCWD, "/data/w", O_WRONLY|O_CREAT) = 3 <0.000010>
12:00:00.000100 write(3, ""..., 4096) = 1000 <0.000050>
12:00:00.000200 close(3) = 0 <0.000005>
`
	_, hdr, ops := importString(t, in)
	for _, op := range ops {
		if op.Op != trace.OpWrite {
			continue
		}
		if *op.Len != 1000 {
			t.Errorf("short WRITE len = %d, want 1000 (bytes that landed)", *op.Len)
		}
		if op.Ret != nil {
			t.Errorf("short WRITE ret = %d, want nil: writes are not partial-transfer ops", *op.Ret)
		}
	}
	if hdr.Version != trace.TraceFormatVersion {
		t.Errorf("header version = %d, want %d", hdr.Version, trace.TraceFormatVersion)
	}
	if !strings.Contains(hdr.CaptureLimitations, "a WRITE op records only the bytes that landed") {
		t.Errorf("capture limitations do not disclose the write asymmetry: %q", hdr.CaptureLimitations)
	}
}

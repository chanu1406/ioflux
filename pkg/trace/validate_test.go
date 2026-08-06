package trace

import (
	"bytes"
	"strings"
	"testing"
)

// mustValidate writes h followed by ops and validates the result, recomputing
// h's op and stream counts from ops first. Validation reconciles the declared
// summary against the actual op stream, so a fixture that exists to exercise an
// op invariant should not have to restate the totals — and cannot drift out of
// agreement with them as ops are added. Tests that exercise the summary fields
// themselves use mustValidateHeaderAsWritten.
func mustValidate(t *testing.T, h Header, ops []Op) Report {
	t.Helper()
	streams := make(map[int64]struct{}, len(ops))
	for _, op := range ops {
		streams[op.S] = struct{}{}
	}
	h.Summary.NumOps = int64(len(ops))
	h.Summary.NumStreams = len(streams)
	return mustValidateHeaderAsWritten(t, h, ops)
}

// mustValidateHeaderAsWritten is mustValidate without recomputing the summary,
// so a test can declare counts that disagree with the ops on purpose.
func mustValidateHeaderAsWritten(t *testing.T, h Header, ops []Op) Report {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteHeader(h); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	for i, op := range ops {
		if err := w.WriteOp(op); err != nil {
			t.Fatalf("WriteOp[%d]: %v", i, err)
		}
	}
	return mustValidateRaw(t, buf.String())
}

func mustValidateRaw(t *testing.T, src string) Report {
	t.Helper()
	r, err := NewReader(strings.NewReader(src))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	rep, err := Validate(r)
	if err != nil {
		t.Fatalf("Validate returned io error: %v", err)
	}
	return rep
}

func validSyntheticHeader() Header {
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

func validCapturedHeader() Header {
	return Header{
		Version:            TraceFormatVersion,
		Kind:               TraceCaptured,
		TimeUnit:           TimeUnitNanoseconds,
		CaptureMethod:      CapturePythonHooks,
		CaptureLimitations: "Python io/os hooks only; mmap and C-extension I/O not captured",
		Targets:            []TargetInfo{{ID: 0, Name: "/data/shard_0000.tar", Kind: TargetFile, Size: 4096}},
		Summary:            Summary{NumOps: 4, NumStreams: 1, NumGroups: 0, TotalBytes: 8192, DurationNS: 1000},
	}
}

func validImportedHeader() Header {
	h := validCapturedHeader()
	h.Kind = TraceImported
	h.CaptureMethod = "import:strace"
	h.CaptureLimitations = "strace syscall trace; mmap page-fault I/O not captured"
	return h
}

func validReadOps() []Op {
	return []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 100, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](4096)},
		{T: 200, OpID: Ptr[int64](2), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](4096), Len: Ptr[int64](4096)},
		{T: 300, OpID: Ptr[int64](3), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
}

func hasErr(rep Report, substr string) bool {
	for _, e := range rep.Errors {
		if strings.Contains(e.Field, substr) || strings.Contains(e.Msg, substr) {
			return true
		}
	}
	return false
}

func hasWarn(rep Report, substr string) bool {
	for _, w := range rep.Warnings {
		if strings.Contains(w.Field, substr) || strings.Contains(w.Msg, substr) {
			return true
		}
	}
	return false
}

func TestValidate_HappyPathSynthetic(t *testing.T) {
	rep := mustValidate(t, validSyntheticHeader(), validReadOps())
	if !rep.OK() {
		t.Fatalf("want OK, got errors=%v warnings=%v", rep.Errors, rep.Warnings)
	}
	if len(rep.Warnings) != 0 {
		t.Errorf("unexpected warnings: %v", rep.Warnings)
	}
	if rep.NumOpsRead != 4 {
		t.Errorf("NumOpsRead = %d, want 4", rep.NumOpsRead)
	}
	if rep.Streams[0] != 4 {
		t.Errorf("Streams[0] = %d, want 4", rep.Streams[0])
	}
}

func TestValidate_HappyPathCaptured(t *testing.T) {
	rep := mustValidate(t, validCapturedHeader(), validReadOps())
	if !rep.OK() {
		t.Fatalf("want OK, got errors=%v", rep.Errors)
	}
}

func TestValidate_HappyPathImported(t *testing.T) {
	rep := mustValidate(t, validImportedHeader(), validReadOps())
	if !rep.OK() {
		t.Fatalf("want OK, got errors=%v", rep.Errors)
	}
}

func TestValidate_MissingVersion(t *testing.T) {
	h := validSyntheticHeader()
	h.Version = 0
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "ioflux_trace_version") {
		t.Fatalf("want ioflux_trace_version error, got %v", rep.Errors)
	}
}

func TestValidate_WrongVersion(t *testing.T) {
	h := validSyntheticHeader()
	h.Version = TraceFormatVersionMax + 1
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "unsupported version") {
		t.Fatalf("want unsupported version error, got %v", rep.Errors)
	}
}

// TestValidate_AcceptsVersionRange pins that widening the range for ret did not
// orphan traces written before it: a v1 trace must keep validating on a build
// that understands v2, or every archived fixture becomes unreadable.
func TestValidate_AcceptsVersionRange(t *testing.T) {
	for v := TraceFormatVersion; v <= TraceFormatVersionMax; v++ {
		h := validSyntheticHeader()
		h.Version = v
		rep := mustValidate(t, h, nil)
		if hasErr(rep, "ioflux_trace_version") {
			t.Errorf("version %d rejected: %v", v, rep.Errors)
		}
	}
}

func TestValidate_MissingKind(t *testing.T) {
	h := validSyntheticHeader()
	h.Kind = ""
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "kind") {
		t.Fatalf("want kind error, got %v", rep.Errors)
	}
}

func TestValidate_InvalidKind(t *testing.T) {
	h := validSyntheticHeader()
	h.Kind = "guessed"
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "invalid kind") {
		t.Fatalf("want invalid kind error, got %v", rep.Errors)
	}
}

func TestValidate_MissingTimeUnit(t *testing.T) {
	h := validSyntheticHeader()
	h.TimeUnit = ""
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "time_unit") {
		t.Fatalf("want time_unit error, got %v", rep.Errors)
	}
}

func TestValidate_UnsupportedTimeUnit(t *testing.T) {
	h := validSyntheticHeader()
	h.TimeUnit = "us"
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "unsupported time_unit") {
		t.Fatalf("want unsupported time_unit error, got %v", rep.Errors)
	}
}

func TestValidate_MissingTargetsHeader(t *testing.T) {
	rep := mustValidateRaw(t, `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"summary":{"num_ops":0,"num_streams":0,"num_groups":0,"total_bytes":0,"duration_ns":0}}`)
	if !hasErr(rep, "targets") {
		t.Fatalf("want targets missing error, got %v", rep.Errors)
	}
}

func TestValidate_MissingSummaryHeader(t *testing.T) {
	rep := mustValidateRaw(t, `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[]}`)
	if !hasErr(rep, "summary") {
		t.Fatalf("want summary missing error, got %v", rep.Errors)
	}
}

func TestValidate_MissingSummaryField(t *testing.T) {
	rep := mustValidateRaw(t, `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[],"summary":{"num_ops":0,"num_streams":0,"total_bytes":0,"duration_ns":0}}`)
	if !hasErr(rep, "summary.num_groups") {
		t.Fatalf("want summary.num_groups missing error, got %v", rep.Errors)
	}
}

func TestValidate_CapturedWithoutMethod(t *testing.T) {
	h := validCapturedHeader()
	h.CaptureMethod = ""
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "capture_method") {
		t.Fatalf("want capture_method error, got %v", rep.Errors)
	}
}

func TestValidate_CapturedWithoutLimitations(t *testing.T) {
	h := validCapturedHeader()
	h.CaptureLimitations = ""
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "capture_limitations") {
		t.Fatalf("want capture_limitations error, got %v", rep.Errors)
	}
}

func TestValidate_ImportedRequiresImportMethod(t *testing.T) {
	h := validImportedHeader()
	h.CaptureMethod = CapturePythonHooks
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "imported trace must use capture_method") {
		t.Fatalf("want imported capture_method error, got %v", rep.Errors)
	}
}

func TestValidate_CapturedRejectsImportMethod(t *testing.T) {
	h := validCapturedHeader()
	h.CaptureMethod = "import:strace"
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "captured trace cannot use capture_method") {
		t.Fatalf("want captured capture_method error, got %v", rep.Errors)
	}
}

func TestValidate_InvalidCaptureMethodEnum(t *testing.T) {
	h := validSyntheticHeader()
	h.CaptureMethod = "ebpf"
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "invalid capture_method") {
		t.Fatalf("want invalid capture_method error, got %v", rep.Errors)
	}
}

func TestValidate_TargetObjectErrors(t *testing.T) {
	h := validSyntheticHeader()
	h.Targets = []TargetInfo{{ID: 7, Name: "", Kind: "bucket", Size: -1}}
	rep := mustValidate(t, h, nil)
	for _, want := range []string{"target at index", "missing required name", "invalid kind", "size -1"} {
		if !hasErr(rep, want) {
			t.Fatalf("want target error containing %q, got %v", want, rep.Errors)
		}
	}
}

func TestValidate_NonMonotonicTimestamp(t *testing.T) {
	ops := []Op{
		{T: 100, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 50, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024)},
		{T: 200, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "non-monotonic timestamp") {
		t.Fatalf("want non-monotonic error, got %v", rep.Errors)
	}
}

func TestValidate_EqualTimestampsAllowed(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](10), Mode: ModeRead},
		{T: 0, OpID: Ptr[int64](1), S: 1, Op: OpOpen, Tgt: Ptr(1), H: Ptr[int64](11), Mode: ModeRead},
		{T: 100, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](10)},
		{T: 100, OpID: Ptr[int64](3), S: 1, Op: OpClose, H: Ptr[int64](11)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !rep.OK() {
		t.Fatalf("equal t should be allowed, got errors=%v", rep.Errors)
	}
}

func TestValidate_MissingOpID(t *testing.T) {
	op := validReadOps()[0]
	op.OpID = nil
	rep := mustValidate(t, validSyntheticHeader(), []Op{op})
	if !hasErr(rep, "op_id") {
		t.Fatalf("want op_id error, got %v", rep.Errors)
	}
}

func TestValidate_DuplicateOpID(t *testing.T) {
	ops := validReadOps()
	ops[1].OpID = Ptr[int64](0)
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "duplicate op_id") {
		t.Fatalf("want duplicate op_id error, got %v", rep.Errors)
	}
}

func TestValidate_StreamGroupOpIDOrder(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](10), S: 0, Op: OpHead, Tgt: Ptr(0)},
		{T: 1, OpID: Ptr[int64](9), S: 0, Op: OpHead, Tgt: Ptr(0)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "not ordered") {
		t.Fatalf("want stream/group order error, got %v", rep.Errors)
	}
}

func TestValidate_DifferentGroupsMayHaveIndependentOpIDOrder(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](10), S: 0, Group: Ptr[int64](1), Op: OpHead, Tgt: Ptr(0)},
		{T: 1, OpID: Ptr[int64](9), S: 0, Group: Ptr[int64](2), Op: OpHead, Tgt: Ptr(0)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !rep.OK() {
		t.Fatalf("different groups should have independent op_id order, got %v", rep.Errors)
	}
}

func TestValidate_TgtOutOfRange(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(5), H: Ptr[int64](42), Mode: ModeRead}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "out of range") {
		t.Fatalf("want tgt out of range, got %v", rep.Errors)
	}
}

func TestValidate_OpenMissingTgt(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, H: Ptr[int64](42), Mode: ModeRead}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "OPEN missing required tgt") {
		t.Fatalf("want OPEN missing tgt, got %v", rep.Errors)
	}
}

func TestValidate_OpenMissingH(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), Mode: ModeRead}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "OPEN missing required h") {
		t.Fatalf("want OPEN missing h, got %v", rep.Errors)
	}
}

func TestValidate_OpenMissingMode(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "missing required mode") {
		t.Fatalf("want OPEN missing mode, got %v", rep.Errors)
	}
}

func TestValidate_OpenInvalidMode(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: "x"}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "invalid mode") {
		t.Fatalf("want invalid mode, got %v", rep.Errors)
	}
}

func TestValidate_ReadWithoutOpen(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "unknown handle") {
		t.Fatalf("want READ unknown-handle error, got %v", rep.Errors)
	}
}

func TestValidate_WriteWithoutOpen(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpWrite, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "unknown handle") {
		t.Fatalf("want WRITE unknown-handle error, got %v", rep.Errors)
	}
}

func TestValidate_CloseWithoutOpen(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpClose, H: Ptr[int64](42)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "unknown handle") {
		t.Fatalf("want CLOSE unknown-handle error, got %v", rep.Errors)
	}
}

func TestValidate_FsyncWithoutOpen(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpFsync, H: Ptr[int64](42)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "unknown handle") {
		t.Fatalf("want FSYNC unknown-handle error, got %v", rep.Errors)
	}
}

func TestValidate_StatWithoutOpenAllowed(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpStat, Tgt: Ptr(0)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !rep.OK() {
		t.Fatalf("STAT without OPEN should be allowed, got errors=%v", rep.Errors)
	}
}

func TestValidate_ObjectOpsWithoutOpenAllowed(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpHead, Tgt: Ptr(0)},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpPut, Tgt: Ptr(0), Len: Ptr[int64](512)},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpGet, Tgt: Ptr(0), Off: Ptr[int64](0), Len: Ptr[int64](512)},
		{T: 3, OpID: Ptr[int64](3), S: 0, Op: OpDelete, Tgt: Ptr(0)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !rep.OK() {
		t.Fatalf("object ops should not require OPEN, got errors=%v", rep.Errors)
	}
}

func TestValidate_ReadMustUseHNotTgt(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, Tgt: Ptr(0), H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "READ must not carry tgt") {
		t.Fatalf("want READ tgt error, got %v", rep.Errors)
	}
}

func TestValidate_ObjectOpsMustUseTgtNotH(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpHead, Tgt: Ptr(0), H: Ptr[int64](42)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "HEAD must not carry h") {
		t.Fatalf("want object h error, got %v", rep.Errors)
	}
}

func TestValidate_ReadMissingOff(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Len: Ptr[int64](1024)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "READ missing required off") {
		t.Fatalf("want READ missing off, got %v", rep.Errors)
	}
}

func TestValidate_ReadMissingLen(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "READ missing required len") {
		t.Fatalf("want READ missing len, got %v", rep.Errors)
	}
}

func TestValidate_ReadNegativeLen(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](-1)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "non-negative") {
		t.Fatalf("want non-negative len, got %v", rep.Errors)
	}
}

func TestValidate_PutMissingLen(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpPut, Tgt: Ptr(0)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "PUT missing required len") {
		t.Fatalf("want PUT missing len, got %v", rep.Errors)
	}
}

func TestValidate_PutNegativeLen(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpPut, Tgt: Ptr(0), Len: Ptr[int64](-5)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "PUT len -5 must be non-negative") {
		t.Fatalf("want PUT negative len, got %v", rep.Errors)
	}
}

// partialReadHeader is validSyntheticHeader declaring the version and count a
// trace carrying ret must declare.
func partialReadHeader() Header {
	h := validSyntheticHeader()
	h.Version = VersionPartialTransfer
	h.Summary.NumPartialReads = 1
	return h
}

func partialReadOps(ret int64) []Op {
	return []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42),
			Off: Ptr[int64](0), Len: Ptr[int64](4096), Ret: Ptr(ret)},
	}
}

func TestValidate_PartialReadAccepted(t *testing.T) {
	rep := mustValidate(t, partialReadHeader(), partialReadOps(1000))
	if !rep.OK() {
		t.Fatalf("want OK, got %v", rep.Errors)
	}
	if rep.NumPartialReads != 1 {
		t.Errorf("NumPartialReads = %d, want 1", rep.NumPartialReads)
	}
}

func TestValidate_RetExceedingLen(t *testing.T) {
	rep := mustValidate(t, partialReadHeader(), partialReadOps(8192))
	if !hasErr(rep, "cannot return more than it asked for") {
		t.Fatalf("want ret>len error, got %v", rep.Errors)
	}
}

func TestValidate_RetNegative(t *testing.T) {
	rep := mustValidate(t, partialReadHeader(), partialReadOps(-1))
	if !hasErr(rep, "ret -1 must be non-negative") {
		t.Fatalf("want negative ret error, got %v", rep.Errors)
	}
}

// TestValidate_RetOnWriteRejected pins the deliberate asymmetry: a partial read
// describes the target's length and is reproducible; a partial write describes
// backend state a healthy replay backend cannot be asked to reproduce.
func TestValidate_RetOnWriteRejected(t *testing.T) {
	for _, kind := range []OpKind{OpWrite, OpPut} {
		h := partialReadHeader()
		h.Summary.NumPartialReads = 0
		ops := []Op{
			{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeWrite},
			{T: 1, OpID: Ptr[int64](1), S: 0, Op: kind, H: Ptr[int64](42),
				Off: Ptr[int64](0), Len: Ptr[int64](4096), Ret: Ptr[int64](1000)},
		}
		if kind == OpPut {
			ops[1] = Op{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpPut, Tgt: Ptr(0),
				Len: Ptr[int64](4096), Ret: Ptr[int64](1000)}
		}
		rep := mustValidate(t, h, ops)
		if !hasErr(rep, "must not carry ret") {
			t.Errorf("%s: want ret-forbidden error, got %v", kind, rep.Errors)
		}
	}
}

// TestValidate_RetRequiresDeclaredVersion is the fail-closed check. A build that
// predates ret ignores the field and reads len as the transferred count, so it
// would replay a different request size and report a green run. Declaring v1
// while carrying ret is therefore an error, not a tolerated inconsistency.
func TestValidate_RetRequiresDeclaredVersion(t *testing.T) {
	h := partialReadHeader()
	h.Version = TraceFormatVersion // v1, but the ops use a v2 field
	rep := mustValidate(t, h, partialReadOps(1000))
	if !hasErr(rep, "ret requires ioflux_trace_version") {
		t.Fatalf("want version-gate error, got %v", rep.Errors)
	}
}

// TestValidate_PartialReadCountReconciled proves the header's count is
// recomputed rather than trusted: it is the only thing a consumer holding just
// the header — a distributed coordinator, a saved report — can rely on.
func TestValidate_PartialReadCountReconciled(t *testing.T) {
	h := partialReadHeader()
	h.Summary.NumOps, h.Summary.NumStreams = 2, 1
	h.Summary.NumPartialReads = 7 // the ops contain exactly 1
	rep := mustValidateHeaderAsWritten(t, h, partialReadOps(1000))
	if !hasErr(rep, "summary.num_partial_reads") {
		t.Fatalf("want num_partial_reads reconciliation error, got %v", rep.Errors)
	}
}

func TestValidate_UnknownOpKind(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: "FRUNGE", Tgt: Ptr(0)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "unknown op kind") {
		t.Fatalf("want unknown op error, got %v", rep.Errors)
	}
}

func TestValidate_ReadAfterCloseRequiresReopen(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpClose, H: Ptr[int64](42)},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "closed handle") {
		t.Fatalf("want READ-after-CLOSE error, got %v", rep.Errors)
	}
}

func TestValidate_DuplicateHandleOpenIsError(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "introduced by more than one OPEN") {
		t.Fatalf("want duplicate handle error, got %v", rep.Errors)
	}
}

func TestValidate_MissingCloseIsWarning(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !rep.OK() {
		t.Fatalf("missing CLOSE should not error, got %v", rep.Errors)
	}
	if !hasWarn(rep, "never CLOSEd") {
		t.Fatalf("want never-CLOSEd warning, got %v", rep.Warnings)
	}
}

func TestValidate_HandlesAreStreamIsolated(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 1, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024)},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "from stream 0 on stream 1") {
		t.Fatalf("want cross-stream handle error, got %v", rep.Errors)
	}
}

func TestValidate_ReportCountsStreams(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](10), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 1, Op: OpOpen, Tgt: Ptr(1), H: Ptr[int64](11), Mode: ModeRead},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpRead, H: Ptr[int64](10), Off: Ptr[int64](0), Len: Ptr[int64](1)},
		{T: 3, OpID: Ptr[int64](3), S: 0, Op: OpClose, H: Ptr[int64](10)},
		{T: 4, OpID: Ptr[int64](4), S: 1, Op: OpClose, H: Ptr[int64](11)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !rep.OK() {
		t.Fatalf("want OK, got %v", rep.Errors)
	}
	if rep.Streams[0] != 3 {
		t.Errorf("Streams[0] = %d, want 3", rep.Streams[0])
	}
	if rep.Streams[1] != 2 {
		t.Errorf("Streams[1] = %d, want 2", rep.Streams[1])
	}
}

func TestIssue_StringFormats(t *testing.T) {
	cases := []struct {
		i    Issue
		want string
	}{
		{Issue{Line: 5, Field: "t", Msg: "bad"}, "line 5 [t] bad"},
		{Issue{Line: 0, Field: "open", Msg: "leaked"}, "[open] leaked"},
	}
	for _, c := range cases {
		if got := c.i.String(); got != c.want {
			t.Errorf("Issue.String() = %q, want %q", got, c.want)
		}
	}
}

// Coverage-gap tests: branches in validate.go not reached by the tests above.

func TestValidate_NegativeOpID(t *testing.T) {
	ops := []Op{{T: 0, OpID: Ptr[int64](-1), S: 0, Op: OpHead, Tgt: Ptr(0)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "must be non-negative") {
		t.Fatalf("want negative op_id error, got %v", rep.Errors)
	}
}

func TestValidate_TargetsNull(t *testing.T) {
	// targets:null is distinct from targets being absent; the validator
	// must reject it explicitly.
	rep := mustValidateRaw(t, `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":null,"summary":{"num_ops":0,"num_streams":0,"num_groups":0,"total_bytes":0,"duration_ns":0}}`)
	if !hasErr(rep, "targets") {
		t.Fatalf("want targets error, got %v", rep.Errors)
	}
}

func TestValidate_SummaryNull(t *testing.T) {
	// summary:null is distinct from summary being absent.
	rep := mustValidateRaw(t, `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[],"summary":null}`)
	if !hasErr(rep, "summary") {
		t.Fatalf("want summary null error, got %v", rep.Errors)
	}
}

func TestValidate_TargetEmptyKind(t *testing.T) {
	h := validSyntheticHeader()
	h.Targets = []TargetInfo{{ID: 0, Name: "shard_0000.tar", Kind: ""}}
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "missing required kind") {
		t.Fatalf("want target missing-kind error, got %v", rep.Errors)
	}
}

func TestValidate_NegativeSummaryFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Summary)
		want   string
	}{
		{"NumOps", func(s *Summary) { s.NumOps = -1 }, "summary.num_ops"},
		{"NumStreams", func(s *Summary) { s.NumStreams = -1 }, "summary.num_streams"},
		{"NumGroups", func(s *Summary) { s.NumGroups = -1 }, "summary.num_groups"},
		{"TotalBytes", func(s *Summary) { s.TotalBytes = -1 }, "summary.total_bytes"},
		{"DurationNS", func(s *Summary) { s.DurationNS = -1 }, "summary.duration_ns"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := validSyntheticHeader()
			c.mutate(&h.Summary)
			rep := mustValidateHeaderAsWritten(t, h, nil)
			if !hasErr(rep, c.want) {
				t.Fatalf("want %s error, got %v", c.want, rep.Errors)
			}
		})
	}
}

func TestValidate_NonOpenOpWithMode(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024), Mode: ModeRead},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "must not carry mode") {
		t.Fatalf("want non-OPEN mode error, got %v", rep.Errors)
	}
}

func TestValidate_NonOpenOpWithFlags(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1024), Flags: []string{"direct"}},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "must not carry flags") {
		t.Fatalf("want non-OPEN flags error, got %v", rep.Errors)
	}
}

func TestValidate_HandleOpMissingH(t *testing.T) {
	// CLOSE with no h field at all — distinct from an unknown handle ID.
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpClose}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "CLOSE missing required h") {
		t.Fatalf("want CLOSE missing-h error, got %v", rep.Errors)
	}
}

func TestValidate_ReadNegativeOff(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](-1), Len: Ptr[int64](1024)},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](42)},
	}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "must be non-negative") {
		t.Fatalf("want negative off error, got %v", rep.Errors)
	}
}

func TestValidate_OpenCarriesOffAndLen(t *testing.T) {
	// OPEN must not carry off or len; forbidPositional enforces both.
	ops := []Op{{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](42), Mode: ModeRead,
		Off: Ptr[int64](0), Len: Ptr[int64](1024)}}
	rep := mustValidate(t, validSyntheticHeader(), ops)
	if !hasErr(rep, "OPEN must not carry off") {
		t.Errorf("want OPEN off error, got %v", rep.Errors)
	}
	if !hasErr(rep, "OPEN must not carry len") {
		t.Errorf("want OPEN len error, got %v", rep.Errors)
	}
}

// --- Summary reconciliation ---

// TestValidate_TruncatedTraceRejected is the regression test for a trace that
// lost ops after it was written: an interrupted copy, a failed transfer, a
// hand-trimmed file. Nothing downstream detects this on its own — replay
// measures coverage against the ops it finds, so a short trace reports full
// coverage of itself and the missing work leaves no mark on the result. The
// declared summary is the only independent record of what should have been
// there, which is why disagreement is an error.
func TestValidate_TruncatedTraceRejected(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(0), H: Ptr[int64](1), Mode: ModeRead},
		{T: 1, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](1), Off: Ptr[int64](0), Len: Ptr[int64](1024)},
		{T: 2, OpID: Ptr[int64](2), S: 0, Op: OpClose, H: Ptr[int64](1)},
	}
	h := validSyntheticHeader()
	h.Summary.NumOps = int64(len(ops))
	h.Summary.NumStreams = 1

	if rep := mustValidateHeaderAsWritten(t, h, ops); !rep.OK() {
		t.Fatalf("the complete trace should validate, got %v", rep.Errors)
	}

	// The same header, with the op stream cut short at a clean boundary — the
	// shape a truncated file actually takes.
	rep := mustValidateHeaderAsWritten(t, h, ops[:1])
	if rep.OK() {
		t.Fatal("truncated trace validated OK; lost ops must not be invisible")
	}
	if !hasErr(rep, "summary.num_ops") {
		t.Fatalf("want a num_ops reconciliation error, got %v", rep.Errors)
	}
	// The count actually read is reported for diagnosis.
	if rep.NumOpsRead != 1 {
		t.Errorf("NumOpsRead=%d, want 1", rep.NumOpsRead)
	}
}

// TestValidate_StaleStreamCountRejected covers the case truncation does not
// always change the op count for: a trace whose declared concurrency no longer
// matches the streams present.
func TestValidate_StaleStreamCountRejected(t *testing.T) {
	ops := []Op{
		{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpStat, Tgt: Ptr(0)},
		{T: 1, OpID: Ptr[int64](0), S: 1, Op: OpStat, Tgt: Ptr(1)},
	}
	h := validSyntheticHeader()
	h.Summary.NumOps = int64(len(ops))
	h.Summary.NumStreams = 1 // stale: two streams are present

	rep := mustValidateHeaderAsWritten(t, h, ops)
	if rep.OK() {
		t.Fatal("stale stream count validated OK")
	}
	if !hasErr(rep, "summary.num_streams") {
		t.Fatalf("want a num_streams reconciliation error, got %v", rep.Errors)
	}
}

// TestValidate_EmptyTraceAgrees verifies the reconciliation does not
// misfire on a legitimately empty trace.
func TestValidate_EmptyTraceAgrees(t *testing.T) {
	h := validSyntheticHeader()
	h.Summary.NumOps = 0
	h.Summary.NumStreams = 0

	rep := mustValidateHeaderAsWritten(t, h, nil)
	if hasErr(rep, "summary.num_ops") || hasErr(rep, "summary.num_streams") {
		t.Fatalf("empty trace should reconcile cleanly, got %v", rep.Errors)
	}
}

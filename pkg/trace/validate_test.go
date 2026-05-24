package trace

import (
	"bytes"
	"strings"
	"testing"
)

func mustValidate(t *testing.T, h Header, ops []Op) Report {
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
	h.Version = 2
	rep := mustValidate(t, h, nil)
	if !hasErr(rep, "unsupported version") {
		t.Fatalf("want unsupported version error, got %v", rep.Errors)
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

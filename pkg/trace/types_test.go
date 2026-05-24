package trace

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestTraceKindValid(t *testing.T) {
	cases := []struct {
		k    TraceKind
		want bool
	}{
		{TraceCaptured, true},
		{TraceImported, true},
		{TraceSynthetic, true},
		{"", false},
		{"other", false},
	}
	for _, c := range cases {
		if got := c.k.Valid(); got != c.want {
			t.Errorf("TraceKind(%q).Valid() = %v, want %v", c.k, got, c.want)
		}
	}
}

func TestCaptureMethodValid(t *testing.T) {
	cases := []struct {
		m    CaptureMethod
		want bool
	}{
		{CapturePythonHooks, true},
		{CaptureEBPFSyscall, true},
		{CaptureMmapAware, true},
		{CaptureSynthetic, true},
		{"import:strace", true},
		{"import:dftracer", true},
		{"", false},
		{"ebpf", false},
		{"strace", false},
		{"import:", false},
	}
	for _, c := range cases {
		if got := c.m.Valid(); got != c.want {
			t.Errorf("CaptureMethod(%q).Valid() = %v, want %v", c.m, got, c.want)
		}
	}
}

func TestCaptureMethodIsImport(t *testing.T) {
	if !CaptureMethod("import:strace").IsImport() {
		t.Fatal("import:strace should be recognized as an import method")
	}
	for _, m := range []CaptureMethod{CaptureSynthetic, CapturePythonHooks, "import:"} {
		if m.IsImport() {
			t.Fatalf("%q should not be recognized as an import method", m)
		}
	}
}

func TestModeValid(t *testing.T) {
	cases := []struct {
		m    Mode
		want bool
	}{
		{ModeRead, true},
		{ModeWrite, true},
		{ModeReadWrite, true},
		{"", false},
		{"a", false},
	}
	for _, c := range cases {
		if got := c.m.Valid(); got != c.want {
			t.Errorf("Mode(%q).Valid() = %v, want %v", c.m, got, c.want)
		}
	}
}

func TestOpKindValid(t *testing.T) {
	all := []OpKind{
		OpOpen, OpClose, OpRead, OpWrite, OpStat, OpFsync,
		OpPut, OpGet, OpHead, OpDelete,
	}
	for _, k := range all {
		if !k.Valid() {
			t.Errorf("OpKind(%q).Valid() = false, want true", k)
		}
	}
	for _, k := range []OpKind{"", "open", "FOO"} {
		if k.Valid() {
			t.Errorf("OpKind(%q).Valid() = true, want false", k)
		}
	}
}

func TestOpKindClassifiers(t *testing.T) {
	objectOps := map[OpKind]bool{
		OpPut: true, OpGet: true, OpHead: true, OpDelete: true,
	}
	handleOps := map[OpKind]bool{
		OpRead: true, OpWrite: true, OpFsync: true, OpClose: true,
	}
	all := []OpKind{
		OpOpen, OpClose, OpRead, OpWrite, OpStat, OpFsync,
		OpPut, OpGet, OpHead, OpDelete,
	}
	for _, k := range all {
		if got, want := k.IsObjectOp(), objectOps[k]; got != want {
			t.Errorf("OpKind(%q).IsObjectOp() = %v, want %v", k, got, want)
		}
		if got, want := k.IsHandleOp(), handleOps[k]; got != want {
			t.Errorf("OpKind(%q).IsHandleOp() = %v, want %v", k, got, want)
		}
	}
}

func TestTargetKindValid(t *testing.T) {
	cases := []struct {
		k    TargetKind
		want bool
	}{
		{TargetFile, true},
		{TargetObject, true},
		{"", false},
		{"bucket", false},
	}
	for _, c := range cases {
		if got := c.k.Valid(); got != c.want {
			t.Errorf("TargetKind(%q).Valid() = %v, want %v", c.k, got, c.want)
		}
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Version:            TraceFormatVersion,
		Kind:               TraceSynthetic,
		Profile:            "training-read",
		GeneratedBy:        "ioflux gen 0.1.0",
		CreatedUTC:         "2026-05-23T19:00:00Z",
		TimeUnit:           TimeUnitNanoseconds,
		CaptureMethod:      CaptureSynthetic,
		CaptureLimitations: "",
		Scrubbed:           false,
		Targets: []TargetInfo{
			{ID: 0, Name: "shard_0000.tar", Kind: TargetFile, Size: 64 << 20},
			{ID: 1, Name: "shard_0001.tar", Kind: TargetFile, Size: 64 << 20},
		},
		Summary: Summary{
			NumOps: 12, NumStreams: 2, NumGroups: 0, TotalBytes: 1 << 20, DurationNS: 500_000,
		},
		Notes: "tiny fixture",
	}

	b, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Header
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(h, got) {
		t.Fatalf("round-trip mismatch:\nwant=%#v\ngot=%#v", h, got)
	}
}

func TestHeaderOmitsEmptyOptionalFields(t *testing.T) {
	h := Header{
		Version:  TraceFormatVersion,
		Kind:     TraceSynthetic,
		TimeUnit: TimeUnitNanoseconds,
		Targets:  []TargetInfo{{ID: 0, Name: "a", Kind: TargetFile}},
		Summary:  Summary{NumGroups: 0},
	}
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, mustAbsent := range []string{
		"capture_method", "capture_limitations", "profile",
		"generated_by", "created_utc", "notes",
	} {
		if strings.Contains(s, mustAbsent) {
			t.Errorf("encoded header unexpectedly contains %q: %s", mustAbsent, s)
		}
	}
	for _, mustPresent := range []string{
		"ioflux_trace_version", "kind", "time_unit", "scrubbed", "targets", "summary",
	} {
		if !strings.Contains(s, mustPresent) {
			t.Errorf("encoded header missing required field %q: %s", mustPresent, s)
		}
	}
}

func TestOpRoundTripEveryKind(t *testing.T) {
	cases := []struct {
		name string
		op   Op
	}{
		{"OPEN", Op{T: 0, OpID: Ptr[int64](0), S: 0, Op: OpOpen, Tgt: Ptr(17), H: Ptr[int64](42), Mode: ModeRead, Flags: []string{"direct"}}},
		{"READ at offset 0", Op{T: 412000, OpID: Ptr[int64](1), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](0), Len: Ptr[int64](1 << 20)}},
		{"READ at offset 1MiB", Op{T: 1_900_000, OpID: Ptr[int64](2), S: 0, Op: OpRead, H: Ptr[int64](42), Off: Ptr[int64](1 << 20), Len: Ptr[int64](1 << 20)}},
		{"WRITE", Op{T: 2_000_000, OpID: Ptr[int64](3), S: 1, Op: OpWrite, H: Ptr[int64](43), Off: Ptr[int64](4096), Len: Ptr[int64](4096)}},
		{"CLOSE", Op{T: 9_100_000, OpID: Ptr[int64](4), S: 0, Op: OpClose, H: Ptr[int64](42)}},
		{"STAT", Op{T: 10, OpID: Ptr[int64](5), S: 2, Op: OpStat, Tgt: Ptr(1)}},
		{"FSYNC", Op{T: 20, OpID: Ptr[int64](6), S: 1, Op: OpFsync, H: Ptr[int64](43)}},
		{"PUT", Op{T: 100, OpID: Ptr[int64](7), S: 4, Op: OpPut, Tgt: Ptr(9), Len: Ptr[int64](2048)}},
		{"GET range", Op{T: 200, OpID: Ptr[int64](8), S: 4, Op: OpGet, Tgt: Ptr(9), Off: Ptr[int64](0), Len: Ptr[int64](512)}},
		{"HEAD", Op{T: 300, OpID: Ptr[int64](9), S: 4, Op: OpHead, Tgt: Ptr(9)}},
		{"DELETE", Op{T: 400, OpID: Ptr[int64](10), S: 4, Op: OpDelete, Tgt: Ptr(9)}},
		{"grouped", Op{T: 500, OpID: Ptr[int64](11), S: 4, Op: OpGet, Group: Ptr[int64](2), Tgt: Ptr(9), Off: Ptr[int64](0), Len: Ptr[int64](1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := json.Marshal(c.op)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got Op
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(c.op, got) {
				t.Fatalf("round-trip mismatch:\n  encoded=%s\n  want=%#v\n  got=%#v",
					string(b), c.op, got)
			}
		})
	}
}

func TestOpReadAtZeroOffsetPreserved(t *testing.T) {
	op := Op{T: 100, OpID: Ptr[int64](0), S: 0, Op: OpRead, H: Ptr[int64](7), Off: Ptr[int64](0), Len: Ptr[int64](4096)}
	b, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"off":0`) {
		t.Fatalf("expected off=0 to be encoded, got %s", string(b))
	}
	var got Op
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Off == nil || *got.Off != 0 {
		t.Fatalf("off lost in round-trip: got %v", got.Off)
	}
}

func TestOpOmitsAbsentVariantFields(t *testing.T) {
	op := Op{T: 10, OpID: Ptr[int64](0), S: 1, Op: OpClose, H: Ptr[int64](10)}
	b, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, k := range []string{`"mode"`, `"tgt"`, `"off"`, `"len"`, `"flags"`} {
		if strings.Contains(s, k) {
			t.Errorf("CLOSE op should not encode %s, got %s", k, s)
		}
	}
}

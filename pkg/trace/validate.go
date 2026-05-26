package trace

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Issue is a single validator finding. Line is 1-based; 0 means the issue is
// not tied to a specific line (e.g., a missing-CLOSE warning surfaced after
// iteration completes).
type Issue struct {
	Line  int
	Field string
	Msg   string
}

func (i Issue) String() string {
	if i.Line == 0 {
		return fmt.Sprintf("[%s] %s", i.Field, i.Msg)
	}
	return fmt.Sprintf("line %d [%s] %s", i.Line, i.Field, i.Msg)
}

// Report is the result of Validate. Errors are spec violations; Warnings are
// suspicious-but-permitted patterns. NumOpsRead and Streams are populated even
// on failure for diagnostic context.
type Report struct {
	Header     Header
	NumOpsRead int64
	Streams    map[int64]int64
	Errors     []Issue
	Warnings   []Issue
}

// OK reports whether the trace passes validation (no errors). Warnings do not
// affect OK.
func (r Report) OK() bool { return len(r.Errors) == 0 }

func (r *Report) addError(line int, field, msg string) {
	r.Errors = append(r.Errors, Issue{Line: line, Field: field, Msg: msg})
}

func (r *Report) addWarning(line int, field, msg string) {
	r.Warnings = append(r.Warnings, Issue{Line: line, Field: field, Msg: msg})
}

// Validate reads every op from r and reports any schema and invariant
// violations, including handle lifecycle, target references, op_id uniqueness,
// and global timestamp ordering.
//
// The returned error is non-nil only for I/O or parse failures that prevent
// iteration. Spec violations are returned via Report.Errors so a CLI user sees
// every problem in one pass.
func Validate(r *Reader) (Report, error) {
	rep := Report{
		Header:  r.Header(),
		Streams: map[int64]int64{},
	}
	validateHeaderPresence(r.headerRaw, &rep)
	validateHeader(rep.Header, &rep)

	handles := map[int64]handleState{}
	seenOpIDs := map[int64]int{}
	lastOpIDByGroup := map[streamGroup]int64{}

	var lastT int64
	var sawAny bool

	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return rep, err
		}
		line := r.Line()
		rep.NumOpsRead++
		rep.Streams[op.S]++

		if sawAny && op.T < lastT {
			rep.addError(line, "t",
				fmt.Sprintf("non-monotonic timestamp %d < previous %d", op.T, lastT))
		}
		if !sawAny || op.T > lastT {
			lastT = op.T
		}
		sawAny = true

		validateOpID(line, op, seenOpIDs, lastOpIDByGroup, &rep)

		if !op.Op.Valid() {
			rep.addError(line, "op", fmt.Sprintf("unknown op kind %q", op.Op))
			continue
		}

		if op.Op != OpOpen {
			if op.Mode != "" {
				rep.addError(line, "mode", fmt.Sprintf("%s must not carry mode", op.Op))
			}
			if len(op.Flags) > 0 {
				rep.addError(line, "flags", fmt.Sprintf("%s must not carry flags", op.Op))
			}
		}

		switch op.Op {
		case OpOpen:
			tgt, ok := requireTarget(line, op, rep.Header, &rep)
			if op.H == nil {
				rep.addError(line, "h", "OPEN missing required h")
			} else if prev, exists := handles[*op.H]; exists {
				rep.addError(line, "h",
					fmt.Sprintf("handle %d introduced by more than one OPEN (first line %d)",
						*op.H, prev.openLine))
			} else if ok {
				handles[*op.H] = handleState{stream: op.S, target: tgt, openLine: line}
			}
			if op.Mode == "" {
				rep.addError(line, "mode", "OPEN missing required mode")
			} else if !op.Mode.Valid() {
				rep.addError(line, "mode", fmt.Sprintf("invalid mode %q", op.Mode))
			}
			forbidPositional(line, op, &rep)
		case OpRead, OpWrite:
			requireOpenHandle(line, op, handles, &rep)
			forbidTarget(line, op, &rep)
			requireOffLen(line, op, &rep)
		case OpFsync, OpClose:
			if requireOpenHandle(line, op, handles, &rep) && op.Op == OpClose && op.H != nil {
				st := handles[*op.H]
				st.closed = true
				handles[*op.H] = st
			}
			forbidTarget(line, op, &rep)
			forbidPositional(line, op, &rep)
		case OpStat:
			requireTarget(line, op, rep.Header, &rep)
			forbidHandle(line, op, &rep)
			forbidPositional(line, op, &rep)
		case OpGet:
			requireTarget(line, op, rep.Header, &rep)
			forbidHandle(line, op, &rep)
			requireOffLen(line, op, &rep)
		case OpPut:
			requireTarget(line, op, rep.Header, &rep)
			forbidHandle(line, op, &rep)
			if op.Off != nil {
				rep.addError(line, "off", "PUT must not carry off")
			}
			requireLen(line, op, &rep)
		case OpHead, OpDelete:
			requireTarget(line, op, rep.Header, &rep)
			forbidHandle(line, op, &rep)
			forbidPositional(line, op, &rep)
		}
	}

	for h, st := range handles {
		if !st.closed {
			rep.addWarning(0, "open",
				fmt.Sprintf("handle %d opened on stream %d target %d never CLOSEd",
					h, st.stream, st.target))
		}
	}

	return rep, nil
}

// ValidateLoadedRaw validates an already-loaded raw header line and op list.
// It preserves header-field presence checks for callers that still have the
// original header bytes.
func ValidateLoadedRaw(headerRaw []byte, ops []Op) (Report, error) {
	var buf bytes.Buffer
	buf.Write(bytes.TrimSpace(headerRaw))
	buf.WriteByte('\n')
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, op := range ops {
		if err := enc.Encode(op); err != nil {
			return Report{}, err
		}
	}
	r, err := NewReader(&buf)
	if err != nil {
		return Report{}, err
	}
	return Validate(r)
}

type handleState struct {
	stream   int64
	target   int
	openLine int
	closed   bool
}

type streamGroup struct {
	stream int64
	group  int64
}

func validateOpID(line int, op Op, seen map[int64]int, lastByGroup map[streamGroup]int64, rep *Report) {
	if op.OpID == nil {
		rep.addError(line, "op_id", "missing required field")
		return
	}
	if *op.OpID < 0 {
		rep.addError(line, "op_id", fmt.Sprintf("op_id %d must be non-negative", *op.OpID))
	}
	if prevLine, ok := seen[*op.OpID]; ok {
		rep.addError(line, "op_id",
			fmt.Sprintf("duplicate op_id %d first seen on line %d", *op.OpID, prevLine))
	} else {
		seen[*op.OpID] = line
	}
	group := int64(0)
	if op.Group != nil {
		group = *op.Group
	}
	sg := streamGroup{stream: op.S, group: group}
	if last, ok := lastByGroup[sg]; ok && *op.OpID <= last {
		rep.addError(line, "op_id",
			fmt.Sprintf("op_id %d is not ordered after previous op_id %d in stream %d group %d",
				*op.OpID, last, op.S, group))
	}
	lastByGroup[sg] = *op.OpID
}

func validateHeaderPresence(raw []byte, rep *Report) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return // NewReader already reports header parse failures.
	}
	for _, field := range []string{"targets", "summary"} {
		if _, ok := obj[field]; !ok {
			rep.addError(1, field, "missing required field")
		}
	}
	if rawTargets, ok := obj["targets"]; ok && string(rawTargets) == "null" {
		rep.addError(1, "targets", "must be an array")
	}
	if rawSummary, ok := obj["summary"]; ok {
		if string(rawSummary) == "null" {
			rep.addError(1, "summary", "must be an object")
			return
		}
		var summary map[string]json.RawMessage
		if err := json.Unmarshal(rawSummary, &summary); err != nil {
			return
		}
		for _, field := range []string{"num_ops", "num_streams", "num_groups", "total_bytes", "duration_ns"} {
			if _, ok := summary[field]; !ok {
				rep.addError(1, "summary."+field, "missing required field")
			}
		}
	}
}

func validateHeader(h Header, rep *Report) {
	if h.Version == 0 {
		rep.addError(1, "ioflux_trace_version", "missing required field")
	} else if h.Version != TraceFormatVersion {
		rep.addError(1, "ioflux_trace_version",
			fmt.Sprintf("unsupported version %d (this build expects %d)",
				h.Version, TraceFormatVersion))
	}

	if h.Kind == "" {
		rep.addError(1, "kind", "missing required field")
	} else if !h.Kind.Valid() {
		rep.addError(1, "kind", fmt.Sprintf("invalid kind %q (want %q, %q, or %q)",
			h.Kind, TraceCaptured, TraceImported, TraceSynthetic))
	}

	if h.TimeUnit == "" {
		rep.addError(1, "time_unit", "missing required field")
	} else if h.TimeUnit != TimeUnitNanoseconds {
		rep.addError(1, "time_unit",
			fmt.Sprintf("unsupported time_unit %q (this build expects %q)",
				h.TimeUnit, TimeUnitNanoseconds))
	}

	if h.CaptureMethod != "" && !h.CaptureMethod.Valid() {
		rep.addError(1, "capture_method",
			fmt.Sprintf("invalid capture_method %q", h.CaptureMethod))
	}

	if h.Kind == TraceCaptured || h.Kind == TraceImported {
		if h.CaptureMethod == "" {
			rep.addError(1, "capture_method",
				fmt.Sprintf("%s trace must declare capture_method", h.Kind))
		}
		if h.CaptureLimitations == "" {
			rep.addError(1, "capture_limitations",
				fmt.Sprintf("%s trace must declare capture_limitations", h.Kind))
		}
	}
	if h.Kind == TraceCaptured && (h.CaptureMethod == CaptureSynthetic || h.CaptureMethod.IsImport()) {
		rep.addError(1, "capture_method",
			fmt.Sprintf("captured trace cannot use capture_method %q", h.CaptureMethod))
	}
	if h.Kind == TraceImported && h.CaptureMethod != "" && !h.CaptureMethod.IsImport() {
		rep.addError(1, "capture_method",
			fmt.Sprintf("imported trace must use capture_method %q", ImportPrefix+"<source>"))
	}
	if h.Kind == TraceSynthetic && h.CaptureMethod != "" && h.CaptureMethod != CaptureSynthetic {
		rep.addError(1, "capture_method",
			fmt.Sprintf("synthetic trace cannot use capture_method %q", h.CaptureMethod))
	}

	for i, tgt := range h.Targets {
		if tgt.ID != i {
			rep.addError(1, "targets",
				fmt.Sprintf("target at index %d has id %d (want %d)", i, tgt.ID, i))
		}
		if tgt.Name == "" {
			rep.addError(1, "targets.name",
				fmt.Sprintf("target %d missing required name", i))
		}
		if tgt.Kind == "" {
			rep.addError(1, "targets.kind",
				fmt.Sprintf("target %d missing required kind", i))
		} else if !tgt.Kind.Valid() {
			rep.addError(1, "targets.kind",
				fmt.Sprintf("target %d has invalid kind %q", i, tgt.Kind))
		}
		if tgt.Size < 0 {
			rep.addError(1, "targets.size",
				fmt.Sprintf("target %d size %d must be non-negative", i, tgt.Size))
		}
	}

	if h.Summary.NumOps < 0 {
		rep.addError(1, "summary.num_ops", "must be non-negative")
	}
	if h.Summary.NumStreams < 0 {
		rep.addError(1, "summary.num_streams", "must be non-negative")
	}
	if h.Summary.NumGroups < 0 {
		rep.addError(1, "summary.num_groups", "must be non-negative")
	}
	if h.Summary.TotalBytes < 0 {
		rep.addError(1, "summary.total_bytes", "must be non-negative")
	}
	if h.Summary.DurationNS < 0 {
		rep.addError(1, "summary.duration_ns", "must be non-negative")
	}
}

func requireTarget(line int, op Op, h Header, rep *Report) (int, bool) {
	if op.Tgt == nil {
		rep.addError(line, "tgt", fmt.Sprintf("%s missing required tgt", op.Op))
		return 0, false
	}
	if *op.Tgt < 0 || *op.Tgt >= len(h.Targets) {
		rep.addError(line, "tgt",
			fmt.Sprintf("tgt %d out of range [0,%d)", *op.Tgt, len(h.Targets)))
		return 0, false
	}
	return *op.Tgt, true
}

func forbidTarget(line int, op Op, rep *Report) {
	if op.Tgt != nil {
		rep.addError(line, "tgt", fmt.Sprintf("%s must not carry tgt", op.Op))
	}
}

func requireOpenHandle(line int, op Op, handles map[int64]handleState, rep *Report) bool {
	if op.H == nil {
		rep.addError(line, "h", fmt.Sprintf("%s missing required h", op.Op))
		return false
	}
	st, ok := handles[*op.H]
	if !ok {
		rep.addError(line, "h",
			fmt.Sprintf("%s references unknown handle %d", op.Op, *op.H))
		return false
	}
	if st.closed {
		rep.addError(line, "h",
			fmt.Sprintf("%s references closed handle %d", op.Op, *op.H))
		return false
	}
	if st.stream != op.S {
		rep.addError(line, "h",
			fmt.Sprintf("%s references handle %d from stream %d on stream %d",
				op.Op, *op.H, st.stream, op.S))
		return false
	}
	return true
}

func forbidHandle(line int, op Op, rep *Report) {
	if op.H != nil {
		rep.addError(line, "h", fmt.Sprintf("%s must not carry h", op.Op))
	}
}

func requireOffLen(line int, op Op, rep *Report) {
	if op.Off == nil {
		rep.addError(line, "off", fmt.Sprintf("%s missing required off", op.Op))
	} else if *op.Off < 0 {
		rep.addError(line, "off",
			fmt.Sprintf("%s off %d must be non-negative", op.Op, *op.Off))
	}
	requireLen(line, op, rep)
}

func requireLen(line int, op Op, rep *Report) {
	if op.Len == nil {
		rep.addError(line, "len", fmt.Sprintf("%s missing required len", op.Op))
	} else if *op.Len < 0 {
		rep.addError(line, "len",
			fmt.Sprintf("%s len %d must be non-negative", op.Op, *op.Len))
	}
}

func forbidPositional(line int, op Op, rep *Report) {
	if op.Off != nil {
		rep.addError(line, "off", fmt.Sprintf("%s must not carry off", op.Op))
	}
	if op.Len != nil {
		rep.addError(line, "len", fmt.Sprintf("%s must not carry len", op.Op))
	}
}

// Package trace defines the IOFlux trace intermediate representation and the
// streaming JSONL codec used to read and write *.ioflux files.
//
// The trace IR is the contract between capture, generation, replay, and
// storage backends. Every other IOFlux subsystem depends on this package and
// nothing else; this package depends on no other IOFlux package.
//
// The on-disk format is line-delimited JSON: one header record followed by
// one operation record per line.
package trace

import "strings"

// TraceFormatVersion is the only ioflux_trace_version value accepted by v1.
const TraceFormatVersion = 1

// TimeUnitNanoseconds is the only time_unit value accepted by v1.
const TimeUnitNanoseconds = "ns"

// TraceKind distinguishes how a trace was produced. Consumers use this to keep
// captured, imported, and synthetic traces clearly separated in reports.
type TraceKind string

const (
	TraceCaptured  TraceKind = "captured"
	TraceImported  TraceKind = "imported"
	TraceSynthetic TraceKind = "synthetic"
)

// Valid reports whether k is one of the recognized trace kinds.
func (k TraceKind) Valid() bool {
	switch k {
	case TraceCaptured, TraceImported, TraceSynthetic:
		return true
	}
	return false
}

// CaptureMethod records how a captured or imported trace was produced.
// Synthetic traces use CaptureSynthetic. Imported traces use the
// "import:<source>" form, e.g. "import:strace".
type CaptureMethod string

const (
	CapturePythonHooks CaptureMethod = "python-io-hooks"
	CaptureEBPFSyscall CaptureMethod = "ebpf-syscall"
	CaptureMmapAware   CaptureMethod = "mmap-aware"
	CaptureSynthetic   CaptureMethod = "synthetic"
)

// ImportPrefix marks a capture_method emitted by ioflux import. The suffix
// names the upstream tracer (e.g., "import:strace", "import:dftracer").
const ImportPrefix = "import:"

// Valid reports whether m is a recognized capture method. The "import:<src>"
// form is accepted for any non-empty source.
func (m CaptureMethod) Valid() bool {
	switch m {
	case CapturePythonHooks, CaptureEBPFSyscall, CaptureMmapAware, CaptureSynthetic:
		return true
	}
	if strings.HasPrefix(string(m), ImportPrefix) && len(m) > len(ImportPrefix) {
		return true
	}
	return false
}

// IsImport reports whether m is an "import:<source>" capture method.
func (m CaptureMethod) IsImport() bool {
	return strings.HasPrefix(string(m), ImportPrefix) && len(m) > len(ImportPrefix)
}

// Mode is the open mode for an OPEN op.
type Mode string

const (
	ModeRead      Mode = "r"
	ModeWrite     Mode = "w"
	ModeReadWrite Mode = "rw"
)

// Valid reports whether m is one of the recognized open modes.
func (m Mode) Valid() bool {
	switch m {
	case ModeRead, ModeWrite, ModeReadWrite:
		return true
	}
	return false
}

// OpKind enumerates the storage operations representable in a trace.
type OpKind string

const (
	OpOpen   OpKind = "OPEN"
	OpClose  OpKind = "CLOSE"
	OpRead   OpKind = "READ"
	OpWrite  OpKind = "WRITE"
	OpStat   OpKind = "STAT"
	OpFsync  OpKind = "FSYNC"
	OpPut    OpKind = "PUT"
	OpGet    OpKind = "GET"
	OpHead   OpKind = "HEAD"
	OpDelete OpKind = "DELETE"
)

// Valid reports whether k is a recognized op kind.
func (k OpKind) Valid() bool {
	switch k {
	case OpOpen, OpClose, OpRead, OpWrite, OpStat, OpFsync,
		OpPut, OpGet, OpHead, OpDelete:
		return true
	}
	return false
}

// IsObjectOp reports whether k is an object-store operation. Object ops carry
// a tgt and never an h.
func (k OpKind) IsObjectOp() bool {
	switch k {
	case OpPut, OpGet, OpHead, OpDelete:
		return true
	}
	return false
}

// IsHandleOp reports whether k operates on an open handle (h). These ops
// carry h and never tgt (the tgt is determined by the prior OPEN).
func (k OpKind) IsHandleOp() bool {
	switch k {
	case OpRead, OpWrite, OpFsync, OpClose:
		return true
	}
	return false
}

// TargetKind classifies a target as a POSIX file path or an object-store key.
type TargetKind string

const (
	TargetFile   TargetKind = "file"
	TargetObject TargetKind = "object"
)

// Valid reports whether k is a recognized target kind.
func (k TargetKind) Valid() bool {
	switch k {
	case TargetFile, TargetObject:
		return true
	}
	return false
}

// TargetInfo describes a single entry in the trace's target table. Size
// records the expected object size where known. A zero size means "unknown".
type TargetInfo struct {
	ID   int        `json:"id"`
	Name string     `json:"name"`
	Kind TargetKind `json:"kind"`
	Size int64      `json:"size"`
}

// Summary records aggregate trace statistics. Populated by the producer
// (generator or capture tool) and treated as advisory metadata by the
// validator. NumGroups is 0 for traces that use only the implicit default
// group (the v1 synthetic case).
type Summary struct {
	NumOps     int64 `json:"num_ops"`
	NumStreams int   `json:"num_streams"`
	NumGroups  int   `json:"num_groups"`
	TotalBytes int64 `json:"total_bytes"`
	DurationNS int64 `json:"duration_ns"`
}

// Header is the first line of an .ioflux file.
//
// Required for every header: Version, Kind, TimeUnit, Targets, Summary.
// For Kind in {TraceCaptured, TraceImported}, CaptureMethod and
// CaptureLimitations are also required so consumers always know the
// fidelity of the trace. Scrubbed indicates whether ioflux scrub has
// anonymized target names.
type Header struct {
	Version            int           `json:"ioflux_trace_version"`
	Kind               TraceKind     `json:"kind"`
	Profile            string        `json:"profile,omitempty"`
	GeneratedBy        string        `json:"generated_by,omitempty"`
	CreatedUTC         string        `json:"created_utc,omitempty"`
	TimeUnit           string        `json:"time_unit"`
	CaptureMethod      CaptureMethod `json:"capture_method,omitempty"`
	CaptureLimitations string        `json:"capture_limitations,omitempty"`
	Scrubbed           bool          `json:"scrubbed"`
	Targets            []TargetInfo  `json:"targets"`
	Summary            Summary       `json:"summary"`
	Notes              string        `json:"notes,omitempty"`
}

// Op is a single storage operation record.
//
// Pointer fields use omitempty so the JSON encoding mirrors the per-op-kind
// schema (e.g., READ has h/off/len but no tgt; OPEN has tgt/h/mode/flags but
// no off/len). Using pointers preserves the legitimate zero value across the
// JSON round-trip, which a plain int with `omitempty` would silently drop.
type Op struct {
	T     int64    `json:"t"`
	OpID  *int64   `json:"op_id,omitempty"`
	S     int64    `json:"s"`
	Op    OpKind   `json:"op"`
	Group *int64   `json:"group,omitempty"`
	Tgt   *int     `json:"tgt,omitempty"`
	H     *int64   `json:"h,omitempty"`
	Mode  Mode     `json:"mode,omitempty"`
	Flags []string `json:"flags,omitempty"`
	Off   *int64   `json:"off,omitempty"`
	Len   *int64   `json:"len,omitempty"`
}

// Ptr returns a pointer to v. Convenience for constructing Op values in
// tests and generators where many fields are optional.
func Ptr[T any](v T) *T { return &v }

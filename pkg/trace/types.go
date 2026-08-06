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

// TraceFormatVersion is the base ioflux_trace_version. A trace that uses no
// field introduced after v1 declares exactly this, so every trace ever written
// by IOFlux remains valid and byte-identical. A writer that emits a later field
// must declare the version that field requires; see Op.MinVersion.
const TraceFormatVersion = 1

// TraceFormatVersionMax is the newest ioflux_trace_version this build
// understands. A trace declaring a higher version is rejected outright rather
// than read with its unrecognized fields silently dropped: a field the reader
// does not know about decodes as a benign default — "the whole request was
// transferred" — which is exactly how an invalid run would look green.
const TraceFormatVersionMax = 2

// VersionPartialTransfer is the ioflux_trace_version required by an op carrying
// ret. A reader that predates ret would interpret len as the transferred count
// and issue a request the size of the source's *result*, so such a trace must
// fail closed on an older build instead of replaying a different workload.
const VersionPartialTransfer = 2

// TimeUnitNanoseconds is the supported time_unit.
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

// TargetKind classifies a target as a local file path or an object-store key.
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
// group.
//
// NumPartialReads counts READ/GET ops whose source transferred fewer bytes than
// it requested (ops carrying Ret). It is optional so pre-v2 traces stay valid,
// and it is reconciled against the op stream rather than trusted, because it is
// what tells a consumer holding only the header — a distributed coordinator, a
// saved report — that the trace contains partial transfers at all.
type Summary struct {
	NumOps          int64 `json:"num_ops"`
	NumStreams      int   `json:"num_streams"`
	NumGroups       int   `json:"num_groups"`
	TotalBytes      int64 `json:"total_bytes"`
	DurationNS      int64 `json:"duration_ns"`
	NumPartialReads int64 `json:"num_partial_reads,omitempty"`
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
//
// Len and Ret are the two halves of a transfer outcome and answer different
// questions. Len is what the source *asked* for; Ret is what it *got*. Storing
// only one collapses them, and the collapse is not neutral: a source read of
// 256 KiB that returned 111 KiB would replay as a 111 KiB request, so the
// backend is measured on a request the application never made and the run
// reports no short read because the replay itself read nothing short.
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
	// Len is the number of bytes the operation requests.
	Len *int64 `json:"len,omitempty"`
	// Ret is the number of bytes the source operation actually transferred,
	// present only on READ/GET and only when it differs from Len. Absent means
	// the full requested length was transferred, which is what every trace
	// written before schema v2 means by its len alone.
	//
	// It is deliberately not carried on WRITE/PUT. A partial read is a
	// reproducible property of the target's length (EOF); a partial write is
	// backend state (a full disk) that a healthy replay backend cannot be asked
	// to reproduce, so recording one would let a trace demand a divergence
	// rather than describe one.
	Ret *int64 `json:"ret,omitempty"`
	Dur *int64 `json:"dur,omitempty"`
}

// MinVersion returns the lowest ioflux_trace_version whose readers can
// interpret op correctly. A writer declares the maximum over its ops, so a
// trace advertises exactly the version it needs and no more — traces using only
// v1 fields keep validating on builds that predate anything later.
func (op Op) MinVersion() int {
	if op.Ret != nil {
		return VersionPartialTransfer
	}
	return TraceFormatVersion
}

// MinVersionForOps returns the ioflux_trace_version a trace containing ops must
// declare.
func MinVersionForOps(ops []Op) int {
	v := TraceFormatVersion
	for _, op := range ops {
		if m := op.MinVersion(); m > v {
			v = m
		}
	}
	return v
}

// IsPartialTransfer reports whether op records a source transfer that moved
// fewer bytes than it requested.
func (op Op) IsPartialTransfer() bool { return op.Ret != nil }

// RequestedBytes returns the number of bytes op asks the backend for, and false
// when op carries no length.
func (op Op) RequestedBytes() (int64, bool) {
	if op.Len == nil {
		return 0, false
	}
	return *op.Len, true
}

// TransferredBytes returns the number of bytes the source operation moved: Ret
// when the transfer was partial, otherwise Len. It is what the replay expects
// the backend to return, and what target extents and byte totals are derived
// from — a short read proves the target *ends* there.
func (op Op) TransferredBytes() (int64, bool) {
	if op.Ret != nil {
		return *op.Ret, true
	}
	return op.RequestedBytes()
}

// Ptr returns a pointer to v. Convenience for constructing Op values in
// tests and generators where many fields are optional.
func Ptr[T any](v T) *T { return &v }

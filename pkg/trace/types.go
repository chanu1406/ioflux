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
// understands. A higher version is rejected outright rather than read with its
// unknown fields dropped, since they decode as benign defaults that would make
// an invalid run look clean.
const TraceFormatVersionMax = 3

// VersionPartialTransfer is the ioflux_trace_version required by an op carrying
// ret. A reader that predates ret would interpret len as the transferred count
// and issue a request the size of the source's *result*, so such a trace must
// fail closed on an older build instead of replaying a different workload.
const VersionPartialTransfer = 2

// VersionTransformed is the ioflux_trace_version required by a header carrying
// a transformation ledger. The ops of a transformed trace are self-describing,
// so an older reader would replay them perfectly well — and report the run as
// an exact replay of a captured workload, because the one field saying
// otherwise is the one it does not know about. Absence reading as "not
// transformed" is precisely the benign default that must fail closed.
const VersionTransformed = 3

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

// Summary records aggregate trace statistics, populated by the producer and
// advisory to the validator. NumGroups is 0 for traces using only the implicit
// default group. NumPartialReads counts READ/GET ops whose source transferred
// fewer bytes than requested; it is optional so pre-v2 traces stay valid, and
// reconciled against the op stream rather than trusted.
type Summary struct {
	NumOps          int64 `json:"num_ops"`
	NumStreams      int   `json:"num_streams"`
	NumGroups       int   `json:"num_groups"`
	TotalBytes      int64 `json:"total_bytes"`
	DurationNS      int64 `json:"duration_ns"`
	NumPartialReads int64 `json:"num_partial_reads,omitempty"`
}

// Header is the first line of an .ioflux file. Version, Kind, TimeUnit,
// Targets, and Summary are always required; captured and imported traces also
// require CaptureMethod and CaptureLimitations so consumers know their fidelity.
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
	// Transformations is the ledger of declared changes applied after capture,
	// oldest first. Empty means the trace still describes what was captured or
	// generated. It travels in the trace so a replay of a transformed trace can
	// never be read as a replay of the source workload.
	Transformations []Transformation `json:"transformations,omitempty"`
}

// Transformation records one declared change to a trace: what was done, with
// what parameters, and to which trace. SourceDigest ties it to the exact bytes
// it came from, which is what lets a comparison tell a declared transformation
// of the baseline apart from an unrelated workload.
type Transformation struct {
	Kind         string            `json:"kind"`
	Params       map[string]string `json:"params,omitempty"`
	SourceDigest string            `json:"source_digest,omitempty"`
	AppliedUTC   string            `json:"applied_utc,omitempty"`
	Note         string            `json:"note,omitempty"`
}

// TransformSplitReads is the kind recorded by `ioflux transform split-reads`.
const TransformSplitReads = "split-reads"

// MinVersionForHeader returns the lowest ioflux_trace_version whose readers can
// interpret h correctly, ignoring its ops (see MinVersionForOps).
func MinVersionForHeader(h Header) int {
	if len(h.Transformations) > 0 {
		return VersionTransformed
	}
	return TraceFormatVersion
}

// IsTransformed reports whether the trace carries a transformation ledger.
func (h Header) IsTransformed() bool { return len(h.Transformations) > 0 }

// DerivedFrom reports whether any transformation in h names digest as the trace
// it was derived from.
func (h Header) DerivedFrom(digest string) bool {
	if digest == "" {
		return false
	}
	for _, t := range h.Transformations {
		if t.SourceDigest == digest {
			return true
		}
	}
	return false
}

// Op is a single storage operation record.
//
// Pointer fields use omitempty so the encoding mirrors the per-op-kind schema
// and a legitimate zero survives the JSON round-trip, which a plain int with
// omitempty would drop. Len is what the source asked for, Ret what it got;
// keeping both apart is what stops a short read replaying as a smaller request.
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
	// Ret is the number of bytes the source actually transferred, present only on
	// READ/GET and only when it differs from Len. Absent means the full requested
	// length was transferred. Not carried on WRITE/PUT: a partial read follows
	// from the target's length, but a partial write is backend state a healthy
	// replay cannot be asked to reproduce.
	Ret *int64 `json:"ret,omitempty"`
	Dur *int64 `json:"dur,omitempty"`
}

// MinVersion returns the lowest ioflux_trace_version that can interpret op. A
// writer declares the maximum over its ops, so a trace advertises exactly the
// version it needs and no more.
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

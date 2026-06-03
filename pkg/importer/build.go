// Package importer provides the shared scaffolding for translating external
// trace formats into IOFlux traces. Format-specific parsers (e.g. strace,
// dftracer) feed operations into a Builder, which interns targets, orders
// streams, assigns op_ids, and writes a self-validated *.ioflux trace.
//
// The Builder buffers the serialized trace and validates it before writing a
// single byte to the caller's writer, so a parser bug can never emit a trace
// that would fail `ioflux validate`.
package importer

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"slices"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// HeaderMeta carries the format-specific header fields an importer supplies.
// The Builder fills in Version, TimeUnit, Targets, and Summary.
type HeaderMeta struct {
	Kind               trace.TraceKind
	CaptureMethod      trace.CaptureMethod
	CaptureLimitations string
	GeneratedBy        string
	Notes              string
	CreatedUTC         string
}

// Report summarizes what an import produced, including everything that was
// dropped, so the CLI can print an honest account of fidelity.
type Report struct {
	NumOps           int
	NumStreams       int
	NumTargets       int
	SkippedOps       int
	TimestampClamped int
	SkippedReasons   map[string]int
}

type streamOp struct {
	streamID int64
	localIdx int64
	op       trace.Op
}

// Builder accumulates ops per stream and produces a validated trace. It is not
// safe for concurrent use; an importer drives it from a single goroutine.
type Builder struct {
	targets   []trace.TargetInfo
	targetIdx map[string]int
	perStream map[int64][]streamOp
	report    Report
}

// NewBuilder returns an empty Builder.
func NewBuilder() *Builder {
	return &Builder{
		targets:   make([]trace.TargetInfo, 0),
		targetIdx: make(map[string]int),
		perStream: make(map[int64][]streamOp),
		report:    Report{SkippedReasons: make(map[string]int)},
	}
}

// Target interns name with the given kind and returns its target-table id.
// Repeated names return the existing id; the kind from the first sighting wins.
func (b *Builder) Target(name string, kind trace.TargetKind) int {
	if id, ok := b.targetIdx[name]; ok {
		return id
	}
	id := len(b.targets)
	b.targetIdx[name] = id
	b.targets = append(b.targets, trace.TargetInfo{ID: id, Name: name, Kind: kind})
	return id
}

// Add appends op to the stream named by op.S, in arrival order. The caller sets
// op.T and the op fields; op_id is assigned by WriteTo.
func (b *Builder) Add(op trace.Op) {
	s := op.S
	b.perStream[s] = append(b.perStream[s], streamOp{
		streamID: s,
		localIdx: int64(len(b.perStream[s])),
		op:       op,
	})
}

// Skip records a dropped source operation under reason.
func (b *Builder) Skip(reason string) {
	b.report.SkippedReasons[reason]++
	b.report.SkippedOps++
}

// ClampedTimestamp records that one op's timestamp was adjusted to preserve
// monotonicity. (WriteTo does the clamping; importers that adjust earlier may
// also report here.)
func (b *Builder) ClampedTimestamp() { b.report.TimestampClamped++ }

// WriteTo assembles the accumulated ops into a trace, validates it, and writes
// it to w only if validation passes. On any error nothing is written to w.
func (b *Builder) WriteTo(w io.Writer, meta HeaderMeta) (Report, error) {
	streamIDs := make([]int64, 0, len(b.perStream))
	for s := range b.perStream {
		streamIDs = append(streamIDs, s)
	}
	slices.Sort(streamIDs)

	// Rebase to the global minimum timestamp so the trace starts at t=0,
	// independent of the source's absolute clock and of the order lines were
	// emitted. (Multi-process strace interleaves syscall-completion order, not
	// arrival-time order, so the earliest event is not necessarily the first
	// line.) Then clamp each stream's timestamps to be non-decreasing: a source
	// clock can still step backward within a stream (NTP, rounding); clamping
	// keeps the trace valid and the count is reported, never hidden.
	minT, haveMin := b.minTimestamp(streamIDs)
	for _, s := range streamIDs {
		ops := b.perStream[s]
		var prev int64
		for i := range ops {
			if haveMin {
				ops[i].op.T -= minT
			}
			if i > 0 && ops[i].op.T < prev {
				ops[i].op.T = prev
				b.report.TimestampClamped++
			}
			prev = ops[i].op.T
		}
	}

	// Merge all streams, sorted by (t, streamID, localIdx) so the file satisfies
	// the global non-decreasing timestamp invariant while each stream's ops stay
	// in causal order.
	var all []streamOp
	for _, s := range streamIDs {
		all = append(all, b.perStream[s]...)
	}
	slices.SortStableFunc(all, func(a, c streamOp) int {
		if a.op.T != c.op.T {
			return cmp.Compare(a.op.T, c.op.T)
		}
		if a.streamID != c.streamID {
			return cmp.Compare(a.streamID, c.streamID)
		}
		return cmp.Compare(a.localIdx, c.localIdx)
	})

	var totalBytes, durationNS int64
	for i := range all {
		id := int64(i)
		all[i].op.OpID = &id
		if countsBytes(all[i].op.Op) && all[i].op.Len != nil {
			totalBytes += *all[i].op.Len
		}
	}
	if len(all) > 0 {
		durationNS = all[len(all)-1].op.T
	}

	hdr := trace.Header{
		Version:            trace.TraceFormatVersion,
		Kind:               meta.Kind,
		GeneratedBy:        meta.GeneratedBy,
		CreatedUTC:         meta.CreatedUTC,
		TimeUnit:           trace.TimeUnitNanoseconds,
		CaptureMethod:      meta.CaptureMethod,
		CaptureLimitations: meta.CaptureLimitations,
		Targets:            b.targets,
		Summary: trace.Summary{
			NumOps:     int64(len(all)),
			NumStreams: len(streamIDs),
			NumGroups:  0,
			TotalBytes: totalBytes,
			DurationNS: durationNS,
		},
		Notes: meta.Notes,
	}

	// Serialize into a buffer so we can validate before touching w.
	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		return Report{}, err
	}
	for i := range all {
		if err := tw.WriteOp(all[i].op); err != nil {
			return Report{}, err
		}
	}

	rd, err := trace.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return Report{}, fmt.Errorf("importer: self-validate: %w", err)
	}
	rep, err := trace.Validate(rd)
	if err != nil {
		return Report{}, fmt.Errorf("importer: self-validate: %w", err)
	}
	if !rep.OK() {
		return Report{}, fmt.Errorf("importer: produced an invalid trace (%d error(s); first: %s)",
			len(rep.Errors), rep.Errors[0])
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return Report{}, err
	}

	b.report.NumOps = len(all)
	b.report.NumStreams = len(streamIDs)
	b.report.NumTargets = len(b.targets)
	return b.report, nil
}

// minTimestamp returns the smallest op timestamp across all streams, and false
// if there are no ops.
func (b *Builder) minTimestamp(streamIDs []int64) (int64, bool) {
	var min int64
	found := false
	for _, s := range streamIDs {
		for _, so := range b.perStream[s] {
			if !found || so.op.T < min {
				min = so.op.T
				found = true
			}
		}
	}
	return min, found
}

// countsBytes reports whether an op moves payload bytes counted in TotalBytes.
func countsBytes(k trace.OpKind) bool {
	switch k {
	case trace.OpRead, trace.OpWrite, trace.OpGet, trace.OpPut:
		return true
	}
	return false
}

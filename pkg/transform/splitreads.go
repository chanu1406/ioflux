// Package transform applies declared, deliberate changes to a trace.
//
// A transformation is not a repair and not a convenience: it produces a trace
// that no longer describes the captured workload, and every one of them is
// recorded in the output's ledger so a replay can never present itself as an
// exact replay of the source.
package transform

import (
	"fmt"
	"time"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// SplitReads rewrites every READ and GET in ops so that no operation requests
// more than block bytes, preserving the targets, the offsets covered, the
// per-stream order, and the total bytes transferred.
//
// This models a reader using a smaller block size. It is the treatment
// qualification/FIXTURE.md §10 predeclared: four times as many reads, each a
// quarter the size, over identical extents — a request-shape change of the kind
// teams actually make, rather than an injected delay.
//
// A source operation that transferred less than it requested is split over the
// bytes it actually moved, and each produced read requests exactly what it will
// receive. The source's short read therefore becomes full reads covering the
// same extent: the extent is what §10 requires be identical, and asking for
// bytes the file does not have would make the transformation demand a failure
// rather than describe a workload.
//
// Operations other than READ and GET pass through untouched.
func SplitReads(ops []trace.Op, block int64) ([]trace.Op, error) {
	if block <= 0 {
		return nil, fmt.Errorf("transform: split-reads: block must be > 0, got %d", block)
	}

	out := make([]trace.Op, 0, len(ops))
	for _, op := range ops {
		pieces, err := splitOne(op, block)
		if err != nil {
			return nil, err
		}
		out = append(out, pieces...)
	}
	// Operation IDs identified positions in the source trace and no longer do.
	// Renumbering keeps them dense and ordered; leaving the originals would
	// duplicate every id across the pieces it was split into.
	for i := range out {
		id := int64(i)
		out[i].OpID = &id
	}
	return out, nil
}

// splitOne returns the operations op becomes.
func splitOne(op trace.Op, block int64) ([]trace.Op, error) {
	if op.Op != trace.OpRead && op.Op != trace.OpGet {
		return []trace.Op{op}, nil
	}
	moved, ok := op.TransferredBytes()
	if !ok || moved <= 0 {
		// A read with no length, or one that moved nothing, has no extent to
		// divide; splitting it would invent operations the source never issued.
		return []trace.Op{op}, nil
	}
	if op.Off == nil {
		return nil, fmt.Errorf("transform: split-reads: %s at t=%d has no offset to split from", op.Op, op.T)
	}
	if moved <= block {
		// Already at or below the target size. Return it unchanged rather than
		// rewriting it into an identical single piece, so a trace that needed no
		// splitting stays byte-identical.
		return []trace.Op{op}, nil
	}

	var pieces []trace.Op
	for covered := int64(0); covered < moved; covered += block {
		size := block
		if rem := moved - covered; rem < size {
			size = rem
		}
		piece := op
		piece.Off = trace.Ptr(*op.Off + covered)
		piece.Len = trace.Ptr(size)
		// Each piece requests exactly what it receives, so none of them is a
		// partial transfer.
		piece.Ret = nil
		// The source duration described one larger operation and cannot be
		// divided among its pieces without inventing a distribution.
		piece.Dur = nil
		pieces = append(pieces, piece)
	}
	return pieces, nil
}

// SplitReadsHeader returns hdr updated for a split-reads output: its summary
// recomputed from ops and the transformation appended to its ledger.
//
// sourceDigest ties the result to the exact trace it was derived from; without
// it the output is merely a different trace, and a comparison cannot tell a
// declared transformation from an unrelated workload.
func SplitReadsHeader(hdr trace.Header, ops []trace.Op, block int64, sourceDigest string) trace.Header {
	hdr.Transformations = append(hdr.Transformations, trace.Transformation{
		Kind:         trace.TransformSplitReads,
		Params:       map[string]string{"block": fmt.Sprint(block)},
		SourceDigest: sourceDigest,
		AppliedUTC:   time.Now().UTC().Format(time.RFC3339),
		Note: fmt.Sprintf("READ/GET operations larger than %d bytes were divided into "+
			"requests of at most that size over identical extents", block),
	})
	hdr.Summary = recomputeSummary(hdr.Summary, ops)
	hdr.Version = headerVersion(hdr, ops)
	return hdr
}

// headerVersion returns the version the transformed trace must declare: the
// higher of what its ops need and what its header needs.
func headerVersion(hdr trace.Header, ops []trace.Op) int {
	v := trace.MinVersionForOps(ops)
	if hv := trace.MinVersionForHeader(hdr); hv > v {
		v = hv
	}
	return v
}

// recomputeSummary rebuilds the counts that the split changed. Stream count,
// duration, and total bytes are preserved by construction, but recomputing
// rather than adjusting is what keeps the header honest if that ever stops
// being true.
func recomputeSummary(s trace.Summary, ops []trace.Op) trace.Summary {
	var totalBytes, partial int64
	streams := make(map[int64]struct{})
	for _, op := range ops {
		streams[op.S] = struct{}{}
		if moved, ok := op.TransferredBytes(); ok && countsBytes(op.Op) {
			totalBytes += moved
		}
		if op.IsPartialTransfer() {
			partial++
		}
	}
	s.NumOps = int64(len(ops))
	s.NumStreams = len(streams)
	s.TotalBytes = totalBytes
	s.NumPartialReads = partial
	return s
}

// countsBytes reports whether an op kind moves payload bytes, matching the
// importer's accounting so a transformed trace's total_bytes stays comparable
// with the trace it came from.
func countsBytes(k trace.OpKind) bool {
	switch k {
	case trace.OpRead, trace.OpWrite, trace.OpGet, trace.OpPut:
		return true
	}
	return false
}

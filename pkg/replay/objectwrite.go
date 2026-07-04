package replay

import (
	"context"
	"fmt"
	"io"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// objectWriteEligibility tracks, during PREPARE's single streaming pass, which
// write handles qualify for object-level coalesced-PUT replay against an
// engine that reports ObjectAPI=true, PartialWrite=false (S3-shaped). A
// handle's OPEN → WRITE* → [FSYNC] → CLOSE lifecycle qualifies only if every
// WRITE is the next sequential, non-overlapping byte range and the total
// coverage matches the target's declared size exactly. Any other pattern
// (read-write handles, out-of-order or overlapping offsets, partial coverage)
// is reported as an error the moment it's detected, so Prepare fails with a
// specific reason instead of silently mishandling an unreplayable pattern.
type objectWriteEligibility struct {
	handles map[int64]owHandle // open handle -> tracking state
	sizes   map[int]int64      // eligible target id -> final coalesced size
}

type owHandle struct {
	target  int
	mode    trace.Mode
	nextOff int64
}

func newObjectWriteEligibility() *objectWriteEligibility {
	return &objectWriteEligibility{
		handles: make(map[int64]owHandle),
		sizes:   make(map[int]int64),
	}
}

// observe processes one op against the (post target-map) target table,
// returning an error the instant a write pattern is found ineligible.
func (e *objectWriteEligibility) observe(op trace.Op, targets []trace.TargetInfo) error {
	switch op.Op {
	case trace.OpOpen:
		if op.H == nil || op.Tgt == nil {
			return nil
		}
		e.handles[*op.H] = owHandle{target: *op.Tgt, mode: op.Mode}

	case trace.OpWrite:
		if op.H == nil {
			return nil
		}
		st, ok := e.handles[*op.H]
		if !ok {
			return nil // unknown handle; the generic validator already flags this
		}
		if st.mode != trace.ModeWrite {
			return fmt.Errorf("target %q: opened with mode %q but object-store engines only "+
				"support object-level replay for write-only (mode=w) handles — read-write or "+
				"in-place-update patterns cannot be coalesced into a single PUT",
				targetName(targets, st.target), st.mode)
		}
		if op.Off == nil || op.Len == nil {
			return nil // structurally invalid; the generic validator already flags this
		}
		if *op.Off != st.nextOff {
			return fmt.Errorf("target %q: WRITE at offset %d is not the next sequential byte "+
				"(expected %d) — object-level replay requires full-coverage sequential writes; "+
				"partial, overlapping, or out-of-order rewrites are not eligible",
				targetName(targets, st.target), *op.Off, st.nextOff)
		}
		st.nextOff += *op.Len
		e.handles[*op.H] = st

	case trace.OpClose:
		if op.H == nil {
			return nil
		}
		st, ok := e.handles[*op.H]
		if !ok {
			return nil
		}
		delete(e.handles, *op.H)
		if st.mode != trace.ModeWrite {
			return nil // nothing was coalesced for this handle
		}
		if st.target < 0 || st.target >= len(targets) {
			return nil // out-of-range tgt; the generic validator already flags this
		}
		if size := targets[st.target].Size; size > 0 && st.nextOff != size {
			return fmt.Errorf("target %q: coalesced WRITEs covered %d bytes but the target "+
				"declares size %d — partial coverage is not eligible for object-level replay",
				targets[st.target].Name, st.nextOff, size)
		}
		e.sizes[st.target] = st.nextOff
	}
	return nil
}

func targetName(targets []trace.TargetInfo, idx int) string {
	if idx < 0 || idx >= len(targets) {
		return "?"
	}
	return targets[idx].Name
}

// objectPipe streams one coalesced WRITE lifecycle into a single engine.Put
// call issued as soon as the handle opens, so the object is never buffered
// whole in memory: each WRITE blocks on the unbuffered pipe until Put's
// reader (or its multipart uploader) consumes it.
type objectPipe struct {
	pw   *io.PipeWriter
	done chan error
}

// startObjectPut launches the Put call in the background and returns the pipe
// WRITEs feed. If Put returns before fully draining its reader (e.g. an early
// network error), the read side is closed with that error so any WRITE
// blocked on (or issued after) the failure returns it immediately instead of
// deadlocking.
func startObjectPut(ctx context.Context, eng engine.Engine, key string, size int64) *objectPipe {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := eng.Put(ctx, key, pr, size)
		pr.CloseWithError(err)
		done <- err
	}()
	return &objectPipe{pw: pw, done: done}
}

func (p *objectPipe) write(b []byte) (int, error) {
	return p.pw.Write(b)
}

// finish closes the write side (signaling EOF to Put's reader when Put hasn't
// already failed) and waits for Put to return. Call exactly once, from CLOSE.
func (p *objectPipe) finish() error {
	closeErr := p.pw.Close()
	if putErr := <-p.done; putErr != nil {
		return putErr
	}
	return closeErr
}

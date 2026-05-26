package replay

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// runASAP executes all streams concurrently in asap (closed-loop) mode: each
// stream's next op is issued as soon as the previous one completes, ignoring
// the trace's 't' timestamps. One goroutine per stream. Returns after all
// goroutines finish.
func runASAP(
	ctx context.Context,
	byStream map[int64][]trace.Op,
	eng engine.Engine,
	hdr trace.Header,
	planInfo results.PlanInfo,
) (*results.Results, error) {
	type streamResult struct {
		rec *metrics.Recorder
	}

	resultsCh := make(chan streamResult, len(byStream))
	start := time.Now()

	var wg sync.WaitGroup
	for _, ops := range byStream {
		wg.Add(1)
		go func(ops []trace.Op) {
			defer wg.Done()
			rec := metrics.NewRecorder()
			replayStream(ctx, ops, eng, hdr, rec)
			resultsCh <- streamResult{rec: rec}
		}(ops)
	}

	wg.Wait()
	close(resultsCh)

	durationNS := time.Since(start).Nanoseconds()

	merged := metrics.NewRecorder()
	for sr := range resultsCh {
		merged.Merge(sr.rec)
	}

	return results.Build(planInfo, merged, durationNS), nil
}

// replayStream executes one stream's ops sequentially against eng, recording
// latency into rec. Strict stream sequentiality is enforced by the for-loop:
// op N+1 is never issued before op N returns. This is the correctness property
// from PRD §8.5: a stream has at most 1 in-flight op at any instant.
func replayStream(
	ctx context.Context,
	ops []trace.Op,
	eng engine.Engine,
	hdr trace.Header,
	rec *metrics.Recorder,
) {
	// handleMap translates trace handle IDs to engine Handles for this stream.
	handleMap := make(map[int64]engine.Handle)
	// buf is reused across ops in this stream; grown on demand.
	buf := make([]byte, 64*1024)

	for _, op := range ops {
		if ctx.Err() != nil {
			return
		}

		var bytesN int64
		var opErr error
		start := time.Now()

		switch op.Op {
		case trace.OpOpen:
			name := hdr.Targets[*op.Tgt].Name
			mode := engine.Mode(op.Mode)
			flags := parseOpenFlags(op.Flags)
			h, err := eng.Open(ctx, name, mode, flags)
			if err == nil {
				handleMap[*op.H] = h
			}
			opErr = err

		case trace.OpRead:
			h := handleMap[*op.H]
			off, length := *op.Off, *op.Len
			buf = growBuf(buf, length)
			n, err := eng.Read(ctx, h, off, length, buf[:length])
			bytesN = int64(n)
			if errors.Is(err, engine.ErrShortRead) {
				err = nil // short reads at EOF are expected
			}
			opErr = err

		case trace.OpWrite:
			h := handleMap[*op.H]
			off, length := *op.Off, *op.Len
			buf = growBuf(buf, length)
			n, err := eng.Write(ctx, h, off, buf[:length])
			bytesN = int64(n)
			opErr = err

		case trace.OpFsync:
			h := handleMap[*op.H]
			opErr = eng.Fsync(ctx, h)

		case trace.OpClose:
			h := handleMap[*op.H]
			opErr = eng.Close(ctx, h)
			if opErr == nil {
				delete(handleMap, *op.H)
			}

		case trace.OpStat:
			name := hdr.Targets[*op.Tgt].Name
			_, opErr = eng.Stat(ctx, name)

		case trace.OpPut:
			key := hdr.Targets[*op.Tgt].Name
			length := *op.Len
			buf = growBuf(buf, length)
			opErr = eng.Put(ctx, key, bytes.NewReader(buf[:length]), length)

		case trace.OpGet:
			key := hdr.Targets[*op.Tgt].Name
			off, length := *op.Off, *op.Len
			buf = growBuf(buf, length)
			n, err := eng.Get(ctx, key, off, length, buf[:length])
			bytesN = int64(n)
			opErr = err

		case trace.OpHead:
			key := hdr.Targets[*op.Tgt].Name
			_, opErr = eng.Head(ctx, key)

		case trace.OpDelete:
			key := hdr.Targets[*op.Tgt].Name
			opErr = eng.Delete(ctx, key)
		}

		elapsed := time.Since(start)
		rec.Record(op.Op, elapsed.Nanoseconds(), bytesN, opErr != nil)
	}
}

// parseOpenFlags translates the string flag list from a trace OPEN op into the
// engine.OpenFlags bitmask. Unrecognized flag strings are silently ignored, per
// PRD §7.2: "Unmodeled flags are ignored by replay but preserved in the IR."
func parseOpenFlags(flags []string) engine.OpenFlags {
	var f engine.OpenFlags
	for _, s := range flags {
		switch s {
		case "direct":
			f |= engine.OpenFlagDirect
		case "seq":
			f |= engine.OpenFlagSeq
		case "rand":
			f |= engine.OpenFlagRand
		case "sync":
			f |= engine.OpenFlagSync
		case "append":
			f |= engine.OpenFlagAppend
		case "create":
			f |= engine.OpenFlagCreate
		case "trunc":
			f |= engine.OpenFlagTrunc
		}
	}
	return f
}

// growBuf returns buf if len(buf) >= need, otherwise a new slice of size need.
func growBuf(buf []byte, need int64) []byte {
	if int64(len(buf)) >= need {
		return buf
	}
	return make([]byte, need)
}

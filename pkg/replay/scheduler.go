package replay

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// SchedulerOpts configures a schedule call.
type SchedulerOpts struct {
	// Mode is "asap", "timeline", or "scaled".
	Mode string

	// MaxInflight is the worker-level maximum concurrent in-flight ops across
	// all streams. 0 defaults to 512.
	MaxInflight int

	// SpeedupFactor is only used in "scaled" mode. 0 or negative = no scaling.
	SpeedupFactor float64

	// RunStart is the logical T=0 for timeline/scaled modes.
	RunStart time.Time

	// PlanInfo is echoed into the returned Results.
	PlanInfo results.PlanInfo

	// RunEnv records cache-state metadata applied before the run.
	RunEnv results.RunEnv
}

// schedule runs all streams with strict per-stream ordering, intended-arrival
// latency accounting, and a shared in-flight cap.
func schedule(ctx context.Context, byStream map[int64][]trace.Op, eng engine.Engine, hdr trace.Header, opts SchedulerOpts) (*results.Results, error) {
	if opts.MaxInflight <= 0 {
		opts.MaxInflight = 512
	}
	sem := make(chan struct{}, opts.MaxInflight)

	runStart := opts.RunStart
	if runStart.IsZero() {
		runStart = time.Now()
	}

	speedup := opts.SpeedupFactor
	if speedup <= 0 {
		speedup = 1
	}

	var (
		currentInflight  atomic.Int64
		maxInflightDepth atomic.Int64
		backlogEvents    atomic.Int64
		backlogBlockedNS atomic.Int64
	)

	type streamResult struct{ rec *metrics.Recorder }
	resultsCh := make(chan streamResult, len(byStream))
	wallStart := time.Now()

	isTimeline := opts.Mode == "timeline" || opts.Mode == "scaled"

	var wg sync.WaitGroup
	for _, ops := range byStream {
		wg.Add(1)
		go func(streamOps []trace.Op) {
			defer wg.Done()
			rec := metrics.NewRecorder()
			handleMap := make(map[int64]engine.Handle)
			buf := make([]byte, 64*1024)

			for _, op := range streamOps {
				if ctx.Err() != nil {
					resultsCh <- streamResult{rec: rec}
					return
				}

				var intendedArrival time.Time
				if isTimeline {
					intendedArrival = runStart.Add(time.Duration(float64(op.T) / speedup))
					if wait := time.Until(intendedArrival); wait > 0 {
						select {
						case <-time.After(wait):
						case <-ctx.Done():
							resultsCh <- streamResult{rec: rec}
							return
						}
					}
				}

				// A non-blocking send separates available capacity from true backlog.
				select {
				case sem <- struct{}{}:
				default:
					waitStart := time.Now()
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						resultsCh <- streamResult{rec: rec}
						return
					}
					waited := time.Since(waitStart).Nanoseconds()
					backlogBlockedNS.Add(waited)
					backlogEvents.Add(1)
				}

				cur := currentInflight.Add(1)
				for {
					old := maxInflightDepth.Load()
					if cur <= old {
						break
					}
					if maxInflightDepth.CompareAndSwap(old, cur) {
						break
					}
				}

				serviceStart := time.Now()
				if isTimeline {
					driftNS := serviceStart.Sub(intendedArrival).Nanoseconds()
					if driftNS < 0 {
						driftNS = 0
					}
					rec.RecordDrift(driftNS)
				}

				bytesN, opErr := dispatchOp(ctx, op, eng, hdr, handleMap, &buf)

				// In timeline/scaled, latency must capture the full backlog (PRD §8.5
				// coordinated-omission rule). In asap, latency is pure service time —
				// semaphore-wait time is not credited to the op.
				var latencyNS int64
				if isTimeline {
					latencyNS = time.Since(intendedArrival).Nanoseconds()
				} else {
					latencyNS = time.Since(serviceStart).Nanoseconds()
				}
				if latencyNS < 0 {
					latencyNS = 0
				}
				rec.Record(op.Op, latencyNS, bytesN, opErr != nil)

				currentInflight.Add(-1)
				<-sem
			}

			resultsCh <- streamResult{rec: rec}
		}(ops)
	}

	wg.Wait()
	close(resultsCh)

	durationNS := time.Since(wallStart).Nanoseconds()

	merged := metrics.NewRecorder()
	for sr := range resultsCh {
		merged.Merge(sr.rec)
	}
	merged.BacklogEvents = backlogEvents.Load()
	merged.BacklogBlockedNS = backlogBlockedNS.Load()
	merged.MaxInflightDepth = maxInflightDepth.Load()

	res := results.Build(opts.PlanInfo, opts.RunEnv, merged, durationNS)
	if err := ctx.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// dispatchOp executes a single op against eng and returns bytes moved and
// any error. It is engine-agnostic: the scheduler controls when to call it.
// handleMap is per-stream (translate trace h → engine Handle). bufp points to
// the stream's reused I/O buffer, grown on demand.
func dispatchOp(
	ctx context.Context,
	op trace.Op,
	eng engine.Engine,
	hdr trace.Header,
	handleMap map[int64]engine.Handle,
	bufp *[]byte,
) (bytesN int64, opErr error) {
	buf := *bufp
	defer func() { *bufp = buf }()

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
			err = nil
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
		opErr = eng.Fsync(ctx, handleMap[*op.H])

	case trace.OpClose:
		h := handleMap[*op.H]
		opErr = eng.Close(ctx, h)
		if opErr == nil {
			delete(handleMap, *op.H)
		}

	case trace.OpStat:
		_, opErr = eng.Stat(ctx, hdr.Targets[*op.Tgt].Name)

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
		_, opErr = eng.Head(ctx, hdr.Targets[*op.Tgt].Name)

	case trace.OpDelete:
		opErr = eng.Delete(ctx, hdr.Targets[*op.Tgt].Name)
	}

	return bytesN, opErr
}

// parseOpenFlags translates trace OPEN flags into engine flags.
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

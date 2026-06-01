package replay

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chanuollala/ioflux/pkg/cpustat"
	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/fidelity"
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

	type streamResult struct {
		sid int64
		rec *metrics.Recorder
	}
	resultsCh := make(chan streamResult, len(byStream))
	wallStart := time.Now()
	cpuStart := cpustat.Now()

	isTimeline := opts.Mode == "timeline" || opts.Mode == "scaled"

	var wg sync.WaitGroup
	for sid, ops := range byStream {
		wg.Add(1)
		go func(sid int64, streamOps []trace.Op) {
			defer wg.Done()
			rec := metrics.NewRecorder()
			handleMap := make(map[int64]engine.Handle)
			buf := make([]byte, 64*1024)
			var streamInflight int64

			for _, op := range streamOps {
				if ctx.Err() != nil {
					resultsCh <- streamResult{sid: sid, rec: rec}
					return
				}

				var intendedArrival time.Time
				if isTimeline {
					intendedArrival = runStart.Add(time.Duration(float64(op.T) / speedup))
					if wait := time.Until(intendedArrival); wait > 0 {
						select {
						case <-time.After(wait):
						case <-ctx.Done():
							resultsCh <- streamResult{sid: sid, rec: rec}
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
						resultsCh <- streamResult{sid: sid, rec: rec}
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

				// Track per-stream concurrency. Sequential streams stay at ≤1.
				streamInflight++
				if streamInflight > rec.PeakInflight {
					rec.PeakInflight = streamInflight
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
					rec.RecordCompletionLag(latencyNS)
				} else {
					latencyNS = time.Since(serviceStart).Nanoseconds()
				}
				if latencyNS < 0 {
					latencyNS = 0
				}
				rec.Record(op.Op, latencyNS, bytesN, opErr != nil)

				streamInflight--
				currentInflight.Add(-1)
				<-sem
			}

			resultsCh <- streamResult{sid: sid, rec: rec}
		}(sid, ops)
	}

	wg.Wait()
	close(resultsCh)

	durationNS := time.Since(wallStart).Nanoseconds()
	cpuDelta := cpustat.Now().Sub(cpuStart)

	merged := metrics.NewRecorder()
	peakByStream := make(map[int64]int64, len(byStream))
	for sr := range resultsCh {
		merged.Merge(sr.rec)
		peakByStream[sr.sid] = sr.rec.PeakInflight
	}
	merged.BacklogEvents = backlogEvents.Load()
	merged.BacklogBlockedNS = backlogBlockedNS.Load()
	merged.MaxInflightDepth = maxInflightDepth.Load()

	// Count actual ops from the loaded stream map; hdr.Summary.NumOps is advisory.
	var actualNumOps int64
	for _, streamOps := range byStream {
		actualNumOps += int64(len(streamOps))
	}

	// Mean inter-arrival: trace duration / actual ops, divided by speedup in
	// scaled mode so the threshold tracks the compressed real-time cadence.
	var meanInterArrivalNS int64
	if actualNumOps > 0 && hdr.Summary.DurationNS > 0 {
		meanInterArrivalNS = hdr.Summary.DurationNS / actualNumOps
		if opts.Mode == "scaled" {
			meanInterArrivalNS = int64(float64(meanInterArrivalNS) / speedup)
		}
	}

	// Use actual op count for coverage; stale advisory header must not hide skips.
	correctedHdr := hdr
	correctedHdr.Summary.NumOps = actualNumOps

	res := results.Build(opts.PlanInfo, opts.RunEnv, merged, durationNS)
	// WallNS comes from durationNS (Go monotonic) — never from cpustat, whose
	// Sample is rusage only. This keeps CPU.WallNS == DurationNS by construction.
	res.CPU = results.CPU{UserNS: cpuDelta.UserNS, SysNS: cpuDelta.SysNS, WallNS: durationNS}
	res.Fidelity = fidelity.Build(merged, correctedHdr, opts.Mode, meanInterArrivalNS, peakByStream)
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

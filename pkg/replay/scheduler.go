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

	// Progress, when non-nil, is called every ProgressInterval with the cumulative
	// ops completed and bytes moved so far. The distributed worker uses it to
	// stream live Progress to the coordinator. Nil disables progress reporting.
	Progress func(ops, bytes int64)

	// ProgressInterval is the cadence for Progress callbacks. 0 defaults to 1s.
	ProgressInterval time.Duration
}

// WorkerOutput is the raw result of replaying one worker's assigned streams.
// The coordinator merges WorkerOutputs from all workers before building the
// final Results, so distributed percentiles come from a single lossless
// histogram merge rather than averaged per-host numbers. For a single-node run
// there is exactly one WorkerOutput.
type WorkerOutput struct {
	// Hostname identifies the worker; set by the coordinator (empty single-node).
	Hostname     string
	Recorder     *metrics.Recorder
	PeakByStream map[int64]int64
	CPU          results.CPU
	// ActualNumOps is the number of ops this worker loaded and replayed.
	ActualNumOps int64
	// FirstDoneNS/LastDoneNS are this worker's earliest/latest stream completion
	// times, relative to its run start.
	FirstDoneNS int64
	LastDoneNS  int64
}

// schedule runs all streams of a single (in-process) worker and builds the
// final Results. It is the single-node path: runStreams produces the raw
// per-worker output and buildResults aggregates the one-element slice, so a
// single-node run and a one-worker distributed run go through identical code.
func schedule(ctx context.Context, byStream map[int64][]trace.Op, eng engine.Engine, hdr trace.Header, opts SchedulerOpts) (*results.Results, error) {
	out, runErr := runStreams(ctx, byStream, eng, hdr, opts)
	return BuildResults([]*WorkerOutput{out}, opts, hdr, 0), runErr
}

// runStreams replays byStream with strict per-stream ordering, intended-arrival
// latency accounting, and a shared in-flight cap, returning the raw per-worker
// recorder and timing. It does not build Results; buildResults does that over
// one or more WorkerOutputs.
func runStreams(ctx context.Context, byStream map[int64][]trace.Op, eng engine.Engine, hdr trace.Header, opts SchedulerOpts) (*WorkerOutput, error) {
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
		// Live cumulative totals, read by the progress ticker (atomics, not the
		// per-stream recorders which are only merged after the run).
		opsDone   atomic.Int64
		bytesDone atomic.Int64
	)

	type streamResult struct {
		sid          int64
		rec          *metrics.Recorder
		completionNS int64
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
			// Report this stream's recorder and completion time exactly once,
			// however the goroutine exits (finished or ctx-cancelled mid-stream).
			defer func() {
				resultsCh <- streamResult{sid: sid, rec: rec, completionNS: time.Since(wallStart).Nanoseconds()}
			}()
			handleMap := make(map[int64]engine.Handle)
			buf := make([]byte, 64*1024)
			var streamInflight int64

			for _, op := range streamOps {
				if ctx.Err() != nil {
					return
				}

				var intendedArrival time.Time
				if isTimeline {
					intendedArrival = runStart.Add(time.Duration(float64(op.T) / speedup))
					if wait := time.Until(intendedArrival); wait > 0 {
						select {
						case <-time.After(wait):
						case <-ctx.Done():
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

				// In timeline/scaled, latency must capture the full backlog
				// (coordinated-omission: include the time spent waiting for the
				// semaphore). In asap, latency is pure service time.
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
				opsDone.Add(1)
				bytesDone.Add(bytesN)

				streamInflight--
				currentInflight.Add(-1)
				<-sem
			}
		}(sid, ops)
	}

	// Stream cumulative progress on a ticker while the run is in flight. The
	// progressWG ensures no callback fires after runStreams returns.
	var progressWG sync.WaitGroup
	stopProgress := make(chan struct{})
	if opts.Progress != nil {
		interval := opts.ProgressInterval
		if interval <= 0 {
			interval = time.Second
		}
		progressWG.Add(1)
		go func() {
			defer progressWG.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					opts.Progress(opsDone.Load(), bytesDone.Load())
				case <-stopProgress:
					return
				}
			}
		}()
	}

	wg.Wait()
	close(stopProgress)
	progressWG.Wait()
	close(resultsCh)

	durationNS := time.Since(wallStart).Nanoseconds()
	cpuDelta := cpustat.Now().Sub(cpuStart)

	merged := metrics.NewRecorder()
	peakByStream := make(map[int64]int64, len(byStream))
	var firstDoneNS, lastDoneNS int64
	haveDone := false
	for sr := range resultsCh {
		merged.Merge(sr.rec)
		peakByStream[sr.sid] = sr.rec.PeakInflight
		if !haveDone || sr.completionNS < firstDoneNS {
			firstDoneNS = sr.completionNS
		}
		if sr.completionNS > lastDoneNS {
			lastDoneNS = sr.completionNS
		}
		haveDone = true
	}
	merged.BacklogEvents = backlogEvents.Load()
	merged.BacklogBlockedNS = backlogBlockedNS.Load()
	merged.MaxInflightDepth = maxInflightDepth.Load()

	var actualNumOps int64
	for _, streamOps := range byStream {
		actualNumOps += int64(len(streamOps))
	}

	out := &WorkerOutput{
		Recorder:     merged,
		PeakByStream: peakByStream,
		// WallNS comes from durationNS (Go monotonic), never from cpustat, whose
		// Sample is rusage only; this keeps CPU.WallNS == DurationNS by construction.
		CPU:          results.CPU{UserNS: cpuDelta.UserNS, SysNS: cpuDelta.SysNS, WallNS: durationNS},
		ActualNumOps: actualNumOps,
		FirstDoneNS:  firstDoneNS,
		LastDoneNS:   lastDoneNS,
	}
	return out, ctx.Err()
}

// BuildResults merges one or more WorkerOutputs into the final Results. For a
// single-node run outs has one element; the distributed coordinator passes one
// WorkerOutput per worker. Histograms merge losslessly, counters sum, CPU
// user/sys sum (wall is the max worker wall), and the per-host breakdown plus
// straggler window are added only for multi-host runs. goSkewNS is the measured
// Go-delivery skew across workers (0 single-node).
func BuildResults(outs []*WorkerOutput, opts SchedulerOpts, hdr trace.Header, goSkewNS int64) *results.Results {
	merged := metrics.NewRecorder()
	peakByStream := make(map[int64]int64)
	var cpu results.CPU
	var totalActualOps, wallNS int64
	for _, o := range outs {
		merged.Merge(o.Recorder)
		for sid, p := range o.PeakByStream {
			if p > peakByStream[sid] {
				peakByStream[sid] = p
			}
		}
		totalActualOps += o.ActualNumOps
		cpu.UserNS += o.CPU.UserNS
		cpu.SysNS += o.CPU.SysNS
		if o.CPU.WallNS > wallNS {
			wallNS = o.CPU.WallNS
		}
	}
	cpu.WallNS = wallNS

	speedup := opts.SpeedupFactor
	if speedup <= 0 {
		speedup = 1
	}
	// Mean inter-arrival: trace duration / actual ops, divided by speedup in
	// scaled mode so the threshold tracks the compressed real-time cadence.
	var meanInterArrivalNS int64
	if totalActualOps > 0 && hdr.Summary.DurationNS > 0 {
		meanInterArrivalNS = hdr.Summary.DurationNS / totalActualOps
		if opts.Mode == "scaled" {
			meanInterArrivalNS = int64(float64(meanInterArrivalNS) / speedup)
		}
	}

	// Use actual op count for coverage; a stale advisory header must not hide skips.
	correctedHdr := hdr
	correctedHdr.Summary.NumOps = totalActualOps

	res := results.Build(opts.PlanInfo, opts.RunEnv, merged, wallNS)
	res.CPU = cpu
	res.Fidelity = fidelity.Build(merged, correctedHdr, opts.Mode, meanInterArrivalNS, peakByStream)

	// Per-host breakdown and straggler window are meaningful only across workers.
	if len(outs) > 1 {
		res.GoDeliverySkewNS = goSkewNS
		res.Hosts = make([]results.HostResult, 0, len(outs))
		for _, o := range outs {
			res.Hosts = append(res.Hosts, results.HostResult{
				Hostname:     o.Hostname,
				OpsCompleted: o.Recorder.TotalOps(),
				BytesMoved:   o.Recorder.Bytes,
				CPU:          o.CPU,
				FirstDoneNS:  o.FirstDoneNS,
				LastDoneNS:   o.LastDoneNS,
			})
		}
		res.Straggler = buildStraggler(outs, res.OpsCompleted, res.BytesMoved)
	}
	return res
}

// buildStraggler computes the completion-skew window across workers. Idle workers
// (no assigned streams → LastDoneNS == 0) are excluded so they cannot collapse
// the first-done time to zero.
//
// first-done = earliest worker completion, last-done = latest. Last-done
// throughput is all work over the full window. First-done throughput is the
// aggregate rate *up to* the earliest worker completion — the rate while every
// worker was still busy, which excludes the straggler tail (PRD §8.7). Since the
// per-worker outputs do not carry per-instant op counts, each still-running
// worker's work-completed-by-first-done is estimated from its uniform average
// rate (ops · firstDone/lastDone); the earliest worker contributes all its ops.
// This is deliberately *not* totalOps/firstDone, which would credit the whole
// run's work to the shorter window and overstate first-done throughput.
func buildStraggler(outs []*WorkerOutput, totalOps, totalBytes int64) *results.StragglerWindow {
	var firstDoneNS, lastDoneNS int64
	haveFirst := false
	for _, o := range outs {
		if o.LastDoneNS <= 0 {
			continue // idle worker
		}
		if !haveFirst || o.LastDoneNS < firstDoneNS {
			firstDoneNS = o.LastDoneNS
		}
		if o.LastDoneNS > lastDoneNS {
			lastDoneNS = o.LastDoneNS
		}
		haveFirst = true
	}

	sw := &results.StragglerWindow{
		FirstDoneNS: firstDoneNS,
		LastDoneNS:  lastDoneNS,
		SkewNS:      lastDoneNS - firstDoneNS,
	}
	if firstDoneNS > 0 {
		var opsByFirst, bytesByFirst float64
		for _, o := range outs {
			if o.LastDoneNS <= 0 {
				continue
			}
			frac := float64(firstDoneNS) / float64(o.LastDoneNS)
			if frac > 1 {
				frac = 1
			}
			opsByFirst += float64(o.Recorder.TotalOps()) * frac
			bytesByFirst += float64(o.Recorder.Bytes) * frac
		}
		secs := float64(firstDoneNS) / 1e9
		sw.FirstDoneOpsPerSec = opsByFirst / secs
		sw.FirstDoneGiBPerSec = bytesByFirst / float64(1<<30) / secs
	}
	if lastDoneNS > 0 {
		secs := float64(lastDoneNS) / 1e9
		sw.LastDoneOpsPerSec = float64(totalOps) / secs
		sw.LastDoneGiBPerSec = float64(totalBytes) / float64(1<<30) / secs
	}
	return sw
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

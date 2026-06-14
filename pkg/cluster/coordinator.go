package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/chanuollala/ioflux/pkg/payload"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// Worker is the coordinator's handle to one replay worker. localWorker drives an
// in-process Session directly; remoteWorker drives a gRPC client. The coordinator
// depends only on this interface, so single-node and distributed runs share the
// PREPARE/RUN/DONE/COLLECT logic — only the transport differs.
type Worker interface {
	// Register returns the worker's identity (hostname, cpus, version).
	Register(ctx context.Context) (WorkerInfo, error)
	// Prepare loads+validates the trace, builds the engine, runs dataset prep,
	// and applies cache controls. Returning nil error is the PREPARE barrier ack.
	Prepare(ctx context.Context, p Plan) (PrepareResult, error)
	// Run replays the assigned streams, scheduling timeline arrivals from goTime.
	// progress, when non-nil, receives cumulative ops/bytes for live display.
	// It returns when the worker has finished all streams (the DONE signal) or
	// the run is cancelled/fails.
	Run(ctx context.Context, goTime time.Time, progress func(ops, bytes int64)) error
	// Collect returns the worker's raw output for the coordinator to merge.
	Collect(ctx context.Context) (*replay.WorkerOutput, error)
	// Close releases transport resources (no-op for localWorker).
	Close() error
}

// localWorker drives an in-process Session. It is the worker used for single-node
// runs and for hermetic in-process distributed tests.
type localWorker struct{ s *Session }

// NewLocalWorker wraps an in-process Session as a Worker.
func NewLocalWorker(s *Session) Worker { return &localWorker{s: s} }

func (w *localWorker) Register(context.Context) (WorkerInfo, error) { return w.s.Info(), nil }
func (w *localWorker) Prepare(ctx context.Context, p Plan) (PrepareResult, error) {
	return w.s.Prepare(ctx, p)
}
func (w *localWorker) Run(ctx context.Context, goTime time.Time, progress func(ops, bytes int64)) error {
	return w.s.Run(ctx, goTime, progress)
}
func (w *localWorker) Collect(ctx context.Context) (*replay.WorkerOutput, error) {
	return w.s.Collect()
}
func (w *localWorker) Close() error { return nil }

// Coordinator owns one run: it partitions a trace's streams across workers,
// synchronizes them through PREPARE/RUN/DONE barriers, and merges their outputs
// into a single honest Results.
type Coordinator struct {
	// Progress, when set, is called from each worker's progress callback with the
	// worker hostname and cumulative ops/bytes. It may be called concurrently from
	// multiple workers, so it must be safe for concurrent use.
	Progress func(host string, ops, bytes int64)
	// Logf, when set, receives human-readable warnings (idle workers, empty trace).
	Logf func(format string, args ...any)
}

func (c *Coordinator) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

// Run drives the full phase protocol over workers and returns the merged Results.
// Per PRD §8.9 v1 failure policy, any worker error during RUN cancels the rest
// and aborts the run with no results.
func (c *Coordinator) Run(ctx context.Context, plan Plan, workers []Worker) (*results.Results, error) {
	if len(workers) == 0 {
		return nil, errors.New("cluster: coordinator: no workers")
	}

	hdr, streamIDs, err := parseTraceStreams(plan.TraceBytes)
	if err != nil {
		return nil, fmt.Errorf("cluster: coordinator: parse trace: %w", err)
	}

	// 1. REGISTER — reject version mismatch or unreachable workers before any work.
	infos := make([]WorkerInfo, len(workers))
	for i, w := range workers {
		info, err := w.Register(ctx)
		if err != nil {
			return nil, fmt.Errorf("cluster: coordinator: register worker %d: %w", i, err)
		}
		if info.Version != Version {
			return nil, fmt.Errorf("cluster: coordinator: worker %d (%s) version %q != coordinator %q",
				i, info.Hostname, info.Version, Version)
		}
		infos[i] = info
	}

	// 2. PARTITION stream IDs round-robin across workers.
	assignments := partitionStreams(streamIDs, len(workers))
	if len(streamIDs) == 0 {
		c.logf("warning: trace has no streams; nothing to replay")
	} else if len(workers) > len(streamIDs) {
		c.logf("warning: %d workers but only %d streams; %d worker(s) will be idle",
			len(workers), len(streamIDs), len(workers)-len(streamIDs))
	}

	// 3. PREPARE — barrier: every worker must ack before any worker runs.
	plan.PrepareScope = ResolvePrepareScope(plan)
	preps, err := c.prepareWorkers(ctx, plan, workers, infos, assignments)
	if err != nil {
		return nil, err
	}

	// 4. RUN — fan out a shared logical T0; cancel all on any failure (DONE barrier
	// is wg.Wait). Each worker schedules timeline arrivals from goTime.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	goTime := time.Now()
	// startDelays[i] is the time from the shared Go reference to worker i's first
	// progress tick — the round-trip for the Go signal to reach that worker and
	// for its first ack to return, measured entirely on the coordinator's clock
	// (no cross-host wall-clock sync needed). Their spread is the Go-delivery skew.
	startDelays := make([]int64, len(workers))
	runErrs := make([]error, len(workers))
	var wg sync.WaitGroup
	for i, w := range workers {
		wg.Add(1)
		go func(i int, w Worker, host string) {
			defer wg.Done()
			var once sync.Once
			progress := func(ops, bytes int64) {
				once.Do(func() { startDelays[i] = time.Since(goTime).Nanoseconds() })
				if c.Progress != nil {
					c.Progress(host, ops, bytes)
				}
			}
			if err := w.Run(runCtx, goTime, progress); err != nil {
				runErrs[i] = err
				cancel() // abort: stop every other worker
			}
		}(i, w, infos[i].Hostname)
	}
	wg.Wait()

	if err := firstRunError(runErrs, infos); err != nil {
		return nil, err
	}

	// 5. COLLECT — merge raw worker outputs into one lossless Results.
	outs := make([]*replay.WorkerOutput, 0, len(workers))
	for i, w := range workers {
		out, err := w.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("cluster: coordinator: collect worker %d (%s): %w", i, infos[i].Hostname, err)
		}
		if out.Hostname == "" {
			out.Hostname = infos[i].Hostname
		}
		outs = append(outs, out)
	}

	opts := schedulerOpts(plan, hdr, preps)
	return replay.BuildResults(outs, opts, hdr, skewNS(startDelays)), nil
}

// firstRunError surfaces the root cause of an aborted run: a worker's own error
// is preferred over the context.Canceled that bystander workers observe once the
// coordinator cancels them.
func firstRunError(errs []error, infos []WorkerInfo) error {
	var canceled error
	for i, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) {
			if canceled == nil {
				canceled = fmt.Errorf("cluster: coordinator: run worker %d (%s): %w", i, infos[i].Hostname, err)
			}
			continue
		}
		return fmt.Errorf("cluster: coordinator: run worker %d (%s): %w", i, infos[i].Hostname, err)
	}
	return canceled
}

func (c *Coordinator) prepareWorkers(
	ctx context.Context,
	plan Plan,
	workers []Worker,
	infos []WorkerInfo,
	assignments [][]int64,
) ([]PrepareResult, error) {
	preps := make([]PrepareResult, len(workers))
	switch plan.PrepareScope {
	case PrepareScopeShared:
		if len(workers) == 0 {
			return preps, nil
		}
		wp := plan
		wp.AssignedStreams = assignments[0]
		pr, err := workers[0].Prepare(ctx, wp)
		if err != nil {
			return nil, fmt.Errorf("cluster: coordinator: prepare worker 0 (%s): %w", infos[0].Hostname, err)
		}
		preps[0] = pr
		if len(workers) == 1 {
			return preps, nil
		}
		verifyPlan := plan
		if verifyPlan.PrepareMode != "" {
			verifyPlan.PrepareMode = "assume-existing"
			verifyPlan.SourceRoot = ""
		}
		verifyPreps, err := prepareWorkerRange(ctx, verifyPlan, workers, infos, assignments, 1)
		if err != nil {
			return nil, err
		}
		copy(preps[1:], verifyPreps[1:])
		return preps, nil
	case PrepareScopePerWorker:
		return prepareWorkerRange(ctx, plan, workers, infos, assignments, 0)
	default:
		return nil, fmt.Errorf("cluster: coordinator: unsupported prepare scope %q", plan.PrepareScope)
	}
}

func prepareWorkerRange(
	ctx context.Context,
	plan Plan,
	workers []Worker,
	infos []WorkerInfo,
	assignments [][]int64,
	start int,
) ([]PrepareResult, error) {
	preps := make([]PrepareResult, len(workers))
	errs := make([]error, len(workers))
	var wg sync.WaitGroup
	for i := start; i < len(workers); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wp := plan
			wp.AssignedStreams = assignments[i]
			pr, err := workers[i].Prepare(ctx, wp)
			if err != nil {
				errs[i] = fmt.Errorf("cluster: coordinator: prepare worker %d (%s): %w", i, infos[i].Hostname, err)
				return
			}
			preps[i] = pr
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return preps, nil
}

// schedulerOpts builds the PlanInfo + RunEnv for BuildResults. Dataset-prep and
// cache metadata are taken from one representative worker: every worker
// materializes the full target table identically (see Session.Prepare), so
// preps are equal for a shared backend and summing would double-count.
func schedulerOpts(plan Plan, hdr trace.Header, preps []PrepareResult) replay.SchedulerOpts {
	var prep PrepareResult
	if len(preps) > 0 {
		prep = preps[0]
	}
	fill := payload.Config{Mode: payload.Mode(plan.FillMode), Seed: plan.FillSeed}.Normalize()
	planInfo := results.PlanInfo{
		TracePath:                 plan.TracePath,
		Engine:                    plan.Engine.Name,
		Mode:                      plan.Mode,
		MaxInflight:               plan.MaxInflight,
		SpeedupFactor:             plan.SpeedupFactor,
		TraceKind:                 string(hdr.Kind),
		Profile:                   hdr.Profile,
		NumStreams:                hdr.Summary.NumStreams,
		NumOps:                    hdr.Summary.NumOps,
		TotalBytes:                hdr.Summary.TotalBytes,
		PrepareMode:               plan.PrepareMode,
		PrepareScope:              plan.PrepareScope,
		FillMode:                  string(fill.Mode),
		FillSeed:                  fill.Seed,
		PrepareTouchedSameData:    prep.PrepStats.TouchedSameData || (plan.CacheMode == "warm" && prep.CacheResult.Primed > 0),
		PrepareVerified:           prep.PrepStats.Verified,
		PrepareCreated:            prep.PrepStats.Created,
		PrepareCopied:             prep.PrepStats.Copied,
		PrepareSkippedSizeUnknown: prep.PrepStats.SkippedSizeUnknown,
		PrepareDerivedSizeFromOps: prep.PrepStats.DerivedSizeFromOps,
	}
	runEnv := results.RunEnv{
		CacheMode:        plan.CacheMode,
		CacheActions:     prep.CacheResult.Actions,
		CacheLimitations: prep.CacheResult.Limitations,
	}
	return replay.SchedulerOpts{
		Mode:          plan.Mode,
		MaxInflight:   plan.MaxInflight,
		SpeedupFactor: plan.SpeedupFactor,
		PlanInfo:      planInfo,
		RunEnv:        runEnv,
		Fill:          fill,
	}
}

// partitionStreams assigns stream IDs round-robin across n workers. Worker i gets
// ids[i], ids[i+n], …; surplus workers get empty (idle) assignments.
func partitionStreams(ids []int64, n int) [][]int64 {
	out := make([][]int64, n)
	for i, id := range ids {
		w := i % n
		out[w] = append(out[w], id)
	}
	return out
}

// parseTraceStreams reads the header and enumerates the distinct stream IDs in
// the trace. Stream IDs are not assumed dense (importers key by source PID/TID),
// so the coordinator partitions the actual IDs rather than a 0..N-1 range.
func parseTraceStreams(b []byte) (trace.Header, []int64, error) {
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		return trace.Header{}, nil, err
	}
	hdr := r.Header()
	seen := make(map[int64]struct{})
	var ids []int64
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return hdr, nil, err
		}
		if _, ok := seen[op.S]; !ok {
			seen[op.S] = struct{}{}
			ids = append(ids, op.S)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return hdr, ids, nil
}

// skewNS returns the spread (max − min) of per-worker start delays — the
// Go-delivery skew reported as a fidelity diagnostic. 0 for a single worker.
func skewNS(delays []int64) int64 {
	if len(delays) < 2 {
		return 0
	}
	min, max := delays[0], delays[0]
	for _, d := range delays[1:] {
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	return max - min
}

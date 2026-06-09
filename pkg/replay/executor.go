// Package replay implements the IOFlux single-node replay executor.
//
// Usage:
//
//	exec, err := replay.Prepare(plan, reader)
//	res, err := exec.Run(ctx)
package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/chanuollala/ioflux/pkg/cache"
	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// Plan holds the configuration for a single replay run.
type Plan struct {
	TracePath  string
	Engine     engine.Engine
	EngineName string
	// Mode is "asap", "timeline", or "scaled".
	Mode string
	// MaxInflight is the worker-level in-flight cap (0 → default 512).
	MaxInflight int
	// SpeedupFactor scales trace timestamps in "scaled" mode (0 → 1×).
	SpeedupFactor float64
	// TargetMap rewrites trace targets before caps validation and replay.
	// When nil, targets are used as-is.
	TargetMap *targetmap.Map
	// Bucket is the S3 bucket configured on the engine; used to validate
	// s3:// URIs in TargetMap. Empty means skip bucket-name validation.
	Bucket string
	// PrepareMode selects the dataset-preparation strategy. Empty means skip
	// preparation (targets must already exist on the backend).
	PrepareMode string
	// SourceRoot is the local FS path for materialize-from-source.
	SourceRoot string
	// CacheMode is "cold" or "warm". Empty means skip cache controls.
	CacheMode string
}

// Executor holds the loaded, validated plan ready for execution.
type Executor struct {
	plan     Plan
	hdr      trace.Header
	byStream map[int64][]trace.Op
	allOps   []trace.Op
	// originalTargets is the pre-rewrite target slice, kept so Materialize can
	// locate source files for materialize-from-source.
	originalTargets []trace.TargetInfo
	prepStats       prepare.Stats
	materialized    bool
}

// Prepare loads all ops from r, validates that every op is compatible with
// plan.Engine's Capabilities, and groups ops by stream. r must be a Reader
// whose header has already been parsed by trace.NewReader. Prepare consumes
// the remainder of r (the op lines).
func Prepare(plan Plan, r *trace.Reader) (*Executor, error) {
	hdr := r.Header()

	byStream := make(map[int64][]trace.Op)
	var ops []trace.Op
	for {
		op, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replay: prepare: read op: %w", err)
		}
		ops = append(ops, op)
		byStream[op.S] = append(byStream[op.S], op)
	}

	rep, err := trace.ValidateLoadedRaw(r.HeaderRaw(), ops)
	if err != nil {
		return nil, fmt.Errorf("replay: prepare: validate trace: %w", err)
	}
	if !rep.OK() {
		return nil, fmt.Errorf("replay: prepare: invalid trace: %s", formatValidationErrors(rep))
	}

	// Apply target map rewrite before caps check so that post-rewrite kinds
	// (file → object) are visible to checkOpCaps. originalTargets preserves
	// pre-rewrite names so materialize-from-source can locate source files
	// regardless of the destination layout.
	originalTargets := append([]trace.TargetInfo(nil), hdr.Targets...)
	if plan.TargetMap != nil {
		ec := targetmap.EngineContext{EngineKind: plan.EngineName, Bucket: plan.Bucket}
		rewritten, _, err := plan.TargetMap.Rewrite(hdr.Targets, ec)
		if err != nil {
			return nil, fmt.Errorf("replay: prepare: target map: %w", err)
		}
		hdr.Targets = rewritten
	}

	caps := plan.Engine.Caps()
	for _, ops := range byStream {
		for _, op := range ops {
			if op.Group != nil && *op.Group != 0 {
				return nil, fmt.Errorf("replay: prepare: non-default group %d is not supported", *op.Group)
			}
			if err := checkOpCaps(op, caps); err != nil {
				return nil, fmt.Errorf("replay: prepare: %w", err)
			}
		}
	}

	// Dataset preparation is deferred to Materialize so it honors a caller's
	// context (a cancelled PREPARE phase must not keep materializing data).
	return &Executor{plan: plan, hdr: hdr, byStream: byStream, allOps: ops, originalTargets: originalTargets}, nil
}

// Materialize runs dataset preparation against the backend for the configured
// PrepareMode. It is the side-effecting half of the PREPARE phase, kept separate
// from trace loading so it honors ctx: a coordinator cancelling PREPARE (or a
// dropped connection) stops in-progress materialization instead of letting a
// large copy run to completion. It is idempotent (a no-op after the first call)
// and a no-op when no PrepareMode is configured. Its I/O is never credited to
// results — it runs before the Recorder exists.
func (e *Executor) Materialize(ctx context.Context) (prepare.Stats, error) {
	if e.materialized {
		return e.prepStats, nil
	}
	if e.plan.PrepareMode != "" {
		prep, err := prepare.For(prepare.Mode(e.plan.PrepareMode), e.plan.SourceRoot)
		if err != nil {
			return prepare.Stats{}, fmt.Errorf("replay: materialize: %w", err)
		}
		stats, err := prep.Prepare(ctx, e.hdr.Targets, e.originalTargets, e.allOps, e.plan.Engine)
		if err != nil {
			return prepare.Stats{}, fmt.Errorf("replay: materialize: %w", err)
		}
		e.prepStats = stats
	}
	e.materialized = true
	return e.prepStats, nil
}

func formatValidationErrors(rep trace.Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d validation error(s)", len(rep.Errors))
	for i, issue := range rep.Errors {
		if i == 3 {
			fmt.Fprintf(&b, "; ...")
			break
		}
		fmt.Fprintf(&b, "; %s", issue.String())
	}
	return b.String()
}

// checkOpCaps returns an error if op requires a capability not present in caps.
func checkOpCaps(op trace.Op, caps engine.Capabilities) error {
	switch op.Op {
	case trace.OpRead:
		if !caps.Seekable {
			return fmt.Errorf("trace contains READ op but engine reports Seekable=false")
		}
	case trace.OpFsync:
		if !caps.Durable {
			return fmt.Errorf("trace contains FSYNC op but engine reports Durable=false")
		}
	case trace.OpWrite:
		if op.Off != nil && !caps.Seekable {
			return fmt.Errorf("trace contains offset WRITE op but engine reports Seekable=false")
		}
		if op.Off != nil && !caps.PartialWrite {
			return fmt.Errorf("trace contains offset WRITE op but engine reports PartialWrite=false")
		}
	case trace.OpStat:
		if !caps.Seekable {
			return fmt.Errorf("trace contains STAT op but engine reports Seekable=false")
		}
	case trace.OpPut, trace.OpGet, trace.OpHead, trace.OpDelete:
		if !caps.ObjectAPI {
			return fmt.Errorf("trace contains object op %s but engine reports ObjectAPI=false", op.Op)
		}
	}
	return nil
}

// Header returns the trace header parsed during Prepare.
func (e *Executor) Header() trace.Header { return e.hdr }

// Run executes the replay and returns Results. Supported modes: "asap",
// "timeline", "scaled".
func (e *Executor) Run(ctx context.Context) (*results.Results, error) {
	switch e.plan.Mode {
	case "asap", "timeline", "scaled":
	default:
		return nil, fmt.Errorf("replay: unsupported mode %q (want asap|timeline|scaled)", e.plan.Mode)
	}

	// Dataset preparation and cache-state controls run before the measured run,
	// so their I/O is never credited to results.
	if _, err := e.Materialize(ctx); err != nil {
		return nil, err
	}
	cacheRes := e.ApplyCache(ctx)

	planInfo := results.PlanInfo{
		TracePath:                 e.plan.TracePath,
		Engine:                    e.plan.EngineName,
		Mode:                      e.plan.Mode,
		MaxInflight:               e.plan.MaxInflight,
		SpeedupFactor:             e.plan.SpeedupFactor,
		TraceKind:                 string(e.hdr.Kind),
		NumStreams:                e.hdr.Summary.NumStreams,
		NumOps:                    e.hdr.Summary.NumOps,
		TotalBytes:                e.hdr.Summary.TotalBytes,
		PrepareMode:               e.plan.PrepareMode,
		PrepareTouchedSameData:    e.prepStats.TouchedSameData || (e.plan.CacheMode == "warm" && cacheRes.Primed > 0),
		PrepareVerified:           e.prepStats.Verified,
		PrepareCreated:            e.prepStats.Created,
		PrepareCopied:             e.prepStats.Copied,
		PrepareSkippedSizeUnknown: e.prepStats.SkippedSizeUnknown,
		PrepareDerivedSizeFromOps: e.prepStats.DerivedSizeFromOps,
	}
	runEnv := results.RunEnv{
		CacheMode:        e.plan.CacheMode,
		CacheActions:     cacheRes.Actions,
		CacheLimitations: cacheRes.Limitations,
	}
	opts := SchedulerOpts{
		Mode:          e.plan.Mode,
		MaxInflight:   e.plan.MaxInflight,
		SpeedupFactor: e.plan.SpeedupFactor,
		RunStart:      time.Now(),
		PlanInfo:      planInfo,
		RunEnv:        runEnv,
	}
	return schedule(ctx, e.byStream, e.plan.Engine, e.hdr, opts)
}

// WithStreams returns a shallow copy of e whose stream set is restricted to
// streamIDs. The distributed coordinator uses it to assign a subset of streams
// to each worker; stream IDs not present in the trace are ignored. The returned
// executor shares the underlying op slices (they are not mutated by replay).
func (e *Executor) WithStreams(streamIDs []int64) *Executor {
	sub := make(map[int64][]trace.Op, len(streamIDs))
	for _, sid := range streamIDs {
		if ops, ok := e.byStream[sid]; ok {
			sub[sid] = ops
		}
	}
	cp := *e
	cp.byStream = sub
	return &cp
}

// StreamIDs returns the executor's stream IDs in ascending order, so a
// coordinator can partition them across workers.
func (e *Executor) StreamIDs() []int64 {
	ids := make([]int64, 0, len(e.byStream))
	for sid := range e.byStream {
		ids = append(ids, sid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ApplyCache applies the configured cache-state controls (cold/warm) against the
// engine's targets and returns the actions/limitations to record in run metadata.
// It belongs to the PREPARE phase: a distributed coordinator must call it on every
// worker before the RUN barrier, so all workers are cache-ready before any worker
// starts issuing ops. No-op (zero Result) when no cache mode is configured.
func (e *Executor) ApplyCache(ctx context.Context) cache.Result {
	if e.plan.CacheMode == "" {
		return cache.Result{}
	}
	return cache.Apply(ctx, cache.Mode(e.plan.CacheMode), e.plan.Engine, e.hdr.Targets)
}

// RunWorker replays this executor's assigned streams starting at runStart,
// returning the raw per-worker output for a coordinator to merge. progress, when
// non-nil, is called periodically with cumulative ops/bytes for live streaming.
// It is the worker-side primitive beneath the gRPC layer: the distributed
// coordinator calls it on each worker and feeds the WorkerOutputs to BuildResults.
// (Single-node Run uses the same scheduler via schedule.)
//
// Cache controls are NOT applied here — they belong to PREPARE (call ApplyCache
// before the RUN barrier) so they never run inside the measured, barrier-gated
// window.
func (e *Executor) RunWorker(ctx context.Context, runStart time.Time, progress func(ops, bytes int64)) (*WorkerOutput, error) {
	switch e.plan.Mode {
	case "asap", "timeline", "scaled":
	default:
		return nil, fmt.Errorf("replay: unsupported mode %q (want asap|timeline|scaled)", e.plan.Mode)
	}
	opts := SchedulerOpts{
		Mode:          e.plan.Mode,
		MaxInflight:   e.plan.MaxInflight,
		SpeedupFactor: e.plan.SpeedupFactor,
		RunStart:      runStart,
		Progress:      progress,
	}
	return runStreams(ctx, e.byStream, e.plan.Engine, e.hdr, opts)
}

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
	"strings"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/results"
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
}

// Executor holds the loaded, validated plan ready for execution.
type Executor struct {
	plan     Plan
	hdr      trace.Header
	byStream map[int64][]trace.Op
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

	caps := plan.Engine.Caps()
	for _, ops := range byStream {
		for _, op := range ops {
			if op.Group != nil {
				return nil, fmt.Errorf("replay: prepare: non-default group %d is not supported", *op.Group)
			}
			if err := checkOpCaps(op, caps); err != nil {
				return nil, fmt.Errorf("replay: prepare: %w", err)
			}
		}
	}

	return &Executor{plan: plan, hdr: hdr, byStream: byStream}, nil
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
	planInfo := results.PlanInfo{
		TracePath:     e.plan.TracePath,
		Engine:        e.plan.EngineName,
		Mode:          e.plan.Mode,
		MaxInflight:   e.plan.MaxInflight,
		SpeedupFactor: e.plan.SpeedupFactor,
		TraceKind:     string(e.hdr.Kind),
		NumStreams:    e.hdr.Summary.NumStreams,
		NumOps:        e.hdr.Summary.NumOps,
		TotalBytes:    e.hdr.Summary.TotalBytes,
	}
	opts := SchedulerOpts{
		Mode:          e.plan.Mode,
		MaxInflight:   e.plan.MaxInflight,
		SpeedupFactor: e.plan.SpeedupFactor,
		RunStart:      time.Now(),
		PlanInfo:      planInfo,
	}
	return schedule(ctx, e.byStream, e.plan.Engine, e.hdr, opts)
}

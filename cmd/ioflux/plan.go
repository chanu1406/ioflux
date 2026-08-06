package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/chanuollala/ioflux/pkg/cluster"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
	"github.com/chanuollala/ioflux/pkg/payload"
	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// runSettings is one replay's complete configuration, independent of where it
// came from. `ioflux run` fills it from flags and `ioflux experiment` from a
// config file; both then build a plan through buildPlan, so a validation rule
// cannot hold on one path and not the other.
type runSettings struct {
	TracePath        string
	EngineName       string
	Mode             string
	MaxInflight      int
	Speedup          float64
	TargetMapPath    string
	AllowPassthrough bool
	PrepareMode      string
	PrepareScope     string
	SourceRoot       string
	CacheMode        string
	FillMode         string
	FillSeed         int64
	TargetRoot       string
	AllowDirect      bool
	DirectFallback   bool
	DirectAlign      int64
	Hosts            string
	S3               s3engine.Config
}

// defaultRunSettings returns the settings a bare `ioflux run` would use, which
// is also what an experiment config starts from before its own values apply.
func defaultRunSettings() runSettings {
	return runSettings{
		EngineName:  "mem",
		Mode:        "asap",
		MaxInflight: 512,
		Speedup:     1.0,
		CacheMode:   "cold",
		FillMode:    string(payload.ModeSeeded),
		FillSeed:    payload.DefaultSeed,
	}
}

// planError carries the exit code a failure should produce alongside its
// message, so the distinction between a usage error and a bad trace survives
// being returned through a shared helper.
type planError struct {
	Code int
	Err  error
}

func (e *planError) Error() string { return e.Err.Error() }

func usageErrorf(format string, args ...any) *planError {
	return &planError{Code: 2, Err: fmt.Errorf(format, args...)}
}

func inputErrorf(format string, args ...any) *planError {
	return &planError{Code: 1, Err: fmt.Errorf(format, args...)}
}

// validate checks the settings that do not require reading the trace, so a
// misconfiguration fails before any I/O.
func (s runSettings) validate() *planError {
	if s.TracePath == "" {
		return usageErrorf("a trace is required")
	}
	switch s.Mode {
	case "asap", "timeline", "scaled":
	default:
		return usageErrorf("unsupported mode %q (want asap | timeline | scaled)", s.Mode)
	}
	if s.MaxInflight <= 0 {
		return usageErrorf("max-inflight must be > 0, got %d", s.MaxInflight)
	}
	if s.Mode == "scaled" && s.Speedup <= 0 {
		return usageErrorf("speedup must be > 0 for scaled mode, got %v", s.Speedup)
	}
	if s.PrepareScope != "" &&
		s.PrepareScope != cluster.PrepareScopeShared && s.PrepareScope != cluster.PrepareScopePerWorker {
		return usageErrorf("unsupported prepare-scope %q (want shared | per-worker)", s.PrepareScope)
	}
	if s.FillMode != string(payload.ModeSeeded) && s.FillMode != string(payload.ModeZero) {
		return usageErrorf("unsupported fill %q (want seeded | zero)", s.FillMode)
	}
	// The containment root is not part of the worker protocol, so a
	// coordinator-side root would apply to nothing on a remote worker. Refuse the
	// combination rather than let the operator believe the run was confined when
	// it was not.
	if s.TargetRoot != "" && len(splitHosts(s.Hosts)) > 0 {
		return usageErrorf("target-root cannot be combined with remote hosts: the containment root is " +
			"not carried in the worker protocol, so it would not apply on remote workers.\n" +
			"  Set it on each worker instead: ioflux worker --listen :7800 --target-root <dir>")
	}
	return nil
}

// buildPlan validates s, reads its trace, and assembles the plan a coordinator
// runs. It performs the same engine pre-flight `ioflux run` always has, so a
// bad bucket or multipart size fails before anything is dispatched.
func buildPlan(s runSettings) (cluster.Plan, *planError) {
	if s.FillSeed == 0 {
		s.FillSeed = payload.DefaultSeed
	}
	if err := s.validate(); err != nil {
		return cluster.Plan{}, err
	}

	// Read the trace into memory; the plan inlines it for every worker.
	traceBytes, err := os.ReadFile(s.TracePath)
	if err != nil {
		return cluster.Plan{}, &planError{Code: 2, Err: fmt.Errorf("open trace: %w", err)}
	}
	r, err := trace.NewReader(bytes.NewReader(traceBytes))
	if err != nil {
		return cluster.Plan{}, inputErrorf("parse trace: %w", err)
	}
	hdr := r.Header()

	spec := cluster.EngineSpec{
		Name:           s.EngineName,
		CacheMode:      s.CacheMode,
		Root:           s.TargetRoot,
		AllowDirect:    s.AllowDirect,
		DirectFallback: s.DirectFallback,
		DirectAlign:    s.DirectAlign,
		S3:             s.S3,
	}
	// Pre-flight: validate the engine config now so usage errors (missing bucket,
	// bad multipart size) fail fast as exit-2 before any distribution. The worker
	// rebuilds the engine from the same spec at PREPARE.
	if _, _, err := buildRunEngine(spec, hdr); err != nil {
		return cluster.Plan{}, &planError{Code: 2, Err: err}
	}

	allowPassthrough := s.AllowPassthrough
	var rewriteRules []targetmap.Rule
	if s.TargetMapPath != "" {
		tmap, err := targetmap.Load(s.TargetMapPath)
		if err != nil {
			return cluster.Plan{}, inputErrorf("%w", err)
		}
		rewriteRules = tmap.Rules
		allowPassthrough = allowPassthrough || tmap.AllowPassthrough
	}

	return cluster.Plan{
		TracePath:        s.TracePath,
		TraceBytes:       traceBytes,
		Engine:           spec,
		Mode:             s.Mode,
		MaxInflight:      s.MaxInflight,
		SpeedupFactor:    s.Speedup,
		TargetRewrite:    rewriteRules,
		AllowPassthrough: allowPassthrough,
		PrepareMode:      s.PrepareMode,
		PrepareScope:     s.PrepareScope,
		SourceRoot:       s.SourceRoot,
		CacheMode:        s.CacheMode,
		FillMode:         s.FillMode,
		FillSeed:         s.FillSeed,
	}, nil
}

package cluster

import (
	"github.com/chanuollala/ioflux/pkg/cache"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/targetmap"
)

// Version identifies the coordinator/worker protocol. A worker whose Version
// differs is rejected at REGISTER: an older worker silently omits evidence the
// coordinator would otherwise read as a clean result.
const Version = "0.3.0"

const (
	PrepareScopeShared    = "shared"
	PrepareScopePerWorker = "per-worker"
)

// ResolvePrepareScope returns the configured prepare scope or the backend
// default: shared object stores are materialized once, node-local backends
// per worker.
func ResolvePrepareScope(p Plan) string {
	switch p.PrepareScope {
	case PrepareScopeShared, PrepareScopePerWorker:
		return p.PrepareScope
	}
	if p.Engine.Name == "s3" {
		return PrepareScopeShared
	}
	return PrepareScopePerWorker
}

// Plan is the transport-agnostic description of one worker's share of a replay
// run. Every worker gets an identical copy differing only in AssignedStreams.
type Plan struct {
	// TracePath is advisory metadata echoed into results; the worker replays
	// TraceBytes, not this path.
	TracePath string
	// TraceBytes is the full .ioflux trace (header line + op lines), inlined.
	TraceBytes []byte
	// AssignedStreams lists the stream IDs this worker replays. Always populated
	// and authoritative: an empty list means no streams, never all of them.
	AssignedStreams []int64

	Engine EngineSpec

	// Mode is "asap", "think", "timeline", or "scaled".
	Mode string
	// MaxInflight is the worker-global in-flight cap (0 → default 512).
	MaxInflight int
	// SpeedupFactor scales trace timestamps in "scaled" mode (0 → 1×).
	SpeedupFactor float64

	// TargetRewrite and AllowPassthrough reconstruct the target map on the worker.
	TargetRewrite    []targetmap.Rule
	AllowPassthrough bool

	// PrepareMode selects dataset preparation; empty skips it. SourceRoot is the
	// local path for materialize-from-source.
	PrepareMode  string
	PrepareScope string
	SourceRoot   string
	// CacheMode is "cold" or "warm"; empty skips cache controls.
	CacheMode string
	// FillMode is "seeded" or "zero". Empty defaults to seeded.
	FillMode string
	// FillSeed controls deterministic seeded payload fill. 0 uses the default.
	FillSeed int64
}

// WorkerInfo is a worker's identity, returned by REGISTER.
type WorkerInfo struct {
	Hostname string
	CPUs     int
	Version  string
}

// PrepareResult is one worker's PREPARE-phase outcome. Workers prepare the full
// target table idempotently, so results agree across workers and the
// coordinator records one representative copy rather than summing.
type PrepareResult struct {
	PrepStats   prepare.Stats
	CacheResult cache.Result
	// ReplayEquivalence is how faithfully this worker will replay the trace,
	// decided at PREPARE.
	ReplayEquivalence string
}

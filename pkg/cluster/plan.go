package cluster

import (
	"github.com/chanuollala/ioflux/pkg/cache"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/targetmap"
)

// Version identifies the coordinator/worker protocol. A worker whose Version
// differs from the coordinator's is rejected at REGISTER, since a stale worker
// could silently mis-replay a plan it does not fully understand.
const Version = "0.1.0"

// Plan is the transport-agnostic description of one worker's share of a replay
// run. The coordinator builds it once and hands an identical copy (differing
// only in AssignedStreams) to every worker. A localWorker passes it straight to
// a Session; a remoteWorker marshals it to protobuf for the wire.
type Plan struct {
	// TracePath is advisory metadata echoed into results; the worker replays
	// TraceBytes, not this path.
	TracePath string
	// TraceBytes is the full .ioflux trace (header line + op lines), inlined.
	TraceBytes []byte
	// AssignedStreams lists exactly the stream IDs this worker replays. The
	// coordinator always populates it (single-node: every stream goes to the one
	// worker; idle worker: empty). It is authoritative — the Session never infers
	// "all streams" from an empty list.
	AssignedStreams []int64

	Engine EngineSpec

	// Mode is "asap", "timeline", or "scaled".
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
	PrepareMode string
	SourceRoot  string
	// CacheMode is "cold" or "warm"; empty skips cache controls.
	CacheMode string
}

// WorkerInfo is a worker's identity, returned by REGISTER.
type WorkerInfo struct {
	Hostname string
	CPUs     int
	Version  string
}

// PrepareResult is one worker's PREPARE-phase outcome. The coordinator collects
// it to record honest run metadata (dataset prep counts, cache actions). Because
// every worker materializes the full target table idempotently (see Session.Prepare),
// these per-worker results are identical for shared backends; the coordinator
// records one representative copy rather than summing.
type PrepareResult struct {
	PrepStats   prepare.Stats
	CacheResult cache.Result
}

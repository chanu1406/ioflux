package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// Session is the pure-Go core of a replay worker. It owns one run at a time and
// drives the existing replay.Executor through PREPARE → RUN → COLLECT. The gRPC
// Server and the in-process localWorker both wrap a Session, so single-node and
// distributed runs share this code path — the only difference is the transport.
//
// A Session is not safe for concurrent runs; the Server serializes calls and
// rejects a second Prepare before the first run is collected.
type Session struct {
	hostname string
	// targetRoot, when non-empty, confines every local-engine target on this
	// worker regardless of what the coordinator's plan asked for.
	targetRoot string

	mu   sync.Mutex
	exec *replay.Executor
	eng  engine.Engine
	out  *replay.WorkerOutput
}

// NewSession returns a Session that reports the local hostname in WorkerInfo and
// COLLECT results, with no target containment.
func NewSession() *Session { return NewSessionWithRoot("") }

// NewSessionWithRoot returns a Session that confines every local-engine target
// to root, overriding whatever the coordinator's plan requested. The root is
// worker-local policy on purpose: a coordinator is not trusted to declare what
// a worker's host may expose, so the host that owns the data sets the boundary.
// An empty root applies no containment.
func NewSessionWithRoot(root string) *Session {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return &Session{hostname: host, targetRoot: root}
}

// Info returns this worker's identity for REGISTER.
func (s *Session) Info() WorkerInfo {
	return WorkerInfo{Hostname: s.hostname, CPUs: runtime.NumCPU(), Version: Version}
}

// Prepare loads the trace, builds the engine, validates capabilities, runs
// dataset preparation, and applies cache controls — every barrier-gated
// precondition for the RUN phase. On success the Session is ready to Run.
//
// Each worker materializes the full target table, not only its assigned
// streams': prep content is deterministic, so replicating it is idempotent and
// the PREPARE barrier guarantees every target is written before any read. The
// coordinator records one worker's PrepareResult rather than summing.
func (s *Session) Prepare(ctx context.Context, p Plan) (PrepareResult, error) {
	return s.PrepareReader(ctx, p, bytes.NewReader(p.TraceBytes))
}

// PrepareReader is Prepare with the trace supplied as a streamable reader. It
// is used by the gRPC PrepareStream path to avoid holding a second giant trace
// byte slice on the worker.
func (s *Session) PrepareReader(ctx context.Context, p Plan, traceData io.Reader) (PrepareResult, error) {
	r, err := trace.NewReader(traceData)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("cluster: prepare: parse trace: %w", err)
	}
	hdr := r.Header()

	spec := p.Engine
	if spec.CacheMode == "" {
		spec.CacheMode = p.CacheMode
	}
	// Worker-local containment wins over the plan: the coordinator cannot widen
	// what this host is willing to expose.
	if s.targetRoot != "" {
		spec.Root = s.targetRoot
	}
	if spec.TargetSizes == nil {
		spec.TargetSizes = make(map[string]int64, len(hdr.Targets))
		for _, tgt := range hdr.Targets {
			spec.TargetSizes[tgt.Name] = tgt.Size
		}
	}
	eng, bucket, err := BuildEngine(spec)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("cluster: prepare: %w", err)
	}

	var tmap *targetmap.Map
	if len(p.TargetRewrite) > 0 || p.AllowPassthrough {
		tmap = &targetmap.Map{
			Rules:            append([]targetmap.Rule(nil), p.TargetRewrite...),
			AllowPassthrough: p.AllowPassthrough,
		}
	}

	exec, err := replay.PrepareAssigned(replay.Plan{
		TracePath:     p.TracePath,
		Engine:        eng,
		EngineName:    p.Engine.Name,
		Mode:          p.Mode,
		MaxInflight:   p.MaxInflight,
		SpeedupFactor: p.SpeedupFactor,
		TargetMap:     tmap,
		Bucket:        bucket,
		PrepareMode:   p.PrepareMode,
		SourceRoot:    p.SourceRoot,
		CacheMode:     p.CacheMode,
		FillMode:      p.FillMode,
		FillSeed:      p.FillSeed,
	}, r, p.AssignedStreams)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("cluster: prepare: %w", err)
	}

	// Dataset preparation honors ctx: if the coordinator cancels PREPARE (or the
	// connection drops), in-progress materialization stops rather than running on.
	prepStats, err := exec.Materialize(ctx)
	if err != nil {
		return PrepareResult{}, fmt.Errorf("cluster: prepare: %w", err)
	}

	cacheRes := exec.ApplyCache(ctx)

	s.mu.Lock()
	prev := s.eng
	s.exec = exec
	s.eng = eng
	s.out = nil
	s.mu.Unlock()
	// A worker serves many runs over its lifetime and builds one engine per run.
	// Release the previous run's engine-held OS resources (the local engine's
	// containment root directory handle) rather than accumulating them.
	shutdownEngine(prev)

	return PrepareResult{
		PrepStats:         prepStats,
		CacheResult:       cacheRes,
		ReplayEquivalence: exec.ReplayEquivalence(),
	}, nil
}

// shutdownEngine releases engine-held OS resources when the engine supports it.
func shutdownEngine(eng engine.Engine) {
	if eng == nil {
		return
	}
	if s, ok := eng.(engine.Shutdowner); ok {
		_ = s.Shutdown()
	}
}

// Run replays the prepared streams, scheduling timeline arrivals from goTime
// (the worker's local zero — no cross-host clock sync is assumed). progress,
// when non-nil, is invoked periodically with cumulative ops/bytes for live
// streaming. It returns ctx.Err() if the run was cancelled; the partial output
// is still stored so Collect can report coverage.
func (s *Session) Run(ctx context.Context, goTime time.Time, progress func(ops, bytes int64)) error {
	s.mu.Lock()
	exec := s.exec
	s.mu.Unlock()
	if exec == nil {
		return errors.New("cluster: Run before Prepare")
	}

	out, err := exec.RunWorker(ctx, goTime, progress)

	s.mu.Lock()
	s.out = out
	s.mu.Unlock()
	return err
}

// Collect returns the worker's raw output with its hostname stamped, for the
// coordinator to merge. It must be called after Run.
func (s *Session) Collect() (*replay.WorkerOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.out == nil {
		return nil, errors.New("cluster: Collect before Run")
	}
	s.out.Hostname = s.hostname
	s.out.EngineLimitations = engineLimitations(s.eng)
	return s.out, nil
}

// engineLimitations extracts the honesty notes an engine recorded about what its
// configuration guaranteed. Asking through engine.Limiter rather than naming a
// concrete type means a new engine's caveats reach the report by implementing
// the interface, instead of silently contributing none.
func engineLimitations(eng engine.Engine) []string {
	if l, ok := eng.(engine.Limiter); ok {
		return l.Limitations()
	}
	return nil
}

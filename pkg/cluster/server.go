package cluster

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	clusterpb "github.com/chanuollala/ioflux/pkg/cluster/proto"
)

// defaultRunLease bounds how long an uncollected run keeps owning a worker after
// its last activity. It is generous relative to the 1s progress cadence, so a
// live run never expires; it only matters when a coordinator abandons a run in a
// gap with no active RPC stream (post-Prepare/pre-Run, or post-Run/pre-Collect).
const defaultRunLease = 2 * time.Minute

// Server is the gRPC adapter over a single Session: a thin translation layer
// that decodes requests, drives the Session through the phase protocol, and
// encodes responses.
//
// A worker serves one run at a time. A run owns the worker from Prepare until
// Collect, guarded by a lease so the result is never clobbered by an overlapping
// run, yet an abandoned coordinator cannot wedge the worker forever: the lease is
// refreshed throughout an active run (each progress tick) and a new Prepare may
// take over only once it goes stale. Mid-run abandonment is handled instantly,
// without waiting for the lease — the Run stream's context is cancelled, so Run
// returns an error and releases the worker.
type Server struct {
	clusterpb.UnimplementedWorkerServer
	session  *Session
	runLease time.Duration

	mu            sync.Mutex
	inUse         bool
	leaseDeadline time.Time
}

// NewServer returns a Server backed by a fresh Session.
func NewServer() *Server { return &Server{session: NewSession(), runLease: defaultRunLease} }

// acquire reserves the worker for a new run, taking over an abandoned run whose
// lease has expired. It returns false if a run is actively in progress.
func (s *Server) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inUse && time.Now().Before(s.leaseDeadline) {
		return false
	}
	s.inUse = true
	s.leaseDeadline = time.Now().Add(s.runLease)
	return true
}

// refreshLease extends the active run's lease. Called as the run makes progress
// so a live run is never mistaken for an abandoned one.
func (s *Server) refreshLease() {
	s.mu.Lock()
	if s.inUse {
		s.leaseDeadline = time.Now().Add(s.runLease)
	}
	s.mu.Unlock()
}

// held reports whether a run currently owns the worker.
func (s *Server) held() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inUse
}

// release frees the worker for the next run.
func (s *Server) release() {
	s.mu.Lock()
	s.inUse = false
	s.mu.Unlock()
}

// Register reports the worker's identity and rejects a coordinator on a
// mismatched protocol version.
func (s *Server) Register(_ context.Context, req *clusterpb.RegisterRequest) (*clusterpb.WorkerInfo, error) {
	if v := req.GetCoordinatorVersion(); v != "" && v != Version {
		return nil, status.Errorf(codes.FailedPrecondition,
			"coordinator version %q != worker version %q", v, Version)
	}
	return workerInfoToProto(s.session.Info()), nil
}

// Prepare loads+validates the trace, builds the engine, runs dataset prep, and
// applies cache controls. A successful response is the PREPARE barrier ack.
func (s *Server) Prepare(ctx context.Context, pb *clusterpb.Plan) (*clusterpb.PrepareAck, error) {
	if !s.acquire() {
		return nil, status.Error(codes.FailedPrecondition, "worker busy: a run is already in progress")
	}
	res, err := s.session.Prepare(ctx, planFromProto(pb))
	if err != nil {
		s.release()
		return nil, status.Errorf(codes.Internal, "prepare: %v", err)
	}
	s.refreshLease()
	return prepareAckToProto(res), nil
}

// PrepareStream is the large-trace PREPARE path. The first client message must
// carry Plan metadata; subsequent messages carry trace chunks.
func (s *Server) PrepareStream(stream grpc.ClientStreamingServer[clusterpb.PrepareChunk, clusterpb.PrepareAck]) error {
	if !s.acquire() {
		return status.Error(codes.FailedPrecondition, "worker busy: a run is already in progress")
	}
	ack, err := s.prepareStream(stream)
	if err != nil {
		s.release()
		return err
	}
	s.refreshLease()
	return stream.SendAndClose(ack)
}

func (s *Server) prepareStream(stream grpc.ClientStreamingServer[clusterpb.PrepareChunk, clusterpb.PrepareAck]) (*clusterpb.PrepareAck, error) {
	first, err := stream.Recv()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "prepare stream: first chunk: %v", err)
	}
	if first.GetPlan() == nil {
		return nil, status.Error(codes.InvalidArgument, "prepare stream: first chunk missing plan")
	}
	p := planFromProto(first.GetPlan())
	tmp, err := os.CreateTemp("", "ioflux-prepare-*.ioflux")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "prepare stream: temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if chunk := first.GetTraceChunk(); len(chunk) > 0 {
		if _, err := tmp.Write(chunk); err != nil {
			return nil, status.Errorf(codes.Internal, "prepare stream: write chunk: %v", err)
		}
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "prepare stream: receive chunk: %v", err)
		}
		if msg.GetPlan() != nil {
			return nil, status.Error(codes.InvalidArgument, "prepare stream: plan may only appear in first chunk")
		}
		if _, err := tmp.Write(msg.GetTraceChunk()); err != nil {
			return nil, status.Errorf(codes.Internal, "prepare stream: write chunk: %v", err)
		}
		s.refreshLease()
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, status.Errorf(codes.Internal, "prepare stream: rewind: %v", err)
	}
	res, err := s.session.PrepareReader(stream.Context(), p, tmp)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "prepare: %v", err)
	}
	return prepareAckToProto(res), nil
}

// Run replays the prepared streams, sending Progress heartbeats over the stream
// until it finishes (the stream close is the DONE/Finished signal). It schedules
// timeline arrivals from the worker's own clock at receipt (no cross-host sync).
func (s *Server) Run(_ *clusterpb.GoSignal, stream grpc.ServerStreamingServer[clusterpb.Progress]) error {
	if !s.held() {
		return status.Error(codes.FailedPrecondition, "run before prepare")
	}
	s.refreshLease()
	progress := func(ops, bytes int64) {
		s.refreshLease() // a live run keeps its lease, so it is never taken over
		_ = stream.Send(&clusterpb.Progress{Ops: ops, Bytes: bytes})
	}
	if err := s.session.Run(stream.Context(), time.Now(), progress); err != nil {
		// Abandonment/abort: the stream context cancelled, so free the worker now
		// rather than waiting out the lease.
		s.release()
		return status.Errorf(codes.Aborted, "run: %v", err)
	}
	// Keep the lease alive for the brief Run→Collect gap; Collect releases it.
	s.refreshLease()
	return nil
}

// Collect returns the worker's raw output and releases the worker for the next run.
func (s *Server) Collect(_ context.Context, _ *clusterpb.CollectRequest) (*clusterpb.WorkerResults, error) {
	out, err := s.session.Collect()
	s.release()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "collect: %v", err)
	}
	return workerOutputToProto(out), nil
}

// ServerOptions returns the gRPC server options IOFlux workers use: keepalive
// parameters so a dropped coordinator or stalled worker is detected as missed
// heartbeats (PRD §8.9 failure handling) rather than hanging indefinitely.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	}
}

// Register attaches s to a grpc.Server.
func (s *Server) RegisterTo(gs *grpc.Server) { clusterpb.RegisterWorkerServer(gs, s) }

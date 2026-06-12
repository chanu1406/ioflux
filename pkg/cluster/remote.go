package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	clusterpb "github.com/chanuollala/ioflux/pkg/cluster/proto"
	"github.com/chanuollala/ioflux/pkg/replay"
)

const (
	maxGRPCMessageBytes     = 64 << 20
	prepareStreamChunkBytes = 1 << 20
)

// remoteWorker drives a worker over gRPC. It is the distributed counterpart of
// localWorker: both satisfy Worker, so the Coordinator's phase logic is identical
// whether a worker is in-process or across the network.
type remoteWorker struct {
	conn   *grpc.ClientConn
	client clusterpb.WorkerClient
	addr   string
}

// DialWorker connects to a worker at addr. extra dial options are appended after
// the defaults (insecure transport + keepalive), letting tests inject a bufconn
// dialer. The connection is lazy; an unreachable worker first surfaces at Register.
func DialWorker(addr string, extra ...grpc.DialOption) (Worker, error) {
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(maxGRPCMessageBytes),
			grpc.MaxCallRecvMsgSize(maxGRPCMessageBytes),
		),
	}, extra...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("cluster: dial worker %q: %w", addr, err)
	}
	return &remoteWorker{conn: conn, client: clusterpb.NewWorkerClient(conn), addr: addr}, nil
}

func (w *remoteWorker) Register(ctx context.Context) (WorkerInfo, error) {
	info, err := w.client.Register(ctx, &clusterpb.RegisterRequest{CoordinatorVersion: Version})
	if err != nil {
		return WorkerInfo{}, fmt.Errorf("cluster: register %q: %w", w.addr, err)
	}
	return workerInfoFromProto(info), nil
}

func (w *remoteWorker) Prepare(ctx context.Context, p Plan) (PrepareResult, error) {
	ack, err := w.prepareStream(ctx, p)
	if status.Code(err) == codes.Unimplemented {
		ack, err = w.client.Prepare(ctx, planToProto(p))
	}
	if err != nil {
		return PrepareResult{}, fmt.Errorf("cluster: prepare %q: %w", w.addr, err)
	}
	return prepareAckFromProto(ack), nil
}

func (w *remoteWorker) prepareStream(ctx context.Context, p Plan) (*clusterpb.PrepareAck, error) {
	stream, err := w.client.PrepareStream(ctx)
	if err != nil {
		return nil, err
	}
	traceBytes := p.TraceBytes
	meta := p
	meta.TraceBytes = nil
	// A Send error (typically a bare io.EOF) means the server already
	// terminated the RPC; the authoritative status — including Unimplemented
	// from a worker without PrepareStream — only comes from CloseAndRecv, so
	// Send errors must never be returned directly.
	if err := stream.Send(&clusterpb.PrepareChunk{Plan: planToProto(meta)}); err != nil {
		return stream.CloseAndRecv()
	}
	for off := 0; off < len(traceBytes); off += prepareStreamChunkBytes {
		end := off + prepareStreamChunkBytes
		if end > len(traceBytes) {
			end = len(traceBytes)
		}
		if err := stream.Send(&clusterpb.PrepareChunk{TraceChunk: traceBytes[off:end]}); err != nil {
			return stream.CloseAndRecv()
		}
	}
	return stream.CloseAndRecv()
}

func (w *remoteWorker) Run(ctx context.Context, goTime time.Time, progress func(ops, bytes int64)) error {
	stream, err := w.client.Run(ctx, &clusterpb.GoSignal{GoEpochNs: goTime.UnixNano()})
	if err != nil {
		return fmt.Errorf("cluster: run %q: %w", w.addr, err)
	}
	// Drain Progress heartbeats until the server closes the stream (Finished) or
	// the run is cancelled/fails.
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cluster: run %q: %w", w.addr, err)
		}
		if progress != nil {
			progress(msg.GetOps(), msg.GetBytes())
		}
	}
}

func (w *remoteWorker) Collect(ctx context.Context) (*replay.WorkerOutput, error) {
	res, err := w.client.Collect(ctx, &clusterpb.CollectRequest{})
	if err != nil {
		return nil, fmt.Errorf("cluster: collect %q: %w", w.addr, err)
	}
	return workerOutputFromProto(res), nil
}

func (w *remoteWorker) Close() error { return w.conn.Close() }

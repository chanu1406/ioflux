package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/chanuollala/ioflux/pkg/cluster"
)

const workerUsage = `Usage:
  ioflux worker --listen :7800

Run a replay worker that a coordinator drives over gRPC. Start one worker per
host, then point a run at them:

  ioflux run --trace t.ioflux --engine local --hosts hostA:7800,hostB:7800 ...

The worker serves one run at a time and shuts down cleanly on SIGINT/SIGTERM.

Security: the gRPC transport is plaintext and unauthenticated, and the plan the
coordinator sends carries the trace and any S3 credentials. Run workers only on a
trusted network (private subnet, SSH tunnel, or VPN); v1 has no TLS.

Flags:
  --listen <addr>   Address to listen on (default :7800)

Exit codes:
  0   served and shut down cleanly
  2   usage error or listen failure
`

// runWorker is the entry point for the `worker` subcommand.
func runWorker(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, workerUsage) }

	var listen string
	fs.StringVar(&listen, "listen", ":7800", "address to listen on")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		fmt.Fprintf(stderr, "ioflux worker: listen %q: %v\n", listen, err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := serveWorker(ctx, lis, stdout); err != nil {
		fmt.Fprintf(stderr, "ioflux worker: %v\n", err)
		return 2
	}
	fmt.Fprintln(stdout, "ioflux worker: stopped")
	return 0
}

// serveWorker runs a gRPC Worker server on lis until ctx is cancelled, then
// drains in-flight RPCs via GracefulStop. Split out so tests can drive shutdown
// without sending a real signal.
func serveWorker(ctx context.Context, lis net.Listener, log io.Writer) error {
	gs := grpc.NewServer(cluster.ServerOptions()...)
	cluster.NewServer().RegisterTo(gs)

	fmt.Fprintf(log, "ioflux worker listening on %s (protocol %s)\n", lis.Addr(), cluster.Version)

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()

	select {
	case <-ctx.Done():
		gs.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

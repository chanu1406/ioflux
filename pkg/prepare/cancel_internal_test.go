package prepare

import (
	"context"
	"errors"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine/mem"
)

// TestWritePOSIXHonorsCancellation proves a single large target write stops on
// cancellation rather than running to completion — the engine's own writes may
// not observe ctx, so the chunk loop checks it directly.
func TestWritePOSIXHonorsCancellation(t *testing.T) {
	eng := mem.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := writePOSIX(ctx, eng, "huge", 64<<20, make([]byte, prepareChunkSize))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writePOSIX with cancelled ctx err=%v, want context.Canceled", err)
	}
}

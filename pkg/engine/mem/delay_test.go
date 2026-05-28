package mem_test

import (
	"context"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/trace"
)

func TestInjectedDelay_UniformSlowsRead(t *testing.T) {
	delay := 50 * time.Millisecond
	eng := mem.New(mem.WithFixedSize(1<<20), mem.WithInjectedDelay(delay))

	h, err := eng.Open(context.Background(), "target", engine.ModeRead, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	buf := make([]byte, 64)
	start := time.Now()
	eng.Read(context.Background(), h, 0, int64(len(buf)), buf) //nolint:errcheck
	elapsed := time.Since(start)

	if elapsed < delay {
		t.Errorf("Read returned in %v, want >= %v", elapsed, delay)
	}
}

func TestInjectedDelayFunc_SlowsOnlyRead(t *testing.T) {
	readDelay := 60 * time.Millisecond
	eng := mem.New(
		mem.WithFixedSize(1<<20),
		mem.WithInjectedDelayFunc(func(op trace.OpKind) time.Duration {
			if op == trace.OpRead {
				return readDelay
			}
			return 0
		}),
	)

	// OPEN should be fast.
	start := time.Now()
	h, err := eng.Open(context.Background(), "target", engine.ModeRead, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= readDelay/2 {
		t.Errorf("Open took %v, expected < %v (should not be delayed)", elapsed, readDelay/2)
	}

	// READ should be slow.
	buf := make([]byte, 64)
	start = time.Now()
	eng.Read(context.Background(), h, 0, int64(len(buf)), buf) //nolint:errcheck
	if elapsed := time.Since(start); elapsed < readDelay {
		t.Errorf("Read returned in %v, want >= %v", elapsed, readDelay)
	}
}

func TestInjectedDelay_NoDelayByDefault(t *testing.T) {
	eng := mem.New(mem.WithFixedSize(1 << 20))

	h, err := eng.Open(context.Background(), "target", engine.ModeRead, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, 64)
	start := time.Now()
	eng.Read(context.Background(), h, 0, int64(len(buf)), buf) //nolint:errcheck
	elapsed := time.Since(start)

	// Without a delay, Read should complete well under 5ms.
	if elapsed >= 5*time.Millisecond {
		t.Errorf("Read took %v without delay configured; expected < 5ms", elapsed)
	}
}

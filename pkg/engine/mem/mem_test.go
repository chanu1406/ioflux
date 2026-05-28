package mem_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
)

var ctx = context.Background()

// openReadClose opens target, reads length bytes from off into buf, and
// closes the handle. Returns the byte count and any non-ErrShortRead error.
func openReadClose(t *testing.T, e *mem.MemEngine, target string, off, length int64) []byte {
	t.Helper()
	h, err := e.Open(ctx, target, engine.ModeRead, 0)
	if err != nil {
		t.Fatalf("Open(%q): %v", target, err)
	}
	buf := make([]byte, length)
	n, err := e.Read(ctx, h, off, length, buf)
	if err != nil && !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("Read(%q, off=%d, len=%d): %v", target, off, length, err)
	}
	if err := e.Close(ctx, h); err != nil {
		t.Fatalf("Close(%q): %v", target, err)
	}
	return buf[:n]
}

// TestHappyPath verifies the basic Open → Read → Close sequence.
func TestHappyPath(t *testing.T) {
	e := mem.New(mem.WithFixedSize(4096))

	h, err := e.Open(ctx, "shard.tar", engine.ModeRead, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := e.Read(ctx, h, 0, 1024, buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 1024 {
		t.Fatalf("Read: got %d bytes, want 1024", n)
	}
	if err := e.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestDeterminism verifies that repeated reads of the same range return
// identical bytes.
func TestDeterminism(t *testing.T) {
	const size = 64 * 1024
	e := mem.New(mem.WithFixedSize(size))

	first := openReadClose(t, e, "obj", 0, size)
	for i := 0; i < 999; i++ {
		got := openReadClose(t, e, "obj", 0, size)
		if !bytes.Equal(got, first) {
			t.Fatalf("iteration %d: bytes differ from first read", i+1)
		}
	}
}

// TestReadBeyondEOF verifies that a read starting at or past the end of an
// object returns ErrShortRead.
func TestReadBeyondEOF(t *testing.T) {
	e := mem.New(mem.WithFixedSize(1024))

	h, _ := e.Open(ctx, "obj", engine.ModeRead, 0)
	defer e.Close(ctx, h)

	buf := make([]byte, 64)

	// Read exactly at the last byte boundary — should succeed.
	n, err := e.Read(ctx, h, 960, 64, buf)
	if err != nil {
		t.Fatalf("read at [960,1024): unexpected error: %v", err)
	}
	if n != 64 {
		t.Fatalf("read at [960,1024): want 64 bytes, got %d", n)
	}

	// Read starting exactly at EOF — no bytes available.
	n, err = e.Read(ctx, h, 1024, 64, buf)
	if !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("read at EOF: want ErrShortRead, got (n=%d, err=%v)", n, err)
	}
	if n != 0 {
		t.Fatalf("read at EOF: want n=0, got %d", n)
	}

	// Read straddling EOF — partial result.
	n, err = e.Read(ctx, h, 1000, 64, buf)
	if !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("read straddling EOF: want ErrShortRead, got err=%v", err)
	}
	if n != 24 {
		t.Fatalf("read straddling EOF: want n=24, got %d", n)
	}
}

// TestWriteAndReadBack verifies that written data is readable.
func TestWriteAndReadBack(t *testing.T) {
	e := mem.New(mem.WithFixedSize(4096))

	// Write mode.
	wh, err := e.Open(ctx, "writable", engine.ModeWrite, 0)
	if err != nil {
		t.Fatalf("Open for write: %v", err)
	}
	payload := []byte("hello, ioflux")
	if n, err := e.Write(ctx, wh, 0, payload); err != nil || n != len(payload) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	e.Close(ctx, wh)

	// Read back.
	got := openReadClose(t, e, "writable", 0, int64(len(payload)))
	if !bytes.Equal(got, payload) {
		t.Fatalf("read back: want %q, got %q", payload, got)
	}
}

// TestWriteGrowsObject verifies that a write extending beyond the current
// object size expands the object.
func TestWriteGrowsObject(t *testing.T) {
	e := mem.New(mem.WithFixedSize(16))

	h, _ := e.Open(ctx, "obj", engine.ModeReadWrite, 0)
	// Write at offset 16, extending the object from 16 to 32 bytes.
	data := []byte("extension")
	n, err := e.Write(ctx, h, 16, data)
	if err != nil || n != len(data) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	e.Close(ctx, h)

	// The object should now be at least 25 bytes; read the new region.
	got := openReadClose(t, e, "obj", 16, int64(len(data)))
	if !bytes.Equal(got, data) {
		t.Fatalf("grown region: want %q, got %q", data, got)
	}
}

// TestFsyncUnsupported verifies that Fsync returns ErrUnsupported.
func TestFsyncUnsupported(t *testing.T) {
	e := mem.New(mem.WithFixedSize(64))
	h, _ := e.Open(ctx, "obj", engine.ModeWrite, 0)
	defer e.Close(ctx, h)
	if err := e.Fsync(ctx, h); !errors.Is(err, engine.ErrUnsupported) {
		t.Fatalf("Fsync: want ErrUnsupported, got %v", err)
	}
}

// TestStatReturnsSize verifies Stat reports correct size for existing and new
// objects.
func TestStatReturnsSize(t *testing.T) {
	const size = 8192
	e := mem.New(mem.WithFixedSize(size))

	info, err := e.Stat(ctx, "obj")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != size {
		t.Fatalf("Stat.Size = %d, want %d", info.Size, size)
	}
	if info.Name != "obj" {
		t.Fatalf("Stat.Name = %q, want %q", info.Name, "obj")
	}
}

// TestUnknownHandleErrors verifies that Read, Write, Fsync, and Close on a
// handle that was never opened (or already closed) return ErrNotFound.
func TestUnknownHandleErrors(t *testing.T) {
	e := mem.New()
	const bogus = engine.Handle(999)

	if _, err := e.Read(ctx, bogus, 0, 1, make([]byte, 1)); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Read unknown handle: want ErrNotFound, got %v", err)
	}
	if _, err := e.Write(ctx, bogus, 0, []byte{0}); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Write unknown handle: want ErrNotFound, got %v", err)
	}
	if err := e.Fsync(ctx, bogus); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Fsync: want ErrUnsupported (Durable==false, handle irrelevant), got %v", err)
	}
	if err := e.Close(ctx, bogus); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Close unknown handle: want ErrNotFound, got %v", err)
	}
}

// TestCloseReleasesHandle verifies that a closed handle is no longer usable.
func TestCloseReleasesHandle(t *testing.T) {
	e := mem.New(mem.WithFixedSize(64))

	h, _ := e.Open(ctx, "obj", engine.ModeRead, 0)
	e.Close(ctx, h)

	if _, err := e.Read(ctx, h, 0, 1, make([]byte, 1)); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Read after Close: want ErrNotFound, got %v", err)
	}
}

// TestNegativeOffsetErrors verifies that Read and Write return errors (not
// panics) when called with negative offsets or lengths.
func TestNegativeOffsetErrors(t *testing.T) {
	e := mem.New(mem.WithFixedSize(64))
	h, _ := e.Open(ctx, "obj", engine.ModeReadWrite, 0)
	defer e.Close(ctx, h)

	buf := make([]byte, 16)
	if _, err := e.Read(ctx, h, -1, 16, buf); err == nil {
		t.Error("Read with negative off: want error, got nil")
	}
	if _, err := e.Read(ctx, h, 0, -1, buf); err == nil {
		t.Error("Read with negative length: want error, got nil")
	}
	if _, err := e.Write(ctx, h, -1, buf); err == nil {
		t.Error("Write with negative off: want error, got nil")
	}
}

// TestCapsEnforcement verifies that object-API methods return ErrUnsupported.
func TestCapsEnforcement(t *testing.T) {
	e := mem.New()

	caps := e.Caps()
	if caps.ObjectAPI {
		t.Fatal("MemEngine.Caps().ObjectAPI should be false")
	}

	if err := e.Put(ctx, "key", bytes.NewReader(nil), 0); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Put: want ErrUnsupported, got %v", err)
	}
	if _, err := e.Get(ctx, "key", 0, 0, nil); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Get: want ErrUnsupported, got %v", err)
	}
	if _, err := e.Head(ctx, "key"); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Head: want ErrUnsupported, got %v", err)
	}
	if err := e.Delete(ctx, "key"); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Delete: want ErrUnsupported, got %v", err)
	}
}

// TestWithSizeFunc verifies per-target sizing.
func TestWithSizeFunc(t *testing.T) {
	sizeMap := map[string]int64{
		"small": 512,
		"large": 1 << 20,
	}
	e := mem.New(mem.WithSizeFunc(func(target string) int64 {
		if s, ok := sizeMap[target]; ok {
			return s
		}
		return 4096
	}))

	for target, want := range sizeMap {
		info, err := e.Stat(ctx, target)
		if err != nil {
			t.Fatalf("Stat(%q): %v", target, err)
		}
		if info.Size != want {
			t.Errorf("Stat(%q).Size = %d, want %d", target, info.Size, want)
		}
	}
}

// TestConcurrentDistinctTargets verifies that 1000 goroutines each working on
// a distinct target complete without data races (run with -race).
func TestConcurrentDistinctTargets(t *testing.T) {
	const n = 1000
	e := mem.New(mem.WithFixedSize(4096))

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := fmt.Sprintf("shard-%04d", i)
			h, err := e.Open(ctx, target, engine.ModeRead, 0)
			if err != nil {
				t.Errorf("goroutine %d Open: %v", i, err)
				return
			}
			buf := make([]byte, 256)
			for off := int64(0); off < 4096; off += 256 {
				if _, err := e.Read(ctx, h, off, 256, buf); err != nil {
					t.Errorf("goroutine %d Read at %d: %v", i, off, err)
					break
				}
			}
			if err := e.Close(ctx, h); err != nil {
				t.Errorf("goroutine %d Close: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

// TestGoschedYielding verifies that MemEngine yields the CPU between ops so
// sibling goroutines get scheduled. On GOMAXPROCS=1, without Gosched each
// engine goroutine would hold the thread until blocked; with Gosched it
// voluntarily yields after every op, allowing the sibling to run.
//
// Note: Go 1.14+ asynchronous preemption means the sibling gets some time
// regardless, so this test guards against gross violations (zero scheduling)
// rather than counting exact yields.
func TestGoschedYielding(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	e := mem.New(mem.WithFixedSize(64))

	var siblingCount atomic.Int64
	stop := make(chan struct{})

	// Sibling goroutine: busy-loop incrementing a counter.
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				siblingCount.Add(1)
			}
		}
	}()

	// Engine goroutines: each opens, reads, closes one target.
	const workers = 50
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := fmt.Sprintf("obj-%d", i)
			h, _ := e.Open(ctx, target, engine.ModeRead, 0)
			buf := make([]byte, 16)
			e.Read(ctx, h, 0, 16, buf)
			e.Close(ctx, h)
		}(i)
	}
	wg.Wait()
	close(stop)

	if siblingCount.Load() == 0 {
		t.Fatal("sibling goroutine never ran; Gosched may not be yielding the scheduler")
	}
}

// TestEngineInterface verifies at compile time that *MemEngine satisfies the
// engine.Engine interface.
var _ engine.Engine = (*mem.MemEngine)(nil)
